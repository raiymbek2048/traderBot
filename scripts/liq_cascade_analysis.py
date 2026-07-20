"""Анализ ликвидационных каскадов: есть ли статистический отскок?

Логика:
  1. Ликвидации из liq_events группируются в каскады (окно 15с, один символ, одна сторона).
  2. Каскад "large" если суммарный объём ≥ MIN_CASCADE_USD.
  3. Для каждого каскада:
       'Sell' ликвидации = ликвидированы лонги → цена вниз → тест LONG-отскока
       'Buy'  ликвидации = ликвидированы шорты → цена вверх → тест SHORT-отката
  4. Тянем 1m-клины Bybit до и после каскада, считаем return на T+1, +5, +15, +60 мин.
  5. Сравниваем со средним движением рынка (baseline BTC за то же окно).
  6. Учитываем комиссии перп-тейкер 0.11% (вход+выход) + слипаж 0.05%.

Итог: JSON-отчёт + печать. Реальная стратегия жизнеспособна если E[net]>0 на N≥30.

Run: python scripts/liq_cascade_analysis.py
"""
from __future__ import annotations
import json, sqlite3, time, urllib.request
from datetime import datetime, timezone
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

WINDOW_S = 15
MIN_CASCADE_USD = 30_000
HORIZONS_MIN = [1, 5, 15, 60]
FEES_ROUND = 0.0011           # taker×2 (Bybit perp)
SLIPPAGE   = 0.0005            # 0.05% на пересечение стакана
MAX_PARALLEL_KLINES = 6


def get(url: str, tries: int = 3):
    for i in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=15) as r:
                return json.loads(r.read())
        except Exception:
            if i == tries - 1:
                raise
            time.sleep(1)


def klines(symbol: str, from_ms: int, to_ms: int) -> list[tuple[int, float, float, float, float]]:
    """1m klines: [(ts_ms, open, high, low, close), ...] по возрастанию."""
    url = (f"https://api.bybit.com/v5/market/kline?category=linear&symbol={symbol}"
           f"&interval=1&start={from_ms}&end={to_ms}&limit=200")
    d = get(url)
    rows = d.get("result", {}).get("list", []) or []
    out = []
    for r in rows:
        try:
            out.append((int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4])))
        except Exception:
            pass
    return sorted(out)


def load_cascades(db_path: str) -> list[dict]:
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("SELECT symbol,side,ts,price,qty,value_usdt FROM liq_events ORDER BY symbol,side,ts")
    rows = c.fetchall()
    conn.close()
    cascades = []
    cur = None
    for sym, side, ts, price, qty, val in rows:
        ts_ms = int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1000)
        if not price or not val:
            continue
        if (cur and cur["symbol"] == sym and cur["side"] == side
                and ts_ms - cur["end_ms"] <= WINDOW_S * 1000):
            cur["end_ms"] = ts_ms
            cur["count"] += 1
            cur["value_usdt"] += val
            cur["last_price"] = price
        else:
            if cur:
                cascades.append(cur)
            cur = {"symbol": sym, "side": side, "start_ms": ts_ms, "end_ms": ts_ms,
                   "count": 1, "value_usdt": val, "last_price": price}
    if cur:
        cascades.append(cur)
    return cascades


def measure_one(cas: dict, btc_klines: list) -> dict | None:
    """Возвращает {horizon: return_pct} для каскада и baseline BTC."""
    end_ms = cas["end_ms"]
    horizon_ms = max(HORIZONS_MIN) * 60_000
    ks = klines(cas["symbol"], end_ms, end_ms + horizon_ms + 60_000)
    if len(ks) < 2:
        return None
    entry = cas["last_price"]
    # для 'Sell' каскада — предполагаем LONG-отскок: return = (P_future - entry) / entry
    # для 'Buy'  каскада — SHORT: return = (entry - P_future) / entry
    sign = 1 if cas["side"] == "Sell" else -1

    out = {"horizons": {}, "baseline": {}}
    for h_min in HORIZONS_MIN:
        target = end_ms + h_min * 60_000
        future = None
        for ts, _o, _h, _l, cl in ks:
            if ts >= target:
                future = cl
                break
        if future is None:
            future = ks[-1][4]
        raw = (future - entry) / entry * sign
        net = raw - FEES_ROUND - SLIPPAGE
        out["horizons"][h_min] = {"raw_pct": round(raw * 100, 4),
                                  "net_pct": round(net * 100, 4)}
        # baseline — то же движение BTC в тот же интервал
        bt_entry = _price_at(btc_klines, end_ms) or 0
        bt_future = _price_at(btc_klines, target) or 0
        if bt_entry and bt_future:
            bt_raw = (bt_future - bt_entry) / bt_entry * sign
            out["baseline"][h_min] = round(bt_raw * 100, 4)
    return out


def _price_at(klines_sorted: list, ts_ms: int) -> float | None:
    for ts, o, _h, _l, c in klines_sorted:
        if ts >= ts_ms:
            return o
    return klines_sorted[-1][4] if klines_sorted else None


def summarize(measures: list[dict], label: str) -> dict:
    """Средние по горизонтам."""
    if not measures:
        return {"label": label, "n": 0}
    n = len(measures)
    result = {"label": label, "n": n}
    for h in HORIZONS_MIN:
        raws = [m["horizons"][h]["raw_pct"] for m in measures if h in m["horizons"]]
        nets = [m["horizons"][h]["net_pct"] for m in measures if h in m["horizons"]]
        bls  = [m["baseline"].get(h) for m in measures if h in m["baseline"]]
        bls  = [b for b in bls if b is not None]
        wins = sum(1 for x in nets if x > 0)
        result[f"h{h}m"] = {
            "avg_raw_pct": round(sum(raws)/len(raws), 4) if raws else None,
            "avg_net_pct": round(sum(nets)/len(nets), 4) if nets else None,
            "median_raw_pct": round(sorted(raws)[len(raws)//2], 4) if raws else None,
            "win_rate_pct": round(wins/n*100, 1) if n else 0,
            "baseline_btc_avg_pct": round(sum(bls)/len(bls), 4) if bls else None,
            "edge_over_btc_pct": (round(sum(raws)/len(raws) - sum(bls)/len(bls), 4)
                                   if raws and bls else None),
        }
    return result


def main() -> None:
    print("=== загрузка ликвидаций и кластеризация ===")
    cascades = load_cascades("traderbot.db")
    print(f"всего каскадов: {len(cascades)}")
    large = [c for c in cascades if c["value_usdt"] >= MIN_CASCADE_USD]
    print(f"крупных ≥${MIN_CASCADE_USD:,}: {len(large)}")

    by_side = defaultdict(int)
    for c in large:
        by_side[c["side"]] += 1
    print(f"по стороне: {dict(by_side)}")

    # BTC-baseline на всё окно
    if large:
        t0 = min(c["start_ms"] for c in large)
        t1 = max(c["end_ms"] for c in large) + max(HORIZONS_MIN) * 60_000
        print(f"тяну BTC 1m клины ({(t1-t0)/1000/3600:.0f}ч)...")
        # bybit отдаёт по 200 свечей — тянем чанками
        step = 200 * 60_000
        btc_klines = []
        cur = t0
        while cur < t1:
            btc_klines += klines("BTCUSDT", cur, min(cur+step, t1))
            cur += step
        btc_klines = sorted({k[0]: k for k in btc_klines}.values())
        print(f"BTC свечей: {len(btc_klines)}")
    else:
        btc_klines = []

    # каскады параллельно
    print(f"измеряю отскоки для {len(large)} каскадов...")
    measured = []
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_KLINES) as ex:
        futs = {ex.submit(measure_one, c, btc_klines): c for c in large}
        for k, f in enumerate(as_completed(futs)):
            m = f.result()
            if m:
                c = futs[f]
                m["symbol"] = c["symbol"]; m["side"] = c["side"]
                m["value_usdt"] = c["value_usdt"]; m["count"] = c["count"]
                measured.append(m)
            if (k+1) % 100 == 0:
                print(f"  ...{k+1}/{len(large)}")

    sell_m = [m for m in measured if m["side"] == "Sell"]   # LONG-отскок
    buy_m  = [m for m in measured if m["side"] == "Buy"]    # SHORT-откат
    report = {
        "min_cascade_usd": MIN_CASCADE_USD,
        "fees_roundtrip_pct": FEES_ROUND * 100,
        "slippage_pct": SLIPPAGE * 100,
        "total_cascades": len(cascades),
        "large_cascades": len(large),
        "measured": len(measured),
        "sell_side_long_rebound": summarize(sell_m, "Sell→LONG-отскок"),
        "buy_side_short_pullback": summarize(buy_m, "Buy→SHORT-откат"),
    }

    # топ крупнейших каскадов
    top = sorted(measured, key=lambda x: -x["value_usdt"])[:10]
    report["top10_by_size"] = [
        {"symbol": m["symbol"], "side": m["side"], "value_usdt": round(m["value_usdt"], 0),
         "count": m["count"],
         "h5m_net": m["horizons"].get(5, {}).get("net_pct"),
         "h15m_net": m["horizons"].get(15, {}).get("net_pct")}
        for m in top
    ]

    with open("logs/liq_cascade_report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)

    # печать
    print("\n=== ИТОГ ===")
    print(f"Комиссии+слипаж (round trip): {(FEES_ROUND+SLIPPAGE)*100:.3f}%")
    for key in ("sell_side_long_rebound", "buy_side_short_pullback"):
        r = report[key]
        print(f"\n>>> {r['label']}  (n={r['n']})")
        for h in HORIZONS_MIN:
            d = r.get(f"h{h}m", {})
            if not d: continue
            print(f"  T+{h}m: raw={d.get('avg_raw_pct')}%  net={d.get('avg_net_pct')}%  "
                  f"win_rate={d.get('win_rate_pct')}%  edge_vs_BTC={d.get('edge_over_btc_pct')}%")
    print("\nТОП-10 каскадов по объёму:")
    for t in report["top10_by_size"]:
        print(f"  {t['symbol']:14} {t['side']:5} ${t['value_usdt']:>10,.0f} "
              f"({t['count']} liq)  T+5m net={t.get('h5m_net')}%  T+15m net={t.get('h15m_net')}%")


if __name__ == "__main__":
    main()

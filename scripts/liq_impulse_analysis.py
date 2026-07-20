"""Sub-second анализ импульса после ликвидационного каскада (Binance aggTrades).

Из общего анализа: после каскада тренд ПРОДОЛЖАЕТСЯ (не отскок). Строим
follow-momentum: Sell-каскад → SHORT, Buy-каскад → LONG. Проверяем сколько
импульса остаётся, если входить с задержкой Δ (наш реальный ордер ~60-130ms).

Горизонты замера: 1с, 5с, 15с, 30с, 60с, 5м после последней ликвидации в каскаде.
Задержки входа (delay): 0с (идеал), 1с, 2с, 5с (реалистично для розницы).

Используем Binance perp aggTrades — миллисекундная точность. Только для символов
торгуемых И на Binance (BTC/ETH/SOL/...); остальные пропускаем.

Run: python scripts/liq_impulse_analysis.py
"""
from __future__ import annotations
import json, sqlite3, time, urllib.request
from datetime import datetime, timezone
from bisect import bisect_left, bisect_right
from concurrent.futures import ThreadPoolExecutor, as_completed

WINDOW_S = 15
MIN_CASCADE_USD = 100_000
HORIZONS_S = [1, 5, 15, 30, 60, 300]      # секунд после конца каскада
DELAYS_MS  = [0, 500, 1000, 2000, 5000]    # задержка нашего входа от конца каскада
FEES_ROUND = 0.0011
SLIPPAGE   = 0.0005
MAX_PARALLEL = 8


def get(url: str, tries: int = 3):
    for i in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=15) as r:
                return json.loads(r.read())
        except Exception:
            if i == tries - 1:
                raise
            time.sleep(0.7)


def binance_symbols() -> set[str]:
    d = get("https://fapi.binance.com/fapi/v1/exchangeInfo")
    return {s["symbol"] for s in d["symbols"] if s.get("status") == "TRADING"}


def agg_trades(symbol: str, start_ms: int, end_ms: int) -> list[tuple[int, float]]:
    """[(ts_ms, price), ...] за окно, отсортировано."""
    out: list[tuple[int, float]] = []
    cur = start_ms
    while cur < end_ms:
        chunk = min(cur + 60_000 * 5, end_ms)   # чанки по 5 минут
        d = get(f"https://fapi.binance.com/fapi/v1/aggTrades?symbol={symbol}"
                f"&startTime={cur}&endTime={chunk}&limit=1000")
        if not d:
            break
        out.extend((int(t["T"]), float(t["p"])) for t in d)
        if len(d) < 1000:
            cur = chunk
        else:
            cur = int(d[-1]["T"]) + 1
    out.sort()
    return out


def price_at(trades: list[tuple[int, float]], ts_ms: int) -> float | None:
    """Ближайшая цена ПОСЛЕ ts_ms."""
    i = bisect_left(trades, (ts_ms,))
    if i < len(trades):
        return trades[i][1]
    return trades[-1][1] if trades else None


def load_cascades(db_path: str) -> list[dict]:
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("SELECT symbol,side,ts,price,qty,value_usdt FROM liq_events ORDER BY symbol,side,ts")
    rows = c.fetchall()
    conn.close()
    cascades, cur = [], None
    for sym, side, ts, price, qty, val in rows:
        if not price or not val:
            continue
        ts_ms = int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1000)
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


def measure(cas: dict) -> dict | None:
    """Для каскада: matrix [delay][horizon] = net_return после следования импульсу."""
    end_ms = cas["end_ms"]
    trades = agg_trades(cas["symbol"], end_ms - 2000, end_ms + max(HORIZONS_S) * 1000 + 2000)
    if len(trades) < 5:
        return None
    sign = -1 if cas["side"] == "Sell" else 1   # follow-momentum: Sell→SHORT (sign=-1)
    out = {"symbol": cas["symbol"], "side": cas["side"], "value_usdt": cas["value_usdt"],
           "count": cas["count"], "matrix": {}}
    for d_ms in DELAYS_MS:
        entry_ts = end_ms + d_ms
        entry = price_at(trades, entry_ts)
        if entry is None:
            continue
        row = {}
        for h_s in HORIZONS_S:
            exit_ts = entry_ts + h_s * 1000
            ex = price_at(trades, exit_ts)
            if ex is None:
                continue
            raw = (ex - entry) / entry * sign
            row[h_s] = {"raw": round(raw * 100, 4),
                        "net": round((raw - FEES_ROUND - SLIPPAGE) * 100, 4)}
        out["matrix"][d_ms] = row
    return out


def main() -> None:
    print("=== загрузка каскадов ===")
    cascades = load_cascades("traderbot.db")
    large = [c for c in cascades if c["value_usdt"] >= MIN_CASCADE_USD]
    print(f"крупных ≥${MIN_CASCADE_USD:,}: {len(large)}")

    print("=== фильтр символов доступных на Binance perp ===")
    bn_syms = binance_symbols()
    tradable = [c for c in large if c["symbol"] in bn_syms]
    print(f"из них торгуемых на Binance: {len(tradable)}")

    print(f"тяну Binance aggTrades вокруг {len(tradable)} каскадов...")
    measured = []
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as ex:
        futs = {ex.submit(measure, c): c for c in tradable}
        for k, f in enumerate(as_completed(futs)):
            try:
                m = f.result()
                if m:
                    measured.append(m)
            except Exception as e:
                pass
            if (k+1) % 50 == 0:
                print(f"  ...{k+1}/{len(tradable)}")

    print(f"измерено: {len(measured)}")

    # агрегация: средние по delay × horizon
    stats = {d: {h: {"raw": [], "net": [], "wins": 0} for h in HORIZONS_S}
             for d in DELAYS_MS}
    for m in measured:
        for d_ms, row in m["matrix"].items():
            for h_s, v in row.items():
                stats[d_ms][h_s]["raw"].append(v["raw"])
                stats[d_ms][h_s]["net"].append(v["net"])
                if v["net"] > 0:
                    stats[d_ms][h_s]["wins"] += 1

    # печать таблицы
    print("\n=== MATRIX: delay(ms) × horizon(s) — avg NET% (win-rate%) ===")
    print(f"комиссии+слипаж вычтены (round trip {(FEES_ROUND+SLIPPAGE)*100:.3f}%)")
    print(f"n каскадов: {len(measured)}")
    hdr = "delay\\horiz  " + "  ".join(f"{h:>4}s" for h in HORIZONS_S)
    print(hdr)
    for d_ms in DELAYS_MS:
        cells = []
        for h_s in HORIZONS_S:
            nets = stats[d_ms][h_s]["net"]
            if not nets:
                cells.append("    -")
                continue
            avg = sum(nets)/len(nets)
            wr = stats[d_ms][h_s]["wins"]/len(nets)*100
            cells.append(f"{avg:+.3f}%")
        print(f"  {d_ms:>4}ms:  " + "  ".join(f"{c:>7}" for c in cells))
    print("\n=== win-rate % (та же таблица) ===")
    print(hdr)
    for d_ms in DELAYS_MS:
        cells = []
        for h_s in HORIZONS_S:
            nets = stats[d_ms][h_s]["net"]
            wr = (stats[d_ms][h_s]["wins"]/len(nets)*100) if nets else 0
            cells.append(f"{wr:.0f}%")
        print(f"  {d_ms:>4}ms:  " + "  ".join(f"{c:>7}" for c in cells))

    # сохранить полный отчёт
    report = {"min_cascade_usd": MIN_CASCADE_USD, "n": len(measured),
              "fees_pct": FEES_ROUND * 100, "slippage_pct": SLIPPAGE * 100,
              "matrix_avg_net": {
                  d: {h: (round(sum(stats[d][h]["net"])/len(stats[d][h]["net"]), 4)
                          if stats[d][h]["net"] else None)
                      for h in HORIZONS_S}
                  for d in DELAYS_MS
              }}
    with open("logs/liq_impulse_report.json", "w") as f:
        json.dump(report, f, indent=2)
    print("\nСохранено: logs/liq_impulse_report.json")


if __name__ == "__main__":
    main()

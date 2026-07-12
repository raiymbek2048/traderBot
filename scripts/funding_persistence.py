"""Persistence-анализ settled-фандинга: отвечает на главный вопрос экономики.

Часть A (spot+perp на Bybit): по ВСЕМ перпам берём фактически начисленные ставки
(~200 сеттлментов ≈ 2 месяца). Эпизод = ставка пересекла ENTRY-порог → держим,
пока ставка ≥ EXIT (0.01%) → сумма собранного. Сравниваем с полной стоимостью
цикла (комиссии 0.31% + basis/слипаж ~0.15% = 0.46%).

Часть B (perp-perp Bybit vs Binance): для кандидатов из funding_spread_snaps
берём истории обеих бирж, ресемплим в 8ч-окна UTC, эпизоды |спреда| ≥ порога,
стоимость = комиссии 0.21% + adverse exec_edge (из наших снимков).

Run: python scripts/funding_persistence.py
Вывод: печать + logs/persistence_report.json
"""
from __future__ import annotations
import json
import sqlite3
import statistics as st
import time
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

BYBIT = "https://api.bybit.com"
BINANCE_F = "https://fapi.binance.com"

EXIT_RATE = 0.0001          # держим пока ставка ≥ 0.01% за сеттлмент
ENTRY_LEVELS = [0.0005, 0.0010, 0.0020]   # 0.05% / 0.10% / 0.20% за сеттлмент
COST_SPOT_PERP = 0.0046     # 0.31% комиссии + 0.15% basis/слипаж (по нашим замерам)
CAP_RATE = 0.015            # |ставка| выше — почти наверняка funding-cap (сломанный рынок)

SPREAD_LEVELS_8H = [0.0010, 0.0020]  # |спред| за 8ч-окно: 0.1% (0.3%/д), 0.2% (0.6%/д)
COST_PERP_PERP = 0.0021     # 0.21% комиссии перп-тейкер ×4


def get(url: str, tries: int = 3):
    for i in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=15) as r:
                return json.loads(r.read())
        except Exception:
            if i == tries - 1:
                raise
            time.sleep(1 + i)


def bybit_symbols() -> list[str]:
    d = get(f"{BYBIT}/v5/market/tickers?category=linear")
    return [it["symbol"] for it in d["result"]["list"]
            if it["symbol"].endswith("USDT") and it.get("fundingRate")]


def bybit_history(sym: str) -> list[tuple[int, float]]:
    """settled-ставки, хронологически (ts_ms, rate)."""
    d = get(f"{BYBIT}/v5/market/funding/history?category=linear&symbol={sym}&limit=200")
    out = []
    for r in d.get("result", {}).get("list", []):
        try:
            out.append((int(r["fundingRateTimestamp"]), float(r["fundingRate"])))
        except (KeyError, ValueError):
            pass
    return sorted(out)


def binance_history(sym: str) -> list[tuple[int, float]]:
    d = get(f"{BINANCE_F}/fapi/v1/fundingRate?symbol={sym}&limit=1000")
    out = []
    for r in d:
        try:
            out.append((int(r["fundingTime"]), float(r["fundingRate"])))
        except (KeyError, ValueError):
            pass
    return sorted(out)


def episodes(rates: list[float], entry: float, exit_: float) -> list[dict]:
    """Эпизоды: вход при r≥entry, держим пока r≥exit_. Возвращает [{len, total, capped}]."""
    eps, i, n = [], 0, len(rates)
    while i < n:
        if rates[i] >= entry:
            j, total, capped = i, 0.0, False
            while j < n and rates[j] >= exit_:
                total += rates[j]
                capped = capped or abs(rates[j]) >= CAP_RATE
                j += 1
            eps.append({"len": j - i, "total": total, "capped": capped})
            i = j + 1
        else:
            i += 1
    return eps


# ── Часть A ────────────────────────────────────────────────────────────────────

def part_a() -> dict:
    syms = bybit_symbols()
    print(f"[A] Bybit перпов: {len(syms)}, тяну settled-истории...")
    hist: dict[str, list] = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(bybit_history, s): s for s in syms}
        for k, f in enumerate(as_completed(futs)):
            s = futs[f]
            try:
                hist[s] = f.result()
            except Exception:
                pass
            if (k + 1) % 100 == 0:
                print(f"  ...{k+1}/{len(syms)}")

    report = {}
    for entry in ENTRY_LEVELS:
        all_eps, per_sym = [], []
        for s, h in hist.items():
            rates = [r for _, r in h]
            if len(rates) < 10:
                continue
            eps = [e for e in episodes(rates, entry, EXIT_RATE) if not e["capped"]]
            if eps:
                all_eps.extend(eps)
                best = sum(e["total"] for e in eps)
                per_sym.append((s, len(eps), best))
        if not all_eps:
            report[f"entry_{entry}"] = {"episodes": 0}
            continue
        lens = [e["len"] for e in all_eps]
        totals = [e["total"] for e in all_eps]
        profitable = [t for t in totals if t > COST_SPOT_PERP]
        report[f"entry_{entry}"] = {
            "episodes": len(all_eps),
            "median_len_settlements": st.median(lens),
            "mean_len": round(st.mean(lens), 2),
            "median_total_pct": round(st.median(totals) * 100, 4),
            "mean_total_pct": round(st.mean(totals) * 100, 4),
            "pct_profitable_vs_cost_0.46": round(len(profitable) / len(all_eps) * 100, 1),
            "expected_net_pct_per_episode": round(
                (st.mean(totals) - COST_SPOT_PERP) * 100, 4),
            "top10_symbols_by_total": sorted(per_sym, key=lambda x: -x[2])[:10],
        }
        print(f"[A] entry={entry*100:.2f}%: {len(all_eps)} эпизодов | "
              f"медиана длины {st.median(lens)} сеттл. | "
              f"прибыльных {len(profitable)/len(all_eps)*100:.0f}% | "
              f"E[net] {(st.mean(totals)-COST_SPOT_PERP)*100:+.3f}%")
    return report


# ── Часть B ────────────────────────────────────────────────────────────────────

def resample_8h(hist: list[tuple[int, float]]) -> dict[int, float]:
    """Суммирует ставки в 8ч-окна UTC (окно = ts // 8h)."""
    out: dict[int, float] = defaultdict(float)
    for ts, r in hist:
        out[ts // (8 * 3600 * 1000)] += r
    return dict(out)


def part_b(db_path: str) -> dict:
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("""SELECT symbol, AVG(spread_daily_pct), AVG(COALESCE(exec_edge_pct,0))
                 FROM funding_spread_snaps GROUP BY symbol
                 HAVING ABS(AVG(spread_daily_pct)) > 0.5""")
    cands = c.fetchall()
    conn.close()
    print(f"\n[B] Кандидатов из снимков (|avg спред|>0.5%/д): {len(cands)}")

    report = {}
    for sym, avg_spread, avg_edge in cands:
        try:
            by = resample_8h(bybit_history(sym))
            bn = resample_8h(binance_history(sym))
        except Exception as e:
            print(f"  {sym}: fetch failed ({e})")
            continue
        common = sorted(set(by) & set(bn))
        if len(common) < 10:
            print(f"  {sym}: мало общих окон ({len(common)})")
            continue
        spread = [by[w] - bn[w] for w in common]
        adverse = max(0.0, -avg_edge / 100)  # наш замер исполнимого входа
        cost = COST_PERP_PERP + adverse
        sym_rep = {"windows_8h": len(common),
                   "avg_edge_pct_snaps": round(avg_edge, 3)}
        for lvl in SPREAD_LEVELS_8H:
            # эпизоды по модулю: направление фиксируем на входе знаком спреда
            eps = []
            i, n = 0, len(spread)
            while i < n:
                if abs(spread[i]) >= lvl:
                    sign = 1 if spread[i] > 0 else -1
                    j, tot = i, 0.0
                    while j < n and sign * spread[j] >= EXIT_RATE:
                        tot += sign * spread[j]
                        j += 1
                    eps.append({"len": j - i, "total": tot})
                    i = j + 1
                else:
                    i += 1
            if eps:
                lens = [e["len"] for e in eps]
                tots = [e["total"] for e in eps]
                prof = [t for t in tots if t > cost]
                sym_rep[f"lvl_{lvl}"] = {
                    "episodes": len(eps),
                    "median_len_8h_windows": st.median(lens),
                    "median_total_pct": round(st.median(tots) * 100, 3),
                    "pct_profitable": round(len(prof) / len(eps) * 100, 1),
                    "expected_net_pct": round((st.mean(tots) - cost) * 100, 3),
                }
        report[sym] = sym_rep
        l1 = sym_rep.get(f"lvl_{SPREAD_LEVELS_8H[0]}", {})
        print(f"  {sym}: окон={len(common)} edge={avg_edge:+.2f}% | "
              f"эпизодов={l1.get('episodes','-')} медиана длины={l1.get('median_len_8h_windows','-')} окон | "
              f"прибыльных={l1.get('pct_profitable','-')}% | E[net]={l1.get('expected_net_pct','-')}%")
    return report


if __name__ == "__main__":
    t0 = time.time()
    rep = {"part_a_spot_perp_bybit": part_a(),
           "part_b_perp_perp_cross": part_b("traderbot.db")}
    with open("logs/persistence_report.json", "w") as f:
        json.dump(rep, f, indent=2, default=str)
    print(f"\nГотово за {time.time()-t0:.0f}с → logs/persistence_report.json")

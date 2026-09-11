"""Два открытых вопроса по гипотезе 14, на 38 днях живых данных.

Q1: Предсказало ли ИСТОРИЧЕСКОЕ ранжирование символов ЖИВОЕ?
    Если HYPE (историч. лидер +10%) сдох до +0.2%, а XRP (отсеян по ликвидности)
    держит — значит символ нельзя выбрать заранее, и конструкция бессмысленна.

Q2: Корзина из 6 символов побьёт одиночный HYPE?
    Убийца был не размер края (~+3-4%/год), а ШУМ basis: ±$0.14/день против
    дохода $0.087/день. Если basis-шум идиосинкратичен, корзина из 6 гасит его
    в ~√6 ≈ 2.4× → маленький, но НАДЁЖНЫЙ carry вместо утонувшего в шуме.

Направление: short_risex + long_bybit (RISEx ставки выше → шорт собирает).
  дневной funding% = Σ(risex rates) − Σ(bybit rates)   за день
  дневной basis%   = bybit_ret − risex_ret  (short risex + long bybit)
  дневной total%   = funding + basis
Источник фандинга — ФАКТИЧЕСКИ начисленные ставки (venue_funding_settled).
"""
import sqlite3
import statistics
from collections import defaultdict
from datetime import datetime

DB = "/Users/raiymbekdaniiaruulu/traderbot_archive/traderbot_20260911.db"
SYMS = ["HYPE", "BNB", "XRP", "ETH", "SOL", "DOGE"]
# историческое ранжирование из scripts/risex_funding_diff.py (91 день, %/год)
HIST = {"HYPE": 10.0, "XRP": 7.5, "BNB": 6.9, "DOGE": 4.9, "SOL": 2.4, "ETH": 2.0}

conn = sqlite3.connect(DB)

# ── дневной фандинг-дифференциал по фактическим начислениям ───────────────────
def daily_funding(sym):
    """{day: (Σrisex − Σbybit) в %} — доход short_risex за день."""
    rows = conn.execute(
        "SELECT venue, funding_rate, settle_ms FROM venue_funding_settled "
        "WHERE symbol=?", (sym,)).fetchall()
    by_day = defaultdict(lambda: [0.0, 0.0])  # day -> [risex_sum, bybit_sum]
    for venue, rate, ms in rows:
        day = datetime.utcfromtimestamp(ms / 1000).strftime("%Y-%m-%d")
        by_day[day][0 if venue == "risex" else 1] += rate
    return {d: (rx - by) * 100 for d, (rx, by) in by_day.items()}

# ── дневной basis по mark-ценам обеих площадок ───────────────────────────────
def daily_basis(sym):
    """{day: (bybit_ret − risex_ret) в %} — basis-PnL short_risex+long_bybit."""
    rows = conn.execute(
        "SELECT venue, mark_price, ts FROM venue_funding_snaps "
        "WHERE symbol=? ORDER BY ts", (sym,)).fetchall()
    # последняя mark за день по каждой площадке
    last = defaultdict(dict)  # day -> {venue: price}
    for venue, price, ts in rows:
        day = ts[:10]
        if price and price > 0:
            last[day][venue] = price
    days = sorted(d for d in last if "risex" in last[d] and "bybit" in last[d])
    out = {}
    for i in range(1, len(days)):
        d, p = days[i], days[i - 1]
        for v in ("risex", "bybit"):
            if v not in last[p]:
                break
        else:
            rx_ret = (last[d]["risex"] - last[p]["risex"]) / last[p]["risex"] * 100
            by_ret = (last[d]["bybit"] - last[p]["bybit"]) / last[p]["bybit"] * 100
            out[d] = by_ret - rx_ret
    return out

# ── собираем дневные ряды total% по каждому символу ──────────────────────────
series = {}   # sym -> {day: total%}
for sym in SYMS:
    f = daily_funding(sym)
    b = daily_basis(sym)
    days = sorted(set(f) & set(b))
    series[sym] = {d: f[d] + b.get(d, 0.0) for d in days}

def stats(daily_vals):
    v = list(daily_vals)
    if len(v) < 2:
        return None
    m = statistics.mean(v)
    sd = statistics.stdev(v)
    sign = sum(1 for x in v if x > 0) / len(v) * 100
    sharpe = (m / sd * (365 ** 0.5)) if sd else 0
    return {"n": len(v), "mean_d": m, "std_d": sd, "annual": m * 365,
            "sign": sign, "sharpe": sharpe}

print("=" * 78)
print("Q1: ИСТОРИЧЕСКОЕ ранжирование vs ЖИВОЕ (38 дней)")
print("=" * 78)
print(f"{'символ':<7s}{'ист.%/год':>10s}{'ист.ранг':>9s}"
      f"{'живой%/год':>12s}{'жив.ранг':>9s}{'знак+':>7s}")
live = {}
for sym in SYMS:
    s = stats(series[sym].values())
    live[sym] = s["annual"] if s else 0
hist_rank = {s: i + 1 for i, s in enumerate(sorted(HIST, key=lambda x: -HIST[x]))}
live_rank = {s: i + 1 for i, s in enumerate(sorted(live, key=lambda x: -live[x]))}
for sym in SYMS:
    s = stats(series[sym].values())
    print(f"{sym:<7s}{HIST[sym]:>+10.1f}{hist_rank[sym]:>9d}"
          f"{s['annual']:>+12.1f}{live_rank[sym]:>9d}{s['sign']:>6.0f}%")
# ранговая корреляция Спирмена
n = len(SYMS)
d2 = sum((hist_rank[s] - live_rank[s]) ** 2 for s in SYMS)
rho = 1 - 6 * d2 / (n * (n * n - 1))
print(f"\nSpearman ρ (ист.ранг → жив.ранг): {rho:+.2f}")
print("  ρ≈1: история предсказывает | ρ≈0: ранжирование бесполезно | ρ<0: инверсия")

print("\n" + "=" * 78)
print("Q2: КОРЗИНА из 6 vs одиночный HYPE — гасит ли шум basis?")
print("=" * 78)
# корзина: равный вес, дневной доход = среднее по символам с данными за день
all_days = sorted(set().union(*[set(series[s]) for s in SYMS]))
basket = []
for d in all_days:
    vals = [series[s][d] for s in SYMS if d in series[s]]
    if len(vals) >= 4:
        basket.append(statistics.mean(vals))

print(f"{'портфель':<22s}{'ср/день%':>10s}{'σ/день%':>9s}"
      f"{'%/год':>8s}{'знак+':>7s}{'Sharpe':>8s}")
for name, vals in [("HYPE одиночный", list(series["HYPE"].values())),
                   ("XRP одиночный", list(series["XRP"].values())),
                   ("корзина 6 (equal-w)", basket)]:
    s = stats(vals)
    print(f"{name:<22s}{s['mean_d']:>+10.4f}{s['std_d']:>9.4f}"
          f"{s['annual']:>+8.1f}{s['sign']:>6.0f}%{s['sharpe']:>8.2f}")

# среднее σ одиночных для сравнения с корзиной
single_std = statistics.mean(stats(series[s].values())["std_d"] for s in SYMS)
basket_std = stats(basket)["std_d"]
print(f"\nСредний σ одиночного символа: {single_std:.4f}%/день")
print(f"σ корзины из 6:               {basket_std:.4f}%/день")
print(f"Гашение шума: {single_std/basket_std:.2f}× (теоретич. предел √6 = 2.45×)")

conn.close()

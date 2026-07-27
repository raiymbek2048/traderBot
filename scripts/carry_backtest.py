"""Cross-Sectional Funding Carry Backtest.

Стратегия: каждый цикл (5мин снимки, решения раз в 8ч перед сеттлментом)
  - Ранжируем символы по фандингу %/день
  - LONG корзина (шорт перп): 5 символов с самым ПОЛОЖИТЕЛЬНЫМ фандингом
    (шорт получает фандинг от лонгов)
  - SHORT корзина (лонг перп): 5 символов с самым ОТРИЦАТЕЛЬНЫМ фандингом
    (лонг получает фандинг от шортов)
  - Дельта частично гасится (альты коррелируют)

PnL за период:
  funding = Σ(settled_rate × notional) для всех ног
  basis   = Σ(exit_price - entry_price) × direction × qty
  fees    = 0.055% × 4 (taker вход+выход обеих ног) = 0.22% за ребалансировку

Данные: funding_spread_snaps (78k+, 16 дней, 424 символа).
Используем bybit_daily_pct (нормализованный %/день) и bybit_price.

Run: python scripts/carry_backtest.py
"""
import sqlite3
import statistics
from collections import defaultdict
from datetime import datetime, timedelta

DB_PATH = "/private/tmp/claude-501/-Users-raiymbekdaniiaruulu-IdealProjects-StartTups-claud/ca35c44a-7b50-4c62-b973-d2f29e7a1363/scratchpad/traderbot.db"

BASKET_SIZE = 5
NOTIONAL_PER_LEG = 50.0  # $50 на символ, $500 всего (5 long + 5 short)
REBAL_HOURS = 8           # ребалансировка каждые 8 часов
FEE_ROUNDTRIP_PCT = 0.0022  # taker 0.055% × 4 legs (вход+выход)
MIN_PRICE = 0.0001        # фильтр мусора
MIN_SNAPS_PER_WINDOW = 3  # минимум снимков символа в окне для включения
QUALITY_FILTERS = True     # фильтр качества (убираем радиоактивные)
MAX_DAILY_RATE = 20.0      # |rate| > 20%/день = пре-делистинг/кап, исключаем
MIN_DAYS_SEEN = 2          # символ должен быть в данных минимум N дней


def load_data(db_path: str):
    conn = sqlite3.connect(db_path)
    cur = conn.execute("""
        SELECT symbol, ts, bybit_daily_pct, bybit_price, spread_daily_pct,
               exec_edge_pct
        FROM funding_spread_snaps
        WHERE bybit_daily_pct IS NOT NULL
          AND bybit_price IS NOT NULL
          AND bybit_price > ?
        ORDER BY ts
    """, (MIN_PRICE,))
    rows = cur.fetchall()
    conn.close()
    return rows


def parse_ts(ts_str: str) -> datetime:
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(ts_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse: {ts_str}")


def _aggregate_window(all_snaps, center, hours=2):
    """Aggregate all snapshots within ±hours of center, keeping latest per symbol."""
    lo = center - timedelta(hours=hours)
    hi = center + timedelta(hours=hours)
    best = {}
    for ts, snap_list in all_snaps.items():
        if lo <= ts <= hi:
            for s in snap_list:
                sym = s["symbol"]
                if sym not in best or ts > best[sym]["_ts"]:
                    best[sym] = {**s, "_ts": ts}
    for v in best.values():
        v.pop("_ts", None)
    return best


def run_backtest():
    print("Loading data...")
    rows = load_data(DB_PATH)
    print(f"Loaded {len(rows)} snapshots")

    snaps_by_time = defaultdict(list)
    symbol_first_seen = {}
    for symbol, ts_str, daily_pct, price, spread_pct, exec_edge in rows:
        ts = parse_ts(ts_str)
        ts_rounded = ts.replace(second=0, microsecond=0)
        ts_rounded = ts_rounded.replace(minute=(ts_rounded.minute // 5) * 5)
        snaps_by_time[ts_rounded].append({
            "symbol": symbol,
            "daily_pct": daily_pct,
            "price": price,
            "spread_pct": spread_pct or 0,
            "exec_edge": exec_edge or 0,
        })
        if symbol not in symbol_first_seen:
            symbol_first_seen[symbol] = ts

    timestamps = sorted(snaps_by_time.keys())
    print(f"Time range: {timestamps[0]} → {timestamps[-1]}")
    print(f"Unique timestamps: {len(timestamps)}")
    print(f"Unique symbols: {len(symbol_first_seen)}")

    settlement_times = []
    t = timestamps[0].replace(hour=0, minute=0, second=0, microsecond=0)
    end = timestamps[-1]
    while t <= end:
        for h in (0, 8, 16):
            st = t.replace(hour=h)
            if timestamps[0] <= st <= end:
                settlement_times.append(st)
        t += timedelta(days=1)
    settlement_times.sort()
    print(f"Settlement windows: {len(settlement_times)}")

    trades = []
    portfolio_history = []
    cumulative_pnl = 0.0
    cumulative_funding = 0.0
    cumulative_basis = 0.0
    cumulative_fees = 0.0

    for i in range(1, len(settlement_times)):
        window_start = settlement_times[i - 1]
        window_end = settlement_times[i]
        window_hours = (window_end - window_start).total_seconds() / 3600

        entry_data = _aggregate_window(snaps_by_time, window_start, hours=2)
        exit_data = _aggregate_window(snaps_by_time, window_end, hours=2)

        if not entry_data or not exit_data:
            continue

        common = set(entry_data.keys()) & set(exit_data.keys())

        if QUALITY_FILTERS:
            filtered = set()
            for sym in common:
                rate = abs(entry_data[sym]["daily_pct"])
                if rate > MAX_DAILY_RATE:
                    continue
                first = symbol_first_seen.get(sym)
                if first and (window_start - first).days < MIN_DAYS_SEEN:
                    continue
                if entry_data[sym]["price"] < 0.001:
                    continue
                filtered.add(sym)
            common = filtered

        if len(common) < BASKET_SIZE * 2:
            continue

        # Rank by funding rate
        ranked = sorted(common, key=lambda s: entry_data[s]["daily_pct"])

        # SHORT basket (most negative funding → we go LONG perp, receive funding)
        short_basket = ranked[:BASKET_SIZE]
        # LONG basket (most positive funding → we go SHORT perp, receive funding)
        long_basket = ranked[-BASKET_SIZE:]

        period_funding = 0.0
        period_basis = 0.0
        period_fees = 0.0

        for sym in long_basket:
            entry_price = entry_data[sym]["price"]
            exit_price = exit_data[sym]["price"]
            rate_daily = entry_data[sym]["daily_pct"] / 100
            qty = NOTIONAL_PER_LEG / entry_price

            # We SHORT this perp (positive funding → shorts receive)
            # funding per period = rate_daily × (window_hours/24) × notional
            funding = rate_daily * (window_hours / 24) * NOTIONAL_PER_LEG
            # basis: short → (entry - exit) × qty
            basis = (entry_price - exit_price) * qty
            fees = NOTIONAL_PER_LEG * FEE_ROUNDTRIP_PCT

            period_funding += funding
            period_basis += basis
            period_fees += fees

            trades.append({
                "window": f"{window_start} → {window_end}",
                "symbol": sym, "side": "SHORT",
                "rate_daily": entry_data[sym]["daily_pct"],
                "entry": entry_price, "exit": exit_price,
                "funding": round(funding, 4),
                "basis": round(basis, 4),
                "fees": round(fees, 4),
            })

        for sym in short_basket:
            entry_price = entry_data[sym]["price"]
            exit_price = exit_data[sym]["price"]
            rate_daily = entry_data[sym]["daily_pct"] / 100
            qty = NOTIONAL_PER_LEG / entry_price

            # We LONG this perp (negative funding → longs receive)
            # funding = -rate × period × notional (negative rate × our long = we receive)
            funding = -rate_daily * (window_hours / 24) * NOTIONAL_PER_LEG
            # basis: long → (exit - entry) × qty
            basis = (exit_price - entry_price) * qty
            fees = NOTIONAL_PER_LEG * FEE_ROUNDTRIP_PCT

            period_funding += funding
            period_basis += basis
            period_fees += fees

            trades.append({
                "window": f"{window_start} → {window_end}",
                "symbol": sym, "side": "LONG",
                "rate_daily": entry_data[sym]["daily_pct"],
                "entry": entry_price, "exit": exit_price,
                "funding": round(funding, 4),
                "basis": round(basis, 4),
                "fees": round(fees, 4),
            })

        period_pnl = period_funding + period_basis - period_fees
        cumulative_pnl += period_pnl
        cumulative_funding += period_funding
        cumulative_basis += period_basis
        cumulative_fees += period_fees

        portfolio_history.append({
            "window_end": window_end,
            "funding": round(period_funding, 4),
            "basis": round(period_basis, 4),
            "fees": round(period_fees, 4),
            "pnl": round(period_pnl, 4),
            "cum_pnl": round(cumulative_pnl, 4),
            "long_basket": long_basket,
            "short_basket": short_basket,
            "long_rates": [round(entry_data[s]["daily_pct"], 2) for s in long_basket],
            "short_rates": [round(entry_data[s]["daily_pct"], 2) for s in short_basket],
        })

    # ─── Results ───
    print("\n" + "=" * 80)
    print("CROSS-SECTIONAL FUNDING CARRY BACKTEST")
    print("=" * 80)
    print(f"Period: {timestamps[0].date()} → {timestamps[-1].date()} ({(timestamps[-1]-timestamps[0]).days} days)")
    print(f"Basket: {BASKET_SIZE} long (short perp) + {BASKET_SIZE} short (long perp)")
    print(f"Notional: ${NOTIONAL_PER_LEG}/leg × {BASKET_SIZE*2} legs = ${NOTIONAL_PER_LEG*BASKET_SIZE*2}")
    print(f"Rebalance: every {REBAL_HOURS}h | Fee: {FEE_ROUNDTRIP_PCT*100:.2f}% roundtrip")
    print(f"Quality filters: max_rate={MAX_DAILY_RATE}%/d, min_days={MIN_DAYS_SEEN}")
    print()

    if not portfolio_history:
        print("NO TRADES GENERATED")
        return

    n_windows = len(portfolio_history)
    pnls = [p["pnl"] for p in portfolio_history]
    wins = sum(1 for p in pnls if p > 0)
    fundings = [p["funding"] for p in portfolio_history]
    bases = [p["basis"] for p in portfolio_history]

    print(f"Periods: {n_windows}")
    print(f"Win rate: {wins}/{n_windows} ({wins/n_windows*100:.0f}%)")
    print()
    print(f"{'':20s} {'Total':>10s} {'Avg/period':>12s} {'Avg/day':>10s}")
    print(f"{'Funding collected':20s} {cumulative_funding:>10.2f} {cumulative_funding/n_windows:>12.4f} {cumulative_funding/((timestamps[-1]-timestamps[0]).days or 1):>10.4f}")
    print(f"{'Basis PnL':20s} {cumulative_basis:>10.2f} {cumulative_basis/n_windows:>12.4f} {cumulative_basis/((timestamps[-1]-timestamps[0]).days or 1):>10.4f}")
    print(f"{'Fees':20s} {-cumulative_fees:>10.2f} {-cumulative_fees/n_windows:>12.4f} {-cumulative_fees/((timestamps[-1]-timestamps[0]).days or 1):>10.4f}")
    print(f"{'NET PnL':20s} {cumulative_pnl:>10.2f} {cumulative_pnl/n_windows:>12.4f} {cumulative_pnl/((timestamps[-1]-timestamps[0]).days or 1):>10.4f}")
    print()

    # Sharpe-like: avg_pnl / std_pnl per period
    if len(pnls) > 1:
        avg = statistics.mean(pnls)
        std = statistics.stdev(pnls)
        sharpe_period = avg / std if std > 0 else 0
        # Annualize: 3 periods/day × 365
        sharpe_annual = sharpe_period * (3 * 365) ** 0.5
        print(f"Sharpe (annualized): {sharpe_annual:.2f}")
        print(f"Avg PnL/period: {avg:.4f} | Std: {std:.4f}")
    print()

    # Max drawdown
    peak = 0
    max_dd = 0
    for p in portfolio_history:
        peak = max(peak, p["cum_pnl"])
        dd = peak - p["cum_pnl"]
        max_dd = max(max_dd, dd)
    print(f"Max drawdown: ${max_dd:.2f}")
    print(f"Final cumulative PnL: ${cumulative_pnl:.2f}")
    print()

    # Decomposition: is funding > fees?
    print("─── Decomposition ───")
    print(f"Funding collected:  ${cumulative_funding:+.2f}")
    print(f"Basis drift:        ${cumulative_basis:+.2f}")
    print(f"Fees paid:          ${-cumulative_fees:+.2f}")
    print(f"Funding − fees:     ${cumulative_funding - cumulative_fees:+.2f} ({'✅ positive' if cumulative_funding > cumulative_fees else '❌ negative'})")
    print(f"Basis as % of PnL:  {abs(cumulative_basis) / (abs(cumulative_funding) + abs(cumulative_basis) + 0.0001) * 100:.0f}% (lower = more carry-driven)")
    print()

    # Daily equity curve
    print("─── Daily Equity Curve ───")
    daily = defaultdict(float)
    for p in portfolio_history:
        day = p["window_end"].date()
        daily[day] += p["pnl"]

    cum = 0
    for day in sorted(daily):
        cum += daily[day]
        bar = "█" * max(0, int(cum * 10)) + "░" * max(0, -int(cum * 10))
        print(f"  {day}: {daily[day]:+.3f} | cum: {cum:+.3f} {bar}")
    print()

    # Worst periods
    print("─── Worst 5 Periods ───")
    worst = sorted(portfolio_history, key=lambda p: p["pnl"])[:5]
    for p in worst:
        print(f"  {p['window_end']}: pnl={p['pnl']:+.4f} funding={p['funding']:+.4f} "
              f"basis={p['basis']:+.4f} fees={p['fees']:.4f}")
        print(f"    SHORT perp (receive funding): {p['long_basket']} rates={p['long_rates']}")
        print(f"    LONG perp (receive funding):  {p['short_basket']} rates={p['short_rates']}")
    print()

    # Best periods
    print("─── Best 5 Periods ───")
    best = sorted(portfolio_history, key=lambda p: -p["pnl"])[:5]
    for p in best:
        print(f"  {p['window_end']}: pnl={p['pnl']:+.4f} funding={p['funding']:+.4f} "
              f"basis={p['basis']:+.4f} fees={p['fees']:.4f}")
        print(f"    SHORT perp (receive funding): {p['long_basket']} rates={p['long_rates']}")
        print(f"    LONG perp (receive funding):  {p['short_basket']} rates={p['short_rates']}")
    print()

    # Sensitivity: vary basket size and fees
    print("─── Sensitivity: basket size ───")
    for bs in [3, 5, 7, 10]:
        print(f"  basket={bs}: (rerun with BASKET_SIZE={bs} for full results)")

    # Without quality filters
    print("\n─── Without quality filters ───")
    print("  (rerun with QUALITY_FILTERS=False)")

    # Verdict
    print("\n" + "=" * 80)
    if cumulative_pnl > 0 and cumulative_funding > cumulative_fees:
        print("VERDICT: ✅ POSITIVE — carry exceeds costs")
        print("  BUT: 16 days is ONE market regime. Not a final answer.")
        print("  Next: live paper test for 2-4 weeks across different regimes.")
    elif cumulative_funding > cumulative_fees:
        print("VERDICT: 🟡 CARRY POSITIVE but basis drift kills it")
        print("  Funding > fees, but price moves overwhelm carry income.")
        print("  Possible fix: tighter hedging or directional filter.")
    else:
        print("VERDICT: ❌ NEGATIVE — carry does not exceed costs")
        print("  Trading direction CLOSED with clean conscience.")
        print("  Focus on non-trading income (affiliate, content, skills).")
    print("=" * 80)


if __name__ == "__main__":
    run_backtest()

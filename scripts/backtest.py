"""Backtesting гибридной стратегии A+B на реальных данных Bybit.

Стратегия A: funding rate MR + RSI + SMA тренд (1h свечи)
Стратегия B: BTC lead-lag (15min свечи)

Запуск:
  python scripts/backtest.py --symbol ETHUSDT --days 365
  python scripts/backtest.py --symbol ETHUSDT --days 365 --strategy A
  python scripts/backtest.py --symbol ETHUSDT --days 90
"""
from __future__ import annotations
import argparse
from datetime import datetime, timedelta, timezone
import ccxt
import pandas as pd
import numpy as np
from loguru import logger


# ── fetch helpers ──────────────────────────────────────────────────────────

def fetch_funding_history(exchange, symbol: str, since_ms: int) -> pd.DataFrame:
    all_funding = []
    cursor = since_ms
    while True:
        batch = exchange.fetch_funding_rate_history(symbol, since=cursor, limit=200)
        if not batch:
            break
        all_funding.extend(batch)
        cursor = batch[-1]["timestamp"] + 1
        if len(batch) < 200:
            break
    df = pd.DataFrame([{
        "time": pd.to_datetime(r["timestamp"], unit="ms", utc=True),
        "funding_rate": r["fundingRate"],
    } for r in all_funding])
    df.set_index("time", inplace=True)
    return df


def fetch_ohlcv(exchange, symbol: str, since_ms: int, timeframe: str = "1h") -> pd.DataFrame:
    """Fetch OHLCV from Binance public API (generous rate limits, no auth needed)."""
    import time as _time
    import httpx

    # Binance uses BTCUSDT / ETHUSDT — same names as Bybit for perps
    url = "https://api.binance.com/api/v3/klines"
    limit = 1000
    tf_ms = {
        "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000,
        "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000,
    }.get(timeframe, 3_600_000)
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    all_candles = []
    cursor = since_ms

    with httpx.Client(timeout=15.0) as client:
        while cursor < now_ms:
            for attempt in range(5):
                try:
                    r = client.get(url, params={
                        "symbol": symbol, "interval": timeframe,
                        "startTime": cursor, "limit": limit,
                    })
                    r.raise_for_status()
                    batch = r.json()
                    break
                except Exception as e:
                    wait = 3 * (attempt + 1)
                    logger.warning(f"Binance fetch error ({e}), retrying in {wait}s")
                    _time.sleep(wait)
            else:
                raise RuntimeError(f"Failed to fetch {symbol} {timeframe} after 5 attempts")

            if not batch:
                break
            new = [c for c in batch if c[0] >= cursor]
            if not new:
                break
            all_candles.extend([[c[0], float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5])] for c in new])
            cursor = new[-1][0] + tf_ms
            if len(batch) < limit:
                break
            _time.sleep(0.1)

    df = pd.DataFrame(all_candles, columns=["ts", "open", "high", "low", "close", "volume"])
    df["time"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    df.set_index("time", inplace=True)
    return df[["open", "high", "low", "close", "volume"]]


# ── индикаторы ─────────────────────────────────────────────────────────────

def compute_rsi_series(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def compute_adx(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    """ADX indicator. <20 = ranging, >25 = trending."""
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)
    up = (high - high.shift()).clip(lower=0)
    dn = (low.shift() - low).clip(lower=0)
    up_clean = up.copy(); dn_clean = dn.copy()
    up_clean[up < dn] = 0; dn_clean[dn < up] = 0
    atr = tr.ewm(span=period, adjust=False).mean()
    pdi = 100 * up_clean.ewm(span=period, adjust=False).mean() / (atr + 1e-9)
    ndi = 100 * dn_clean.ewm(span=period, adjust=False).mean() / (atr + 1e-9)
    dx = (pdi - ndi).abs() / (pdi + ndi + 1e-9) * 100
    return dx.ewm(span=period, adjust=False).mean()


def compute_dfa(prices: np.ndarray, min_scale: int = 4, max_scale: int = 50) -> float:
    """DFA exponent: <0.5 mean-reverting, >0.5 trending."""
    n = len(prices)
    if n < max_scale * 2:
        return 0.5
    y = np.cumsum(prices - prices.mean())
    scales = np.unique(np.logspace(np.log10(min_scale), np.log10(min(max_scale, n // 4)), 15).astype(int))
    flucts = []
    valid_scales = []
    for s in scales:
        n_seg = n // s
        if n_seg < 2:
            continue
        seg_flucts = []
        for i in range(n_seg):
            seg = y[i * s:(i + 1) * s]
            x = np.arange(len(seg))
            coef = np.polyfit(x, seg, 1)
            seg_flucts.append(np.mean((seg - np.polyval(coef, x)) ** 2))
        flucts.append(np.sqrt(np.mean(seg_flucts)))
        valid_scales.append(s)
    if len(flucts) < 3:
        return 0.5
    alpha, _ = np.polyfit(np.log(valid_scales), np.log(flucts), 1)
    return float(np.clip(alpha, 0.1, 0.9))


def simulate_slippage(price: float, direction: str, is_limit: bool = True) -> float:
    slip = 0.0005 if is_limit else 0.001
    return price * (1 + slip) if direction == "long" else price * (1 - slip)


# ── Strategy A: funding rate MR ────────────────────────────────────────────

def run_strategy_a(
    funding_df: pd.DataFrame,
    ohlcv_1h: pd.DataFrame,
    threshold: float,
    sl_pct: float,
    tp_pct: float,
    maker_fee: float,
    taker_fee: float,
    occupied: set,
    adx_max: float = 100.0,   # 100 = no filter; 25 = only ranging markets
) -> list[dict]:
    rsi_1h = compute_rsi_series(ohlcv_1h["close"])
    ohlcv_4h_close = ohlcv_1h["close"].resample("4h").last().dropna()
    ohlcv_4h_high  = ohlcv_1h["high"].resample("4h").max().dropna()
    ohlcv_4h_low   = ohlcv_1h["low"].resample("4h").min().dropna()
    sma_4h = ohlcv_4h_close.rolling(20).mean()
    adx_4h = compute_adx(ohlcv_4h_high, ohlcv_4h_low, ohlcv_4h_close)

    # ATR volatility filter: не торгуем если vol spike > 2x нормы (1h)
    tr_1h = pd.concat([
        ohlcv_1h["high"] - ohlcv_1h["low"],
        (ohlcv_1h["high"] - ohlcv_1h["close"].shift()).abs(),
        (ohlcv_1h["low"]  - ohlcv_1h["close"].shift()).abs(),
    ], axis=1).max(axis=1)
    atr_1h = tr_1h.rolling(24).mean()   # 24h ATR baseline
    vol_ratio = tr_1h / (atr_1h + 1e-9)

    trades = []
    in_pos = False
    entry_price = sl_price = tp_price = 0.0
    direction = ""
    entry_time = None
    stats = {"funding_pass": 0, "rsi_kill": 0, "sma_kill": 0, "entered": 0}

    for ts, row in ohlcv_1h.iterrows():
        if ts in occupied:
            continue

        fr_window = funding_df[
            (funding_df.index >= ts - pd.Timedelta("8h")) &
            (funding_df.index <= ts)
        ]
        fr = float(fr_window["funding_rate"].iloc[-1]) if not fr_window.empty else 0.0

        high, low, close = row["high"], row["low"], row["close"]

        if not in_pos:
            if abs(fr) < threshold:
                continue
            stats["funding_pass"] += 1

            # ATR volatility spike filter: пропускаем если текущая свеча > 2.5x ATR
            vol_now = vol_ratio.get(ts, 1.0)
            if pd.isna(vol_now):
                vol_now = 1.0
            if vol_now > 2.5:
                continue

            # ADX filter (опционально)
            if adx_max < 100:
                adx_candidates = adx_4h[adx_4h.index <= ts].dropna()
                if len(adx_candidates) > 0 and float(adx_candidates.iloc[-1]) > adx_max:
                    continue

            rsi_now = rsi_1h.get(ts, 50.0)
            if pd.isna(rsi_now):
                rsi_now = 50.0
            if fr > 0 and rsi_now < 45:
                stats["rsi_kill"] += 1
                continue
            if fr < 0 and rsi_now > 55:
                stats["rsi_kill"] += 1
                continue

            direction = "short" if fr > 0 else "long"
            sma_vals = sma_4h[sma_4h.index <= ts].dropna()
            if len(sma_vals) > 0:
                sma_val = float(sma_vals.iloc[-1])
                if direction == "short" and close > sma_val:
                    stats["sma_kill"] += 1
                    continue
                if direction == "long" and close < sma_val:
                    stats["sma_kill"] += 1
                    continue

            entry_price = simulate_slippage(close, direction, is_limit=True)
            sl_price = entry_price * (1 + sl_pct) if direction == "short" else entry_price * (1 - sl_pct)
            tp_price = entry_price * (1 - tp_pct) if direction == "short" else entry_price * (1 + tp_pct)
            in_pos = True
            entry_time = ts
            stats["entered"] += 1

        else:
            hit_sl = (direction == "short" and high >= sl_price) or \
                     (direction == "long" and low <= sl_price)
            hit_tp = (direction == "short" and low <= tp_price) or \
                     (direction == "long" and high >= tp_price)

            if hit_sl:
                exit_price = sl_price
            elif hit_tp:
                exit_price = tp_price
            elif entry_time and (ts - entry_time) >= pd.Timedelta("24h"):
                exit_price = close
                hit_tp = True
            else:
                occupied.add(ts)
                continue

            raw_pnl = (exit_price - entry_price) / entry_price * (1 if direction == "long" else -1)
            net_pnl = raw_pnl - (maker_fee + taker_fee)
            net_pnl += abs(fr) * (1 if (direction == "short" and fr > 0) or (direction == "long" and fr < 0) else -1)

            trades.append({
                "strategy": "A",
                "entry_time": entry_time, "exit_time": ts,
                "direction": direction,
                "entry_price": entry_price, "exit_price": exit_price,
                "net_pnl_pct": net_pnl,
                "outcome": "sl" if hit_sl else "tp",
            })
            in_pos = False

    logger.info(
        f"[A] funding_pass={stats['funding_pass']} rsi_kill={stats['rsi_kill']} "
        f"sma_kill={stats['sma_kill']} trades={len(trades)}"
    )
    return trades


# ── Strategy B: BTC lead-lag ───────────────────────────────────────────────

def run_strategy_b(
    eth_15m: pd.DataFrame,
    btc_15m: pd.DataFrame,
    sl_pct: float = 0.007,
    tp_pct: float = 0.015,
    maker_fee: float = 0.0002,
    taker_fee: float = 0.00055,
    btc_threshold: float = 0.0035,
    eth_max_move: float = 0.0015,
    occupied: set = None,
) -> list[dict]:
    if occupied is None:
        occupied = set()

    btc_ret = btc_15m["close"].pct_change()
    eth_ret = eth_15m["close"].pct_change()
    btc_vol_ma = btc_15m["volume"].rolling(10).mean()

    trades = []
    in_pos = False
    entry_price = sl_price = tp_price = 0.0
    direction = ""
    entry_time = None
    stats = {"candidates": 0, "vol_kill": 0, "entered": 0}

    # Итерируемся по 15min свечам ETH
    for ts in eth_15m.index:
        if ts not in btc_ret.index or ts not in eth_ret.index:
            continue

        eth_row = eth_15m.loc[ts]
        high, low, close = eth_row["high"], eth_row["low"], eth_row["close"]

        if not in_pos:
            if ts in occupied:
                continue
            br = btc_ret.get(ts, 0.0)
            er = abs(eth_ret.get(ts, 0.0))

            if abs(br) < btc_threshold or er > eth_max_move:
                continue
            stats["candidates"] += 1

            # Volume confirmation
            bvma = btc_vol_ma.get(ts, 0.0)
            bvol = btc_15m.loc[ts, "volume"] if ts in btc_15m.index else 0.0
            if bvma > 0 and bvol < bvma * 1.5:
                stats["vol_kill"] += 1
                continue

            direction = "long" if br > 0 else "short"
            entry_price = simulate_slippage(close, direction, is_limit=False)  # taker вход
            sl_price = entry_price * (1 - sl_pct) if direction == "long" else entry_price * (1 + sl_pct)
            tp_price = entry_price * (1 + tp_pct) if direction == "long" else entry_price * (1 - tp_pct)
            in_pos = True
            entry_time = ts
            stats["entered"] += 1

        else:
            hit_sl = (direction == "long" and low <= sl_price) or \
                     (direction == "short" and high >= sl_price)
            hit_tp = (direction == "long" and high >= tp_price) or \
                     (direction == "short" and low <= tp_price)

            if hit_sl:
                exit_price = sl_price
            elif hit_tp:
                exit_price = tp_price
            elif entry_time and (ts - entry_time) >= pd.Timedelta("4h"):
                exit_price = close  # max hold для B = 4h
                hit_tp = True
            else:
                occupied.add(ts)
                continue

            raw_pnl = (exit_price - entry_price) / entry_price * (1 if direction == "long" else -1)
            net_pnl = raw_pnl - (taker_fee * 2)  # taker вход + taker выход

            trades.append({
                "strategy": "B",
                "entry_time": entry_time, "exit_time": ts,
                "direction": direction,
                "entry_price": entry_price, "exit_price": exit_price,
                "net_pnl_pct": net_pnl,
                "outcome": "sl" if hit_sl else "tp",
            })
            in_pos = False

    logger.info(
        f"[B] candidates={stats['candidates']} vol_kill={stats['vol_kill']} trades={len(trades)}"
    )
    return trades


# ── статистика ─────────────────────────────────────────────────────────────

def calc_stats(trades: list[dict], label: str) -> dict:
    if not trades:
        return {"label": label, "trades": 0}
    df = pd.DataFrame(trades)
    wins = (df["net_pnl_pct"] > 0).sum()
    total = df["net_pnl_pct"].sum()
    avg = df["net_pnl_pct"].mean()
    std = df["net_pnl_pct"].std()
    sharpe = avg / (std + 1e-9) * np.sqrt(len(df))
    equity = (1 + df["net_pnl_pct"]).cumprod()
    mdd = (equity / equity.cummax() - 1).min()
    pf_num = df[df["net_pnl_pct"] > 0]["net_pnl_pct"].sum()
    pf_den = abs(df[df["net_pnl_pct"] < 0]["net_pnl_pct"].sum()) + 1e-9
    return {
        "label": label,
        "trades": len(df),
        "win_rate": wins / len(df),
        "total_return": total,
        "avg_pnl": avg,
        "sharpe": sharpe,
        "max_drawdown": mdd,
        "profit_factor": pf_num / pf_den,
    }


def print_stats(s: dict) -> None:
    if s["trades"] == 0:
        print(f"\n=== {s['label']} — no trades ===")
        return
    print(f"\n=== {s['label']} ===")
    print(f"  Trades:         {s['trades']} (~{s['trades']//12}/month)")
    print(f"  Win rate:       {s['win_rate']:.1%}")
    print(f"  Total return:   {s['total_return']:+.2%}")
    print(f"  Avg PnL/trade:  {s['avg_pnl']:+.3%}")
    print(f"  Sharpe:         {s['sharpe']:.2f}")
    print(f"  Max Drawdown:   {s['max_drawdown']:.2%}")
    print(f"  Profit Factor:  {s['profit_factor']:.2f}")


# ── main ───────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="ETHUSDT")
    parser.add_argument("--days", type=int, default=365)
    parser.add_argument("--threshold", type=float, default=0.0001)
    parser.add_argument("--sl_a", type=float, default=0.01, help="Strategy A SL")
    parser.add_argument("--tp_a", type=float, default=0.02, help="Strategy A TP")
    parser.add_argument("--sl_b", type=float, default=0.007, help="Strategy B SL")
    parser.add_argument("--tp_b", type=float, default=0.015, help="Strategy B TP")
    parser.add_argument("--strategy", default="AB", choices=["A", "B", "AB"])
    parser.add_argument("--adx_max", type=float, default=100.0, help="Max ADX(4h) to allow entry (100=no filter)")
    args = parser.parse_args()

    exchange = ccxt.bybit({"options": {"defaultType": "linear"}})
    since_ms = int((datetime.now(timezone.utc) - timedelta(days=args.days)).timestamp() * 1000)
    maker_fee, taker_fee = 0.0002, 0.00055

    # Fetch данные
    logger.info(f"Fetching funding history for {args.symbol}...")
    funding_df = fetch_funding_history(exchange, args.symbol, since_ms)
    logger.info(f"Got {len(funding_df)} funding records")

    logger.info(f"Fetching ETH OHLCV 1h...")
    eth_1h = fetch_ohlcv(exchange, args.symbol, since_ms, "1h")
    logger.info(f"Got {len(eth_1h)} 1h candles")

    eth_15m = btc_15m = None
    if args.strategy in ("B", "AB"):
        logger.info(f"Fetching ETH OHLCV 15m (may take ~30s)...")
        eth_15m = fetch_ohlcv(exchange, args.symbol, since_ms, "15m")
        logger.info(f"Got {len(eth_15m)} ETH 15m candles")

        logger.info(f"Fetching BTC OHLCV 15m...")
        btc_15m = fetch_ohlcv(exchange, "BTCUSDT", since_ms, "15m")
        logger.info(f"Got {len(btc_15m)} BTC 15m candles")

    # DFA режим (последние 500h)
    if len(eth_1h) >= 200:
        closes = eth_1h["close"].values[-500:]
        dfa = compute_dfa(closes)
        regime = "trending" if dfa > 0.58 else "mean_reverting" if dfa < 0.42 else "transition"
        logger.info(f"DFA={dfa:.3f} regime={regime} (on full period)")

    # Запускаем стратегии
    occupied: set = set()
    trades_a, trades_b = [], []

    if args.strategy in ("A", "AB"):
        trades_a = run_strategy_a(
            funding_df, eth_1h, args.threshold,
            args.sl_a, args.tp_a, maker_fee, taker_fee, occupied,
            adx_max=args.adx_max,
        )

    if args.strategy in ("B", "AB") and eth_15m is not None:
        trades_b = run_strategy_b(
            eth_15m, btc_15m,
            args.sl_b, args.tp_b, maker_fee, taker_fee,
            occupied=occupied,
        )

    all_trades = trades_a + trades_b
    all_trades.sort(key=lambda t: t["entry_time"])

    # Вывод результатов
    print(f"\nSymbol: {args.symbol} | Period: {args.days}d | Strategy: {args.strategy}")
    print(f"Threshold: {args.threshold:.4%} | A: SL={args.sl_a:.1%}/TP={args.tp_a:.1%}"
          f" | B: SL={args.sl_b:.1%}/TP={args.tp_b:.1%}")

    if args.strategy in ("A", "AB"):
        print_stats(calc_stats(trades_a, "Strategy A (Funding MR)"))
    if args.strategy in ("B", "AB"):
        print_stats(calc_stats(trades_b, "Strategy B (BTC Lead-Lag)"))
    if args.strategy == "AB":
        print_stats(calc_stats(all_trades, "COMBINED A+B"))


if __name__ == "__main__":
    main()

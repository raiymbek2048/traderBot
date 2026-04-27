"""Momentum strategy backtest on historical 5m data.

Запуск:
  python scripts/momentum_backtest.py --days 90
  python scripts/momentum_backtest.py --days 180 --sl 0.003 --tp 0.006
"""
from __future__ import annotations
import argparse
import sys, os
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from scripts.backtest import fetch_ohlcv
from momentum.signal import (
    generate_momentum_signal, classify_regime,
    compute_oi_delta_pct, _ema, compute_vwap,
)


def run_momentum_backtest(
    eth_5m: pd.DataFrame,
    btc_5m: pd.DataFrame,
    eth_1h: pd.DataFrame,
    sl_pct: float,
    tp_pct: float,
    ema_fast: int,
    ema_slow: int,
    vwap_threshold: float,
    btc_threshold: float,
    max_hold_bars: int,
    branch_filter: str | None = None,  # "eth", "btc", or None = both
) -> list[dict]:
    maker_fee, taker_fee = 0.0002, 0.00055
    trades = []
    in_pos = False
    entry_price = sl_price = tp_price = 0.0
    direction = ""
    entry_time = None
    hold_bars = 0
    stats = {"signals": 0, "entered": 0}

    timestamps = eth_5m.index.tolist()

    for i, ts in enumerate(timestamps):
        if i < max(ema_slow + 5, 25):
            continue

        eth_slice   = [{"open": r["open"], "high": r["high"], "low": r["low"],
                         "close": r["close"], "volume": r["volume"]}
                        for _, r in eth_5m.iloc[max(0, i-60):i].iterrows()]
        btc_slice   = [{"open": r["open"], "high": r["high"], "low": r["low"],
                         "close": r["close"], "volume": r["volume"]}
                        for _, r in btc_5m.iloc[max(0, i-60):i].iterrows()]
        eth_1h_slice = [{"open": r["open"], "high": r["high"], "low": r["low"],
                          "close": r["close"], "volume": r["volume"]}
                         for _, r in eth_1h[eth_1h.index <= ts].iloc[-50:].iterrows()]

        row = eth_5m.iloc[i]
        high, low, close = row["high"], row["low"], row["close"]

        if in_pos:
            hold_bars += 1
            hit_tp = (direction == "long" and high >= tp_price) or \
                     (direction == "short" and low <= tp_price)
            hit_sl = (direction == "long" and low <= sl_price) or \
                     (direction == "short" and high >= sl_price)
            timeout = hold_bars >= max_hold_bars

            if hit_tp or hit_sl or timeout:
                exit_price = tp_price if hit_tp else (sl_price if hit_sl else close)
                raw = (exit_price - entry_price) / entry_price * (1 if direction == "long" else -1)
                net = raw - maker_fee - taker_fee
                trades.append({
                    "entry_time": entry_time, "exit_time": ts,
                    "direction": direction, "entry": entry_price, "exit": exit_price,
                    "net_pnl": net, "outcome": "tp" if hit_tp else ("sl" if hit_sl else "timeout"),
                })
                in_pos = False
            continue

        # Generate signal
        oi_proxy = [{"oi": eth_5m["volume"].iloc[max(0,i-j)]} for j in range(50, 0, -1)]

        sig = generate_momentum_signal(
            eth_5m=eth_slice,
            btc_5m=btc_slice,
            eth_oi_5m=oi_proxy,
            ohlcv_1h=eth_1h_slice,
            sl_pct=sl_pct, tp_pct=tp_pct,
            ema_fast=ema_fast, ema_slow=ema_slow,
            vwap_threshold=vwap_threshold,
            btc_threshold=btc_threshold,
        )

        if sig and (branch_filter is None or sig.branch == branch_filter):
            stats["signals"] += 1
            offset = 0.0005
            entry_price = close * (1 - offset) if sig.direction == "long" else close * (1 + offset)
            sl_price = sig.sl_price
            tp_price = sig.tp_price
            direction = sig.direction
            entry_time = ts
            hold_bars = 0
            in_pos = True
            stats["entered"] += 1

    logger.info(f"Signals: {stats['signals']} | Entered: {stats['entered']} | Trades: {len(trades)}")
    return trades


def print_stats(trades: list[dict], label: str) -> None:
    if not trades:
        print(f"\n{label}: no trades")
        return
    df = pd.DataFrame(trades)
    wins = (df["net_pnl"] > 0).sum()
    wr = wins / len(df)
    total = df["net_pnl"].sum()
    avg = df["net_pnl"].mean()
    sharpe = avg / (df["net_pnl"].std() + 1e-9) * np.sqrt(len(df))
    equity = (1 + df["net_pnl"]).cumprod()
    mdd = (equity / equity.cummax() - 1).min()
    trades_per_day = len(df) / max(1, (df["exit_time"].max() - df["entry_time"].min()).days)

    print(f"\n=== {label} ===")
    print(f"  Trades:         {len(df)} (~{trades_per_day:.1f}/day)")
    print(f"  Win rate:       {wr:.1%}  (breakeven: 53.5%)")
    print(f"  Total return:   {total:+.2%}")
    print(f"  Avg PnL/trade:  {avg:+.4%}")
    print(f"  Sharpe:         {sharpe:.2f}")
    print(f"  Max Drawdown:   {mdd:.2%}")
    go = "✅ PASSES" if wr >= 0.535 else "❌ BELOW breakeven"
    print(f"  Gate:           {go}")

    # Outcome breakdown
    oc = df["outcome"].value_counts()
    print(f"  TP/SL/Timeout:  {oc.get('tp',0)}/{oc.get('sl',0)}/{oc.get('timeout',0)}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--sl",  type=float, default=0.0035)
    parser.add_argument("--tp",  type=float, default=0.007)
    parser.add_argument("--ema_fast", type=int, default=8)
    parser.add_argument("--ema_slow", type=int, default=21)
    parser.add_argument("--vwap", type=float, default=0.002)
    parser.add_argument("--btc_threshold", type=float, default=0.0035)
    parser.add_argument("--max_hold", type=int, default=24)
    args = parser.parse_args()

    since_ms = int((datetime.now(timezone.utc) - timedelta(days=args.days)).timestamp() * 1000)

    print(f"Fetching {args.days}d of 5m data from Binance (fast, ~20s)...")
    eth_5m = fetch_ohlcv(None, "ETHUSDT", since_ms, "5m")
    print(f"  ETH 5m: {len(eth_5m)} candles")
    btc_5m = fetch_ohlcv(None, "BTCUSDT", since_ms, "5m")
    print(f"  BTC 5m: {len(btc_5m)} candles")
    eth_1h = fetch_ohlcv(None, "ETHUSDT", since_ms, "1h")
    print(f"  ETH 1h: {len(eth_1h)} candles")
    print(f"ETH 5m: {len(eth_5m)} candles | BTC 5m: {len(btc_5m)} | ETH 1h: {len(eth_1h)}")

    trades = run_momentum_backtest(
        eth_5m, btc_5m, eth_1h,
        sl_pct=args.sl, tp_pct=args.tp,
        ema_fast=args.ema_fast, ema_slow=args.ema_slow,
        vwap_threshold=args.vwap, btc_threshold=args.btc_threshold,
        max_hold_bars=args.max_hold,
    )

    print(f"\nPeriod: {args.days}d | SL={args.sl:.2%} TP={args.tp:.2%} | "
          f"EMA {args.ema_fast}/{args.ema_slow} | VWAP_th={args.vwap:.2%}")
    print_stats(trades, "Momentum 5m (both branches)")

    # Branch isolation
    for branch in ("eth", "btc"):
        t = run_momentum_backtest(
            eth_5m, btc_5m, eth_1h,
            sl_pct=args.sl, tp_pct=args.tp,
            ema_fast=args.ema_fast, ema_slow=args.ema_slow,
            vwap_threshold=args.vwap, btc_threshold=args.btc_threshold,
            max_hold_bars=args.max_hold, branch_filter=branch,
        )
        print_stats(t, f"Branch: {branch.upper()}")

    # VWAP threshold sweep
    print(f"\n{'='*55}")
    print("VWAP DEVIATION THRESHOLD SWEEP")
    print(f"{'='*55}")
    for vwap_th in [0.003, 0.004, 0.005, 0.006, 0.007, 0.008, 0.010]:
        t = run_momentum_backtest(
            eth_5m, btc_5m, eth_1h,
            sl_pct=args.sl, tp_pct=args.tp,
            ema_fast=args.ema_fast, ema_slow=args.ema_slow,
            vwap_threshold=vwap_th, btc_threshold=args.btc_threshold,
            max_hold_bars=args.max_hold,
        )
        if t:
            df = pd.DataFrame(t)
            wr = (df["net_pnl"] > 0).mean()
            total = df["net_pnl"].sum()
            per_day = len(t) / max(1, args.days)
            gate = "✅" if wr >= 0.535 else "❌"
            print(f"  VWAP_th={vwap_th:.1%}: n={len(t):4d} ({per_day:.1f}/d) "
                  f"WR={wr:.1%} total={total:+.2%} {gate}")
        else:
            print(f"  VWAP_th={vwap_th:.1%}: no trades")

    # SL/TP sweep at best threshold
    print(f"\n{'='*55}")
    print("SL/TP SWEEP (VWAP_th=0.50%)")
    print(f"{'='*55}")
    for sl, tp in [(0.002, 0.004), (0.003, 0.006), (0.0035, 0.007), (0.004, 0.008), (0.005, 0.01)]:
        t = run_momentum_backtest(
            eth_5m, btc_5m, eth_1h,
            sl_pct=sl, tp_pct=tp,
            ema_fast=args.ema_fast, ema_slow=args.ema_slow,
            vwap_threshold=0.005, btc_threshold=args.btc_threshold,
            max_hold_bars=args.max_hold,
        )
        if t:
            df = pd.DataFrame(t)
            wr = (df["net_pnl"] > 0).mean()
            total = df["net_pnl"].sum()
            per_day = len(t) / max(1, args.days)
            gate = "✅" if wr >= 0.535 else "❌"
            print(f"  SL={sl:.2%} TP={tp:.2%}: n={len(t):4d} ({per_day:.1f}/d) "
                  f"WR={wr:.1%} total={total:+.2%} {gate}")


if __name__ == "__main__":
    main()

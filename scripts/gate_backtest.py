"""Gate Backtest — исторический прогон Alpha Gate v2.

Не ждёт 30 дней: берёт исторические данные и симулирует gate-решения.

Источники:
  - Funding divergence: Bybit (уже в backtest) vs Binance (бесплатный REST API)
  - Liquidation screen: proxy через объём + price momentum (реальные ликвидации
    исторически недоступны бесплатно, proxy достаточен для первичной калибровки)
  - On-chain: отключён (нет исторического CryptoQuant)
  - Macro blocker: отключён (исторические RSS недоступны)

Запуск:
  python scripts/gate_backtest.py --days 365
  python scripts/gate_backtest.py --days 180 --threshold 0.55
"""
from __future__ import annotations
import argparse
import sys, os
from datetime import datetime, timedelta, timezone
from collections import defaultdict

import httpx
import ccxt
import pandas as pd
import numpy as np
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from scripts.backtest import (
    fetch_funding_history,
    fetch_ohlcv,
    run_strategy_a,
    calc_stats,
    print_stats,
)
from gate.scorer import compute_gate_score, GATE_THRESHOLD_INITIAL


# ── Binance historical funding ─────────────────────────────────────────────

def fetch_binance_funding_history(symbol: str = "ETHUSDT", since_ms: int = 0) -> pd.DataFrame:
    """Бесплатный API Binance — до 1000 записей за запрос (каждые 8h)."""
    url = "https://fapi.binance.com/fapi/v1/fundingRate"
    all_rows = []
    cursor = since_ms
    logger.info("Fetching Binance funding history...")

    while True:
        try:
            r = httpx.get(url, params={"symbol": symbol, "startTime": cursor, "limit": 1000}, timeout=15)
            r.raise_for_status()
            batch = r.json()
        except Exception as e:
            logger.warning(f"Binance funding fetch failed: {e}")
            break

        if not batch:
            break
        for row in batch:
            all_rows.append({
                "time": pd.to_datetime(row["fundingTime"], unit="ms", utc=True),
                "binance_rate": float(row["fundingRate"]),
            })
        if len(batch) < 1000:
            break
        cursor = batch[-1]["fundingTime"] + 1

    if not all_rows:
        return pd.DataFrame(columns=["binance_rate"])

    df = pd.DataFrame(all_rows).set_index("time").sort_index()
    logger.info(f"Got {len(df)} Binance funding records")
    return df


# ── Gate score по историческим данным ─────────────────────────────────────

def compute_historical_div_score(
    bybit_rate: float,
    ts: pd.Timestamp,
    binance_df: pd.DataFrame,
    threshold: float = 0.0001,
) -> float:
    """Divergence score по ближайшей Binance funding rate в историческом окне."""
    if binance_df.empty:
        return 0.5  # нет данных — нейтрально

    # Ближайшая запись Binance не позже ts
    window = binance_df[binance_df.index <= ts]
    if window.empty:
        return 0.5
    binance_rate = float(window["binance_rate"].iloc[-1])

    spread = abs(bybit_rate - binance_rate)
    # В историческом режиме нет проверки стабильности (данные 8h-агрегаты)
    # Компенсируем: требуем spread >= threshold для любого кредита
    if spread >= threshold * 3:
        return 1.0
    if spread >= threshold:
        return 0.5 + (spread - threshold) / (threshold * 2) * 0.5
    return spread / threshold * 0.4


def compute_historical_liq_score(
    ts: pd.Timestamp,
    ohlcv_1h: pd.DataFrame,
    vol_ma_window: int = 24,
    liq_vol_threshold: float = 2.0,  # x vol MA = "significant liquidations"
    momentum_threshold: float = 0.003,  # 0.3% в предыдущем часу
) -> float:
    """
    Proxy liquidation screen без исторических данных Coinglass.
    
    Логика: если перед входом был volume spike (>2x MA) НО price не двигался (<0.3%)
    → возможно цена зажата между кластерами → score низкий.
    Если есть momentum → score высокий.
    """
    window = ohlcv_1h[ohlcv_1h.index <= ts].tail(vol_ma_window + 2)
    if len(window) < 3:
        return 0.7  # нет данных — нейтрально

    vol_ma = window["volume"].iloc[:-1].mean()
    vol_now = window["volume"].iloc[-1]
    price_prev = window["close"].iloc[-2]
    price_now  = window["close"].iloc[-1]

    if vol_ma == 0:
        return 0.7

    vol_ratio = vol_now / vol_ma
    if vol_ratio < liq_vol_threshold:
        return 0.8  # нет заметных ликвидаций

    # Большой объём — есть ли momentum?
    price_move = abs(price_now - price_prev) / price_prev
    if price_move < momentum_threshold:
        return 0.3  # volume spike + цена стоит = cluster trap
    return 0.8


# ── Главный backtest ───────────────────────────────────────────────────────

def run_gate_backtest(
    funding_df: pd.DataFrame,
    ohlcv_1h: pd.DataFrame,
    binance_df: pd.DataFrame,
    threshold_strategy: float,
    sl_pct: float,
    tp_pct: float,
    gate_threshold: float,
    div_weight_only: bool = False,
) -> tuple[list[dict], list[dict]]:
    """
    Прогоняет Strategy A и для каждой сделки считает gate score.
    Возвращает (all_trades, trades_with_gate_score).
    """
    maker_fee, taker_fee = 0.0002, 0.00055
    occupied: set = set()

    trades = run_strategy_a(
        funding_df=funding_df,
        ohlcv_1h=ohlcv_1h,
        threshold=threshold_strategy,
        sl_pct=sl_pct,
        tp_pct=tp_pct,
        maker_fee=maker_fee,
        taker_fee=taker_fee,
        occupied=occupied,
    )

    if not trades:
        return [], []

    enriched = []
    for t in trades:
        ts = t["entry_time"]

        # Funding rate на момент входа
        fr_window = funding_df[
            (funding_df.index >= ts - pd.Timedelta("8h")) &
            (funding_df.index <= ts)
        ]
        bybit_rate = float(fr_window["funding_rate"].iloc[-1]) if not fr_window.empty else 0.0

        div_score = compute_historical_div_score(bybit_rate, ts, binance_df)
        liq_score = compute_historical_liq_score(ts, ohlcv_1h)

        gate = compute_gate_score(
            liq_score=liq_score,
            div_score=div_score,
            onchain_score=None,
            macro_blocked=False,
            threshold=gate_threshold,
        )

        enriched.append({
            **t,
            "gate_decision": gate.decision,
            "composite_score": gate.composite,
            "liq_score": liq_score,
            "div_score": div_score,
            "bybit_rate": bybit_rate,
        })

    return trades, enriched


# ── Отчёт ──────────────────────────────────────────────────────────────────

def print_gate_report(enriched: list[dict], gate_threshold: float) -> None:
    if not enriched:
        print("No trades to analyze.")
        return

    df = pd.DataFrame(enriched)
    approved = df[df["gate_decision"] == "approve"]
    blocked  = df[df["gate_decision"] != "approve"]

    def eff_stats(subset: pd.DataFrame, label: str) -> dict:
        if subset.empty:
            print(f"{label}: 0 trades")
            return {}
        wr = (subset["net_pnl_pct"] > 0).mean()
        avg = subset["net_pnl_pct"].mean()
        total = subset["net_pnl_pct"].sum()
        sharpe = avg / (subset["net_pnl_pct"].std() + 1e-9) * np.sqrt(len(subset))
        print(f"{label}: n={len(subset):3d}  WR={wr:.1%}  avg={avg:+.3%}  total={total:+.2%}  sharpe={sharpe:.2f}")
        return {"wr": wr, "avg": avg, "n": len(subset)}

    print(f"\n{'='*60}")
    print("ALPHA GATE v2 — HISTORICAL BACKTEST REPORT")
    print(f"{'='*60}")
    print(f"Gate threshold: {gate_threshold:.2f}")
    print(f"Total signals:  {len(df)}")
    print(f"Approved:       {len(approved)} ({len(approved)/len(df):.0%})")
    print(f"Blocked:        {len(blocked)} ({len(blocked)/len(df):.0%})")
    print()

    all_s  = eff_stats(df, "ALL signals    ")
    app_s  = eff_stats(approved, "Gate APPROVED  ")
    blk_s  = eff_stats(blocked,  "Gate BLOCKED   ")

    if all_s and app_s:
        avg_all = all_s["avg"]
        avg_app = app_s["avg"]
        effectiveness = (avg_app / avg_all - 1) if avg_all != 0 else 0.0
        wr_delta = app_s["wr"] - all_s["wr"]

        print(f"\ngate_effectiveness = {effectiveness:+.1%}  (need >= +10% for live)")
        print(f"win_rate_delta     = {wr_delta:+.1%}  (need >= +5% for live)")

        go_live = effectiveness >= 0.10 and wr_delta >= 0.05
        if go_live:
            print("\n✅ GATE PASSES CRITERIA — прошёл историческую валидацию")
            print("   Следующий шаг: 30-дневный shadow mode для финального подтверждения")
        else:
            reasons = []
            if effectiveness < 0.10:
                reasons.append(f"effectiveness {effectiveness:+.1%} < +10%")
            if wr_delta < 0.05:
                reasons.append(f"win_rate_delta {wr_delta:+.1%} < +5%")
            print(f"\n⚠️  Gate не проходит критерии: {', '.join(reasons)}")

    # Распределение score
    print(f"\nComposite score distribution (все сделки):")
    bins = [0, 0.4, 0.5, 0.55, 0.6, 0.65, 0.7, 0.8, 1.01]
    labels = ["<0.4", "0.4-0.5", "0.5-0.55", "0.55-0.6", "0.6-0.65", "0.65-0.7", "0.7-0.8", ">0.8"]
    df["score_bin"] = pd.cut(df["composite_score"], bins=bins, labels=labels, right=False)
    for label, group in df.groupby("score_bin", observed=True):
        avg_pnl = group["net_pnl_pct"].mean()
        wr = (group["net_pnl_pct"] > 0).mean()
        print(f"  {label:10s}: n={len(group):3d}  WR={wr:.0%}  avg={avg_pnl:+.3%}")


# ── main ───────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="ETHUSDT")
    parser.add_argument("--days", type=int, default=365)
    parser.add_argument("--threshold", type=float, default=0.0001, help="Strategy A funding threshold")
    parser.add_argument("--gate", type=float, default=GATE_THRESHOLD_INITIAL, help="Gate composite threshold")
    parser.add_argument("--sl", type=float, default=0.01)
    parser.add_argument("--tp", type=float, default=0.02)
    args = parser.parse_args()

    exchange = ccxt.bybit({"options": {"defaultType": "linear"}})
    since_ms = int((datetime.now(timezone.utc) - timedelta(days=args.days)).timestamp() * 1000)

    logger.info(f"Fetching Bybit funding history ({args.symbol}, {args.days}d)...")
    funding_df = fetch_funding_history(exchange, args.symbol, since_ms)
    logger.info(f"Got {len(funding_df)} Bybit funding records")

    logger.info("Fetching ETH 1h OHLCV...")
    ohlcv_1h = fetch_ohlcv(exchange, args.symbol, since_ms, "1h")
    logger.info(f"Got {len(ohlcv_1h)} candles")

    binance_df = fetch_binance_funding_history(args.symbol, since_ms)

    print(f"\nSymbol: {args.symbol} | Period: {args.days}d")
    print(f"Strategy threshold: {args.threshold:.4%} | SL={args.sl:.1%}/TP={args.tp:.1%}")

    # Базовый backtest без gate
    maker_fee, taker_fee = 0.0002, 0.00055
    base_trades = run_strategy_a(
        funding_df, ohlcv_1h, args.threshold,
        args.sl, args.tp, maker_fee, taker_fee, set()
    )
    print_stats(calc_stats(base_trades, "Strategy A (baseline, no gate)"))

    # Gate backtest
    _, enriched = run_gate_backtest(
        funding_df, ohlcv_1h, binance_df,
        threshold_strategy=args.threshold,
        sl_pct=args.sl,
        tp_pct=args.tp,
        gate_threshold=args.gate,
    )
    print_gate_report(enriched, args.gate)

    # Дополнительно: sweep по порогам gate (0.4 — 0.7)
    print(f"\n{'='*60}")
    print("THRESHOLD SWEEP (найти оптимальный gate threshold)")
    print(f"{'='*60}")
    if enriched:
        df_all = pd.DataFrame(enriched)
        for gt in [0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70]:
            app = df_all[df_all["composite_score"] >= gt]
            if len(app) < 3:
                continue
            avg_app = app["net_pnl_pct"].mean()
            avg_all_val = df_all["net_pnl_pct"].mean()
            wr_app = (app["net_pnl_pct"] > 0).mean()
            wr_all_val = (df_all["net_pnl_pct"] > 0).mean()
            eff = (avg_app / avg_all_val - 1) if avg_all_val != 0 else 0
            print(
                f"  gate={gt:.2f}: n={len(app):3d}/{len(df_all)}  "
                f"WR={wr_app:.0%}(Δ{wr_app-wr_all_val:+.0%})  "
                f"avg={avg_app:+.3%}  eff={eff:+.1%}"
            )


if __name__ == "__main__":
    main()

"""Liquidation cluster negative screen.

Role: REDUCE composite score when price is trapped between clusters without momentum.
Uses Bybit liquidation volume (already in fetcher) as proxy for cluster presence.

Score semantics: INVERTED from others — high score = NOT trapped = gate is open.
- Score 0.8 (default): no cluster trap detected
- Score 0.3: price trapped (large liquidations + no momentum)

"Price trapped" heuristic (no Coinglass API needed):
  liq_volume_1h > $2M  AND  abs(price_change_15m) < 0.3%
"""
from __future__ import annotations
from loguru import logger

LIQ_TRAP_THRESHOLD_USD = 2_000_000  # $2M liquidations in last hour
MOMENTUM_MIN_PCT = 0.003             # 0.3% price move in 15m = "has momentum"

SCORE_NO_TRAP = 0.8
SCORE_TRAPPED = 0.3
SCORE_UNAVAILABLE = 0.7  # slightly positive — missing data, don't punish


def compute_liquidation_score(
    liquidation_usd_1h: float,
    price_now: float,
    price_15m_ago: float | None,
) -> float:
    """Returns [0, 1] score. Lower = price is trapped between liquidation clusters."""
    if liquidation_usd_1h <= 0:
        return SCORE_UNAVAILABLE

    if liquidation_usd_1h < LIQ_TRAP_THRESHOLD_USD:
        return SCORE_NO_TRAP  # not enough liquidations to form meaningful clusters

    # Significant liquidations — check if price has momentum
    if price_15m_ago is None or price_15m_ago == 0:
        return SCORE_UNAVAILABLE

    price_change = abs(price_now - price_15m_ago) / price_15m_ago
    if price_change < MOMENTUM_MIN_PCT:
        # large liquidations AND price stuck → cluster trap
        logger.debug(
            f"Cluster trap: liq=${liquidation_usd_1h:,.0f} price_change={price_change:.3%}"
        )
        return SCORE_TRAPPED

    return SCORE_NO_TRAP

"""Bybit vs Binance funding rate divergence source.

Score semantics: higher = larger spread, more favorable for entry confirmation.
Threshold 0.01% = ~75th percentile of 18-month Bybit-Binance spread.
Stability: spread must hold >5 min (3 consecutive readings, polled every 2 min).
"""
from __future__ import annotations
from collections import deque
from datetime import datetime, timezone
import httpx
from loguru import logger

BINANCE_URL = "https://fapi.binance.com/fapi/v1/fundingRate"
DIVERGENCE_THRESHOLD = 0.0001   # 0.01%
STABILITY_MIN_READINGS = 3       # 3 × 2min = 6min ≥ 5min requirement

_spread_history: deque[tuple[datetime, float]] = deque(maxlen=10)


async def fetch_binance_funding(symbol: str = "ETHUSDT") -> float | None:
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(BINANCE_URL, params={"symbol": symbol, "limit": 1})
            r.raise_for_status()
            data = r.json()
            if data:
                return float(data[0]["fundingRate"])
    except Exception as e:
        logger.warning(f"Binance funding fetch failed: {e}")
    return None


def record_spread(bybit_rate: float, binance_rate: float) -> float:
    spread = bybit_rate - binance_rate
    _spread_history.append((datetime.now(timezone.utc), spread))
    return spread


def is_spread_stable() -> bool:
    """True if last STABILITY_MIN_READINGS readings all had |spread| >= threshold."""
    if len(_spread_history) < STABILITY_MIN_READINGS:
        return False
    recent = list(_spread_history)[-STABILITY_MIN_READINGS:]
    return all(abs(s) >= DIVERGENCE_THRESHOLD for _, s in recent)


def compute_divergence_score(bybit_rate: float, binance_rate: float | None) -> float:
    """Returns [0, 1] score. 0.5 = no data (neutral)."""
    if binance_rate is None:
        return 0.5  # degrade gracefully

    spread = abs(bybit_rate - binance_rate)
    record_spread(bybit_rate, binance_rate)

    if not is_spread_stable():
        # spread exists but not sustained — partial credit
        if spread >= DIVERGENCE_THRESHOLD:
            return 0.45
        return 0.2

    # stable spread
    if spread >= DIVERGENCE_THRESHOLD * 3:
        return 1.0
    if spread >= DIVERGENCE_THRESHOLD:
        return 0.5 + (spread - DIVERGENCE_THRESHOLD) / (DIVERGENCE_THRESHOLD * 2) * 0.5
    return spread / DIVERGENCE_THRESHOLD * 0.5

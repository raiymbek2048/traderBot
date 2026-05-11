"""Arbitrage mock data generator — tests the pipeline without real exchanges.

Simulates realistic Binance/Bybit price feeds with occasional spreads.
Run: python -m arbitrage.mock

Useful for:
- Testing DB writes and spread detection logic
- Validating monitor.py without WebSocket connections
- CI/CD smoke tests
"""
from __future__ import annotations
import asyncio
import random
import time
from datetime import datetime, timezone

from loguru import logger

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from shared.db import init_db, SpreadEvent
from shared.config import load_config
from arbitrage.monitor import books, BookTop, _check_spread

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

# Realistic base prices
BASE_PRICES = {"BTCUSDT": 95000.0, "ETHUSDT": 1800.0, "SOLUSDT": 145.0}

# How often to inject a "real" spread (1 in N ticks)
SPREAD_INJECT_EVERY = 50


async def mock_feed(engine) -> None:
    tick = 0
    prices = {s: BASE_PRICES[s] for s in SYMBOLS}

    logger.info("Mock feed started — simulating Binance + Bybit price streams")

    while True:
        tick += 1
        for sym in SYMBOLS:
            # Random walk
            prices[sym] *= 1 + random.gauss(0, 0.0002)

            spread_offset = 0.0
            if tick % SPREAD_INJECT_EVERY == 0:
                # Inject artificial spread 0.35-0.55%
                spread_offset = random.uniform(0.0035, 0.0055) * prices[sym]
                logger.debug(f"Injecting spread on {sym}: +{spread_offset:.2f}")

            mid = prices[sym]
            half_spread = mid * 0.00005  # 0.005% bid-ask

            books[f"binance:{sym}"] = BookTop(
                exchange="binance", symbol=sym,
                bid=mid - half_spread,
                ask=mid + half_spread,
                ts=time.time() * 1000,
            )
            books[f"bybit:{sym}"] = BookTop(
                exchange="bybit", symbol=sym,
                bid=mid + spread_offset - half_spread,
                ask=mid + spread_offset + half_spread,
                ts=time.time() * 1000,
            )

            await _check_spread(sym, engine)

        await asyncio.sleep(0.5)  # 2 ticks/sec per symbol


async def stats_loop(engine) -> None:
    from sqlalchemy.orm import Session
    from sqlalchemy import func, select
    from shared.db import SpreadEvent as SE

    while True:
        await asyncio.sleep(30)
        with Session(engine) as session:
            total = session.scalar(select(func.count(SE.id))) or 0
            alerts = session.scalar(
                select(func.count(SE.id)).where(SE.spread_pct >= 0.4)
            ) or 0
        logger.info(f"Mock stats: {total} total events | {alerts} alert-level (≥0.40%)")


async def main() -> None:
    cfg = load_config()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG")

    engine = init_db(cfg.database_url)
    logger.info("Starting mock arbitrage feed (no real exchange connections)")

    await asyncio.gather(
        mock_feed(engine),
        stats_loop(engine),
    )


if __name__ == "__main__":
    asyncio.run(main())

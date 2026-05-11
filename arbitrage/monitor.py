"""Arbitrage Phase 0 — spread monitoring only, zero capital risk.

Connects to Binance + Bybit WebSocket simultaneously, logs every spread
≥ MIN_SPREAD_PCT to DB. No orders are placed.

Run: python -m arbitrage.monitor
"""
from __future__ import annotations
import asyncio
import json
import time
from dataclasses import dataclass

import websockets
from loguru import logger
from sqlalchemy.orm import Session

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from shared.db import init_db, SpreadEvent
from shared.config import load_config
from shared.utils import utcnow

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
MIN_SPREAD_PCT = 0.003   # 0.30% — log threshold
ALERT_SPREAD_PCT = 0.004 # 0.40% — potential trade threshold

BINANCE_WS = "wss://stream.binance.com:9443/stream?streams="
BYBIT_WS   = "wss://stream.bybit.com/v5/public/linear"


@dataclass
class BookTop:
    exchange: str
    symbol: str
    bid: float
    ask: float
    ts: float  # unix ms


# ── shared state ──────────────────────────────────────────────────────────────

books: dict[str, BookTop] = {}  # key: "binance:BTCUSDT" / "bybit:BTCUSDT"


def compute_spread(sym: str) -> tuple[float, float] | None:
    """Returns (spread_pct, net_pct_after_fees) or None if books missing."""
    b = books.get(f"binance:{sym}")
    y = books.get(f"bybit:{sym}")
    if not b or not y:
        return None
    # Buy on cheaper ask, sell on more expensive bid
    if b.ask < y.bid:
        gross = (y.bid - b.ask) / b.ask
        net   = gross - 0.001  # ~0.05% taker each side × 2
        return gross, net
    if y.ask < b.bid:
        gross = (b.bid - y.ask) / y.ask
        net   = gross - 0.001
        return gross, net
    return None


# ── Binance WebSocket ─────────────────────────────────────────────────────────

async def binance_listener(engine) -> None:
    streams = "/".join(f"{s.lower()}@bookTicker" for s in SYMBOLS)
    url = BINANCE_WS + streams
    backoff = 2
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                logger.info("Binance WS connected")
                backoff = 2
                async for raw in ws:
                    msg = json.loads(raw)
                    data = msg.get("data", msg)
                    sym = data.get("s")
                    if sym not in SYMBOLS:
                        continue
                    books[f"binance:{sym}"] = BookTop(
                        exchange="binance", symbol=sym,
                        bid=float(data["b"]), ask=float(data["a"]),
                        ts=time.time() * 1000,
                    )
                    await _check_spread(sym, engine)
        except Exception as e:
            logger.warning(f"Binance WS error: {e}. Reconnect in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 300)


# ── Bybit WebSocket ───────────────────────────────────────────────────────────

async def bybit_listener(engine) -> None:
    backoff = 2
    while True:
        try:
            async with websockets.connect(BYBIT_WS, ping_interval=20) as ws:
                logger.info("Bybit WS connected")
                backoff = 2
                sub = {"op": "subscribe", "args": [f"orderbook.1.{s}" for s in SYMBOLS]}
                await ws.send(json.dumps(sub))
                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("topic", "").startswith("orderbook"):
                        data = msg.get("data", {})
                        sym = msg["topic"].split(".")[-1]
                        if sym not in SYMBOLS:
                            continue
                        bids = data.get("b", [])
                        asks = data.get("a", [])
                        if not bids or not asks:
                            continue
                        books[f"bybit:{sym}"] = BookTop(
                            exchange="bybit", symbol=sym,
                            bid=float(bids[0][0]), ask=float(asks[0][0]),
                            ts=float(msg.get("ts", time.time() * 1000)),
                        )
                        await _check_spread(sym, engine)
        except Exception as e:
            logger.warning(f"Bybit WS error: {e}. Reconnect in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 300)


# ── spread checker ────────────────────────────────────────────────────────────

async def _check_spread(sym: str, engine) -> None:
    result = compute_spread(sym)
    if result is None:
        return
    gross, net = result
    if gross < MIN_SPREAD_PCT:
        return

    b = books.get(f"binance:{sym}")
    y = books.get(f"bybit:{sym}")
    buy_ex  = "binance" if b.ask < y.bid else "bybit"
    sell_ex = "bybit"   if buy_ex == "binance" else "binance"

    event = SpreadEvent(
        symbol=sym,
        buy_exchange=buy_ex,
        sell_exchange=sell_ex,
        buy_price=b.ask if buy_ex == "binance" else y.ask,
        sell_price=y.bid if sell_ex == "bybit" else b.bid,
        spread_pct=round(gross * 100, 4),
        net_pct=round(net * 100, 4),
        ts=utcnow(),
    )

    with Session(engine) as session:
        session.add(event)
        session.commit()

    level = "🚨 ALERT" if gross >= ALERT_SPREAD_PCT else "📊"
    logger.info(
        f"{level} {sym} spread={gross*100:.3f}% net={net*100:.3f}% "
        f"buy@{buy_ex} sell@{sell_ex}"
    )


# ── stats printer ─────────────────────────────────────────────────────────────

async def stats_printer(engine) -> None:
    """Print hourly summary to stdout."""
    while True:
        await asyncio.sleep(3600)
        with Session(engine) as session:
            from sqlalchemy import func, select
            from shared.db import SpreadEvent as SE
            rows = session.execute(
                select(SE.symbol, func.count(), func.avg(SE.spread_pct), func.max(SE.spread_pct))
                .where(SE.spread_pct >= ALERT_SPREAD_PCT * 100)
                .group_by(SE.symbol)
            ).all()
        if rows:
            logger.info("=== Hourly arbitrage summary (≥0.40% spreads) ===")
            for sym, cnt, avg_sp, max_sp in rows:
                logger.info(f"  {sym}: {cnt} signals | avg={avg_sp:.3f}% | max={max_sp:.3f}%")
        else:
            logger.info("=== No spreads ≥0.40% in last hour ===")


# ── entrypoint ────────────────────────────────────────────────────────────────

async def main() -> None:
    cfg = load_config()
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    logger.add("logs/arbitrage_monitor.log", rotation="50 MB", retention="90 days")

    engine = init_db(cfg.database_url)
    logger.info(f"Arbitrage monitor started. Symbols: {SYMBOLS}, threshold: {MIN_SPREAD_PCT*100:.2f}%")

    await asyncio.gather(
        binance_listener(engine),
        bybit_listener(engine),
        stats_printer(engine),
    )


if __name__ == "__main__":
    asyncio.run(main())

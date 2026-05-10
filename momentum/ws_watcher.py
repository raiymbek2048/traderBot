"""Real-time SL/TP monitoring via Bybit WebSocket ticker stream."""
from __future__ import annotations
import asyncio
import json
from typing import Callable, Awaitable

import websockets
from loguru import logger

_WS_URL = "wss://stream.bybit.com/v5/public/linear"
_RECONNECT_DELAY = 5


async def run_price_watcher(
    symbol: str,
    on_price: Callable[[float], Awaitable[None]],
) -> None:
    """Subscribe to mark price stream and call on_price on every tick.

    Reconnects automatically on disconnect. Runs forever — cancel the task to stop.
    """
    topic = f"tickers.{symbol}"
    subscribe_msg = json.dumps({"op": "subscribe", "args": [topic]})

    while True:
        try:
            async with websockets.connect(
                _WS_URL,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
            ) as ws:
                await ws.send(subscribe_msg)
                logger.info(f"[WS] Connected → {topic}")

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    if msg.get("topic") != topic:
                        continue

                    price_str = msg.get("data", {}).get("markPrice")
                    if not price_str:
                        continue

                    try:
                        await on_price(float(price_str))
                    except Exception as e:
                        logger.error(f"[WS] on_price error: {e}")

        except asyncio.CancelledError:
            logger.info("[WS] Watcher cancelled")
            return
        except Exception as e:
            logger.warning(f"[WS] Disconnected ({e}), retry in {_RECONNECT_DELAY}s")
            await asyncio.sleep(_RECONNECT_DELAY)

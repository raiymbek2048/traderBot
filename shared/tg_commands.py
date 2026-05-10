"""Telegram command handler — /status and /stats via long-polling."""
from __future__ import annotations
import asyncio
from loguru import logger


async def run_command_bot(token: str, chat_id: int, get_status_fn, get_stats_fn) -> None:
    """Poll Telegram for /status and /stats commands.

    get_status_fn() -> str  — current open position
    get_stats_fn()  -> str  — cumulative PnL stats
    """
    try:
        import httpx
    except ImportError:
        logger.warning("[TG_CMD] httpx not available, command bot disabled")
        return

    base = f"https://api.telegram.org/bot{token}"
    offset = 0

    logger.info("[TG_CMD] Command bot started (/status, /stats)")

    while True:
        try:
            async with httpx.AsyncClient(timeout=35.0) as client:
                r = await client.get(
                    f"{base}/getUpdates",
                    params={"offset": offset, "timeout": 30, "allowed_updates": ["message"]},
                )
                updates = r.json().get("result", [])

            for upd in updates:
                offset = upd["update_id"] + 1
                msg = upd.get("message", {})
                text = msg.get("text", "").strip()
                from_id = msg.get("chat", {}).get("id")

                if from_id != chat_id:
                    continue

                if text in ("/status", "/status@traderbot"):
                    reply = get_status_fn()
                elif text in ("/stats", "/stats@traderbot"):
                    reply = get_stats_fn()
                else:
                    continue

                async with httpx.AsyncClient(timeout=10.0) as client:
                    await client.post(
                        f"{base}/sendMessage",
                        json={"chat_id": chat_id, "text": reply},
                    )

        except asyncio.CancelledError:
            logger.info("[TG_CMD] Command bot cancelled")
            return
        except Exception as e:
            logger.warning(f"[TG_CMD] Poll error: {e}, retry in 10s")
            await asyncio.sleep(10)

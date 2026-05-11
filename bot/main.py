"""Telegram command bot — отвечает на /status и /stats."""
from __future__ import annotations
import asyncio
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger
from sqlalchemy.orm import Session
from sqlalchemy import select, func

from shared.config import load_config
from shared.db import init_db, Trade
from shared.utils import utcnow


def _fetch_price(cfg) -> float | None:
    try:
        import ccxt
        exchange = ccxt.bybit({
            "apiKey": cfg.bybit_api_key,
            "secret": cfg.bybit_api_secret,
            "options": {"defaultType": "linear"},
        })
        if cfg.bybit_testnet:
            exchange.set_sandbox_mode(True)
        return exchange.fetch_ticker(cfg.symbol)["last"]
    except Exception as e:
        logger.warning(f"Price fetch failed: {e}")
        return None


def _fmt_duration(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    if h:
        return f"{h}h {m}m"
    return f"{m}m"


def build_status(cfg, engine) -> str:
    with Session(engine) as session:
        trade = session.scalars(
            select(Trade)
            .where(Trade.status == "open")
            .where(Trade.paper == cfg.paper_trading)
        ).first()

        if not trade:
            return "No open position"

        current = _fetch_price(cfg)
        mode = "PAPER " if cfg.paper_trading else ""
        direction = trade.direction.upper()

        if current:
            if trade.direction == "long":
                sl_dist_pct = (current - trade.stop_loss) / current * 100
                tp_dist_pct = (trade.take_profit - current) / current * 100
            else:
                sl_dist_pct = (trade.stop_loss - current) / current * 100
                tp_dist_pct = (current - trade.take_profit) / current * 100

            price_line = f"Current: ${current:.2f}"
            sl_arrow = "🔴" if sl_dist_pct < 0.5 else "🟡" if sl_dist_pct < 1.5 else "🟢"
            tp_arrow = "🔴" if tp_dist_pct < 0.5 else "🟡" if tp_dist_pct < 1.5 else "🟢"
            sl_line = f"{sl_arrow} SL: ${trade.stop_loss:.2f}  ({sl_dist_pct:+.2f}% away)"
            tp_line = f"{tp_arrow} TP: ${trade.take_profit:.2f}  ({tp_dist_pct:+.2f}% away)"
        else:
            price_line = "Current: unavailable"
            sl_line = f"SL: ${trade.stop_loss:.2f}"
            tp_line = f"TP: ${trade.take_profit:.2f}"

        now = utcnow()
        hold_secs = (now - trade.opened_at).total_seconds() if trade.opened_at else 0
        timeout_secs = max(0, 86400 - hold_secs)

        lines = [
            f"📊 {mode}OPEN POSITION",
            f"{direction} {trade.symbol}",
            f"Entry: ${trade.entry_price:.2f}",
            price_line,
            "",
            sl_line,
            tp_line,
            "",
            f"Opened: {_fmt_duration(hold_secs)} ago",
            f"Timeout in: {_fmt_duration(timeout_secs)}",
        ]
        return "\n".join(lines)


def build_stats(cfg, engine) -> str:
    with Session(engine) as session:
        trades = session.scalars(
            select(Trade)
            .where(Trade.status != "open")
            .where(Trade.paper == cfg.paper_trading)
        ).all()

        open_count = session.scalar(
            select(func.count(Trade.id))
            .where(Trade.status == "open")
            .where(Trade.paper == cfg.paper_trading)
        ) or 0

        total_pnl_closed = sum(t.pnl or 0 for t in trades)
        equity = 100.0 + total_pnl_closed

        if not trades:
            mode = "PAPER" if cfg.paper_trading else "LIVE"
            return (
                f"📈 {mode} Stats\n"
                f"No closed trades yet\n"
                f"Equity: ${equity:.2f}"
            )

        wins = sum(1 for t in trades if (t.pnl or 0) > 0)
        losses = len(trades) - wins
        win_rate = wins / len(trades)
        avg_pnl = total_pnl_closed / len(trades)

        stopped = sum(1 for t in trades if t.status == "stopped")
        closed = sum(1 for t in trades if t.status == "closed")
        timeout = sum(1 for t in trades if t.status == "timeout")

        mode = "PAPER" if cfg.paper_trading else "LIVE"
        lines = [
            f"📈 {mode} Stats",
            f"Equity: ${equity:.2f}",
            "",
            f"Trades: {len(trades)} closed + {open_count} open",
            f"W/L: {wins}/{losses}  ({win_rate:.0%} WR)",
            f"Total PnL: {total_pnl_closed:+.4f} USDT",
            f"Avg PnL: {avg_pnl:+.4f} USDT",
            "",
            f"TP hit: {closed}  |  SL hit: {stopped}  |  Timeout: {timeout}",
        ]
        return "\n".join(lines)


async def poll_commands(cfg, engine) -> None:
    try:
        import httpx
    except ImportError:
        logger.error("httpx not installed")
        return

    token = cfg.telegram_token
    chat_id = int(cfg.telegram_chat_id)
    base = f"https://api.telegram.org/bot{token}"
    offset = 0

    logger.info("[BOT] Command bot started — /status, /stats")

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
                text = msg.get("text", "").strip().lower().split("@")[0]
                from_id = msg.get("chat", {}).get("id")

                if from_id != chat_id:
                    continue

                if text == "/status":
                    reply = build_status(cfg, engine)
                elif text == "/stats":
                    reply = build_stats(cfg, engine)
                else:
                    continue

                async with httpx.AsyncClient(timeout=10.0) as client:
                    await client.post(
                        f"{base}/sendMessage",
                        json={"chat_id": chat_id, "text": reply},
                    )
                logger.info(f"[BOT] Replied to {text}")

        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.warning(f"[BOT] Poll error: {e}, retry in 10s")
            await asyncio.sleep(10)


def main():
    cfg = load_config()
    logger.remove()
    logger.add(sys.stderr, level=cfg.log_level)
    logger.add("logs/bot.log", rotation="10 MB", retention="30 days")

    engine = init_db(cfg.database_url)
    logger.info("[BOT] Starting")

    asyncio.run(poll_commands(cfg, engine))


if __name__ == "__main__":
    main()

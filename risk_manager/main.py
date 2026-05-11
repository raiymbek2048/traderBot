"""RISK_MANAGER — мониторит капитал, circuit breaker, heartbeat."""
from __future__ import annotations
import time
from datetime import datetime, timezone, timedelta
from loguru import logger
from sqlalchemy.orm import Session
from sqlalchemy import select, func

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.utils import utcnow
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import ccxt
from shared.config import load_config
from shared.db import init_db, Trade
from shared.notifier import Notifier

TERMINAL_STOP_PCT = 0.50
INITIAL_EQUITY = 100.0
MARGIN_RATIO_ALERT = 0.70  # алерт если margin ratio > 70%


def get_equity(session: Session, paper: bool) -> float:
    total_pnl = session.scalar(
        select(func.sum(Trade.pnl))
        .where(Trade.status != "open")
        .where(Trade.paper == paper)
    ) or 0.0
    return INITIAL_EQUITY + total_pnl


def get_stats(session: Session, paper: bool) -> dict:
    trades = session.scalars(
        select(Trade).where(Trade.status != "open").where(Trade.paper == paper)
    ).all()
    if not trades:
        return {"count": 0, "win_rate": 0, "total_pnl": 0}
    wins = sum(1 for t in trades if (t.pnl or 0) > 0)
    total_pnl = sum(t.pnl or 0 for t in trades)
    return {
        "count": len(trades),
        "win_rate": wins / len(trades),
        "total_pnl": total_pnl,
    }


def check_margin_ratio(cfg, notifier: Notifier) -> None:
    """Проверяет margin ratio на бирже для открытых позиций (только live)."""
    if cfg.paper_trading:
        return
    try:
        exchange = ccxt.bybit({
            "apiKey": cfg.bybit_api_key,
            "secret": cfg.bybit_api_secret,
            "options": {"defaultType": "linear"},
        })
        if cfg.bybit_testnet:
            exchange.set_sandbox_mode(True)
        positions = exchange.fetch_positions([cfg.symbol])
        for pos in positions:
            if pos.get("contracts", 0) == 0:
                continue
            margin_ratio = pos.get("marginRatio") or 0.0
            if margin_ratio > MARGIN_RATIO_ALERT:
                msg = (
                    f"🚨 HIGH MARGIN RATIO: {margin_ratio:.0%}\n"
                    f"Symbol: {cfg.symbol}\n"
                    f"Consider closing position manually!"
                )
                logger.warning(msg)
                notifier.send(msg)
    except Exception as e:
        logger.warning(f"Margin ratio check failed: {e}")


def run_check(cfg, engine, notifier: Notifier, terminal_stop_triggered: list) -> None:
    if terminal_stop_triggered[0]:
        logger.warning("TERMINAL STOP active — not trading")
        return

    with Session(engine) as session:
        equity = get_equity(session, cfg.paper_trading)
        drawdown = (INITIAL_EQUITY - equity) / INITIAL_EQUITY

        if drawdown >= TERMINAL_STOP_PCT:
            terminal_stop_triggered[0] = True
            msg = (
                f"🚨 TERMINAL STOP TRIGGERED\n"
                f"Equity: ${equity:.2f} (drawdown {drawdown:.0%})\n"
                f"Trading halted. Manual review required."
            )
            logger.critical(msg)
            notifier.send(msg)
            return

        # Статистика каждый час (в лог)
        stats = get_stats(session, cfg.paper_trading)
        logger.info(
            f"Equity: ${equity:.2f} | Trades: {stats['count']} | "
            f"WR: {stats['win_rate']:.0%} | PnL: {stats['total_pnl']:+.4f}"
        )

        # Heartbeat каждые 6 часов
        now = utcnow()
        if not hasattr(run_check, "_last_heartbeat") or \
                (now - run_check._last_heartbeat) > timedelta(hours=6):
            notifier.send(
                f"💓 TraderBot alive\n"
                f"Equity: ${equity:.2f} | WR: {stats['win_rate']:.0%} | "
                f"Trades: {stats['count']}"
            )
            run_check._last_heartbeat = now


def main():
    cfg = load_config()
    logger.remove()
    logger.add(sys.stderr, level=cfg.log_level)
    logger.add("logs/risk_manager.log", rotation="10 MB", retention="30 days")

    engine = init_db(cfg.database_url)
    notifier = Notifier(cfg.telegram_token, cfg.telegram_chat_id)
    terminal_stop = [False]

    logger.info("RISK_MANAGER started")
    notifier.send("🤖 TraderBot RISK_MANAGER started")

    while True:
        try:
            run_check(cfg, engine, notifier, terminal_stop)
            check_margin_ratio(cfg, notifier)
        except Exception as e:
            logger.error(f"Risk manager error: {e}")
        time.sleep(5 * 60)  # каждые 5 минут


if __name__ == "__main__":
    main()

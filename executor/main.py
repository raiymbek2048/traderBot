"""EXECUTOR — читает сигналы из БД и открывает/закрывает позиции (paper или live)."""
from __future__ import annotations
import time
from datetime import datetime, timezone, timedelta
from loguru import logger
from sqlalchemy.orm import Session
from sqlalchemy import select

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.utils import utcnow
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from sqlalchemy import func
from shared.config import load_config
from shared.db import init_db, Signal as SignalRow, Trade
from shared.notifier import Notifier
from executor.position import PositionManager


def process_new_signals(cfg, engine, pm: PositionManager, notifier: Notifier) -> None:
    with Session(engine, expire_on_commit=False) as session:
        # Синхронизируем состояние позиции с БД (защита от рестарта)
        if not pm.has_open_position():
            db_trade = pm.get_open_trade_from_db(engine)
            if db_trade:
                pm._open_trade = db_trade
                logger.info(f"Restored open trade #{db_trade.id} from DB")

        # Проверяем открытую позицию прямо в БД (идемпотентность)
        open_count = session.scalar(
            select(func.count(Trade.id))
            .where(Trade.status == "open")
            .where(Trade.paper == cfg.paper_trading)
        )

        max_signal_age = utcnow() - timedelta(hours=2)
        signals = session.scalars(
            select(SignalRow)
            .where(SignalRow.acted_on == False)
            .where(SignalRow.confidence >= 0.60)
            .where(SignalRow.created_at >= max_signal_age)
            .order_by(SignalRow.created_at.asc())
        ).all()

        for sig in signals:
            # Макс 1 открытая позиция — проверяем в БД, не в памяти
            if open_count > 0 or pm.has_open_position():
                logger.info(f"Signal {sig.id} skipped — position already open")
                sig.acted_on = True
                continue

            trade = pm.open_position(
                signal_id=sig.id,
                direction=sig.direction,
                mark_price=sig.mark_price,
            )
            session.add(trade)
            sig.acted_on = True
            open_count += 1  # блокируем следующий сигнал в этом же цикле

            msg = (
                f"{'📈' if sig.direction == 'long' else '📉'} {'PAPER ' if cfg.paper_trading else ''}OPEN\n"
                f"{sig.direction.upper()} {cfg.symbol} @ {sig.mark_price:.2f}\n"
                f"Size: {trade.size:.4f} | SL: {trade.stop_loss:.2f} | TP: {trade.take_profit:.2f}\n"
                f"Confidence: {sig.confidence:.0%}"
            )
            notifier.send(msg)
            logger.info(f"Opened {sig.direction} @ {sig.mark_price:.2f}")

        session.commit()


def check_open_positions(cfg, engine, pm: PositionManager, notifier: Notifier) -> None:
    with Session(engine, expire_on_commit=False) as session:
        open_trades = session.scalars(
            select(Trade).where(Trade.status == "open").where(Trade.paper == cfg.paper_trading)
        ).all()

        for trade in open_trades:
            current_price = pm.get_current_price()
            if current_price is None:
                continue

            result = pm.check_exit(trade, current_price)
            if result:
                trade.exit_price = result.get("exit_price", current_price)
                trade.closed_at = utcnow()
                trade.status = result["status"]
                trade.pnl = result["pnl"]
                trade.pnl_pct = result["pnl_pct"]

                emoji = "✅" if trade.pnl >= 0 else "❌"
                msg = (
                    f"{emoji} {'PAPER ' if cfg.paper_trading else ''}CLOSED\n"
                    f"{trade.direction.upper()} {cfg.symbol}\n"
                    f"Entry: {trade.entry_price:.2f} → Exit: {trade.exit_price:.2f}\n"
                    f"PnL: {trade.pnl:.4f} USDT ({trade.pnl_pct:+.2%})\n"
                    f"Status: {trade.status}"
                )
                notifier.send(msg)
                logger.info(f"Closed trade #{trade.id}: pnl={trade.pnl:.4f} ({trade.pnl_pct:+.2%})")

        session.commit()


def main():
    cfg = load_config()
    logger.remove()
    logger.add(sys.stderr, level=cfg.log_level)
    logger.add("logs/executor.log", rotation="10 MB", retention="30 days")

    engine = init_db(cfg.database_url)
    notifier = Notifier(cfg.telegram_token, cfg.telegram_chat_id)
    pm = PositionManager(cfg)

    # Mark all stale unprocessed signals as acted_on to avoid acting on old data
    with Session(engine) as session:
        stale_cutoff = utcnow() - timedelta(hours=2)
        stale = session.scalars(
            select(SignalRow)
            .where(SignalRow.acted_on == False)
            .where(SignalRow.created_at < stale_cutoff)
        ).all()
        for s in stale:
            s.acted_on = True
        if stale:
            logger.info(f"Marked {len(stale)} stale signals as acted_on on startup")
        session.commit()

    logger.info(f"EXECUTOR started | paper={cfg.paper_trading}")
    notifier.send(f"🤖 TraderBot EXECUTOR started | paper={cfg.paper_trading}")

    while True:
        try:
            process_new_signals(cfg, engine, pm, notifier)
            check_open_positions(cfg, engine, pm, notifier)
        except Exception as e:
            logger.error(f"Error in executor loop: {e}")
            notifier.send(f"⚠️ Executor error: {e}")
        time.sleep(60)  # каждую минуту


if __name__ == "__main__":
    main()

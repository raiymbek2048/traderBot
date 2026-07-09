"""Funding Rate Arb — запуск.

PAPER_TRADING=true   → симуляция (по умолчанию)
PAPER_TRADING=false  → реальные ордера
FUNDING_SIZE_USDT=50 → размер позиции в USDT
ENTRY_THRESHOLD=0.0005 → порог входа (0.05% / 8h)

Run: python -m funding.run
"""
from __future__ import annotations
import asyncio
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger
from shared.config import load_config
from shared.db import init_db

import funding.monitor as monitor
import funding.executor as executor
from funding.executor import BybitClient, signal_queue


async def main() -> None:
    cfg = load_config()

    logger.remove()
    logger.add(sys.stderr, level="INFO")
    # файл пишет systemd (StandardOutput=append), отдельный sink не нужен

    paper      = cfg.paper_trading
    size_usdt  = float(os.environ.get("FUNDING_SIZE_USDT", "50.0"))
    entry_thr  = float(os.environ.get("ENTRY_THRESHOLD", "0.0005"))
    exit_thr   = float(os.environ.get("EXIT_THRESHOLD",  "0.0001"))

    monitor.ENTRY_THRESHOLD = entry_thr
    monitor.EXIT_THRESHOLD  = exit_thr
    monitor.signal_queue    = signal_queue
    monitor._tg_token       = cfg.telegram_token
    monitor._tg_chat        = cfg.telegram_chat_id

    executor._tg_token    = cfg.telegram_token
    executor._tg_chat     = cfg.telegram_chat_id
    executor.PAPER_TRADING = paper

    engine = init_db(cfg.database_url)
    bybit  = BybitClient(cfg.bybit_api_key, cfg.bybit_api_secret)

    open_positions: dict[str, int] = {}

    mode = "PAPER" if paper else "🔴 LIVE"
    logger.info(
        f"Funding Arb [{mode}] | symbols={monitor.SYMBOLS} | "
        f"size=${size_usdt} | entry>{entry_thr*100:.3f}%/8h"
    )

    await asyncio.gather(
        monitor.rate_monitor(open_positions),
        monitor.stats_printer(open_positions),
        executor.run(engine, bybit, paper, size_usdt, open_positions),
        executor.accrual_loop(engine, open_positions),
    )


if __name__ == "__main__":
    asyncio.run(main())

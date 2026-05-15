"""SpreadArb — единый запуск monitor + executor.

PAPER_TRADING=true  → только симуляция (по умолчанию)
PAPER_TRADING=false → реальные ордера
ARB_SIZE_USDT=10    → размер позиции в USDT

Run: python -m arbitrage.run
"""
from __future__ import annotations
import asyncio
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger
from shared.config import load_config
from shared.db import init_db

import arbitrage.monitor as monitor
from arbitrage.executor import (
    BinanceClient, BybitClient, run as executor_run, signal_queue,
    _tg,
)


async def main() -> None:
    global _tg_token, _tg_chat

    cfg = load_config()

    logger.remove()
    logger.add(sys.stderr, level="INFO")
    logger.add("logs/spreadarb.log", rotation="50 MB", retention="90 days")

    paper     = cfg.paper_trading
    size_usdt = float(os.environ.get("ARB_SIZE_USDT", "10.0"))

    engine  = init_db(cfg.database_url)
    binance = BinanceClient(cfg.binance_api_key, cfg.binance_api_secret)
    bybit   = BybitClient(cfg.bybit_api_key, cfg.bybit_api_secret)

    # подключаем очередь сигналов monitor → executor
    monitor._exec_queue  = signal_queue
    monitor._tg_token    = cfg.telegram_token
    monitor._tg_chat     = cfg.telegram_chat_id

    from arbitrage import executor as _exec_mod
    _exec_mod._tg_token = cfg.telegram_token
    _exec_mod._tg_chat  = cfg.telegram_chat_id

    mode = "PAPER" if paper else "🔴 LIVE"
    logger.info(f"SpreadArb [{mode}] starting | symbols={monitor.SYMBOLS} | size=${size_usdt:.0f}")

    await asyncio.gather(
        # монитор: слушает WS, детектирует спреды, пушит в очередь
        monitor.binance_listener(engine),
        monitor.bybit_listener(engine),
        monitor.stats_printer(engine),
        monitor.watchdog(),
        # executor: читает из очереди, исполняет ордера
        executor_run(engine, binance, bybit, paper, size_usdt),
    )


if __name__ == "__main__":
    asyncio.run(main())

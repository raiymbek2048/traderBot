"""ANALYST daemon — funding rate + BTC lead-lag + liquidation cascade сигналы."""
from __future__ import annotations
import time
from datetime import datetime, timezone
from loguru import logger
from sqlalchemy.orm import Session

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from shared.config import load_config
from shared.db import init_db, FundingRate, Signal as SignalRow
from shared.notifier import Notifier
from analyst.fetcher import BybitFetcher
from analyst.signal import generate_signal, classify_regime


_oi_cache: list[dict] = []
_closes_500h: list[float] = []


def _update_oi_cache(fetcher: BybitFetcher, symbol: str) -> list[dict]:
    """Обновляет in-memory OI кэш (TTL ~55 мин = обновляется каждые 30 мин)."""
    global _oi_cache
    fresh = fetcher.get_oi_history(symbol, timeframe="1h", limit=200)
    if fresh:
        _oi_cache = fresh
    return _oi_cache


def _update_500h_closes(fetcher: BybitFetcher, symbol: str) -> list[float]:
    """Обновляет кэш 500h closes для DFA классификатора."""
    global _closes_500h
    ohlcv = fetcher.get_ohlcv(symbol, timeframe="1h", limit=500)
    if ohlcv:
        _closes_500h = [c["close"] for c in ohlcv]
    return _closes_500h


def run_once(cfg, engine, fetcher: BybitFetcher, notifier: Notifier) -> None:
    logger.info(f"Checking signals for {cfg.symbol}...")

    # Основные данные
    data = fetcher.get_funding_rate(cfg.symbol)
    ohlcv_1h = fetcher.get_ohlcv(cfg.symbol, timeframe="1h", limit=50)
    ohlcv_4h = fetcher.get_ohlcv(cfg.symbol, timeframe="4h", limit=30)
    # Strategy B (BTC lead-lag) отключена: backtest показал WR=23.8%, Sharpe=-2.48
    # Включить обратно если рыночные условия изменятся и edge подтвердится
    btc_15m = None
    eth_15m = None

    # OI история + 500h closes (обновляем каждый вызов)
    oi_history = _update_oi_cache(fetcher, cfg.symbol)
    closes_500h = _update_500h_closes(fetcher, cfg.symbol)

    funding_rate = data["current_rate"]
    mark_price = data["mark_price"]
    history_rates = [h["rate"] for h in data["history"]]

    # Режим рынка для логирования
    if len(closes_500h) >= 100:
        regime, dfa = classify_regime(closes_500h)
        logger.info(f"Regime: {regime} (DFA={dfa:.3f}) | Funding: {funding_rate:.4%} | Price: {mark_price}")
    else:
        logger.info(f"Funding: {funding_rate:.4%} | Price: {mark_price}")

    # Сохраняем funding rate
    with Session(engine) as session:
        row = FundingRate(
            symbol=cfg.symbol,
            funding_rate=funding_rate,
            funding_time=datetime.now(timezone.utc),
            mark_price=mark_price,
        )
        session.merge(row)
        session.commit()

    # Генерируем сигнал
    signal = generate_signal(
        funding_rate=funding_rate,
        mark_price=mark_price,
        funding_history=history_rates,
        ohlcv_1h=ohlcv_1h,
        oi_current=None,
        oi_prev=None,
        ohlcv_4h=ohlcv_4h,
        oi_history=oi_history if oi_history else None,
        btc_ohlcv_15m=btc_15m,
        eth_ohlcv_15m=eth_15m,
        closes_500h=closes_500h if closes_500h else None,
        threshold=cfg.funding_threshold,
    )

    if signal is None:
        logger.info("No signal")
        return

    logger.info(
        f"SIGNAL [{signal.strategy}]: {signal.direction.upper()} "
        f"| confidence={signal.confidence:.0%} | {signal.reason}"
    )

    with Session(engine) as session:
        row = SignalRow(
            symbol=cfg.symbol,
            direction=signal.direction,
            funding_rate=signal.funding_rate,
            mark_price=signal.mark_price,
            confidence=signal.confidence,
        )
        session.add(row)
        session.commit()

    strategy_label = f"[{signal.strategy}] "
    msg = (
        f"🔔 TraderBot Signal {strategy_label}\n"
        f"Symbol: {cfg.symbol}\n"
        f"Direction: {signal.direction.upper()}\n"
        f"Confidence: {signal.confidence:.0%}\n"
        f"Price: {mark_price:.2f}\n"
        f"Funding: {funding_rate:.4%}\n"
        f"Reason: {signal.reason}\n"
        f"Paper: {cfg.paper_trading}"
    )
    notifier.send(msg)


def main():
    cfg = load_config()
    logger.remove()
    logger.add(sys.stderr, level=cfg.log_level)
    logger.add("logs/analyst.log", rotation="10 MB", retention="30 days")

    engine = init_db(cfg.database_url)
    fetcher = BybitFetcher(cfg.bybit_api_key, cfg.bybit_api_secret, cfg.bybit_testnet)
    notifier = Notifier(cfg.telegram_token, cfg.telegram_chat_id)

    logger.info("ANALYST started (hybrid: A/B/C + DFA regime)")
    notifier.send("🤖 TraderBot ANALYST started (hybrid A/B/C)")

    while True:
        try:
            run_once(cfg, engine, fetcher, notifier)
        except Exception as e:
            logger.error(f"Error in analyst loop: {e}")
            notifier.send(f"⚠️ Analyst error: {e}")
        time.sleep(30 * 60)


if __name__ == "__main__":
    main()

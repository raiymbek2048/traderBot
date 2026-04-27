"""Управление позицией: расчёт размера, SL/TP, проверка выхода."""
from __future__ import annotations
from datetime import datetime
import ccxt
from loguru import logger

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from shared.config import Config
from shared.db import Trade


class PositionManager:
    def __init__(self, cfg: Config):
        self._cfg = cfg
        self._exchange = ccxt.bybit({
            "apiKey": cfg.bybit_api_key,
            "secret": cfg.bybit_api_secret,
            "options": {"defaultType": "linear"},
        })
        if cfg.bybit_testnet:
            self._exchange.set_sandbox_mode(True)
        self._open_trade: Trade | None = None

    def has_open_position(self) -> bool:
        return self._open_trade is not None and self._open_trade.status == "open"

    def get_current_price(self) -> float | None:
        try:
            ticker = self._exchange.fetch_ticker(self._cfg.symbol)
            return ticker["last"]
        except Exception as e:
            logger.warning(f"Price fetch failed: {e}")
            return None

    def _get_equity(self) -> float:
        """Текущий капитал из БД (начальный + закрытые PnL)."""
        from shared.db import Trade as TradeRow
        from sqlalchemy.orm import Session
        from sqlalchemy import select, func
        from shared.db import get_engine
        engine = get_engine(self._cfg.database_url)
        with Session(engine) as session:
            total_pnl = session.scalar(
                select(func.sum(TradeRow.pnl))
                .where(TradeRow.status != "open")
                .where(TradeRow.paper == self._cfg.paper_trading)
            ) or 0.0
        return 100.0 + total_pnl

    def _calc_size(self, mark_price: float) -> float:
        """Размер позиции: risk_per_trade% от текущего капитала / sl_pct."""
        equity = self._get_equity()
        risk_amount = equity * self._cfg.risk_per_trade
        size_usd = risk_amount / self._cfg.stop_loss_pct
        # Ограничение: нотионал ≤ equity * leverage
        max_notional = equity * self._cfg.leverage
        size_usd = min(size_usd, max_notional)
        return size_usd / mark_price

    def open_position(self, signal_id: int, direction: str, mark_price: float) -> Trade:
        size = self._calc_size(mark_price)

        if direction == "long":
            stop_loss = mark_price * (1 - self._cfg.stop_loss_pct)
            take_profit = mark_price * (1 + self._cfg.take_profit_pct)
        else:
            stop_loss = mark_price * (1 + self._cfg.stop_loss_pct)
            take_profit = mark_price * (1 - self._cfg.take_profit_pct)

        trade = Trade(
            symbol=self._cfg.symbol,
            direction=direction,
            entry_price=mark_price,
            size=size,
            stop_loss=stop_loss,
            take_profit=take_profit,
            paper=self._cfg.paper_trading,
            signal_id=signal_id,
            status="open",
        )

        if not self._cfg.paper_trading:
            self._place_live_order(direction, size, mark_price, stop_loss, take_profit)

        self._open_trade = trade
        return trade

    def get_open_trade_from_db(self, engine) -> "Trade | None":
        """Синхронизирует _open_trade с БД при перезапуске."""
        from sqlalchemy.orm import Session
        from sqlalchemy import select
        with Session(engine) as session:
            trade = session.scalars(
                select(Trade)
                .where(Trade.status == "open")
                .where(Trade.paper == self._cfg.paper_trading)
            ).first()
            if trade:
                session.expunge(trade)
            return trade

    def check_exit(self, trade: Trade, current_price: float) -> dict | None:
        """Проверяет SL/TP по реальной цене. Использует worst-case исполнение."""
        hit_sl = (
            (trade.direction == "long" and current_price <= trade.stop_loss) or
            (trade.direction == "short" and current_price >= trade.stop_loss)
        )
        hit_tp = (
            (trade.direction == "long" and current_price >= trade.take_profit) or
            (trade.direction == "short" and current_price <= trade.take_profit)
        )

        # Максимальное время удержания: 24 часа
        max_hold_exceeded = (
            trade.opened_at is not None and
            (datetime.utcnow() - trade.opened_at).total_seconds() > 86400
        )

        if not hit_sl and not hit_tp and not max_hold_exceeded:
            return None

        # Для SL используем цену SL (не текущую) — стоп исполнился по цене
        if hit_sl:
            exit_price = trade.stop_loss
        elif hit_tp:
            exit_price = trade.take_profit
        else:
            exit_price = current_price  # закрытие по времени

        if trade.direction == "long":
            raw_pnl = (exit_price - trade.entry_price) * trade.size
        else:
            raw_pnl = (trade.entry_price - exit_price) * trade.size

        # Вычитаем комиссию taker на выход (0.055%)
        fee = exit_price * trade.size * 0.00055
        pnl = raw_pnl - fee
        pnl_pct = pnl / (trade.entry_price * trade.size)

        self._open_trade = None
        status = "stopped" if hit_sl else ("timeout" if max_hold_exceeded else "closed")
        return {"status": status, "pnl": pnl, "pnl_pct": pnl_pct, "exit_price": exit_price}

    def _place_live_order(
        self, direction: str, size: float, price: float, sl: float, tp: float
    ) -> None:
        side = "buy" if direction == "long" else "sell"
        try:
            self._exchange.create_order(
                symbol=self._cfg.symbol,
                type="limit",
                side=side,
                amount=size,
                price=price * (0.9995 if direction == "long" else 1.0005),
                params={
                    "stopLoss": {"type": "STOP_MARKET", "price": sl},
                    "takeProfit": {"type": "TAKE_PROFIT_MARKET", "price": tp},
                    "timeInForce": "GTC",
                },
            )
        except Exception as e:
            logger.error(f"Live order failed: {e}")
            raise

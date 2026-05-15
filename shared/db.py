from __future__ import annotations
from datetime import datetime, timezone
from shared.utils import utcnow
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, Boolean, Text
from sqlalchemy.orm import DeclarativeBase, Session
from sqlalchemy import select


class Base(DeclarativeBase):
    pass


class FundingRate(Base):
    __tablename__ = "funding_rates"
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False)
    funding_rate = Column(Float, nullable=False)
    funding_time = Column(DateTime, nullable=False, unique=True)
    mark_price = Column(Float)
    created_at = Column(DateTime, default=utcnow)


class Signal(Base):
    __tablename__ = "signals"
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False)
    direction = Column(String(10), nullable=False)  # long / short
    funding_rate = Column(Float, nullable=False)
    mark_price = Column(Float, nullable=False)
    confidence = Column(Float)
    created_at = Column(DateTime, default=utcnow)
    acted_on = Column(Boolean, default=False)


class Trade(Base):
    __tablename__ = "trades"
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False)
    direction = Column(String(10), nullable=False)
    entry_price = Column(Float, nullable=False)
    exit_price = Column(Float)
    size = Column(Float, nullable=False)
    stop_loss = Column(Float, nullable=False)
    take_profit = Column(Float, nullable=False)
    pnl = Column(Float)
    pnl_pct = Column(Float)
    status = Column(String(20), default="open")  # open / closed / stopped
    paper = Column(Boolean, default=True)
    signal_id = Column(Integer)
    opened_at = Column(DateTime, default=utcnow)
    closed_at = Column(DateTime)
    notes = Column(Text)


class MomentumTrade(Base):
    __tablename__ = "momentum_trades"
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False)
    direction = Column(String(10), nullable=False)
    branch = Column(String(10))          # "eth" or "btc"
    regime = Column(String(20))          # trending/transition/ranging
    size_multiplier = Column(Float)
    entry_price = Column(Float)
    exit_price = Column(Float)
    sl_price = Column(Float)
    tp_price = Column(Float)
    pnl_pct = Column(Float)
    outcome = Column(String(10))         # tp / sl / timeout / open
    paper = Column(Boolean, default=True)
    opened_at = Column(DateTime, default=utcnow)
    closed_at = Column(DateTime)
    reason = Column(Text)


class GateDecision(Base):
    __tablename__ = "gate_decisions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    signal_id = Column(Integer, nullable=True)
    symbol = Column(String(20), nullable=False)
    direction = Column(String(10))
    composite_score = Column(Float)
    liq_score = Column(Float)       # liquidation screen component
    div_score = Column(Float)       # funding divergence component
    onchain_score = Column(Float)   # on-chain context (None = disabled)
    gate_decision = Column(String(15))  # approve / block / blocked_macro
    threshold = Column(Float)
    macro_blocked = Column(Boolean, default=False)
    shadow_mode = Column(Boolean, default=True)
    binance_funding = Column(Float)
    bybit_funding = Column(Float)
    funding_spread = Column(Float)
    created_at = Column(DateTime, default=utcnow)
    # filled by analysis script after trade closes
    outcome_pnl = Column(Float)
    outcome_win = Column(Boolean)


class SpreadEvent(Base):
    __tablename__ = "spread_events"
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False)
    buy_exchange = Column(String(20), nullable=False)
    sell_exchange = Column(String(20), nullable=False)
    buy_price = Column(Float, nullable=False)     # ask цена на бирже покупки
    sell_price = Column(Float, nullable=False)    # bid цена на бирже продажи
    spread_pct = Column(Float, nullable=False)    # gross %
    net_pct = Column(Float, nullable=False)       # после комиссий %
    # полные book tops — нужны для симуляции исполнения в Phase 1
    binance_bid = Column(Float)
    binance_ask = Column(Float)
    bybit_bid = Column(Float)
    bybit_ask = Column(Float)
    book_age_ms = Column(Integer)                 # макс. возраст данных в мс (staleness)
    ts = Column(DateTime, nullable=False)


class ArbPaperTrade(Base):
    __tablename__ = "arb_paper_trades"
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False)
    buy_exchange = Column(String(20), nullable=False)
    sell_exchange = Column(String(20), nullable=False)
    buy_price = Column(Float, nullable=False)
    sell_price = Column(Float, nullable=False)
    size_usdt = Column(Float, nullable=False)
    gross_pct = Column(Float, nullable=False)
    net_pct = Column(Float, nullable=False)
    pnl_usdt = Column(Float, nullable=False)
    book_age_ms = Column(Integer)
    ts = Column(DateTime, nullable=False)


class ArbRealTrade(Base):
    __tablename__ = "arb_real_trades"
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False)
    buy_exchange = Column(String(20), nullable=False)
    sell_exchange = Column(String(20), nullable=False)
    # ордера
    buy_order_id = Column(String(50))
    sell_order_id = Column(String(50))
    # цены и объём
    target_size_usdt = Column(Float, nullable=False)
    buy_price_target = Column(Float)    # цена в момент сигнала
    sell_price_target = Column(Float)
    buy_price_filled = Column(Float)    # реальная цена исполнения
    sell_price_filled = Column(Float)
    buy_qty_filled = Column(Float)      # реально куплено (в монетах)
    sell_qty_filled = Column(Float)
    # результат
    gross_pct = Column(Float)           # спред в момент сигнала
    net_pct = Column(Float)
    pnl_usdt = Column(Float)            # реальный PnL после исполнения
    slippage_pct = Column(Float)        # gross_target - gross_filled
    # статус
    status = Column(String(20), default="open")  # open / filled / failed / partial
    error = Column(Text)
    ts_signal = Column(DateTime, nullable=False)
    ts_filled = Column(DateTime)


def get_engine(database_url: str):
    return create_engine(database_url, echo=False)


def init_db(database_url: str):
    engine = get_engine(database_url)
    Base.metadata.create_all(engine)
    return engine

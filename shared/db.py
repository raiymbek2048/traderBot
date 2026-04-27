from __future__ import annotations
from datetime import datetime
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
    created_at = Column(DateTime, default=datetime.utcnow)


class Signal(Base):
    __tablename__ = "signals"
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False)
    direction = Column(String(10), nullable=False)  # long / short
    funding_rate = Column(Float, nullable=False)
    mark_price = Column(Float, nullable=False)
    confidence = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)
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
    opened_at = Column(DateTime, default=datetime.utcnow)
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
    opened_at = Column(DateTime, default=datetime.utcnow)
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
    created_at = Column(DateTime, default=datetime.utcnow)
    # filled by analysis script after trade closes
    outcome_pnl = Column(Float)
    outcome_win = Column(Boolean)


def get_engine(database_url: str):
    return create_engine(database_url, echo=False)


def init_db(database_url: str):
    engine = get_engine(database_url)
    Base.metadata.create_all(engine)
    return engine

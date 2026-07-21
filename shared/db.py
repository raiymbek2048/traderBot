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
    gross_pct = Column(Float, nullable=False)   # реалистичный VWAP-спред по глубине
    net_pct = Column(Float, nullable=False)     # gross_pct - комиссии(×2) - реализм
    pnl_usdt = Column(Float, nullable=False)     # SIZE * net (может быть отрицательным)
    book_age_ms = Column(Integer)
    # --- realism v2 ---
    naive_gross_pct = Column(Float)   # старый спред по топу стакана (для сравнения)
    naive_net_pct = Column(Float)     # старый net (1 комиссия) — как было раньше
    slippage_pct = Column(Float)      # наивный - реальный спред (стоимость глубины)
    fillable = Column(Boolean)        # хватило ли ликвидности на size_usdt
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


class FundingPosition(Base):
    __tablename__ = "funding_positions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False)
    spot_exchange = Column(String(20), default="bybit")
    perp_exchange = Column(String(20), default="bybit")
    spot_order_id = Column(String(50))
    perp_order_id = Column(String(50))
    size_usdt = Column(Float, nullable=False)
    spot_qty = Column(Float)              # сколько монет куплено в споте
    perp_qty = Column(Float)              # сколько монет зашортено в перпе
    spot_entry_price = Column(Float)
    perp_entry_price = Column(Float)
    spot_exit_price = Column(Float)
    perp_exit_price = Column(Float)
    funding_rate_open = Column(Float)     # фандинг в момент открытия
    funding_rate_close = Column(Float)    # фандинг в момент закрытия
    funding_collected_usdt = Column(Float, default=0.0)  # накоплено фандинга (settled-ставки)
    fees_usdt = Column(Float)            # комиссии полного цикла (per-symbol ставки)
    basis_pnl_usdt = Column(Float)       # PnL ног: spot leg + perp leg (bid/ask)
    pnl_usdt = Column(Float)             # итоговый PnL = basis + funding - fees
    status = Column(String(20), default="open")  # open / closed / failed
    error = Column(Text)
    paper = Column(Boolean, default=True)
    opened_at = Column(DateTime, default=utcnow)
    closed_at = Column(DateTime)


def get_engine(database_url: str):
    return create_engine(database_url, echo=False)


class FundingSpreadSnap(Base):
    """Снимок спреда фандинга Bybit vs Binance (нормализовано в %/день)."""
    __tablename__ = "funding_spread_snaps"
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(24), nullable=False)
    bybit_fr = Column(Float)         # raw за интервал
    binance_fr = Column(Float)
    bybit_daily_pct = Column(Float)  # нормализовано %/день
    binance_daily_pct = Column(Float)
    spread_daily_pct = Column(Float) # bybit - binance, %/день
    bybit_price = Column(Float)      # lastPrice перпа
    binance_price = Column(Float)
    price_gap_pct = Column(Float)    # (bybit-binance)/binance*100 — по last/mark (справочно)
    exec_edge_pct = Column(Float)    # исполнимый вход по bid/ask в направлении сделки (+= помогает)
    ts = Column(DateTime, nullable=False)


class SpreadPosition(Base):
    """Перп-перп funding-spread позиция (Bybit vs Binance), обе ноги в USDT."""
    __tablename__ = "spread_positions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(24), nullable=False)
    direction = Column(String(16), nullable=False)  # short_bybit | short_binance
    size_usdt = Column(Float, nullable=False)       # notional одной ноги
    qty = Column(Float)                              # одинаковое кол-во монет обеих ног
    bybit_entry_price = Column(Float)                # исполнимые bid/ask на входе
    binance_entry_price = Column(Float)
    bybit_exit_price = Column(Float)
    binance_exit_price = Column(Float)
    entry_spread_daily_pct = Column(Float)           # спред фандинга на входе, %/день
    entry_exec_edge_pct = Column(Float)              # исполнимый вход (+= помогает)
    funding_collected_usdt = Column(Float, default=0.0)  # Σ(шорт-нога − лонг-нога), settled
    basis_pnl_usdt = Column(Float)
    fees_usdt = Column(Float)
    pnl_usdt = Column(Float)
    status = Column(String(16), default="open")      # open / closed
    paper = Column(Boolean, default=True)
    variant = Column(String(16), default="strict")   # A/B/C-тест правил входа/выхода
    opened_at = Column(DateTime, default=utcnow)
    closed_at = Column(DateTime)


class LiqMomentumTrade(Base):
    """Follow-momentum сделка после каскада ликвидаций (директивная, не delta-neutral).

    Sell-каскад (лонги ликвидированы, цена вниз) → мы SHORT (следуем импульсу).
    Buy-каскад (шорты ликвидированы, цена вверх) → мы LONG.
    Держим 15 минут, выход по таймеру.
    """
    __tablename__ = "liq_momentum_trades"
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(24), nullable=False)
    cascade_side = Column(String(8), nullable=False)     # Sell / Buy (сторона ликвидаций)
    cascade_value_usdt = Column(Float)
    cascade_count = Column(Integer)
    cascade_end_ts = Column(DateTime, nullable=False)    # для дедупа
    direction = Column(String(8), nullable=False)        # short / long
    size_usdt = Column(Float, nullable=False)
    qty = Column(Float)
    entry_price = Column(Float)
    exit_price = Column(Float)
    entry_ts = Column(DateTime)
    exit_ts = Column(DateTime)
    raw_pnl_pct = Column(Float)
    fees_usdt = Column(Float)
    pnl_usdt = Column(Float)
    status = Column(String(16), default="open")
    paper = Column(Boolean, default=True)


class LiqEvent(Base):
    """Ликвидация с Bybit WS (allLiquidation) — для анализа каскадов/отскоков."""
    __tablename__ = "liq_events"
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(24), nullable=False)
    side = Column(String(8))         # Buy = ликвидирован шорт, Sell = ликвидирован лонг
    qty = Column(Float)
    price = Column(Float)
    value_usdt = Column(Float)       # qty * price
    ts = Column(DateTime, nullable=False)


def _migrate_arb_paper(engine) -> None:
    """Добавляет новые колонки в существующие таблицы (SQLite ALTER)."""
    from sqlalchemy import text
    migrations = {
        "arb_paper_trades": {
            "naive_gross_pct": "FLOAT",
            "naive_net_pct": "FLOAT",
            "slippage_pct": "FLOAT",
            "fillable": "BOOLEAN",
        },
        "funding_spread_snaps": {
            "bybit_price": "FLOAT",
            "binance_price": "FLOAT",
            "price_gap_pct": "FLOAT",
            "exec_edge_pct": "FLOAT",
        },
        "funding_positions": {
            "fees_usdt": "FLOAT",
            "basis_pnl_usdt": "FLOAT",
        },
        "spread_positions": {
            "variant": "TEXT",
        },
    }
    with engine.begin() as conn:
        for table, cols in migrations.items():
            existing = {row[1] for row in conn.execute(text(f"PRAGMA table_info({table})"))}
            if not existing:
                continue  # таблицы ещё нет — create_all создаст с полной схемой
            for col, typ in cols.items():
                if col not in existing:
                    conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {col} {typ}"))


def init_db(database_url: str):
    engine = get_engine(database_url)
    Base.metadata.create_all(engine)
    try:
        _migrate_arb_paper(engine)
    except Exception:
        pass  # таблицы ещё нет — create_all уже создал с новыми колонками
    return engine

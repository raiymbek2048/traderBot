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


class HoldPosition(Base):
    """Перп-перп с МИНИМАЛЬНЫМ ХОЛДОМ до сеттлмента (тест 27.07.2026).

    Отдельная таблица от spread_positions: тот журнал испорчен A/B-размножением
    (11 вариантов на одном потоке → одна возможность в 2.5 записи). Здесь строго
    один вариант, одна позиция на символ → каждая строка = уникальное наблюдение.

    PnL считается СРАЗУ в двух режимах комиссий — бесплатная чувствительность
    без повторного прогона.
    """
    __tablename__ = "hold_positions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(24), nullable=False)
    direction = Column(String(16), nullable=False)     # short_bybit | short_binance
    size_usdt = Column(Float, nullable=False)
    qty = Column(Float)
    bybit_entry_price = Column(Float)
    binance_entry_price = Column(Float)
    bybit_exit_price = Column(Float)
    binance_exit_price = Column(Float)
    entry_spread_daily_pct = Column(Float)
    entry_book_width_pct = Column(Float)
    hours_to_settle_at_entry = Column(Float)   # проверка правила входа
    settlements_survived = Column(Integer, default=0)  # ГЛАВНАЯ метрика теста
    hold_hours = Column(Float)
    funding_collected_usdt = Column(Float, default=0.0)
    basis_pnl_usdt = Column(Float)
    fees_taker_usdt = Column(Float)
    fees_maker_usdt = Column(Float)
    pnl_taker_usdt = Column(Float)   # реальность
    pnl_maker_usdt = Column(Float)   # апсайд при мейкер-исполнении
    exit_reason = Column(String(64))
    status = Column(String(16), default="open")
    paper = Column(Boolean, default=True)
    opened_at = Column(DateTime, default=utcnow)
    closed_at = Column(DateTime)


class MakerFillProbe(Base):
    """Замер исполнимости ЛИМИТНЫХ ордеров по реальной ленте сделок.

    Зачем: единственный доказанный лом — край равен комиссии (liq-momentum
    0% → +$1.24, перп-перп maker −$20.97 vs taker −$31.19). Но мейкер полезен
    только если лимитник реально наливается И не наливается преимущественно
    в плохих сценариях (adverse selection).

    Метод: ставим ВИРТУАЛЬНЫЙ лимитник на текущем bid/ask и слушаем ленту.
    Консервативное правило залива: цена должна пройти СТРОГО через нашу
    (для buy@bid — сделка ниже bid), т.к. в очереди на своём уровне мы последние.

    Ключевые метрики:
      filled / secs_to_fill        — наливается ли и как быстро
      mid_move_after_fill_bps      — adverse selection: куда ушла цена ПОСЛЕ залива
      (для delta-neutral важен joint-fill: обе ноги в одном окне)
    """
    __tablename__ = "maker_fill_probes"
    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange = Column(String(12), nullable=False)      # bybit | binance
    symbol = Column(String(24), nullable=False)
    side = Column(String(8), nullable=False)           # buy@bid | sell@ask
    probe_group = Column(String(40))                   # связка двух ног одного замера
    limit_price = Column(Float, nullable=False)
    mid_at_place = Column(Float)
    book_width_pct = Column(Float)
    turnover24h = Column(Float)
    filled = Column(Boolean, default=False)
    secs_to_fill = Column(Float)                       # None если не налился
    window_secs = Column(Float)                        # сколько ждали
    mid_at_fill = Column(Float)
    mid_after_30s = Column(Float)                      # для adverse selection
    adverse_bps = Column(Float)                        # >0 = цена ушла против нас
    trades_seen = Column(Integer, default=0)
    placed_at = Column(DateTime, default=utcnow)
    resolved_at = Column(DateTime)

    # ── v2 (28.07): то, что v1 не измеряла ───────────────────────────────
    # v1 провалилась по критерию B (+2.99 bps), НО метрика была неверной:
    # усредняла ноги независимо, хотя в delta-neutral паре они гасят adverse
    # друг друга. Чистый adverse на пару оказался −2.89 bps (в нашу пользу).
    # Метрика сменена ПОСЛЕ результата → это новая гипотеза, а не спасение
    # старой. v2 собирается заново, с критериями, заданными до сбора.
    probe_version = Column(Integer, default=1)
    # цена разруливания непарного залива (главное, что осталось неизвестным:
    # 17% входов оставляли ГОЛУЮ ногу, стоимость этого не измерялась вообще)
    partial_leg = Column(Boolean, default=False)   # эта нога осталась одна
    chase_cost_bps = Column(Float)    # догнать недостающую ногу тейкером
    unwind_cost_bps = Column(Float)   # развернуть залившуюся ногу тейкером


class VenueFundingSnap(Base):
    """Снимок ставки фандинга и цены по площадке (RISEx / Bybit).

    Нужен для замера ДИФФЕРЕНЦИАЛА в реальном времени. Историческая проверка
    (scripts/risex_funding_diff.py) дала: дифференциал положителен почти на всех
    символах (+2…+10%/год), но устойчивость знака 52-87%. Формально критерии не
    прошли (1 символ из 3 требуемых — HYPE). Живой замер проверяет, держится ли
    79% устойчивости HYPE на свежих данных.
    """
    __tablename__ = "venue_funding_snaps"
    id = Column(Integer, primary_key=True, autoincrement=True)
    venue = Column(String(12), nullable=False)      # risex | bybit
    symbol = Column(String(24), nullable=False)     # базовый тикер: HYPE, XRP...
    funding_rate = Column(Float)                    # за интервал площадки
    interval_h = Column(Float)                      # длина интервала, часов
    rate_daily_pct = Column(Float)                  # нормализовано в %/день
    mark_price = Column(Float)
    open_interest_usd = Column(Float)
    next_funding_ms = Column(Float)
    ts = Column(DateTime, nullable=False)


class VenueFundingSettled(Base):
    """ФАКТИЧЕСКИ начисленная ставка (не предсказанная).

    Урок №4 и гипотеза 13: предсказанное ≠ полученное. NVDL показывал 0.4255%,
    начислялось 0.0000%. Поэтому accrual считаем только по этой таблице.
    Уникальность (venue, symbol, settle_ms) держит идемпотентность.
    """
    __tablename__ = "venue_funding_settled"
    id = Column(Integer, primary_key=True, autoincrement=True)
    venue = Column(String(12), nullable=False)
    symbol = Column(String(24), nullable=False)
    funding_rate = Column(Float, nullable=False)
    settle_ms = Column(Float, nullable=False)
    index_price = Column(Float)
    recorded_at = Column(DateTime, default=utcnow)


class RisexPaperPosition(Base):
    """PAPER delta-neutral позиция RISEx ↔ Bybit — носитель для поинтов Ignite.

    Конструкция: одна нога на RISEx, противоположная на Bybit, один актив.
    Доход = дифференциал фандинга. Поинты Ignite начисляются за open interest ×
    время, поэтому churn не нужен — позиция просто держится.

    Комиссии моделируются по ФАКТИЧЕСКИМ ставкам (проверены через API 3 авг):
      RISEx Tier 1: taker 3.0 bps / maker 1.0 bps (газ спонсируется)
      Bybit:        taker 10.0 bps / maker 3.6 bps
    """
    __tablename__ = "risex_paper_positions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(24), nullable=False)
    direction = Column(String(24), nullable=False)   # short_risex | long_risex
    size_usdt = Column(Float, nullable=False)        # нотионал одной ноги
    risex_entry_price = Column(Float)
    bybit_entry_price = Column(Float)
    risex_exit_price = Column(Float)
    bybit_exit_price = Column(Float)
    entry_diff_daily_pct = Column(Float)             # дифференциал на входе
    funding_risex_usdt = Column(Float, default=0.0)  # начислено по ноге RISEx
    funding_bybit_usdt = Column(Float, default=0.0)  # начислено по ноге Bybit
    funding_net_usdt = Column(Float, default=0.0)    # сумма по обеим ногам
    basis_pnl_usdt = Column(Float)                   # расхождение цен площадок
    fees_taker_usdt = Column(Float)
    fees_maker_usdt = Column(Float)
    pnl_taker_usdt = Column(Float)
    pnl_maker_usdt = Column(Float)
    days_held = Column(Float)
    status = Column(String(16), default="open")
    paper = Column(Boolean, default=True)
    opened_at = Column(DateTime, default=utcnow)
    closed_at = Column(DateTime)


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
        "maker_fill_probes": {
            "probe_version": "INTEGER",
            "partial_leg": "BOOLEAN",
            "chase_cost_bps": "FLOAT",
            "unwind_cost_bps": "FLOAT",
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

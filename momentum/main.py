"""Momentum daemon — 5m limit-entry, paper mode only until validated.

Loop: каждые 5 минут (синхронизировано с закрытием 5m свечи + 5s задержка).
Paper mode: симулирует лимитные ордера, отслеживает SL/TP.

Активация live: только после 2 недель paper с WR >= 53.5%.
"""
from __future__ import annotations
import asyncio
import sys
import os
import time
from datetime import datetime, timezone, timedelta

from loguru import logger
from sqlalchemy.orm import Session

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from shared.config import load_config
from shared.db import init_db, MomentumTrade
from shared.notifier import Notifier
from analyst.fetcher import BybitFetcher
from momentum.signal import generate_momentum_signal
from momentum.ws_watcher import run_price_watcher
from shared.tg_commands import run_command_bot

# ── open position state ───────────────────────────────────────────────────────

_open_trade: MomentumTrade | None = None
_open_trade_id: int | None = None
_hold_bars: int = 0
_last_ws_price: float = 0.0


# ── helpers ───────────────────────────────────────────────────────────────────

def _seconds_to_next_5m() -> float:
    """Seconds until next 5m candle close + 5s buffer."""
    now = time.time()
    interval = 300
    next_close = (int(now / interval) + 1) * interval + 5
    return max(1.0, next_close - now)


def _do_close_position(engine, exit_price: float, outcome: str, notifier: Notifier) -> None:
    """Write close to DB and notify. Caller must hold no lock."""
    global _open_trade, _open_trade_id, _hold_bars

    if _open_trade is None:
        return

    direction = _open_trade.direction
    entry = _open_trade.entry_price
    raw_pnl = (exit_price - entry) / entry * (1 if direction == "long" else -1)
    net_pnl = raw_pnl - 0.0002 - 0.00055  # maker entry + taker exit
    emoji = "✅" if net_pnl > 0 else "❌"

    with Session(engine) as session:
        trade = session.get(MomentumTrade, _open_trade_id)
        if trade:
            trade.exit_price = exit_price
            trade.pnl_pct = net_pnl
            trade.outcome = outcome
            trade.closed_at = datetime.now(timezone.utc)
            session.commit()

    logger.info(
        f"[MOMENTUM] CLOSE {outcome.upper()} {direction.upper()} "
        f"entry={entry:.2f} exit={exit_price:.2f} pnl={net_pnl:+.3%}"
    )
    notifier.send(
        f"{emoji} Momentum CLOSE [{outcome.upper()}]\n"
        f"{direction.upper()} {_open_trade.symbol}\n"
        f"Entry: {entry:.2f} → Exit: {exit_price:.2f}\n"
        f"PnL: {net_pnl:+.3%} (paper)"
    )

    _open_trade = None
    _open_trade_id = None
    _hold_bars = 0


def _restore_open_position(engine) -> None:
    """On startup, restore any open paper position from DB."""
    global _open_trade, _open_trade_id, _hold_bars
    from sqlalchemy import select
    with Session(engine) as session:
        trade = session.execute(
            select(MomentumTrade)
            .where(MomentumTrade.outcome == "open", MomentumTrade.paper == True)
            .order_by(MomentumTrade.opened_at.desc())
        ).scalars().first()

        if trade is None:
            return

        opened_at = trade.opened_at
        if opened_at.tzinfo is None:
            opened_at = opened_at.replace(tzinfo=timezone.utc)
        elapsed = (datetime.now(timezone.utc) - opened_at).total_seconds()
        bars_elapsed = max(0, int(elapsed / 300))

        _open_trade_id = trade.id
        _open_trade = trade
        _hold_bars = bars_elapsed

    logger.info(
        f"[MOMENTUM] Restored open {_open_trade.direction.upper()} "
        f"entry={_open_trade.entry_price:.2f} sl={_open_trade.sl_price:.2f} "
        f"tp={_open_trade.tp_price:.2f} bars_elapsed={bars_elapsed}"
    )


def _check_open_position(engine, cfg, notifier: Notifier) -> bool:
    """Check timeout on open paper position. Returns True if still open.

    SL/TP are handled in real-time by the WebSocket watcher.
    """
    global _hold_bars

    if _open_trade is None:
        return False

    _hold_bars += 1
    if _hold_bars >= cfg.momentum_max_hold_bars:
        exit_price = _last_ws_price if _last_ws_price > 0 else _open_trade.entry_price
        _do_close_position(engine, exit_price, "timeout", notifier)
        return False

    return True


def _open_position(engine, signal, cfg, notifier: Notifier) -> None:
    global _open_trade, _open_trade_id, _hold_bars

    if _open_trade is not None:
        return  # already in position

    # Simulate limit fill: entry = mark ± 0.05% offset
    offset = 0.0005
    entry = signal.mark_price * (1 - offset) if signal.direction == "long" \
        else signal.mark_price * (1 + offset)

    with Session(engine) as session:
        trade = MomentumTrade(
            symbol=cfg.symbol,
            direction=signal.direction,
            branch=signal.branch,
            regime=signal.regime,
            size_multiplier=signal.size_multiplier,
            entry_price=entry,
            sl_price=signal.sl_price,
            tp_price=signal.tp_price,
            outcome="open",
            paper=True,
            opened_at=datetime.now(timezone.utc),
            reason=signal.reason,
        )
        session.add(trade)
        session.commit()
        _open_trade_id = trade.id
        _open_trade = trade

    _hold_bars = 0

    logger.info(
        f"[MOMENTUM] OPEN {signal.direction.upper()} {signal.branch.upper()}-branch "
        f"regime={signal.regime}(ADX={signal.adx:.1f}) x{signal.size_multiplier} "
        f"entry={entry:.2f} sl={signal.sl_price:.2f} tp={signal.tp_price:.2f}"
    )
    notifier.send(
        f"⚡ Momentum OPEN [{signal.branch.upper()}]\n"
        f"{signal.direction.upper()} {cfg.symbol}\n"
        f"Entry: {entry:.2f} | SL: {signal.sl_price:.2f} | TP: {signal.tp_price:.2f}\n"
        f"Regime: {signal.regime} (ADX={signal.adx:.1f}) x{signal.size_multiplier}\n"
        f"(paper mode)"
    )


# ── caches ────────────────────────────────────────────────────────────────────

_ohlcv_1h: list[dict] = []


async def _main_loop(engine, fetcher: BybitFetcher, cfg, notifier: Notifier) -> None:
    global _ohlcv_1h

    while True:
        wait = _seconds_to_next_5m()
        logger.debug(f"[MOMENTUM] Next check in {wait:.0f}s")
        await asyncio.sleep(wait)

        try:
            # Check timeout on open position (SL/TP handled by WebSocket)
            still_open = _check_open_position(engine, cfg, notifier)
            if still_open:
                continue

            # Fetch data — 0.5s between calls to avoid anonymous rate limit
            eth_5m  = fetcher.get_ohlcv(cfg.symbol, "5m", 60)
            await asyncio.sleep(0.5)
            btc_5m  = fetcher.get_ohlcv("BTCUSDT",  "5m", 60)
            await asyncio.sleep(0.5)
            eth_oi  = fetcher.get_oi_history(cfg.symbol, "1h", 50)
            await asyncio.sleep(0.5)
            _ohlcv_1h = fetcher.get_ohlcv(cfg.symbol, "1h", 50) or _ohlcv_1h
            await asyncio.sleep(0.5)

            # Existing funding position direction (for mutex)
            fund_data = fetcher.get_funding_rate(cfg.symbol)
            funding_rate = fund_data["current_rate"]
            funding_dir = None
            if abs(funding_rate) >= cfg.funding_threshold:
                funding_dir = "short" if funding_rate > 0 else "long"

            signal = generate_momentum_signal(
                eth_5m=eth_5m,
                btc_5m=btc_5m,
                eth_oi_5m=eth_oi,
                ohlcv_1h=_ohlcv_1h,
                existing_funding_direction=funding_dir,
                sl_pct=cfg.momentum_sl_pct,
                tp_pct=cfg.momentum_tp_pct,
                ema_fast=cfg.momentum_ema_fast,
                ema_slow=cfg.momentum_ema_slow,
                vwap_threshold=cfg.momentum_vwap_threshold,
                btc_threshold=cfg.momentum_btc_threshold,
            )

            if signal:
                _open_position(engine, signal, cfg, notifier)
            else:
                logger.debug(f"[MOMENTUM] No signal | price={eth_5m[-1]['close']:.2f}")

        except Exception as e:
            logger.error(f"[MOMENTUM] Loop error: {e}")


# ── stats helper ──────────────────────────────────────────────────────────────

def _build_stats_message(engine) -> str:
    from sqlalchemy import select
    with Session(engine) as session:
        trades = session.execute(
            select(MomentumTrade).where(
                MomentumTrade.paper == True,
                MomentumTrade.outcome != "open",
            )
        ).scalars().all()

    if not trades:
        return "📊 Momentum: no closed trades yet."

    pnls = [t.pnl_pct for t in trades if t.pnl_pct is not None]
    wins = sum(1 for p in pnls if p > 0)
    wr = wins / len(pnls) if pnls else 0
    total = sum(pnls)
    status = "✅ PASSING" if wr >= 0.535 else "❌ NOT YET"
    pos = f"📍 Open: {_open_trade.direction.upper()} entry={_open_trade.entry_price:.2f}" \
        if _open_trade else "📍 No open position"
    return (
        f"📊 Momentum Daily Report\n"
        f"Trades: {len(pnls)} | WR: {wr:.1%} | Total PnL: {total:+.2%}\n"
        f"Target WR: 53.5% → {status}\n"
        f"{pos}"
    )


def print_paper_stats(engine) -> None:
    print(_build_stats_message(engine))


async def _daily_report_loop(engine, notifier: Notifier) -> None:
    """Send stats to Telegram once a day at 00:05 UTC."""
    while True:
        now = datetime.now(timezone.utc)
        next_report = now.replace(hour=0, minute=5, second=0, microsecond=0)
        if next_report <= now:
            next_report += timedelta(days=1)
        await asyncio.sleep((next_report - now).total_seconds())
        try:
            notifier.send(_build_stats_message(engine))
        except Exception as e:
            logger.error(f"[MOMENTUM] Daily report error: {e}")


# ── entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    cfg = load_config()
    logger.remove()
    logger.add(sys.stderr, level=cfg.log_level)
    logger.add("logs/momentum.log", rotation="10 MB", retention="30 days")

    if not cfg.momentum_enabled:
        logger.info("MOMENTUM disabled via config")
        return

    engine = init_db(cfg.database_url)
    fetcher = BybitFetcher(cfg.bybit_api_key, cfg.bybit_api_secret, cfg.bybit_testnet)
    notifier = Notifier(cfg.telegram_token, cfg.telegram_chat_id)

    _restore_open_position(engine)

    logger.info("MOMENTUM started (paper mode, 5m, breakeven WR=53.5%)")
    notifier.send(
        "⚡ Momentum started (PAPER MODE)\n"
        f"VWAP reversion 5m | ADX<35 filter\n"
        f"SL={cfg.momentum_sl_pct:.2%} | TP={cfg.momentum_tp_pct:.2%} | Breakeven WR=53.5%\n"
        "Live after 2 weeks + WR>=53.5%"
    )

    async def _on_ws_price(price: float) -> None:
        """Called on every WebSocket tick — check SL/TP in real-time."""
        global _last_ws_price
        _last_ws_price = price
        if _open_trade is None:
            return
        direction = _open_trade.direction
        sl = _open_trade.sl_price
        tp = _open_trade.tp_price
        hit_tp = (direction == "long" and price >= tp) or (direction == "short" and price <= tp)
        hit_sl = (direction == "long" and price <= sl) or (direction == "short" and price >= sl)
        if hit_tp:
            _do_close_position(engine, tp, "tp", notifier)
        elif hit_sl:
            _do_close_position(engine, sl, "sl", notifier)

    def _get_status() -> str:
        if _open_trade is None:
            price = f"{_last_ws_price:.2f}" if _last_ws_price else "?"
            return f"📍 No open position\nLast price: {price}"
        pct_from_entry = (_last_ws_price - _open_trade.entry_price) / _open_trade.entry_price
        if _open_trade.direction == "short":
            pct_from_entry = -pct_from_entry
        return (
            f"📍 OPEN {_open_trade.direction.upper()} {cfg.symbol}\n"
            f"Entry: {_open_trade.entry_price:.2f}\n"
            f"SL: {_open_trade.sl_price:.2f} | TP: {_open_trade.tp_price:.2f}\n"
            f"Now: {_last_ws_price:.2f} ({pct_from_entry:+.2%})\n"
            f"Bars held: {_hold_bars}/{cfg.momentum_max_hold_bars}"
        )

    async def _run() -> None:
        await asyncio.gather(
            _main_loop(engine, fetcher, cfg, notifier),
            run_price_watcher(cfg.symbol, _on_ws_price),
            _daily_report_loop(engine, notifier),
            run_command_bot(
                cfg.telegram_token,
                int(cfg.telegram_chat_id),
                _get_status,
                lambda: _build_stats_message(engine),
            ),
        )

    asyncio.run(_run())


if __name__ == "__main__":
    main()

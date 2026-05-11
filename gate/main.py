"""Alpha Gate v2 — Shadow Mode Daemon.

Shadow mode = observes analyst signals, computes composite score,
records what the gate WOULD have decided. Does NOT block real entries.

Loop:
  Every 60s  → check RSS macro feeds
  Every 2min → refresh Binance funding (for divergence stability tracking)
  Every 5min → evaluate any new unprocessed signals in DB
"""
from __future__ import annotations
import asyncio
import sys
import os
from datetime import datetime, timezone, timezone, timedelta

from loguru import logger
from sqlalchemy.orm import Session
from sqlalchemy import select

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from shared.config import load_config
from shared.db import init_db, Signal as SignalRow, GateDecision
from shared.notifier import Notifier
from analyst.fetcher import BybitFetcher

from gate.sources.funding_divergence import (
    fetch_binance_funding,
    compute_divergence_score,
)
from gate.sources.liquidation_screen import compute_liquidation_score
from gate.sources.macro_blocker import check_feeds, is_blocked
from gate.scorer import compute_gate_score, GATE_THRESHOLD_INITIAL

# ── state ────────────────────────────────────────────────────────────────────

_binance_funding: float | None = None
_price_15m_ago: float | None = None
_last_price_update: datetime | None = None

# ── helpers ──────────────────────────────────────────────────────────────────

async def refresh_binance_funding(symbol: str) -> None:
    global _binance_funding
    val = await fetch_binance_funding(symbol)
    if val is not None:
        _binance_funding = val
        logger.debug(f"Binance funding refreshed: {val:.5%}")


def _update_price_history(price: float) -> None:
    global _price_15m_ago, _last_price_update
    now = datetime.now(timezone.utc)
    if _last_price_update is None or (now - _last_price_update).total_seconds() >= 900:
        _price_15m_ago = price
        _last_price_update = now


async def evaluate_signals(engine, fetcher: BybitFetcher, cfg, notifier: Notifier) -> None:
    """Find signals created in last 30 min without a gate decision → evaluate."""
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=30)

    with Session(engine) as session:
        # signals not yet evaluated by gate
        evaluated_ids = session.execute(
            select(GateDecision.signal_id).where(GateDecision.signal_id.isnot(None))
        ).scalars().all()

        new_signals = session.execute(
            select(SignalRow).where(
                SignalRow.created_at >= cutoff,
                SignalRow.id.not_in(evaluated_ids) if evaluated_ids else SignalRow.id.isnot(None),
            )
        ).scalars().all()

    if not new_signals:
        return

    # Fetch live data once for this batch
    try:
        fund_data = fetcher.get_funding_rate(cfg.symbol)
        bybit_rate = fund_data["current_rate"]
        mark_price = fund_data["mark_price"]
        liq_usd = fetcher.get_liquidation_volume(cfg.symbol)
    except Exception as e:
        logger.warning(f"Gate data fetch failed: {e}")
        return

    _update_price_history(mark_price)

    macro_blocked, macro_reason = is_blocked()
    div_score = compute_divergence_score(bybit_rate, _binance_funding)
    liq_score = compute_liquidation_score(liq_usd, mark_price, _price_15m_ago)
    gate = compute_gate_score(
        liq_score=liq_score,
        div_score=div_score,
        onchain_score=None,  # CryptoQuant disabled — no API key
        macro_blocked=macro_blocked,
        threshold=GATE_THRESHOLD_INITIAL,
    )

    with Session(engine) as session:
        for sig in new_signals:
            decision = GateDecision(
                signal_id=sig.id,
                symbol=sig.symbol,
                direction=sig.direction,
                composite_score=gate.composite,
                liq_score=gate.liq,
                div_score=gate.div,
                onchain_score=gate.onchain,
                gate_decision=gate.decision,
                threshold=gate.threshold,
                macro_blocked=gate.macro_blocked,
                shadow_mode=True,
                binance_funding=_binance_funding,
                bybit_funding=bybit_rate,
                funding_spread=(
                    bybit_rate - _binance_funding
                    if _binance_funding is not None else None
                ),
            )
            session.add(decision)

            label = "✅ APPROVE" if gate.decision == "approve" else (
                "🚫 MACRO BLOCK" if gate.decision == "blocked_macro" else "⛔ BLOCK"
            )
            logger.info(
                f"[GATE SHADOW] Signal #{sig.id} {sig.direction.upper()} → "
                f"{label} | {gate.reason}"
            )

            if gate.decision != "approve":
                detail = macro_reason if macro_blocked else gate.reason
                notifier.send(
                    f"🔍 Gate Shadow #{sig.id}: {label}\n"
                    f"Signal: {sig.direction.upper()} {sig.symbol}\n"
                    f"{detail}\n"
                    f"(shadow mode — entry NOT blocked)"
                )

        session.commit()


# ── main loop ────────────────────────────────────────────────────────────────

async def _rss_loop() -> None:
    while True:
        try:
            await check_feeds()
        except Exception as e:
            logger.warning(f"RSS loop error: {e}")
        await asyncio.sleep(60)


async def _funding_loop(symbol: str) -> None:
    while True:
        try:
            await refresh_binance_funding(symbol)
        except Exception as e:
            logger.warning(f"Funding loop error: {e}")
        await asyncio.sleep(120)


async def _signal_loop(engine, fetcher, cfg, notifier) -> None:
    while True:
        try:
            await evaluate_signals(engine, fetcher, cfg, notifier)
        except Exception as e:
            logger.error(f"Signal eval error: {e}")
        await asyncio.sleep(300)


async def async_main() -> None:
    cfg = load_config()
    logger.remove()
    logger.add(sys.stderr, level=cfg.log_level)
    logger.add("logs/gate.log", rotation="10 MB", retention="30 days")

    engine = init_db(cfg.database_url)
    fetcher = BybitFetcher(cfg.bybit_api_key, cfg.bybit_api_secret, cfg.bybit_testnet)
    notifier = Notifier(cfg.telegram_token, cfg.telegram_chat_id)

    logger.info("GATE started (shadow mode — observation only)")
    notifier.send(
        "🔍 Alpha Gate v2 started (shadow mode)\n"
        "Monitoring signals: will log gate decisions without blocking entries."
    )

    await asyncio.gather(
        _rss_loop(),
        _funding_loop(cfg.symbol),
        _signal_loop(engine, fetcher, cfg, notifier),
    )


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()

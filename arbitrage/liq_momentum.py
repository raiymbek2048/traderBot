"""Liquidation Momentum — PAPER executor (follow-impulse после каскадов).

Вход из анализа 659 каскадов ≥$30k за 9 дней (scripts/liq_cascade_analysis.py):
  Sell-каскад (лонги ликвидированы) → цена продолжает падать → мы SHORT
  Buy-каскад  (шорты ликвидированы) → цена продолжает расти → мы LONG
  E[net raw] на T+15m: +0.54% (Sell), +1.04% (Buy)
  Комиссии round-trip 0.11% + слипаж → E[net] ~+0.4-0.9%

Правила:
  каскад: |группа ликвидаций одного символа/стороны в окне 15с|
  вход: если Σvalue ≥ 100k$ И символ в TRADEABLE → ждём 60с после последней ликвидации
  ждать 60с потому что sub-second анализ показал микро mean-revert в первые ~5с
  hold: ровно 15 минут, exit по таймеру
  size: $50, только BTC/ETH/SOL
  cooldown: после сделки на символе — 5 минут паузы
  no stop-loss: измеряли среднее, стопы деформируют бэктест

Run: python -m arbitrage.liq_momentum
"""
from __future__ import annotations
import asyncio
import json
import time
import urllib.request
import urllib.parse
from collections import defaultdict
from datetime import datetime, timezone, timedelta

from loguru import logger
from sqlalchemy import select, case, func
from sqlalchemy.orm import Session

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from shared.db import init_db, LiqEvent, LiqMomentumTrade
from shared.config import load_config
from funding.executor import BybitClient

# ── параметры (из анализа) ───────────────────────────────────────────────────
CASCADE_WINDOW_S = 15
MIN_CASCADE_USD  = 100_000
ENTRY_DELAY_S    = 60
HOLD_MINUTES     = 15
SIZE_USDT        = float(os.environ.get("LIQMOM_SIZE_USDT", "50"))
TRADEABLE        = {"BTCUSDT", "ETHUSDT", "SOLUSDT"}
FEES_ROUND_PCT   = 0.0011      # taker × 2 (Bybit perp)
COOLDOWN_S       = 300
POLL_SEC         = 5

_tg_token = ""
_tg_chat  = ""
_cooldown: dict[str, float] = {}   # symbol → unix ts последней сделки


def _tg(text: str) -> None:
    if not _tg_token or not _tg_chat:
        return
    try:
        data = urllib.parse.urlencode({"chat_id": _tg_chat, "text": text}).encode()
        urllib.request.urlopen(
            f"https://api.telegram.org/bot{_tg_token}/sendMessage", data, timeout=5)
    except Exception as e:
        logger.warning(f"TG failed: {e}")


def _round_qty(qty: float, symbol: str) -> float:
    steps = {"BTCUSDT": 0.001, "ETHUSDT": 0.01, "SOLUSDT": 0.1}
    step = steps.get(symbol, 0.01)
    return round(int(qty / step) * step, 8)


def _cluster(events) -> list[dict]:
    """Группирует ликвидации в каскады по (symbol, side)."""
    groups = defaultdict(list)
    for e in events:
        groups[(e.symbol, e.side)].append(e)
    out = []
    for (sym, side), evs in groups.items():
        evs.sort(key=lambda x: x.ts)
        cur = None
        for e in evs:
            ts_ms = int(e.ts.replace(tzinfo=timezone.utc).timestamp() * 1000)
            if cur and ts_ms - cur["end_ms"] <= CASCADE_WINDOW_S * 1000:
                cur["end_ms"] = ts_ms
                cur["count"] += 1
                cur["value_usdt"] += (e.value_usdt or 0)
                cur["last_price"] = e.price
            else:
                if cur:
                    out.append(cur)
                cur = {"symbol": sym, "side": side,
                       "start_ms": ts_ms, "end_ms": ts_ms,
                       "count": 1, "value_usdt": e.value_usdt or 0,
                       "last_price": e.price}
        if cur:
            out.append(cur)
    return out


async def open_position(engine, bybit: BybitClient, cas: dict) -> int | None:
    sym = cas["symbol"]
    # Sell-каскад → SHORT (продаём по bid); Buy-каскад → LONG (покупаем по ask)
    direction = "short" if cas["side"] == "Sell" else "long"
    try:
        t = bybit.get_ticker(sym, "linear")
        bid = float(t.get("bid1Price") or 0)
        ask = float(t.get("ask1Price") or 0)
        if not bid or not ask:
            return None
        entry_price = bid if direction == "short" else ask
        qty = _round_qty(SIZE_USDT / entry_price, sym)
        if qty <= 0:
            logger.warning(f"qty=0 для {sym} (цена {entry_price})")
            return None

        pos = LiqMomentumTrade(
            symbol=sym, cascade_side=cas["side"],
            cascade_value_usdt=cas["value_usdt"], cascade_count=cas["count"],
            cascade_end_ts=datetime.fromtimestamp(cas["end_ms"] / 1000, tz=timezone.utc),
            direction=direction, size_usdt=SIZE_USDT, qty=qty,
            entry_price=entry_price, entry_ts=datetime.now(timezone.utc),
            status="open", paper=True,
        )
        with Session(engine) as s:
            s.add(pos); s.commit(); pid = pos.id

        logger.info(f"OPEN {sym} {direction.upper()} caskade={cas['side']} "
                    f"${cas['value_usdt']:,.0f}/{cas['count']} liq | entry={entry_price} "
                    f"qty={qty} id={pid}")
        _tg(f"🟢 [PAPER/liq-mom] OPEN {sym}\n"
            f"Каскад {cas['side']}: ${cas['value_usdt']:,.0f} ({cas['count']} liq)\n"
            f"→ {direction.upper()} qty={qty} @ ${entry_price}\n"
            f"Держим 15 мин, ID: {pid}")
        return pid
    except Exception as e:
        logger.error(f"open {sym}: {e}")
        return None


async def close_position(engine, bybit: BybitClient, pid: int, reason: str) -> None:
    try:
        with Session(engine) as s:
            pos = s.get(LiqMomentumTrade, pid)
            if not pos or pos.status != "open":
                return
            sym, direction, qty, entry, size = (pos.symbol, pos.direction, pos.qty,
                                                pos.entry_price, pos.size_usdt)

        t = bybit.get_ticker(sym, "linear")
        bid = float(t.get("bid1Price") or 0)
        ask = float(t.get("ask1Price") or 0)
        # закрытие SHORT: покупаем по ask; закрытие LONG: продаём по bid
        exit_price = ask if direction == "short" else bid
        if not exit_price:
            return

        # PnL: SHORT = (entry-exit)*qty; LONG = (exit-entry)*qty
        raw_pct = ((entry - exit_price) if direction == "short"
                   else (exit_price - entry)) / entry * 100
        raw_pnl = size * raw_pct / 100
        fees = size * FEES_ROUND_PCT
        pnl = round(raw_pnl - fees, 4)

        with Session(engine) as s:
            pos = s.get(LiqMomentumTrade, pid)
            pos.exit_price = exit_price
            pos.exit_ts = datetime.now(timezone.utc)
            pos.raw_pnl_pct = round(raw_pct, 4)
            pos.fees_usdt = round(fees, 4)
            pos.pnl_usdt = pnl
            pos.status = "closed"
            s.commit()

        _cooldown[sym] = time.time()
        logger.info(f"CLOSE {sym} {direction.upper()} ({reason}) raw={raw_pct:+.3f}% "
                    f"pnl={pnl:+.4f} entry={entry} exit={exit_price}")
        _tg(f"🔴 [PAPER/liq-mom] CLOSE {sym} ({reason})\n"
            f"{direction.upper()}: {entry} → {exit_price} ({raw_pct:+.3f}%)\n"
            f"PnL: {'+' if pnl>=0 else ''}{pnl:.4f} USDT (fees −{fees:.3f})")
    except Exception as e:
        logger.error(f"close {pid}: {e}")


async def hourly_summary(engine) -> None:
    while True:
        await asyncio.sleep(3600)
        try:
            with Session(engine) as s:
                cnt, total, wins = s.execute(
                    select(func.count(LiqMomentumTrade.id),
                           func.sum(LiqMomentumTrade.pnl_usdt),
                           func.sum(case((LiqMomentumTrade.pnl_usdt > 0, 1), else_=0)))
                    .where(LiqMomentumTrade.status == "closed")).first()
            if cnt:
                logger.info(f"[summary] закрытых={cnt} Σpnl={total or 0:+.4f} wins={wins or 0}")
        except Exception as e:
            logger.warning(f"summary: {e}")


async def main() -> None:
    global _tg_token, _tg_chat
    cfg = load_config()
    _tg_token, _tg_chat = cfg.telegram_token, cfg.telegram_chat_id
    logger.remove(); logger.add(sys.stderr, level="INFO")
    engine = init_db(cfg.database_url)
    bybit = BybitClient(cfg.bybit_api_key, cfg.bybit_api_secret)

    logger.info(f"Liq-Momentum PAPER | tradeable={TRADEABLE} | "
                f"size=${SIZE_USDT} | cascade≥${MIN_CASCADE_USD:,} | "
                f"delay={ENTRY_DELAY_S}s | hold={HOLD_MINUTES}m")
    _tg(f"⚡ Liq-Momentum PAPER запущен\n"
        f"Символы: {', '.join(sorted(TRADEABLE))} | size=${SIZE_USDT:.0f}\n"
        f"Триггер: каскад ≥${MIN_CASCADE_USD:,} → жду {ENTRY_DELAY_S}с → hold {HOLD_MINUTES}м\n"
        f"Ожидание: +0.4-0.9% net на сделку (E из бэктеста 659 каскадов)")
    asyncio.create_task(hourly_summary(engine))

    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            now_ms = time.time() * 1000

            # 1. читаем свежие ликвидации (окно 20с ДО + delay)
            cutoff = now_utc - timedelta(seconds=ENTRY_DELAY_S + CASCADE_WINDOW_S + 10)
            with Session(engine) as s:
                events = s.execute(
                    select(LiqEvent).where(LiqEvent.ts >= cutoff)
                    .order_by(LiqEvent.ts)).scalars().all()

            cascades = _cluster(events)

            # 2. проверяем какие каскады созрели для входа
            with Session(engine) as s:
                open_syms = set(row[0] for row in s.execute(
                    select(LiqMomentumTrade.symbol)
                    .where(LiqMomentumTrade.status == "open")).all())
                # дедуп по cascade_end_ts (не входить дважды в один каскад)
                # SQLite возвращает naive datetime — форсим UTC-aware
                naive_cutoff = (now_utc - timedelta(hours=2)).replace(tzinfo=None)
                seen_ends_raw = [row[0] for row in s.execute(
                    select(LiqMomentumTrade.cascade_end_ts)
                    .where(LiqMomentumTrade.entry_ts >= naive_cutoff)).all()]
                seen_ends = set(
                    (se.replace(tzinfo=timezone.utc) if se and se.tzinfo is None else se)
                    for se in seen_ends_raw if se)

            for cas in cascades:
                if cas["symbol"] not in TRADEABLE:
                    continue
                if cas["value_usdt"] < MIN_CASCADE_USD:
                    continue
                if cas["symbol"] in open_syms:
                    continue
                if time.time() - _cooldown.get(cas["symbol"], 0) < COOLDOWN_S:
                    continue
                # созрел ли? end_ms + delay должно быть В ПРОШЛОМ (готовы входить)
                entry_target = cas["end_ms"] + ENTRY_DELAY_S * 1000
                if now_ms < entry_target:
                    continue
                # но не старше 15с — иначе опоздали
                if now_ms - entry_target > 15_000:
                    continue
                # уже входили в этот каскад?
                cas_end = datetime.fromtimestamp(cas["end_ms"] / 1000, tz=timezone.utc)
                if any(abs((cas_end - se).total_seconds()) < CASCADE_WINDOW_S
                       for se in seen_ends if se):
                    continue

                pid = await open_position(engine, bybit, cas)
                if pid:
                    open_syms.add(cas["symbol"])
                    seen_ends.add(cas_end)

            # 3. закрываем позиции по таймеру
            with Session(engine) as s:
                to_close = s.execute(
                    select(LiqMomentumTrade.id, LiqMomentumTrade.entry_ts)
                    .where(LiqMomentumTrade.status == "open")).all()
            for pid, ets in to_close:
                if ets is None:
                    continue
                age_min = (now_utc - ets.replace(tzinfo=timezone.utc)).total_seconds() / 60
                if age_min >= HOLD_MINUTES:
                    await close_position(engine, bybit, pid, f"timer {age_min:.1f}m")

        except Exception as e:
            logger.error(f"loop: {e}")

        await asyncio.sleep(POLL_SEC)


if __name__ == "__main__":
    asyncio.run(main())

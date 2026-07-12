"""Perp-Perp Funding-Spread — PAPER executor (Bybit vs Binance).

Стратегия (подтверждена persistence-анализом 2 мес settled-данных):
  шорт перпа на бирже с высоким фандингом + лонг на другой, одинаковое кол-во монет.
  Зарабатываем разницу фандингов; обе ноги в USDT — выводы токенов не нужны.
  E[net] +0.3-0.5% на эпизод (AERGO 69% приб., BMNR 80%), медиана 16-32ч.

Правила (данные, не интуиция):
  ВХОД:  |спред| ≥ ENTRY_DAILY 6 проверок подряд (30 мин)
         И break-even ≤ MAX_BE_HOURS (комиссии + adverse exec_edge)
         И |фандинг Bybit| ≤ MAX_ABS_DAILY (кап/делистинг — PARTI-фильтр)
  ВЫХОД: спред (в нашу сторону) < EXIT_DAILY 6 проверок подряд,
         ИЛИ мгновенно если спред развернулся сильно против (платим >1%/день).

PnL при закрытии = basis-ноги (bid/ask обеих бирж) + settled-фандинг − комиссии.
Фандинг пересчитывается идемпотентно из settled-историй ОБЕИХ бирж каждые 30 мин.

Run: python -m funding.spread_paper
"""
from __future__ import annotations
import asyncio
import json
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone

from loguru import logger
from sqlalchemy import select
from sqlalchemy.orm import Session

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from shared.db import init_db, SpreadPosition
from shared.config import load_config
from funding.spread_scan import scan_once
from funding.executor import BybitClient

# ── параметры (из persistence-анализа) ──────────────────────────────────────────
ENTRY_DAILY    = 3.0     # %/день — вход (соотв. 0.1%/8h-окно из анализа)
EXIT_DAILY     = 0.3     # %/день — выход (хвост эпизода)
ADVERSE_FLIP   = -1.0    # %/день против нас — аварийный выход сразу
ENTRY_CONFIRM  = 6       # проверок подряд (6×5мин = 30 мин)
EXIT_CONFIRM   = 6
MAX_BE_HOURS   = 10.0    # break-even (комиссии+adverse вход) не дольше
MAX_ABS_DAILY  = 25.0    # |фандинг любой ноги| выше — кап/делистинг, не лезем
MAX_POSITIONS  = 3
SIZE_USDT      = float(os.environ.get("SPREAD_SIZE_USDT", "50"))
FEES_RT_PCT    = 0.0021  # перп-тейкер ×4: Bybit 0.055×2 + Binance 0.05×2
CHECK_SEC      = 300     # такт = такт сканера
ACCRUE_EVERY   = 6       # пересчёт фандинга каждые N тактов (30 мин)

REENTRY_COOLDOWN_S = 4 * 3600   # после закрытия символа не входим 4ч (анти флип-флоп)
SETTLED_CONFIRM_24H = 0.001     # |Σ settled-спреда за 24ч| ≥ 0.1% и знак совпадает

_tg_token = ""
_tg_chat = ""
_above: dict[str, int] = {}
_below: dict[str, int] = {}
_cooldown: dict[str, float] = {}   # symbol → unix ts закрытия


def _tg(text: str) -> None:
    if not _tg_token or not _tg_chat:
        return
    try:
        data = urllib.parse.urlencode({"chat_id": _tg_chat, "text": text}).encode()
        urllib.request.urlopen(
            f"https://api.telegram.org/bot{_tg_token}/sendMessage", data, timeout=5)
    except Exception as e:
        logger.warning(f"TG failed: {e}")


def _get(url: str):
    with urllib.request.urlopen(url, timeout=15) as r:
        return json.loads(r.read())


def _binance_book(symbol: str) -> tuple[float, float]:
    d = _get(f"https://fapi.binance.com/fapi/v1/ticker/bookTicker?symbol={symbol}")
    return float(d["bidPrice"]), float(d["askPrice"])


def _binance_settled(symbol: str, since_ms: int) -> float:
    """Σ settled-ставок Binance с since_ms."""
    d = _get(f"https://fapi.binance.com/fapi/v1/fundingRate?symbol={symbol}"
             f"&startTime={since_ms}&limit=1000")
    return sum(float(r["fundingRate"]) for r in d)


def _entry_prices(bybit: BybitClient, symbol: str, direction: str) -> tuple[float, float]:
    """(bybit_price, binance_price) — исполнимые цены входа."""
    t = bybit.get_ticker(symbol, "linear")
    by_bid, by_ask = float(t.get("bid1Price") or 0), float(t.get("ask1Price") or 0)
    bn_bid, bn_ask = _binance_book(symbol)
    if direction == "short_bybit":     # sell Bybit @bid, buy Binance @ask
        return by_bid, bn_ask
    return by_ask, bn_bid              # short_binance: buy Bybit @ask, sell Binance @bid


def _exit_prices(bybit: BybitClient, symbol: str, direction: str) -> tuple[float, float]:
    t = bybit.get_ticker(symbol, "linear")
    by_bid, by_ask = float(t.get("bid1Price") or 0), float(t.get("ask1Price") or 0)
    bn_bid, bn_ask = _binance_book(symbol)
    if direction == "short_bybit":     # закрытие: buy Bybit @ask, sell Binance @bid
        return by_ask, bn_bid
    return by_bid, bn_ask


def _settled_spread_24h(bybit: BybitClient, symbol: str) -> float:
    """Σ(Bybit) − Σ(Binance) фактически начисленных ставок за последние 24ч."""
    since = int((time.time() - 24 * 3600) * 1000)
    by_sum = sum(r for _, r in bybit.get_settled_fundings(symbol, since))
    bn_sum = _binance_settled(symbol, since)
    return by_sum - bn_sum


def _accrue(bybit: BybitClient, pos: SpreadPosition) -> float:
    """collected = size × (Σ ставок шорт-ноги − Σ ставок лонг-ноги), settled."""
    since = int(pos.opened_at.replace(tzinfo=timezone.utc).timestamp() * 1000)
    by_sum = sum(r for _, r in bybit.get_settled_fundings(pos.symbol, since))
    bn_sum = _binance_settled(pos.symbol, since)
    if pos.direction == "short_bybit":
        net = by_sum - bn_sum
    else:
        net = bn_sum - by_sum
    return round(pos.size_usdt * net, 6)


async def open_pos(engine, bybit: BybitClient, r: dict) -> None:
    sym, sp = r["symbol"], r["spread_daily_pct"]
    direction = "short_bybit" if sp > 0 else "short_binance"
    try:
        by_px, bn_px = _entry_prices(bybit, sym, direction)
        if not by_px or not bn_px:
            return
        qty = round(SIZE_USDT / by_px, 8)
        pos = SpreadPosition(
            symbol=sym, direction=direction, size_usdt=SIZE_USDT, qty=qty,
            bybit_entry_price=by_px, binance_entry_price=bn_px,
            entry_spread_daily_pct=sp, entry_exec_edge_pct=r.get("exec_edge_pct"),
            status="open", paper=True, opened_at=datetime.now(timezone.utc),
        )
        with Session(engine) as s:
            s.add(pos); s.commit(); pid = pos.id
        edge = r.get("exec_edge_pct")
        logger.info(f"OPEN {sym} {direction} sp={sp:+.2f}%/д edge={edge} id={pid}")
        _tg(f"🟢 [PAPER] Perp-Perp OPEN\n{sym}: {direction.replace('_',' ').upper()}\n"
            f"Спред: {sp:+.2f}%/день | вход: {edge:+.2f}%\n"
            f"Bybit @{by_px} | Binance @{bn_px} | qty={qty}\nID: {pid}")
    except Exception as e:
        logger.error(f"open_pos {sym}: {e}")


async def close_pos(engine, bybit: BybitClient, pos_id: int, reason: str) -> None:
    try:
        with Session(engine) as s:
            pos = s.get(SpreadPosition, pos_id)
            if not pos or pos.status != "open":
                return
            sym, direction, qty = pos.symbol, pos.direction, pos.qty

        by_exit, bn_exit = _exit_prices(bybit, sym, direction)
        collected = _accrue(bybit, pos)

        with Session(engine) as s:
            pos = s.get(SpreadPosition, pos_id)
            if direction == "short_bybit":
                by_leg = qty * (pos.bybit_entry_price - by_exit)     # short
                bn_leg = qty * (bn_exit - pos.binance_entry_price)   # long
            else:
                by_leg = qty * (by_exit - pos.bybit_entry_price)     # long
                bn_leg = qty * (pos.binance_entry_price - bn_exit)   # short
            basis = round(by_leg + bn_leg, 4)
            fees = round(pos.size_usdt * FEES_RT_PCT, 4)  # 0.21% = все 4 сделки
            pos.bybit_exit_price, pos.binance_exit_price = by_exit, bn_exit
            pos.funding_collected_usdt = collected
            pos.basis_pnl_usdt = basis
            pos.fees_usdt = fees
            pos.pnl_usdt = round(basis + collected - fees, 4)
            pos.status = "closed"
            pos.closed_at = datetime.now(timezone.utc)
            s.commit()
            pnl = pos.pnl_usdt

        _cooldown[sym] = time.time()
        logger.info(f"CLOSE {sym} ({reason}) basis={basis:+.4f} funding={collected:+.4f} "
                    f"fees={fees:.4f} pnl={pnl:+.4f}")
        _tg(f"🔴 [PAPER] Perp-Perp CLOSE ({reason})\n{sym}\n"
            f"Basis: {basis:+.4f} | Фандинг: {collected:+.4f} | Комиссии: −{fees:.4f}\n"
            f"PnL: {'+' if pnl>=0 else ''}{pnl:.4f} USDT\nID: {pos_id}")
    except Exception as e:
        logger.error(f"close_pos {pos_id}: {e}")


async def main() -> None:
    global _tg_token, _tg_chat
    cfg = load_config()
    _tg_token, _tg_chat = cfg.telegram_token, cfg.telegram_chat_id
    logger.remove(); logger.add(sys.stderr, level="INFO")
    engine = init_db(cfg.database_url)
    bybit = BybitClient(cfg.bybit_api_key, cfg.bybit_api_secret)

    logger.info(f"Perp-Perp PAPER | entry≥{ENTRY_DAILY}%/д×{ENTRY_CONFIRM} | "
                f"exit<{EXIT_DAILY}%/д×{EXIT_CONFIRM} | BE≤{MAX_BE_HOURS}ч | "
                f"size=${SIZE_USDT} | max={MAX_POSITIONS}")
    _tg(f"🔀 Perp-Perp PAPER executor запущен\n"
        f"Вход: |спред|≥{ENTRY_DAILY}%/д 30мин + BE≤{MAX_BE_HOURS}ч\n"
        f"Size: ${SIZE_USDT:.0f}/нога | макс позиций: {MAX_POSITIONS}")

    tick = 0
    while True:
        try:
            rows = await asyncio.get_event_loop().run_in_executor(None, scan_once)
            by_sym = {r["symbol"]: r for r in rows}

            with Session(engine) as s:
                open_ps = s.execute(
                    select(SpreadPosition).where(SpreadPosition.status == "open")
                ).scalars().all()
                open_map = {p.symbol: p.id for p in open_ps}
                open_dirs = {p.symbol: p.direction for p in open_ps}

            # стрики
            for r in rows:
                sym, sp = r["symbol"], r["spread_daily_pct"]
                edge = r.get("exec_edge_pct")
                ok_entry = (
                    abs(sp) >= ENTRY_DAILY
                    and edge is not None
                    and abs(r["bybit_daily_pct"]) <= MAX_ABS_DAILY
                    and abs(r["binance_daily_pct"]) <= MAX_ABS_DAILY
                    and (FEES_RT_PCT * 100 + max(0.0, -edge)) / abs(sp) * 24 <= MAX_BE_HOURS
                )
                _above[sym] = _above.get(sym, 0) + 1 if ok_entry else 0

            # выходы
            for sym, pid in open_map.items():
                r = by_sym.get(sym)
                sign = 1 if open_dirs[sym] == "short_bybit" else -1
                cur = sign * r["spread_daily_pct"] if r else 0.0
                if cur <= ADVERSE_FLIP:
                    await close_pos(engine, bybit, pid, f"adverse {cur:+.2f}%/д")
                    _below.pop(sym, None)
                    continue
                _below[sym] = _below.get(sym, 0) + 1 if cur < EXIT_DAILY else 0
                if _below.get(sym, 0) >= EXIT_CONFIRM:
                    await close_pos(engine, bybit, pid, f"spread<{EXIT_DAILY}%/д")
                    _below.pop(sym, None)

            # входы
            slots = MAX_POSITIONS - len(open_map)
            cands = sorted((r for r in rows
                            if _above.get(r["symbol"], 0) >= ENTRY_CONFIRM
                            and r["symbol"] not in open_map
                            and time.time() - _cooldown.get(r["symbol"], 0) > REENTRY_COOLDOWN_S),
                           key=lambda x: -abs(x["spread_daily_pct"]))
            opened = 0
            for r in cands:
                if opened >= max(0, slots):
                    break
                # подтверждение settled-историей: предсказанный спред должен
                # совпадать по знаку с фактически начисленным за 24ч (анти-SXT)
                try:
                    settled = await asyncio.get_event_loop().run_in_executor(
                        None, _settled_spread_24h, bybit, r["symbol"])
                except Exception as e:
                    logger.warning(f"settled check {r['symbol']}: {e}")
                    continue
                sign = 1 if r["spread_daily_pct"] > 0 else -1
                if sign * settled < SETTLED_CONFIRM_24H:
                    logger.info(f"skip {r['symbol']}: settled 24h {settled*100:+.3f}% "
                                f"не подтверждает предсказанный {r['spread_daily_pct']:+.2f}%/д")
                    continue
                await open_pos(engine, bybit, r)
                opened += 1

            # периодический пересчёт фандинга
            tick += 1
            if tick % ACCRUE_EVERY == 0 and open_map:
                # снимок id/значений до похода в сеть
                with Session(engine) as s:
                    snap = [(p.id, p.funding_collected_usdt or 0.0)
                            for p in s.execute(select(SpreadPosition)
                                               .where(SpreadPosition.status == "open")
                                               ).scalars().all()]
                for pid, old in snap:
                    try:
                        with Session(engine) as s:
                            pos = s.get(SpreadPosition, pid)
                            if not pos or pos.status != "open":
                                continue
                            new = await asyncio.get_event_loop().run_in_executor(
                                None, _accrue, bybit, pos)
                            if abs(new - old) > 1e-9:
                                pos.funding_collected_usdt = new
                                s.commit()
                                logger.info(f"[accrue] {pos.symbol}: {old:+.4f}→{new:+.4f}")
                    except Exception as e:
                        logger.warning(f"[accrue] pos={pid}: {e}")

            top = rows[0] if rows else None
            logger.info(f"tick | открыто={len(open_map)} | стрик-кандидатов="
                        f"{sum(1 for v in _above.values() if v >= ENTRY_CONFIRM)} | "
                        f"топ={top['symbol']} {top['spread_daily_pct']:+.2f}%/д" if top else "tick")
        except Exception as e:
            logger.error(f"loop error: {e}")

        await asyncio.sleep(CHECK_SEC)


if __name__ == "__main__":
    asyncio.run(main())

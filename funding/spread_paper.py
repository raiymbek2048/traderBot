"""Perp-Perp Funding-Spread — PAPER executor с A/B/C-тестом правил.

Три варианта правил торгуют ПАРАЛЛЕЛЬНО на одном потоке данных (paper позволяет).
Каждая сделка тегируется вариантом → через несколько дней таблица PnL по вариантам
покажет, какие фильтры реально зарабатывают, без последовательного гадания.

  base    — исходные правила: |спред|≥3%/д 30мин + BE≤10ч + кап-фильтр
  settled — base + подтверждение settled-историей 24ч + кулдаун 4ч
  strict  — settled + сеттлмент ≤2.5ч + ширина стаканов ≤0.3% + no-pay выход 3.5ч

Общее для всех: exit спред<0.3%/д 30мин, adverse-flip −1%/д, PnL = basis(bid/ask)
+ settled-фандинг обеих бирж − комиссии 0.21%.

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

# ── общие параметры ──────────────────────────────────────────────────────────────
ENTRY_DAILY    = 3.0
EXIT_DAILY     = 0.3
ADVERSE_FLIP   = -1.0
ENTRY_CONFIRM  = 6
EXIT_CONFIRM   = 6
MAX_BE_HOURS   = 10.0
MAX_ABS_DAILY  = 25.0
MAX_POS_PER_VARIANT = 3
SIZE_USDT      = float(os.environ.get("SPREAD_SIZE_USDT", "50"))
FEES_RT_PCT    = 0.0021
CHECK_SEC      = 300
ACCRUE_EVERY   = 6
SETTLED_CONFIRM_24H = 0.001

# ── варианты правил: решётка, изолирующая вклад каждого фильтра ─────────────────
# dims: entry_daily / exit_daily / settled_confirm / cooldown / окно_сеттлмента /
#       ширина_стаканов / no-pay / aligned_only (вход только при edge≥0)
def _v(entry=3.0, exit_=0.3, settled=False, cd=0, window=None, width=None,
       nopay=None, aligned=False):
    return dict(entry_daily=entry, exit_daily=exit_, settled_confirm=settled,
                cooldown_s=cd, max_to_settle_h=window, max_book_width=width,
                nopay_exit_h=nopay, aligned_only=aligned)

CD = 4 * 3600
VARIANTS: dict[str, dict] = {
    # базовые ступени (история развития правил)
    "base":           _v(),
    "settled":        _v(settled=True, cd=CD),
    "strict":         _v(settled=True, cd=CD, window=2.5, width=0.30, nopay=3.5),
    # изоляция вклада каждого фильтра поверх settled
    "window_only":    _v(settled=True, cd=CD, window=2.5),
    "width_only":     _v(settled=True, cd=CD, width=0.30),
    "nopay_only":     _v(settled=True, cd=CD, nopay=3.5),
    # гипотеза aligned carry (обе победы были с edge≥0)
    "aligned":        _v(settled=True, cd=CD, aligned=True),
    "aligned_strict": _v(settled=True, cd=CD, window=2.5, width=0.30, nopay=3.5,
                         aligned=True),
    # чувствительность к порогам
    "hi_entry":       _v(entry=5.0, settled=True, cd=CD, window=2.5, width=0.30,
                         nopay=3.5),
    "fast_exit":      _v(exit_=1.0, settled=True, cd=CD, window=2.5, width=0.30,
                         nopay=3.5),
}

# Telegram-алерты сделок — только от реперных (остальные молча в БД + сводка 12ч)
TG_VARIANTS = {"strict", "aligned_strict"}

_tg_token = ""
_tg_chat = ""
# состояние по (variant, symbol)
_above: dict[tuple, int] = {}
_below: dict[tuple, int] = {}
_cooldown: dict[tuple, float] = {}


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
    d = _get(f"https://fapi.binance.com/fapi/v1/fundingRate?symbol={symbol}"
             f"&startTime={since_ms}&limit=1000")
    return sum(float(r["fundingRate"]) for r in d)


def _settled_spread_24h(bybit: BybitClient, symbol: str) -> float:
    since = int((time.time() - 24 * 3600) * 1000)
    by_sum = sum(r for _, r in bybit.get_settled_fundings(symbol, since))
    bn_sum = _binance_settled(symbol, since)
    return by_sum - bn_sum


def _entry_prices(bybit: BybitClient, symbol: str, direction: str) -> tuple[float, float]:
    t = bybit.get_ticker(symbol, "linear")
    by_bid, by_ask = float(t.get("bid1Price") or 0), float(t.get("ask1Price") or 0)
    bn_bid, bn_ask = _binance_book(symbol)
    if direction == "short_bybit":
        return by_bid, bn_ask
    return by_ask, bn_bid


def _exit_prices(bybit: BybitClient, symbol: str, direction: str) -> tuple[float, float]:
    t = bybit.get_ticker(symbol, "linear")
    by_bid, by_ask = float(t.get("bid1Price") or 0), float(t.get("ask1Price") or 0)
    bn_bid, bn_ask = _binance_book(symbol)
    if direction == "short_bybit":
        return by_ask, bn_bid
    return by_bid, bn_ask


def _accrue(bybit: BybitClient, pos: SpreadPosition) -> float:
    since = int(pos.opened_at.replace(tzinfo=timezone.utc).timestamp() * 1000)
    by_sum = sum(r for _, r in bybit.get_settled_fundings(pos.symbol, since))
    bn_sum = _binance_settled(pos.symbol, since)
    net = (by_sum - bn_sum) if pos.direction == "short_bybit" else (bn_sum - by_sum)
    return round(pos.size_usdt * net, 6)


async def open_pos(engine, bybit: BybitClient, r: dict, variant: str) -> None:
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
            status="open", paper=True, variant=variant,
            opened_at=datetime.now(timezone.utc),
        )
        with Session(engine) as s:
            s.add(pos); s.commit(); pid = pos.id
        edge = r.get("exec_edge_pct")
        logger.info(f"OPEN[{variant}] {sym} {direction} sp={sp:+.2f}%/д edge={edge} id={pid}")
        if variant in TG_VARIANTS:
            _tg(f"🟢 [PAPER/{variant}] OPEN {sym}\n{direction.replace('_',' ').upper()} | "
                f"спред {sp:+.2f}%/д | вход {edge:+.2f}%\nID: {pid}")
    except Exception as e:
        logger.error(f"open_pos[{variant}] {sym}: {e}")


async def close_pos(engine, bybit: BybitClient, pos_id: int, reason: str) -> None:
    try:
        with Session(engine) as s:
            pos = s.get(SpreadPosition, pos_id)
            if not pos or pos.status != "open":
                return
            sym, direction, qty, variant = pos.symbol, pos.direction, pos.qty, pos.variant

        by_exit, bn_exit = _exit_prices(bybit, sym, direction)
        with Session(engine) as s:
            pos = s.get(SpreadPosition, pos_id)
            collected = _accrue(bybit, pos)
            if direction == "short_bybit":
                by_leg = qty * (pos.bybit_entry_price - by_exit)
                bn_leg = qty * (bn_exit - pos.binance_entry_price)
            else:
                by_leg = qty * (by_exit - pos.bybit_entry_price)
                bn_leg = qty * (pos.binance_entry_price - bn_exit)
            basis = round(by_leg + bn_leg, 4)
            fees = round(pos.size_usdt * FEES_RT_PCT, 4)
            pos.bybit_exit_price, pos.binance_exit_price = by_exit, bn_exit
            pos.funding_collected_usdt = collected
            pos.basis_pnl_usdt = basis
            pos.fees_usdt = fees
            pos.pnl_usdt = round(basis + collected - fees, 4)
            pos.status = "closed"
            pos.closed_at = datetime.now(timezone.utc)
            s.commit()
            pnl = pos.pnl_usdt

        _cooldown[(variant, sym)] = time.time()
        logger.info(f"CLOSE[{variant}] {sym} ({reason}) basis={basis:+.4f} "
                    f"funding={collected:+.4f} fees={fees:.4f} pnl={pnl:+.4f}")
        if variant in TG_VARIANTS:
            _tg(f"🔴 [PAPER/{variant}] CLOSE {sym} ({reason})\n"
                f"basis {basis:+.4f} | funding {collected:+.4f} | fees −{fees:.4f}\n"
                f"PnL: {'+' if pnl>=0 else ''}{pnl:.4f} USDT")
    except Exception as e:
        logger.error(f"close_pos {pos_id}: {e}")


async def variant_summary(engine) -> None:
    """Каждые 12ч — сравнительная таблица PnL по вариантам в Telegram."""
    from sqlalchemy import func
    while True:
        await asyncio.sleep(12 * 3600)
        try:
            with Session(engine) as s:
                rows = s.execute(
                    select(SpreadPosition.variant,
                           func.count(SpreadPosition.id),
                           func.sum(SpreadPosition.pnl_usdt))
                    .where(SpreadPosition.status == "closed")
                    .group_by(SpreadPosition.variant)
                ).all()
            if not rows:
                continue
            rows = sorted(rows, key=lambda r: -(r[2] or 0))
            lines = ["📊 A/B-тест вариантов (закрытые сделки):"]
            for v, n, pnl in rows:
                lines.append(f"  {v or '?'}: {n} сделок, PnL {pnl or 0:+.3f} USDT")
            _tg("\n".join(lines))
        except Exception as e:
            logger.warning(f"summary error: {e}")


async def main() -> None:
    global _tg_token, _tg_chat
    cfg = load_config()
    _tg_token, _tg_chat = cfg.telegram_token, cfg.telegram_chat_id
    logger.remove(); logger.add(sys.stderr, level="INFO")
    engine = init_db(cfg.database_url)
    bybit = BybitClient(cfg.bybit_api_key, cfg.bybit_api_secret)

    logger.info(f"Perp-Perp PAPER A/B/C | варианты={list(VARIANTS)} | "
                f"size=${SIZE_USDT} | max/вариант={MAX_POS_PER_VARIANT}")
    _tg(f"🔬 Perp-Perp A/B-тест запущен: {len(VARIANTS)} вариантов\n"
        f"{', '.join(VARIANTS)}\n"
        f"Алерты сделок: только strict и aligned_strict.\n"
        f"Сводка-сравнение всех — каждые 12ч.")
    asyncio.get_event_loop().create_task(variant_summary(engine))

    tick = 0
    while True:
        try:
            rows = await asyncio.get_event_loop().run_in_executor(None, scan_once)
            now_ms = time.time() * 1000
            settled_cache: dict[str, float] = {}   # общий кэш на тик

            with Session(engine) as s:
                open_ps = s.execute(
                    select(SpreadPosition).where(SpreadPosition.status == "open")
                ).scalars().all()
                open_by_var: dict[str, dict[str, int]] = {v: {} for v in VARIANTS}
                dirs: dict[int, str] = {}
                meta: dict[int, tuple] = {}
                for p in open_ps:
                    open_by_var.setdefault(p.variant or "strict", {})[p.symbol] = p.id
                    dirs[p.id] = p.direction
                    age_h = (datetime.now(timezone.utc) -
                             p.opened_at.replace(tzinfo=timezone.utc)).total_seconds() / 3600
                    meta[p.id] = (age_h, p.funding_collected_usdt or 0.0)

            by_sym = {r["symbol"]: r for r in rows}

            # ── пред-фильтры и стрики по вариантам ──
            for r in rows:
                sym, sp = r["symbol"], r["spread_daily_pct"]
                edge = r.get("exec_edge_pct")
                width = r.get("book_width_pct")
                to_settle_h = (r.get("next_funding_ms", 2**62) - now_ms) / 3.6e6
                common_ok = (
                    edge is not None
                    and abs(r["bybit_daily_pct"]) <= MAX_ABS_DAILY
                    and abs(r["binance_daily_pct"]) <= MAX_ABS_DAILY
                    and (FEES_RT_PCT * 100 + max(0.0, -edge)) / max(abs(sp), 1e-9) * 24
                        <= MAX_BE_HOURS
                )
                for v, cfg_v in VARIANTS.items():
                    ok = common_ok and abs(sp) >= cfg_v["entry_daily"]
                    if ok and cfg_v["aligned_only"]:
                        ok = edge >= 0
                    if ok and cfg_v["max_book_width"] is not None:
                        ok = width is not None and width <= cfg_v["max_book_width"]
                    if ok and cfg_v["max_to_settle_h"] is not None:
                        ok = 0 <= to_settle_h <= cfg_v["max_to_settle_h"]
                    key = (v, sym)
                    _above[key] = _above.get(key, 0) + 1 if ok else 0

            # ── выходы (общая логика; no-pay только где включён) ──
            for v, cfg_v in VARIANTS.items():
                for sym, pid in list(open_by_var.get(v, {}).items()):
                    age_h, collected = meta.get(pid, (0, 0))
                    if cfg_v["nopay_exit_h"] and age_h >= cfg_v["nopay_exit_h"] and collected <= 0:
                        await close_pos(engine, bybit, pid, f"no-pay {age_h:.1f}ч")
                        open_by_var[v].pop(sym, None)
                        continue
                    r = by_sym.get(sym)
                    sign = 1 if dirs[pid] == "short_bybit" else -1
                    cur = sign * r["spread_daily_pct"] if r else 0.0
                    key = (v, sym)
                    if cur <= ADVERSE_FLIP:
                        await close_pos(engine, bybit, pid, f"adverse {cur:+.2f}%/д")
                        open_by_var[v].pop(sym, None)
                        _below.pop(key, None)
                        continue
                    _below[key] = _below.get(key, 0) + 1 if cur < cfg_v["exit_daily"] else 0
                    if _below.get(key, 0) >= EXIT_CONFIRM:
                        await close_pos(engine, bybit, pid, f"spread<{cfg_v['exit_daily']}%/д")
                        open_by_var[v].pop(sym, None)
                        _below.pop(key, None)

            # ── входы по вариантам ──
            for v, cfg_v in VARIANTS.items():
                opened_now = open_by_var.get(v, {})
                slots = MAX_POS_PER_VARIANT - len(opened_now)
                if slots <= 0:
                    continue
                cands = sorted(
                    (r for r in rows
                     if _above.get((v, r["symbol"]), 0) >= ENTRY_CONFIRM
                     and r["symbol"] not in opened_now
                     and time.time() - _cooldown.get((v, r["symbol"]), 0) > cfg_v["cooldown_s"]),
                    key=lambda x: -abs(x["spread_daily_pct"]))
                done = 0
                for r in cands:
                    if done >= slots:
                        break
                    if cfg_v["settled_confirm"]:
                        sym = r["symbol"]
                        if sym not in settled_cache:
                            try:
                                settled_cache[sym] = await asyncio.get_event_loop(
                                ).run_in_executor(None, _settled_spread_24h, bybit, sym)
                            except Exception as e:
                                logger.warning(f"settled check {sym}: {e}")
                                continue
                        sign = 1 if r["spread_daily_pct"] > 0 else -1
                        if sign * settled_cache[sym] < SETTLED_CONFIRM_24H:
                            logger.info(f"skip[{v}] {sym}: settled 24h "
                                        f"{settled_cache[sym]*100:+.3f}% не подтверждает "
                                        f"{r['spread_daily_pct']:+.2f}%/д")
                            continue
                    await open_pos(engine, bybit, r, v)
                    done += 1

            # ── пересчёт фандинга ──
            tick += 1
            if tick % ACCRUE_EVERY == 0:
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
                                logger.info(f"[accrue] {pos.symbol}[{pos.variant}]: "
                                            f"{old:+.4f}→{new:+.4f}")
                    except Exception as e:
                        logger.warning(f"[accrue] pos={pid}: {e}")

            n_open = {v: len(m) for v, m in open_by_var.items()}
            top = rows[0] if rows else None
            logger.info(
                f"tick | открыто={n_open} | топ={top['symbol']} "
                f"{top['spread_daily_pct']:+.2f}%/д" if top else "tick | нет данных")
        except Exception as e:
            logger.error(f"loop error: {e}")

        await asyncio.sleep(CHECK_SEC)


if __name__ == "__main__":
    asyncio.run(main())

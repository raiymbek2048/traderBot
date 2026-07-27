"""Perp-perp funding spread с МИНИМАЛЬНЫМ ХОЛДОМ до сеттлмента — PAPER.

═══ ЗАЧЕМ ЭТОТ ТЕСТ ═══
Разбор 146 сделок A/B-решётки (scripts/loophole_analysis.py) нашёл структурный
баг НАШЕЙ логики, а не стратегии:

  107/146 сделок (73%) закрылись, НЕ ПЕРЕЖИВ НИ ОДНОГО сеттлмента.
  Средний холд 2.16ч при интервале фандинга 8ч.
  → заплатили комиссию, не получили выплату, за которой шли.

Фильтры `no_pay_exit` и `fast_exit`, добавленные «чтобы тормозить убытки»,
выбивали позицию ДО начисления. Убыток −$30 объясняется этим.

═══ ЧТО ИСПРАВЛЕНО ═══
Единственное изменение: **нельзя выходить, пока не переживёшь начисление.**
Всё остальное — как было.

═══ ПОЧЕМУ ОДИН ВАРИАНТ, А НЕ РЕШЁТКА ═══
A/B-решётка размножала одну возможность в 2.5 раза (11 вариантов на одном
потоке). Из-за этого ретро-анализ обманул: «правило спред 3-5%/д + вход ≤4ч»
показывало +$0.40 на 34 записях, но после дедупликации до 14 уникальных
возможностей стало −$1.42. Одна удачная HOMEUSDT считалась 7 раз.
Поэтому здесь СТРОГО один вариант и одна позиция на символ.

═══ КРИТЕРИИ УСПЕХА (зафиксированы ЗАРАНЕЕ, 27.07.2026) ═══
Тест считается пройденным ТОЛЬКО при выполнении ВСЕХ условий:
  1. n ≥ 25 уникальных закрытых позиций
  2. Σ PnL при TAKER-комиссиях > 0   (не при maker — taker это реальность)
  3. Медиана PnL > 0                 (не держится на 1-2 хвостах)
  4. Вклад лучшей сделки < 40% от Σ  (защита от «одна HOME спасла всё»)
  5. Доля дожития до сеттлмента > 90% (правило реально соблюдается)
Провал любого пункта = торговый трек закрыт окончательно.

Run: python -m funding.hold_paper
"""
from __future__ import annotations
import asyncio
import json
import os
import statistics
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

from loguru import logger
from sqlalchemy import select, func
from sqlalchemy.orm import Session

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from shared.config import load_config
from shared.db import init_db, HoldPosition

# ── параметры входа (из журнала, но БЕЗ переобучения на дедуплицированный шум) ──
MIN_SPREAD_DAILY = 3.0      # %/день — ниже не окупает разовую стоимость
MAX_SPREAD_DAILY = 8.0      # выше = радиоактивно (кап/пре-делистинг)
MAX_HOURS_TO_SETTLE = 4.0   # вход только в окне перед начислением
MAX_BOOK_WIDTH = 0.30       # % суммарная ширина стаканов обеих бирж

# ⚠️ ОДИН ТИКЕР ≠ ОДИН АКТИВ. Проверено 27.07: 4 из 587 общих тикеров —
# РАЗНЫЕ токены на двух биржах (ONUSDT $88.20 vs $0.177 = расхождение 49708%,
# SNTUSDT 257%, WAVESUSDT 214%, VINEUSDT 95%). Открыть на таком «delta-neutral»
# позицию = голая направленная ставка на два несвязанных актива сразу.
# Легитимные перп-перп гэпы одного актива < 5% (даже сломанный PARTI был 4%),
# поэтому порог 10% чисто разделяет «дислокация» и «другой актив».
MAX_PRICE_DISLOCATION = 10.0
SIZE_USDT = float(os.environ.get("HOLD_SIZE_USDT", "50"))
MAX_POSITIONS = 3
COOLDOWN_S = 4 * 3600       # на символ после закрытия

# ── правило удержания (СУТЬ ТЕСТА) ──
MIN_SETTLEMENTS = 1         # не выходим, пока не переживём столько начислений
MAX_HOLD_HOURS = 30.0       # предохранитель от вечной позиции
EMERG_BASIS_PCT = 1.20      # аварийный выход: basis-убыток > этого % нотионала

# ── комиссии (считаем ОБА режима на каждой сделке — бесплатная чувствительность) ──
TAKER_CYCLE = 0.00055 * 4   # 0.220%
MAKER_CYCLE = 0.00020 * 4   # 0.080%

POLL_SEC = 60
FUNDING_HOURS = (0, 8, 16)

_tg_token = ""
_tg_chat = ""
_cooldown: dict[str, float] = {}
_by_intervals: dict[str, int] = {}
_bn_intervals: dict[str, int] = {}
_int_ts = 0.0


def _get(url: str, tries: int = 3):
    for i in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=20) as r:
                return json.loads(r.read())
        except Exception:
            if i == tries - 1:
                raise
            time.sleep(0.5 * (i + 1))


def _tg(text: str) -> None:
    if not _tg_token or not _tg_chat:
        return
    try:
        data = urllib.parse.urlencode({"chat_id": _tg_chat, "text": text}).encode()
        urllib.request.urlopen(
            f"https://api.telegram.org/bot{_tg_token}/sendMessage", data, timeout=8)
    except Exception as e:
        logger.warning(f"TG failed: {e}")


def _next_settlement(now: datetime) -> datetime:
    cands = []
    for d in (0, 1):
        base = (now + timedelta(days=d)).replace(minute=0, second=0, microsecond=0)
        for h in FUNDING_HOURS:
            t = base.replace(hour=h)
            if t > now:
                cands.append(t)
    return min(cands)


def _settlements_between(a: datetime, b: datetime) -> int:
    n, t = 0, a.replace(minute=0, second=0, microsecond=0)
    while t <= b:
        if t.hour in FUNDING_HOURS and a < t <= b:
            n += 1
        t += timedelta(hours=1)
    return n


def _refresh_intervals() -> None:
    global _by_intervals, _bn_intervals, _int_ts
    if _by_intervals and time.time() - _int_ts < 3600:
        return
    try:
        out, cursor = {}, ""
        while True:
            u = f"https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000"
            if cursor:
                u += f"&cursor={urllib.parse.quote(cursor)}"
            d = _get(u)
            for it in d["result"]["list"]:
                out[it["symbol"]] = int(it.get("fundingInterval", 480))
            cursor = d["result"].get("nextPageCursor", "")
            if not cursor:
                break
        _by_intervals = out
        d = _get("https://fapi.binance.com/fapi/v1/fundingInfo")
        _bn_intervals = {it["symbol"]: int(it.get("fundingIntervalHours", 8))
                         for it in d if it.get("symbol")}
        _int_ts = time.time()
    except Exception as e:
        logger.warning(f"intervals refresh failed: {e}")
        _int_ts = time.time()


def scan() -> list[dict]:
    """Кандидаты: спред фандинга + исполнимые bid/ask обеих бирж."""
    _refresh_intervals()
    by = _get("https://api.bybit.com/v5/market/tickers?category=linear")
    byd = {}
    for it in by["result"]["list"]:
        s, fr = it.get("symbol", ""), it.get("fundingRate", "")
        if not s.endswith("USDT") or not fr:
            continue
        try:
            byd[s] = {
                "fr": float(fr),
                "bid": float(it.get("bid1Price") or 0),
                "ask": float(it.get("ask1Price") or 0),
                "next": int(it.get("nextFundingTime") or 0),
                "turn": float(it.get("turnover24h") or 0),
            }
        except ValueError:
            continue

    bn = _get("https://fapi.binance.com/fapi/v1/premiumIndex")
    bnd = {}
    for it in bn:
        s, fr = it.get("symbol", ""), it.get("lastFundingRate", "")
        if s.endswith("USDT") and fr:
            try:
                bnd[s] = {"fr": float(fr), "next": int(it.get("nextFundingTime") or 0)}
            except ValueError:
                continue
    try:
        for it in _get("https://fapi.binance.com/fapi/v1/ticker/bookTicker"):
            s = it.get("symbol", "")
            if s in bnd:
                bnd[s]["bid"] = float(it.get("bidPrice") or 0)
                bnd[s]["ask"] = float(it.get("askPrice") or 0)
    except Exception as e:
        logger.warning(f"binance book failed: {e}")

    out = []
    skipped_mismatch = []
    for s in set(byd) & set(bnd):
        b, n = byd[s], bnd[s]
        if not all((b["bid"], b["ask"], n.get("bid"), n.get("ask"))):
            continue

        # ⚠️ ГЛАВНАЯ ЗАЩИТА: один тикер ≠ один актив (см. MAX_PRICE_DISLOCATION)
        by_mid = (b["bid"] + b["ask"]) / 2
        bn_mid = (n["bid"] + n["ask"]) / 2
        if by_mid <= 0 or bn_mid <= 0:
            continue
        disloc = abs(by_mid - bn_mid) / min(by_mid, bn_mid) * 100
        if disloc > MAX_PRICE_DISLOCATION:
            skipped_mismatch.append((s, by_mid, bn_mid, disloc))
            continue

        by_daily = b["fr"] * (1440 / _by_intervals.get(s, 480)) * 100
        bn_daily = n["fr"] * (24 / _bn_intervals.get(s, 8)) * 100
        spread = by_daily - bn_daily
        width = ((b["ask"] - b["bid"]) / ((b["ask"] + b["bid"]) / 2)
                 + (n["ask"] - n["bid"]) / ((n["ask"] + n["bid"]) / 2)) * 100
        out.append({
            "symbol": s, "spread": spread, "by_daily": by_daily,
            "bn_daily": bn_daily, "width": width, "turn": b["turn"],
            "by_bid": b["bid"], "by_ask": b["ask"],
            "bn_bid": n["bid"], "bn_ask": n["ask"],
        })
    if skipped_mismatch:
        top = sorted(skipped_mismatch, key=lambda x: -x[3])[:4]
        logger.warning(
            "отсеяно по расхождению цен (РАЗНЫЕ активы под одним тикером): "
            + ", ".join(f"{s} {bm:g}/{nm:g} ({d:.0f}%)" for s, bm, nm, d in top))
    out.sort(key=lambda r: -abs(r["spread"]))
    return out


def _settled_sum(symbol: str, exchange: str, since_ms: int) -> float:
    """Σ фактически начисленных ставок с since_ms."""
    try:
        if exchange == "bybit":
            d = _get(f"https://api.bybit.com/v5/market/funding/history"
                     f"?category=linear&symbol={symbol}&limit=200")
            rows = [(int(r["fundingRateTimestamp"]), float(r["fundingRate"]))
                    for r in d["result"]["list"]]
        else:
            d = _get(f"https://fapi.binance.com/fapi/v1/fundingRate"
                     f"?symbol={symbol}&limit=200")
            rows = [(int(r["fundingTime"]), float(r["fundingRate"])) for r in d]
        return sum(r for t, r in rows if t >= since_ms)
    except Exception as e:
        logger.warning(f"settled {exchange} {symbol}: {e}")
        return 0.0


async def open_pos(engine, c: dict) -> int | None:
    now = datetime.now(timezone.utc)
    hts = (_next_settlement(now) - now).total_seconds() / 3600
    # spread>0: фандинг Bybit выше → шортим Bybit, лонгуем Binance
    short_bybit = c["spread"] > 0
    by_entry = c["by_bid"] if short_bybit else c["by_ask"]
    bn_entry = c["bn_ask"] if short_bybit else c["bn_bid"]
    qty = round(SIZE_USDT / by_entry, 8)
    if qty <= 0:
        return None
    pos = HoldPosition(
        symbol=c["symbol"],
        direction="short_bybit" if short_bybit else "short_binance",
        size_usdt=SIZE_USDT, qty=qty,
        bybit_entry_price=by_entry, binance_entry_price=bn_entry,
        entry_spread_daily_pct=round(c["spread"], 4),
        entry_book_width_pct=round(c["width"], 4),
        hours_to_settle_at_entry=round(hts, 2),
        settlements_survived=0,
        status="open", paper=True, opened_at=now,
    )
    with Session(engine) as s:
        s.add(pos); s.commit(); pid = pos.id
    logger.info(f"OPEN {c['symbol']} {pos.direction} spread={c['spread']:+.2f}%/д "
                f"width={c['width']:.3f}% до_сеттл={hts:.1f}ч id={pid}")
    _tg(f"🟢 [HOLD-TEST] OPEN {c['symbol']}\n"
        f"{pos.direction} | спред {c['spread']:+.2f}%/день\n"
        f"By {c['by_daily']:+.2f} / Bn {c['bn_daily']:+.2f} %/д\n"
        f"Ширина стаканов {c['width']:.3f}% | до сеттлмента {hts:.1f}ч\n"
        f"⏳ МИН.ХОЛД: не выйду до начисления | ID {pid}")
    return pid


async def close_pos(engine, pid: int, reason: str) -> None:
    with Session(engine) as s:
        p = s.get(HoldPosition, pid)
        if not p or p.status != "open":
            return
        sym, direction, qty, size = p.symbol, p.direction, p.qty, p.size_usdt
        by_in, bn_in = p.bybit_entry_price, p.binance_entry_price
        opened = p.opened_at.replace(tzinfo=timezone.utc)

    try:
        cands = {c["symbol"]: c for c in scan()}
        c = cands.get(sym)
        if not c:
            logger.warning(f"close {sym}: нет котировок, отложено")
            return
    except Exception as e:
        logger.error(f"close scan {sym}: {e}")
        return

    short_bybit = direction == "short_bybit"
    by_out = c["by_ask"] if short_bybit else c["by_bid"]
    bn_out = c["bn_bid"] if short_bybit else c["bn_ask"]
    # basis: шорт-нога (entry−exit), лонг-нога (exit−entry)
    by_leg = qty * ((by_in - by_out) if short_bybit else (by_out - by_in))
    bn_leg = qty * ((bn_out - bn_in) if short_bybit else (bn_in - bn_out))
    basis = round(by_leg + bn_leg, 4)

    since = int(opened.timestamp() * 1000)
    by_s = _settled_sum(sym, "bybit", since)
    bn_s = _settled_sum(sym, "binance", since)
    # шортим Bybit → получаем by_rate; лонг Binance → платим bn_rate
    funding = round(size * ((by_s - bn_s) if short_bybit else (bn_s - by_s)), 4)

    fees_t = round(size * TAKER_CYCLE, 4)
    fees_m = round(size * MAKER_CYCLE, 4)
    now = datetime.now(timezone.utc)
    n_sett = _settlements_between(opened, now)

    with Session(engine) as s:
        p = s.get(HoldPosition, pid)
        p.bybit_exit_price, p.binance_exit_price = by_out, bn_out
        p.basis_pnl_usdt = basis
        p.funding_collected_usdt = funding
        p.fees_taker_usdt, p.fees_maker_usdt = fees_t, fees_m
        p.pnl_taker_usdt = round(basis + funding - fees_t, 4)
        p.pnl_maker_usdt = round(basis + funding - fees_m, 4)
        p.settlements_survived = n_sett
        p.hold_hours = round((now - opened).total_seconds() / 3600, 2)
        p.exit_reason = reason
        p.status = "closed"
        p.closed_at = now
        s.commit()
        pt, pm, hh = p.pnl_taker_usdt, p.pnl_maker_usdt, p.hold_hours

    _cooldown[sym] = time.time()
    logger.info(f"CLOSE {sym} ({reason}) сеттл={n_sett} холд={hh:.1f}ч "
                f"fund={funding:+.4f} basis={basis:+.4f} "
                f"PnL taker={pt:+.4f} maker={pm:+.4f}")
    _tg(f"🔴 [HOLD-TEST] CLOSE {sym} ({reason})\n"
        f"Сеттлментов пережито: {n_sett} | холд {hh:.1f}ч\n"
        f"Фандинг {funding:+.4f} | Basis {basis:+.4f}\n"
        f"PnL taker {pt:+.4f} | maker {pm:+.4f}\n"
        f"ID {pid}")


async def manage(engine) -> None:
    """Ключевая логика: НЕ выходим до начисления (кроме аварии)."""
    now = datetime.now(timezone.utc)
    with Session(engine) as s:
        rows = s.execute(select(HoldPosition).where(
            HoldPosition.status == "open")).scalars().all()
        opens = [{"id": p.id, "symbol": p.symbol, "direction": p.direction,
                  "qty": p.qty, "size": p.size_usdt,
                  "by_in": p.bybit_entry_price, "bn_in": p.binance_entry_price,
                  "opened": p.opened_at.replace(tzinfo=timezone.utc)} for p in rows]
    if not opens:
        return

    try:
        cands = {c["symbol"]: c for c in scan()}
    except Exception as e:
        logger.error(f"manage scan: {e}")
        return

    for p in opens:
        c = cands.get(p["symbol"])
        n_sett = _settlements_between(p["opened"], now)
        hold_h = (now - p["opened"]).total_seconds() / 3600

        # текущий нереализованный basis — для аварийного выхода
        if c:
            sb = p["direction"] == "short_bybit"
            by_out = c["by_ask"] if sb else c["by_bid"]
            bn_out = c["bn_bid"] if sb else c["bn_ask"]
            by_leg = p["qty"] * ((p["by_in"] - by_out) if sb else (by_out - p["by_in"]))
            bn_leg = p["qty"] * ((bn_out - p["bn_in"]) if sb else (p["bn_in"] - bn_out))
            basis_pct = (by_leg + bn_leg) / p["size"] * 100
            if basis_pct < -EMERG_BASIS_PCT:
                logger.warning(f"{p['symbol']}: АВАРИЯ basis {basis_pct:.2f}%")
                await close_pos(engine, p["id"], f"emergency basis {basis_pct:.2f}%")
                continue

        # ГЛАВНОЕ ПРАВИЛО: не выходим, пока не пережили начисление
        if n_sett < MIN_SETTLEMENTS:
            if hold_h > MAX_HOLD_HOURS:
                await close_pos(engine, p["id"], f"max_hold {hold_h:.1f}ч без сеттлмента")
            continue

        # начисление пережито → выходим, если спред больше не в нашу пользу
        if c is None:
            continue
        sb = p["direction"] == "short_bybit"
        still_good = (c["spread"] > MIN_SPREAD_DAILY * 0.5) if sb \
            else (c["spread"] < -MIN_SPREAD_DAILY * 0.5)
        if not still_good:
            await close_pos(engine, p["id"], f"спред выдохся ({c['spread']:+.2f}%/д)")
        elif hold_h > MAX_HOLD_HOURS:
            await close_pos(engine, p["id"], f"max_hold {hold_h:.1f}ч")


async def entries(engine) -> None:
    now = datetime.now(timezone.utc)
    hts = (_next_settlement(now) - now).total_seconds() / 3600
    if hts > MAX_HOURS_TO_SETTLE:
        return

    with Session(engine) as s:
        open_syms = {r[0] for r in s.execute(select(HoldPosition.symbol).where(
            HoldPosition.status == "open")).all()}
    if len(open_syms) >= MAX_POSITIONS:
        return

    try:
        cands = scan()
    except Exception as e:
        logger.error(f"entries scan: {e}")
        return

    slots = MAX_POSITIONS - len(open_syms)
    for c in cands:
        if slots <= 0:
            break
        s_abs = abs(c["spread"])
        if not (MIN_SPREAD_DAILY <= s_abs <= MAX_SPREAD_DAILY):
            continue
        if c["width"] > MAX_BOOK_WIDTH:
            continue
        if c["symbol"] in open_syms:
            continue
        if time.time() - _cooldown.get(c["symbol"], 0) < COOLDOWN_S:
            continue
        if await open_pos(engine, c):
            open_syms.add(c["symbol"])
            slots -= 1


async def report(engine) -> None:
    """Сводка против ЗАРАНЕЕ зафиксированных критериев."""
    while True:
        await asyncio.sleep(6 * 3600)
        try:
            with Session(engine) as s:
                rows = s.execute(select(
                    HoldPosition.pnl_taker_usdt, HoldPosition.pnl_maker_usdt,
                    HoldPosition.settlements_survived
                ).where(HoldPosition.status == "closed")).all()
            if not rows:
                continue
            pt = [r[0] or 0 for r in rows]
            pm = [r[1] or 0 for r in rows]
            surv = sum(1 for r in rows if (r[2] or 0) >= MIN_SETTLEMENTS)
            n = len(rows)
            best = max(pt) if pt else 0
            tot = sum(pt)
            c1 = n >= 25
            c2 = tot > 0
            c3 = statistics.median(pt) > 0
            c4 = (best / tot < 0.40) if tot > 0 else False
            c5 = surv / n > 0.90
            logger.info(f"[report] n={n} Σtaker={tot:+.3f} Σmaker={sum(pm):+.3f} "
                        f"surv={surv}/{n}")
            _tg(f"📊 HOLD-TEST против критериев\n"
                f"Закрыто: {n}\n"
                f"Σ PnL taker: {tot:+.4f} | maker: {sum(pm):+.4f}\n"
                f"Медиана taker: {statistics.median(pt):+.4f}\n"
                f"Дожили до начисления: {surv}/{n} ({surv/n*100:.0f}%)\n"
                f"\nКритерии:\n"
                f"{'✅' if c1 else '⬜'} n≥25 ({n})\n"
                f"{'✅' if c2 else '❌'} Σtaker>0\n"
                f"{'✅' if c3 else '❌'} медиана>0\n"
                f"{'✅' if c4 else '❌'} лучшая<40% Σ\n"
                f"{'✅' if c5 else '❌'} дожитие>90%")
        except Exception as e:
            logger.warning(f"report: {e}")


async def main() -> None:
    global _tg_token, _tg_chat
    cfg = load_config()
    _tg_token, _tg_chat = cfg.telegram_token, cfg.telegram_chat_id
    logger.remove(); logger.add(sys.stderr, level="INFO")
    engine = init_db(cfg.database_url)

    logger.info(f"HOLD-TEST PAPER | спред {MIN_SPREAD_DAILY}-{MAX_SPREAD_DAILY}%/д | "
                f"вход ≤{MAX_HOURS_TO_SETTLE}ч до сеттлмента | "
                f"мин.холд {MIN_SETTLEMENTS} начисление | size=${SIZE_USDT}")
    _tg(f"⏳ HOLD-TEST запущен (единственный вариант, без A/B)\n"
        f"Исправляем баг: 73% прошлых сделок не дожили до начисления\n"
        f"Вход: спред {MIN_SPREAD_DAILY}-{MAX_SPREAD_DAILY}%/д, ≤{MAX_HOURS_TO_SETTLE}ч "
        f"до сеттлмента, стаканы ≤{MAX_BOOK_WIDTH}%\n"
        f"Выход: ТОЛЬКО после начисления (авария при basis <−{EMERG_BASIS_PCT}%)\n"
        f"Критерии успеха зафиксированы заранее: n≥25, Σtaker>0, медиана>0,\n"
        f"лучшая<40%Σ, дожитие>90%")

    asyncio.create_task(report(engine))
    while True:
        try:
            await manage(engine)
            await entries(engine)
        except Exception as e:
            logger.error(f"loop: {e}")
        await asyncio.sleep(POLL_SEC)


if __name__ == "__main__":
    asyncio.run(main())

"""RISEx ↔ Bybit: живой замер дифференциала + PAPER delta-neutral позиция.

═══ ЗАЧЕМ ═══
Гипотеза 14 (`scripts/risex_funding_diff.py`) на 91 дне истории показала:
дифференциал фандинга RISEx vs Bybit **положителен почти на всех символах**
(+2…+10%/год), потому что на новой площадке ставки систематически выше — розничный
лонг-байас и отсутствие клампа. Но формальные критерии НЕ пройдены: прошёл один
символ из требуемых трёх (HYPE +10.0%/год, устойчивость знака 79%).

Ключевая неопределённость — именно устойчивость знака: 79% означает, что **каждый
пятый день дифференциал против нас**. На истории это видно, на живых данных — нет.
Этот модуль записывает живой поток и ведёт paper-позицию, чтобы проверить.

Поинты RISEx Ignite Season 1 начисляются за **open interest × время удержания**
(не за оборот), поэтому churn не нужен — позиция просто держится. Значит
дифференциал это ЕДИНСТВЕННАЯ измеримая статья дохода, а поинты — бесплатное
приложение с неизвестной стоимостью.

═══ КРИТЕРИИ (ЗАФИКСИРОВАНЫ 4 АВГ ДО ЗАПУСКА) ═══
Переходим к реальным деньгам, только если выполнено ВСЁ:
  1. n ≥ 14 дней живой записи
  2. МЕДИАНА дифференциала > +0.02%/день — ТОТ ЖЕ порог, что в историческом
     тесте. Задним числом не двигаем (урок №9: вывод фазы 21 опровергнут через
     сутки именно из-за подгонки под результат)
  3. Устойчивость знака ≥ 70% дней на ЖИВЫХ данных
  4. Paper-PnL по ФАКТИЧЕСКИ начисленным ставкам > 0 после комиссий тейкера
  5. Расхождение mark-цен площадок ≤ 0.5% в 95-м процентиле
     ← новый критерий: если цены разъезжаются, «delta-neutral» им не является,
       и это ровно тот риск голой ноги, что убил гипотезу мейкера (17% непарных
       заливов ценой +34 bps)

Провал любого → носитель не годится, к деньгам не переходим.

═══ УЧТЁННЫЕ ЛОВУШКИ ═══
  • RISEx отдаёт 403 на дефолтный User-Agent urllib (curl работает) — ставим UA
  • предсказанное ≠ начисленное: accrual только по settled-истории (урок №4)
  • идемпотентность: settled пишется по уникальному (venue, symbol, settle_ms)
  • нулевые данные ≠ отсутствие края: логируем причину каждого пропуска

Run: python -m funding.risex_paper
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
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from loguru import logger
from sqlalchemy import select, func
from sqlalchemy.orm import Session

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from shared.config import load_config
from shared.db import (init_db, VenueFundingSnap, VenueFundingSettled,
                       RisexPaperPosition)

RISEX = "https://api.rise.trade"
BYBIT = "https://api.bybit.com"
_UA = {"User-Agent": "Mozilla/5.0 (traderbot research)"}

# HYPE — единственный символ, прошедший исторические критерии.
# Остальные пишем для наблюдения: BNB и XRP отсеклись на волосок (порог 0.02%/д
# и OI $0.20M), интересно увидеть их на живых данных.
TRACK = ["HYPE", "BNB", "XRP", "ETH", "SOL", "DOGE"]
PRIMARY = "HYPE"                 # на нём открываем paper-позицию

SNAP_EVERY_S = 300               # снимок каждые 5 мин
SETTLED_EVERY_S = 1800           # settled-история каждые 30 мин
REPORT_EVERY_S = 6 * 3600
SIZE_USDT = float(os.environ.get("RISEX_SIZE_USDT", "500"))

# ФАКТИЧЕСКИЕ ставки (проверены через API 3 авг 2026)
FEE_RISEX_TAKER, FEE_RISEX_MAKER = 0.0003, 0.0001
FEE_BYBIT_TAKER, FEE_BYBIT_MAKER = 0.0010, 0.00036
CYCLE_TAKER = (FEE_RISEX_TAKER + FEE_BYBIT_TAKER) * 2   # 0.26%
CYCLE_MAKER = (FEE_RISEX_MAKER + FEE_BYBIT_MAKER) * 2   # 0.092%

_tg_token = _tg_chat = ""
_rx_ids: dict[str, str] = {}     # BASE → market_id
_by_intervals: dict[str, float] = {}


def _get(url: str, tries: int = 3):
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers=_UA)
            with urllib.request.urlopen(req, timeout=25) as r:
                return json.loads(r.read())
        except Exception as e:
            if i == tries - 1:
                logger.warning(f"GET {url[:60]}… → {type(e).__name__}: {str(e)[:50]}")
                return None
            time.sleep(0.5 * (i + 1))
    return None


def _tg(text: str) -> None:
    if not _tg_token or not _tg_chat:
        return
    try:
        data = urllib.parse.urlencode({"chat_id": _tg_chat, "text": text}).encode()
        urllib.request.urlopen(
            f"https://api.telegram.org/bot{_tg_token}/sendMessage", data, timeout=8)
    except Exception as e:
        logger.warning(f"TG: {e}")


# ── источники ────────────────────────────────────────────────────────────────
def risex_snapshot() -> dict[str, dict]:
    d = _get(f"{RISEX}/v1/markets")
    if not d:
        return {}
    out = {}
    for m in d.get("data", {}).get("markets", []):
        name = m.get("display_name") or ""
        if not m.get("active") or "deprecated" in name or "/" not in name:
            continue
        base = name.split("/")[0]
        if base not in TRACK:
            continue
        try:
            mark = float(m.get("mark_price") or 0)
            iv_h = float(m.get("funding_interval") or 3.6e12) / 3.6e12
            rate = float(m.get("current_funding_rate") or 0)
            oi = float(m.get("open_interest") or 0)
        except (TypeError, ValueError):
            continue
        _rx_ids[base] = str(m.get("market_id"))
        out[base] = {"rate": rate, "interval_h": iv_h, "mark": mark,
                     "oi_usd": oi * mark,
                     "next_ms": float(m.get("next_funding_time") or 0) / 1e6}
    return out


def bybit_snapshot() -> dict[str, dict]:
    d = _get(f"{BYBIT}/v5/market/tickers?category=linear")
    if not d:
        return {}
    out = {}
    for it in d.get("result", {}).get("list", []):
        s = it.get("symbol", "")
        if not s.endswith("USDT"):
            continue
        base = s[:-4]
        if base not in TRACK:
            continue
        try:
            out[base] = {
                "rate": float(it.get("fundingRate") or 0),
                "interval_h": _by_intervals.get(s, 8.0),
                "mark": float(it.get("markPrice") or it.get("lastPrice") or 0),
                "oi_usd": float(it.get("openInterestValue") or 0),
                "next_ms": float(it.get("nextFundingTime") or 0),
            }
        except (TypeError, ValueError):
            continue
    return out


def load_bybit_intervals() -> None:
    cursor = ""
    while True:
        u = f"{BYBIT}/v5/market/instruments-info?category=linear&limit=1000"
        if cursor:
            u += f"&cursor={urllib.parse.quote(cursor)}"
        d = _get(u)
        if not d:
            return
        for it in d["result"]["list"]:
            _by_intervals[it["symbol"]] = int(it.get("fundingInterval", 480)) / 60
        cursor = d["result"].get("nextPageCursor", "")
        if not cursor:
            return


# ── запись снимков ───────────────────────────────────────────────────────────
async def snapshot_loop(engine) -> None:
    while True:
        try:
            rx, by = risex_snapshot(), bybit_snapshot()
            now = datetime.now(timezone.utc)
            rows = []
            for venue, data in (("risex", rx), ("bybit", by)):
                for base, v in data.items():
                    daily = v["rate"] * (24 / v["interval_h"]) * 100
                    rows.append(VenueFundingSnap(
                        venue=venue, symbol=base, funding_rate=v["rate"],
                        interval_h=v["interval_h"], rate_daily_pct=daily,
                        mark_price=v["mark"], open_interest_usd=v["oi_usd"],
                        next_funding_ms=v["next_ms"], ts=now))
            if rows:
                with Session(engine) as s:
                    s.add_all(rows); s.commit()
            # живой дифференциал в лог
            line = []
            for b in TRACK:
                if b in rx and b in by:
                    r = rx[b]["rate"] * (24 / rx[b]["interval_h"]) * 100
                    q = by[b]["rate"] * (24 / by[b]["interval_h"]) * 100
                    line.append(f"{b} {r-q:+.4f}")
            logger.info(f"снимок: {len(rows)} строк | дифф %/д: {' | '.join(line)}")
        except Exception as e:
            logger.error(f"snapshot_loop: {e}")
        await asyncio.sleep(SNAP_EVERY_S)


# ── settled-история (единственный источник для accrual) ──────────────────────
def fetch_risex_settled(base: str, since_ms: int):
    mid = _rx_ids.get(base)
    if not mid:
        return []
    d = _get(f"{RISEX}/v1/markets/id/{mid}/funding-rate-history"
             f"?page=1&limit=1000&start_time={int(since_ms*1e6)}"
             f"&end_time={int(time.time()*1e9)}")
    if not d:
        return []
    recs = (d.get("data") or {}).get("records") or []
    out = []
    for r in recs:
        try:
            out.append((int(r["start_time"]) / 1e6, float(r["funding_rate"]),
                        float(r.get("index_price") or 0)))
        except (KeyError, ValueError):
            continue
    return out


def fetch_bybit_settled(base: str, since_ms: int):
    d = _get(f"{BYBIT}/v5/market/funding/history?category=linear"
             f"&symbol={base}USDT&startTime={int(since_ms)}"
             f"&endTime={int(time.time()*1000)}&limit=200")
    if not d:
        return []
    out = []
    for r in d.get("result", {}).get("list", []):
        try:
            out.append((float(r["fundingRateTimestamp"]),
                        float(r["fundingRate"]), 0.0))
        except (KeyError, ValueError):
            continue
    return out


async def settled_loop(engine) -> None:
    while True:
        try:
            with Session(engine) as s:
                pos = s.execute(select(RisexPaperPosition).where(
                    RisexPaperPosition.status == "open")).scalars().first()
                since = (pos.opened_at.replace(tzinfo=timezone.utc).timestamp() * 1000
                         if pos and pos.opened_at
                         else (time.time() - 7 * 86400) * 1000)
                have = {(v, sy, int(ms)) for v, sy, ms in s.execute(select(
                    VenueFundingSettled.venue, VenueFundingSettled.symbol,
                    VenueFundingSettled.settle_ms)).all()}

            added = 0
            for base in TRACK:
                for venue, fn in (("risex", fetch_risex_settled),
                                  ("bybit", fetch_bybit_settled)):
                    for ms, rate, idx in fn(base, since):
                        if (venue, base, int(ms)) in have:
                            continue
                        with Session(engine) as s:
                            s.add(VenueFundingSettled(
                                venue=venue, symbol=base, funding_rate=rate,
                                settle_ms=ms, index_price=idx))
                            s.commit()
                        have.add((venue, base, int(ms)))
                        added += 1
                    time.sleep(0.12)
            if added:
                logger.info(f"settled: +{added} новых начислений")
            await accrue(engine)
        except Exception as e:
            logger.error(f"settled_loop: {e}")
        await asyncio.sleep(SETTLED_EVERY_S)


async def accrue(engine) -> None:
    """Пересчёт paper-позиции по ФАКТИЧЕСКИ начисленным ставкам. Идемпотентно."""
    with Session(engine) as s:
        pos = s.execute(select(RisexPaperPosition).where(
            RisexPaperPosition.status == "open")).scalars().first()
        if not pos:
            return
        opened_ms = pos.opened_at.replace(tzinfo=timezone.utc).timestamp() * 1000
        rows = s.execute(select(
            VenueFundingSettled.venue, VenueFundingSettled.funding_rate
        ).where(VenueFundingSettled.symbol == pos.symbol,
                VenueFundingSettled.settle_ms >= opened_ms)).all()

        short_risex = pos.direction == "short_risex"
        f_rx = f_by = 0.0
        for venue, rate in rows:
            if venue == "risex":
                f_rx += (rate if short_risex else -rate)
            else:
                f_by += (-rate if short_risex else rate)
        pos.funding_risex_usdt = round(pos.size_usdt * f_rx, 6)
        pos.funding_bybit_usdt = round(pos.size_usdt * f_by, 6)
        pos.funding_net_usdt = round(pos.funding_risex_usdt
                                     + pos.funding_bybit_usdt, 6)
        pos.days_held = round((datetime.now(timezone.utc)
                               - pos.opened_at.replace(tzinfo=timezone.utc))
                              .total_seconds() / 86400, 3)
        pos.fees_taker_usdt = round(pos.size_usdt * CYCLE_TAKER, 4)
        pos.fees_maker_usdt = round(pos.size_usdt * CYCLE_MAKER, 4)
        pos.pnl_taker_usdt = round(pos.funding_net_usdt
                                   + (pos.basis_pnl_usdt or 0)
                                   - pos.fees_taker_usdt, 4)
        pos.pnl_maker_usdt = round(pos.funding_net_usdt
                                   + (pos.basis_pnl_usdt or 0)
                                   - pos.fees_maker_usdt, 4)
        s.commit()


async def open_paper(engine) -> None:
    """Открывает paper-позицию на PRIMARY, если её нет."""
    with Session(engine) as s:
        if s.execute(select(RisexPaperPosition).where(
                RisexPaperPosition.status == "open")).scalars().first():
            return
    rx, by = risex_snapshot(), bybit_snapshot()
    if PRIMARY not in rx or PRIMARY not in by:
        logger.warning(f"{PRIMARY}: нет котировок на обеих площадках, "
                       f"позиция не открыта")
        return
    r = rx[PRIMARY]["rate"] * (24 / rx[PRIMARY]["interval_h"]) * 100
    q = by[PRIMARY]["rate"] * (24 / by[PRIMARY]["interval_h"]) * 100
    diff = r - q
    # ставка RISEx выше → шортим RISEx (получаем) и лонгуем Bybit
    direction = "short_risex" if diff > 0 else "long_risex"
    with Session(engine) as s:
        p = RisexPaperPosition(
            symbol=PRIMARY, direction=direction, size_usdt=SIZE_USDT,
            risex_entry_price=rx[PRIMARY]["mark"],
            bybit_entry_price=by[PRIMARY]["mark"],
            entry_diff_daily_pct=round(abs(diff), 5),
            basis_pnl_usdt=0.0, status="open", paper=True,
            opened_at=datetime.now(timezone.utc))
        s.add(p); s.commit(); pid = p.id
    logger.info(f"PAPER OPEN {PRIMARY} {direction} дифф={abs(diff):+.4f}%/д "
                f"RISEx={rx[PRIMARY]['mark']} Bybit={by[PRIMARY]['mark']} id={pid}")
    _tg(f"🧪 RISEx paper-позиция открыта\n{PRIMARY} {direction}\n"
        f"Дифференциал входа: {abs(diff):+.4f}%/день ({abs(diff)*365:+.1f}%/год)\n"
        f"Нотионал ${SIZE_USDT:.0f}/нога | комиссии цикла {CYCLE_TAKER*100:.3f}%\n"
        f"Поинты Ignite копятся за OI × время — churn не нужен")


async def mark_basis(engine) -> None:
    """Обновляет basis по расхождению mark-цен площадок (критерий 5)."""
    while True:
        await asyncio.sleep(600)
        try:
            rx, by = risex_snapshot(), bybit_snapshot()
            with Session(engine) as s:
                pos = s.execute(select(RisexPaperPosition).where(
                    RisexPaperPosition.status == "open")).scalars().first()
                if not pos or pos.symbol not in rx or pos.symbol not in by:
                    continue
                rx_now, by_now = rx[pos.symbol]["mark"], by[pos.symbol]["mark"]
                sr = pos.direction == "short_risex"
                # шорт RISEx: (entry−now); лонг Bybit: (now−entry)
                leg_rx = (pos.risex_entry_price - rx_now) / pos.risex_entry_price
                leg_by = (by_now - pos.bybit_entry_price) / pos.bybit_entry_price
                if not sr:
                    leg_rx, leg_by = -leg_rx, -leg_by
                pos.basis_pnl_usdt = round(pos.size_usdt * (leg_rx + leg_by), 4)
                pos.risex_exit_price, pos.bybit_exit_price = rx_now, by_now
                s.commit()
        except Exception as e:
            logger.error(f"mark_basis: {e}")


async def report(engine) -> None:
    """Сводка против ЗАРАНЕЕ зафиксированных критериев."""
    while True:
        await asyncio.sleep(REPORT_EVERY_S)
        try:
            with Session(engine) as s:
                snaps = s.execute(select(
                    VenueFundingSnap.venue, VenueFundingSnap.symbol,
                    VenueFundingSnap.rate_daily_pct, VenueFundingSnap.mark_price,
                    VenueFundingSnap.ts)).all()
                pos = s.execute(select(RisexPaperPosition).where(
                    RisexPaperPosition.status == "open")).scalars().first()
            if not snaps:
                continue

            # дифференциал по дням для PRIMARY
            byday = defaultdict(dict)
            marks = defaultdict(dict)
            for venue, sym, daily, mark, ts in snaps:
                if sym != PRIMARY:
                    continue
                d = ts.strftime("%Y-%m-%d")
                byday[d].setdefault(venue, []).append(daily or 0)
                marks[d].setdefault(venue, []).append(mark or 0)
            diffs, gaps = [], []
            for d, v in byday.items():
                if "risex" in v and "bybit" in v:
                    diffs.append(statistics.median(v["risex"])
                                 - statistics.median(v["bybit"]))
            for d, v in marks.items():
                if "risex" in v and "bybit" in v:
                    r, b = statistics.median(v["risex"]), statistics.median(v["bybit"])
                    if b:
                        gaps.append(abs(r - b) / b * 100)
            if not diffs:
                continue

            days = len(diffs)
            med = statistics.median(diffs)
            stab = sum(1 for x in diffs if (x > 0) == (med > 0)) / len(diffs)
            gaps.sort()
            p95 = gaps[int(len(gaps) * 0.95)] if gaps else 0.0

            c1 = days >= 14
            c2 = abs(med) > 0.02
            c3 = stab >= 0.70
            c4 = (pos.pnl_taker_usdt or 0) > 0 if pos else False
            c5 = p95 <= 0.5
            passed = all((c1, c2, c3, c4, c5))

            logger.info(f"[report] дней={days} медиана={med:+.4f}%/д "
                        f"знак={stab*100:.0f}% p95_gap={p95:.3f}% passed={passed}")
            msg = (f"🧪 RISEx ДИФФЕРЕНЦИАЛ — живой замер\n\n"
                   f"Дней записи: {days}\n"
                   f"Медиана дифференциала: {med:+.4f}%/день "
                   f"({med*365:+.1f}%/год)\n"
                   f"Устойчивость знака: {stab*100:.0f}%\n"
                   f"Расхождение цен p95: {p95:.3f}%\n")
            if pos:
                msg += (f"\nPaper {pos.symbol} {pos.direction}, "
                        f"{pos.days_held or 0:.1f}д:\n"
                        f"  фандинг RISEx {pos.funding_risex_usdt or 0:+.4f}\n"
                        f"  фандинг Bybit {pos.funding_bybit_usdt or 0:+.4f}\n"
                        f"  фандинг НЕТТО {pos.funding_net_usdt or 0:+.4f}\n"
                        f"  basis {pos.basis_pnl_usdt or 0:+.4f}\n"
                        f"  PnL taker {pos.pnl_taker_usdt or 0:+.4f} | "
                        f"maker {pos.pnl_maker_usdt or 0:+.4f}\n")
            msg += (f"\nКритерии (заданы до запуска):\n"
                    f"{'✅' if c1 else '⬜'} 1. n≥14 дней ({days})\n"
                    f"{'✅' if c2 else '❌'} 2. медиана>0.02%/д ({abs(med):.4f})\n"
                    f"{'✅' if c3 else '❌'} 3. знак≥70% ({stab*100:.0f}%)\n"
                    f"{'✅' if c4 else '❌'} 4. paper PnL taker>0\n"
                    f"{'✅' if c5 else '❌'} 5. расхожд.цен p95≤0.5% ({p95:.3f})\n\n"
                    + ("→ носитель подтверждён, можно к малым деньгам"
                       if passed else
                       ("→ рано, копим выборку" if not c1
                        else "→ критерии не пройдены, к деньгам НЕ переходим")))
            _tg(msg)
        except Exception as e:
            logger.warning(f"report: {e}")


async def main() -> None:
    global _tg_token, _tg_chat
    cfg = load_config()
    _tg_token, _tg_chat = cfg.telegram_token, cfg.telegram_chat_id
    logger.remove(); logger.add(sys.stderr, level="INFO")
    engine = init_db(cfg.database_url)

    load_bybit_intervals()
    logger.info(f"RISEx paper | символы={TRACK} | основной={PRIMARY} | "
                f"нотионал=${SIZE_USDT:.0f}/нога")
    logger.info(f"комиссии цикла: тейкер {CYCLE_TAKER*100:.3f}% | "
                f"мейкер {CYCLE_MAKER*100:.3f}%")
    _tg(f"📡 RISEx ↔ Bybit замер запущен\n"
        f"Символы: {', '.join(TRACK)}\n"
        f"Paper-позиция на {PRIMARY}, ${SIZE_USDT:.0f}/нога\n"
        f"Критерии заданы заранее: n≥14д, медиана>0.02%/д, знак≥70%,\n"
        f"PnL taker>0, расхождение цен p95≤0.5%\n"
        f"Сводка каждые 6ч")

    await open_paper(engine)
    await asyncio.gather(snapshot_loop(engine), settled_loop(engine),
                         mark_basis(engine), report(engine))


if __name__ == "__main__":
    asyncio.run(main())

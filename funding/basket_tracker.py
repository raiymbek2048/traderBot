"""Форвард-трекер КОРЗИНЫ RISEx↔Bybit из 6 символов (гипотеза 14b).

═══ ЗАЧЕМ ═══
Анализ 38 дней (scratchpad/risex_basket_analysis.py) на IN-SAMPLE данных показал:
одиночный символ выбрать заранее нельзя (Spearman ист.→жив = −0.03), НО корзина из
6 равновзвешенных символов бьёт одиночный HYPE по всем осям:
    HYPE:    +4.0%/год, σ 0.058, знак 58%, Sharpe 3.6
    корзина: +5.7%/год, σ 0.025, знак 74%, Sharpe 12.0
Диверсификация гасит идиосинкратический basis-шум в 1.88× (√6 предел = 2.45×),
и знак впервые выходит выше порога 70%.

НО это найдено на тех же днях, где стояла позиция → возможен in-sample overfit.
Этот трекер проверяет корзину ВПЕРЁД, на данных строго ПОСЛЕ старта.

Данные пишет сервис risexpaper (venue_funding_snaps + venue_funding_settled по
6 символам). Трекер только читает и считает — живую позицию HYPE не трогает.

Конструкция: short_risex + long_bybit по каждому символу (RISEx ставки выше).
  дневной funding% = Σ(risex settled) − Σ(bybit settled)   за день
  дневной basis%   = bybit_ret − risex_ret (по mark-ценам)
  дневной total%   = funding + basis
  корзина = среднее total% по символам с данными за день (равный вес)

═══ КРИТЕРИИ (ЗАФИКСИРОВАНЫ 11 сен 2026 ДО ФОРВАРД-ЗАПУСКА) ═══
Через ≥14 дней СВЕЖИХ данных корзина подтверждена, если ВСЁ:
  1. n ≥ 14 форвардных дней
  2. медиана дневного дохода корзины > 0
  3. устойчивость знака ≥ 70%          ← in-sample дал 74%
  4. σ корзины < средний σ одиночных   (диверсификация реальна, не артефакт)
  5. Sharpe корзины > 5                 (in-sample дал 12; порог с большим запасом вниз)
Провал любого → корзина = переобучение на один режим, направление закрыто.

Форвардное окно начинается 2026-09-12 00:00 UTC — строго после in-sample (по
2026-09-11). Всё, что раньше, в расчёт форварда НЕ идёт.

Run: python -m funding.basket_tracker
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
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from shared.config import load_config
from shared.db import init_db, VenueFundingSnap, VenueFundingSettled

SYMS = ["HYPE", "BNB", "XRP", "ETH", "SOL", "DOGE"]
# форвардное окно строго ПОСЛЕ in-sample (in-sample: 04.08–11.09)
FORWARD_START_MS = int(datetime(2026, 9, 12, tzinfo=timezone.utc).timestamp() * 1000)
FORWARD_START_DAY = "2026-09-12"
REPORT_EVERY_S = 6 * 3600

_tg_token = _tg_chat = ""


def _tg(text: str) -> None:
    if not _tg_token or not _tg_chat:
        return
    try:
        data = urllib.parse.urlencode({"chat_id": _tg_chat, "text": text}).encode()
        urllib.request.urlopen(
            f"https://api.telegram.org/bot{_tg_token}/sendMessage", data, timeout=8)
    except Exception as e:
        logger.warning(f"TG: {e}")


def _daily_funding(engine, sym: str) -> dict[str, float]:
    """{day: (Σrisex − Σbybit)%} по фактическим начислениям, форвардное окно."""
    with Session(engine) as s:
        rows = s.execute(select(
            VenueFundingSettled.venue, VenueFundingSettled.funding_rate,
            VenueFundingSettled.settle_ms
        ).where(VenueFundingSettled.symbol == sym,
                VenueFundingSettled.settle_ms >= FORWARD_START_MS)).all()
    by_day = defaultdict(lambda: [0.0, 0.0])
    for venue, rate, ms in rows:
        day = datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        by_day[day][0 if venue == "risex" else 1] += rate
    return {d: (rx - by) * 100 for d, (rx, by) in by_day.items()}


def _daily_basis(engine, sym: str) -> dict[str, float]:
    """{day: (bybit_ret − risex_ret)%} по mark-ценам, форвардное окно."""
    with Session(engine) as s:
        rows = s.execute(select(
            VenueFundingSnap.venue, VenueFundingSnap.mark_price, VenueFundingSnap.ts
        ).where(VenueFundingSnap.symbol == sym).order_by(VenueFundingSnap.ts)).all()
    last = defaultdict(dict)
    for venue, price, ts in rows:
        day = ts.strftime("%Y-%m-%d") if hasattr(ts, "strftime") else str(ts)[:10]
        if day < FORWARD_START_DAY:
            continue
        if price and price > 0:
            last[day][venue] = price
    days = sorted(d for d in last if "risex" in last[d] and "bybit" in last[d])
    out = {}
    for i in range(1, len(days)):
        d, p = days[i], days[i - 1]
        if "risex" not in last[p] or "bybit" not in last[p]:
            continue
        rx = (last[d]["risex"] - last[p]["risex"]) / last[p]["risex"] * 100
        by = (last[d]["bybit"] - last[p]["bybit"]) / last[p]["bybit"] * 100
        out[d] = by - rx
    return out


def _stats(vals: list[float]) -> dict | None:
    if len(vals) < 2:
        return None
    m = statistics.mean(vals)
    sd = statistics.stdev(vals)
    return {"n": len(vals), "mean": m, "std": sd, "annual": m * 365,
            "sign": sum(1 for x in vals if x > 0) / len(vals) * 100,
            "sharpe": (m / sd * (365 ** 0.5)) if sd else 0}


def compute(engine) -> dict:
    series = {}
    for sym in SYMS:
        f = _daily_funding(engine, sym)
        b = _daily_basis(engine, sym)
        days = sorted(set(f) & set(b))
        series[sym] = {d: f[d] + b.get(d, 0.0) for d in days}

    all_days = sorted(set().union(*[set(series[s]) for s in SYMS]) or {""})
    basket = []
    for d in all_days:
        vals = [series[s][d] for s in SYMS if d in series[s]]
        if len(vals) >= 4:
            basket.append(statistics.mean(vals))

    single_stds = [_stats(list(series[s].values()))["std"]
                   for s in SYMS if _stats(list(series[s].values()))]
    avg_single_std = statistics.mean(single_stds) if single_stds else 0.0
    bs = _stats(basket)
    return {"basket": bs, "basket_daily": basket, "avg_single_std": avg_single_std,
            "per_symbol": {s: _stats(list(series[s].values())) for s in SYMS}}


async def report_loop(engine) -> None:
    while True:
        try:
            r = compute(engine)
            bs = r["basket"]
            if not bs:
                logger.info(f"[basket] накопление, форвард с {FORWARD_START_DAY}")
                await asyncio.sleep(REPORT_EVERY_S)
                continue
            n = bs["n"]
            median_day = statistics.median(r["basket_daily"])
            c1 = n >= 14
            c2 = median_day > 0            # критерий из докстринга — МЕДИАНА, не mean
            c3 = bs["sign"] >= 70
            c4 = bs["std"] < r["avg_single_std"]
            c5 = bs["sharpe"] > 5
            passed = all((c1, c2, c3, c4, c5))
            damp = (r["avg_single_std"] / bs["std"]) if bs["std"] else 0

            logger.info(f"[basket] дней={n} год={bs['annual']:+.1f}% знак={bs['sign']:.0f}% "
                        f"σ={bs['std']:.4f} sharpe={bs['sharpe']:.1f} passed={passed}")
            msg = (f"🧺 КОРЗИНА RISEx↔Bybit (форвард с {FORWARD_START_DAY})\n\n"
                   f"Форвардных дней: {n}\n"
                   f"Доход: {bs['annual']:+.1f}%/год (медиана {median_day:+.4f}%/день)\n"
                   f"Устойчивость знака: {bs['sign']:.0f}%\n"
                   f"σ корзины: {bs['std']:.4f} | σ ср.одиночного: {r['avg_single_std']:.4f}\n"
                   f"Гашение шума: {damp:.2f}× | Sharpe: {bs['sharpe']:.1f}\n\n")
            ps = r["per_symbol"]
            msg += "По символам (%/год):\n  " + " ".join(
                f"{s}{ps[s]['annual']:+.0f}" for s in SYMS if ps[s]) + "\n\n"
            msg += (f"Критерии (заданы до форварда):\n"
                    f"{'✅' if c1 else '⬜'} 1. n≥14 ({n})\n"
                    f"{'✅' if c2 else '❌'} 2. доход>0\n"
                    f"{'✅' if c3 else '❌'} 3. знак≥70% ({bs['sign']:.0f}%)\n"
                    f"{'✅' if c4 else '❌'} 4. σ корзины<одиночных\n"
                    f"{'✅' if c5 else '❌'} 5. Sharpe>5 ({bs['sharpe']:.1f})\n\n"
                    + ("→ корзина подтверждена out-of-sample"
                       if passed else
                       ("→ рано, копим форвард" if not c1
                        else "→ не подтвердилось, переобучение")))
            _tg(msg)
        except Exception as e:
            logger.error(f"report_loop: {e}")
        await asyncio.sleep(REPORT_EVERY_S)


async def main() -> None:
    global _tg_token, _tg_chat
    cfg = load_config()
    _tg_token, _tg_chat = cfg.telegram_token, cfg.telegram_chat_id
    logger.remove(); logger.add(sys.stderr, level="INFO")
    engine = init_db(cfg.database_url)
    logger.info(f"Basket tracker | символы={SYMS} | форвард с {FORWARD_START_DAY} | "
                f"данные от сервиса risexpaper")
    _tg(f"🧺 Трекер корзины запущен\n"
        f"6 символов, равный вес, форвард с {FORWARD_START_DAY}\n"
        f"In-sample дал +5.7%/год, Sharpe 12, знак 74%\n"
        f"Критерии заранее: n≥14, доход>0, знак≥70%, σ<одиночных, Sharpe>5\n"
        f"Сводка каждые 6ч")
    await report_loop(engine)


if __name__ == "__main__":
    asyncio.run(main())

"""Public Alert Service — красивые алерты для публичного Telegram-канала.

Источники:
  1. Ликвидации (liq_events в БД) → каскад ≥$100k → алерт
  2. Фандинг (API Bybit) → топ-5 экстремальных ставок каждые 4ч
  3. Дневная сводка в 00:05 UTC

Канал настраивается через PUBLIC_CHANNEL_ID в .env (формат: @channel_name или -100...).
Если не задан — алерты идут только в приватный чат (дебаг-режим).

Run: python -m alerts.public
"""
from __future__ import annotations
import asyncio
import json
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone, timedelta

from loguru import logger
from sqlalchemy import select, func
from sqlalchemy.orm import Session

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from shared.db import init_db, LiqEvent
from shared.config import load_config

CASCADE_WINDOW_S = 15
MIN_CASCADE_USD = 100_000
POLL_SEC = 10
FUNDING_INTERVAL_SEC = 4 * 3600
DAILY_SUMMARY_HOUR = 0

_tg_token = ""
_private_chat = ""
_public_channel = ""
_last_cascade_ts: dict[str, float] = {}
_last_funding_ts = 0.0
_last_summary_date = ""


def _tg(text: str, chat_id: str, parse_mode: str = "HTML") -> bool:
    if not _tg_token or not chat_id:
        return False
    try:
        data = json.dumps({
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{_tg_token}/sendMessage",
            data=data, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10)
        return True
    except Exception as e:
        logger.warning(f"TG send failed ({chat_id}): {e}")
        return False


def _send_public(text: str) -> None:
    if _public_channel:
        _tg(text, _public_channel)
    if _private_chat:
        _tg(f"[PUB] {text}", _private_chat, parse_mode="")


def _send_private(text: str) -> None:
    if _private_chat:
        _tg(text, _private_chat, parse_mode="")


def _fmt_usd(val: float) -> str:
    if val >= 1_000_000:
        return f"${val/1_000_000:.1f}M"
    if val >= 1_000:
        return f"${val/1_000:.0f}K"
    return f"${val:.0f}"


def _cluster_cascades(events) -> list[dict]:
    groups = defaultdict(list)
    for e in events:
        groups[(e.symbol, e.side)].append(e)
    out = []
    for (sym, side), evs in groups.items():
        evs.sort(key=lambda x: x.ts)
        cur = None
        for e in evs:
            ts_s = e.ts.replace(tzinfo=timezone.utc).timestamp()
            if cur and ts_s - cur["end_s"] <= CASCADE_WINDOW_S:
                cur["end_s"] = ts_s
                cur["count"] += 1
                cur["value"] += (e.value_usdt or 0)
                cur["last_price"] = e.price
            else:
                if cur:
                    out.append(cur)
                cur = {
                    "symbol": sym, "side": side,
                    "start_s": ts_s, "end_s": ts_s,
                    "count": 1, "value": e.value_usdt or 0,
                    "first_price": e.price, "last_price": e.price,
                }
        if cur:
            out.append(cur)
    return [c for c in out if c["value"] >= MIN_CASCADE_USD]


SIDE_EMOJI = {"Sell": "🔴", "Buy": "🟢"}
SIDE_TEXT = {"Sell": "LONGS ликвидированы", "Buy": "SHORTS ликвидированы"}
COIN_EMOJI = {
    "BTCUSDT": "₿", "ETHUSDT": "Ξ", "SOLUSDT": "◎",
    "DOGEUSDT": "🐕", "XRPUSDT": "✕", "BNBUSDT": "⬡",
}


def _format_cascade(c: dict) -> str:
    sym = c["symbol"].replace("USDT", "")
    emoji = SIDE_EMOJI.get(c["side"], "⚡")
    coin = COIN_EMOJI.get(c["symbol"], "🪙")
    side_text = SIDE_TEXT.get(c["side"], c["side"])
    val = _fmt_usd(c["value"])
    price_move = ""
    if c["first_price"] and c["last_price"] and c["first_price"] > 0:
        pct = (c["last_price"] - c["first_price"]) / c["first_price"] * 100
        arrow = "↗" if pct > 0 else "↘"
        price_move = f" | цена {arrow} {abs(pct):.2f}%"

    return (
        f"{emoji} <b>Каскад ликвидаций {coin} {sym}</b>\n"
        f"\n"
        f"💰 Объём: <b>{val}</b> ({c['count']} ликвидаций за {CASCADE_WINDOW_S}с)\n"
        f"📋 {side_text}{price_move}\n"
        f"💵 Цена: ${c['last_price']:,.2f}\n"
        f"\n"
        f"<i>Каскад = массовое принудительное закрытие позиций.\n"
        f"Сигнал повышенной волатильности.</i>"
    )


async def cascade_monitor(engine) -> None:
    global _last_cascade_ts
    logger.info("Cascade monitor started")
    seen_cascades: set[str] = set()

    while True:
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(seconds=120)
            with Session(engine) as s:
                events = s.execute(
                    select(LiqEvent).where(LiqEvent.ts >= cutoff)
                    .order_by(LiqEvent.ts)
                ).scalars().all()

            cascades = _cluster_cascades(events)

            for c in cascades:
                key = f"{c['symbol']}_{c['side']}_{int(c['start_s'])}"
                if key in seen_cascades:
                    continue

                cooldown = _last_cascade_ts.get(c["symbol"], 0)
                if time.time() - cooldown < 300:
                    continue

                msg = _format_cascade(c)
                _send_public(msg)
                seen_cascades.add(key)
                _last_cascade_ts[c["symbol"]] = time.time()
                logger.info(f"CASCADE ALERT: {c['symbol']} {c['side']} "
                            f"{_fmt_usd(c['value'])} ({c['count']} liq)")

            if len(seen_cascades) > 500:
                seen_cascades.clear()

        except Exception as e:
            logger.error(f"cascade_monitor: {e}")

        await asyncio.sleep(POLL_SEC)


def _fetch_funding() -> list[dict]:
    url = "https://api.bybit.com/v5/market/tickers?category=linear"
    with urllib.request.urlopen(url, timeout=15) as r:
        data = json.loads(r.read())
    rows = []
    for it in data.get("result", {}).get("list", []):
        sym = it.get("symbol", "")
        fr = it.get("fundingRate", "")
        if sym.endswith("USDT") and fr:
            try:
                rate = float(fr)
                rows.append({
                    "symbol": sym,
                    "rate": rate,
                    "annual_pct": rate * 3 * 365 * 100,
                    "price": float(it.get("lastPrice", 0)),
                })
            except ValueError:
                pass
    rows.sort(key=lambda r: -abs(r["rate"]))
    return rows


def _format_funding(rows: list[dict]) -> str:
    now = datetime.now(timezone.utc).strftime("%H:%M UTC")
    lines = [f"📊 <b>Экстремальный фандинг</b> ({now})\n"]
    for r in rows[:7]:
        sym = r["symbol"].replace("USDT", "")
        rate_8h = r["rate"] * 100
        annual = r["annual_pct"]
        emoji = "🔥" if abs(annual) > 200 else ("⚠️" if abs(annual) > 50 else "📈")
        direction = "шорты платят лонгам" if r["rate"] < 0 else "лонги платят шортам"
        lines.append(
            f"{emoji} <b>{sym}</b>: {rate_8h:+.4f}%/8ч ({annual:+.0f}%/год)\n"
            f"    └ {direction} | ${r['price']:,.4f}"
        )

    lines.append(
        f"\n<i>Фандинг = плата за удержание позиции.\n"
        f"Высокий → рынок перегрет в одну сторону.</i>"
    )
    return "\n".join(lines)


async def funding_monitor() -> None:
    global _last_funding_ts
    logger.info("Funding monitor started (every 4h)")
    while True:
        try:
            if time.time() - _last_funding_ts >= FUNDING_INTERVAL_SEC:
                rows = await asyncio.get_event_loop().run_in_executor(
                    None, _fetch_funding)
                extreme = [r for r in rows if abs(r["annual_pct"]) > 30]
                if extreme:
                    msg = _format_funding(extreme)
                    _send_public(msg)
                    logger.info(f"FUNDING ALERT: {len(extreme)} extreme rates")
                _last_funding_ts = time.time()
        except Exception as e:
            logger.error(f"funding_monitor: {e}")
        await asyncio.sleep(60)


async def daily_summary(engine) -> None:
    global _last_summary_date
    logger.info("Daily summary started (00:05 UTC)")
    while True:
        try:
            now = datetime.now(timezone.utc)
            today = now.strftime("%Y-%m-%d")
            if now.hour == 0 and now.minute >= 5 and _last_summary_date != today:
                yesterday = now - timedelta(days=1)
                with Session(engine) as s:
                    total_count = s.scalar(
                        select(func.count(LiqEvent.id))
                        .where(LiqEvent.ts >= yesterday)) or 0
                    total_vol = s.scalar(
                        select(func.sum(LiqEvent.value_usdt))
                        .where(LiqEvent.ts >= yesterday)) or 0
                    top_symbols = s.execute(
                        select(LiqEvent.symbol,
                               func.count(LiqEvent.id).label("cnt"),
                               func.sum(LiqEvent.value_usdt).label("vol"))
                        .where(LiqEvent.ts >= yesterday)
                        .group_by(LiqEvent.symbol)
                        .order_by(func.sum(LiqEvent.value_usdt).desc())
                        .limit(5)
                    ).all()

                if total_count > 0:
                    date_str = yesterday.strftime("%d.%m.%Y")
                    lines = [f"📅 <b>Итоги дня {date_str}</b>\n"]
                    lines.append(f"⚡ Ликвидаций: <b>{total_count:,}</b>")
                    lines.append(f"💰 Объём: <b>{_fmt_usd(total_vol)}</b>\n")
                    lines.append("🏆 Топ по объёму:")
                    for sym, cnt, vol in top_symbols:
                        sym_short = sym.replace("USDT", "")
                        lines.append(f"  {sym_short}: {_fmt_usd(vol)} ({cnt} ликв.)")

                    funding = await asyncio.get_event_loop().run_in_executor(
                        None, _fetch_funding)
                    if funding:
                        lines.append(f"\n📊 Самый высокий фандинг:")
                        for r in funding[:3]:
                            sym_s = r["symbol"].replace("USDT", "")
                            lines.append(
                                f"  {sym_s}: {r['rate']*100:+.4f}%/8ч "
                                f"({r['annual_pct']:+.0f}%/год)")

                    msg = "\n".join(lines)
                    _send_public(msg)
                    logger.info(f"DAILY SUMMARY sent: {total_count} liq, "
                                f"{_fmt_usd(total_vol)}")
                _last_summary_date = today

        except Exception as e:
            logger.error(f"daily_summary: {e}")
        await asyncio.sleep(30)


async def main() -> None:
    global _tg_token, _private_chat, _public_channel
    cfg = load_config()
    _tg_token = cfg.telegram_token
    _private_chat = cfg.telegram_chat_id
    _public_channel = os.environ.get("PUBLIC_CHANNEL_ID", "")

    logger.remove()
    logger.add(sys.stderr, level="INFO")
    engine = init_db(cfg.database_url)

    mode = "PUBLIC" if _public_channel else "PRIVATE-ONLY (set PUBLIC_CHANNEL_ID)"
    logger.info(f"Public Alert Service | mode={mode}")
    _send_private(
        f"📡 Public Alert Service запущен\n"
        f"Mode: {mode}\n"
        f"Channel: {_public_channel or 'не задан'}\n"
        f"Алерты: каскады ≥${MIN_CASCADE_USD:,}, фандинг каждые 4ч, дневная сводка"
    )

    await asyncio.gather(
        cascade_monitor(engine),
        funding_monitor(),
        daily_summary(engine),
    )


if __name__ == "__main__":
    asyncio.run(main())

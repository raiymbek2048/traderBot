"""SpreadArb — Phase 1: мониторинг + бумажная симуляция.

Подключается к Binance + Bybit WebSocket, при спреде ≥ ALERT_SPREAD_PCT:
  - записывает SpreadEvent в БД
  - создаёт ArbPaperTrade (бумажная сделка)
  - отправляет Telegram алерт с PnL и дневной статистикой

Run: python -m arbitrage.monitor
"""
from __future__ import annotations
import asyncio
import json
import time
import urllib.request
import urllib.parse
from dataclasses import dataclass

import websockets
from loguru import logger
from sqlalchemy.orm import Session
from sqlalchemy import func, select

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from shared.db import init_db, SpreadEvent, ArbPaperTrade
from shared.config import load_config
from shared.utils import utcnow

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
MIN_SPREAD_PCT   = 0.003   # 0.30% — log threshold
ALERT_SPREAD_PCT = 0.004   # 0.40% — paper trade threshold
MAX_BOOK_AGE_MS  = 5000    # данные старше 5 сек считаем stale
SAVE_COOLDOWN    = 1.0     # секунд между записями одного символа
WS_SILENCE_SEC   = 30      # если нет обновлений дольше — переподключаемся
PAPER_SIZE_USDT  = 1000.0  # размер бумажной позиции в USDT

BINANCE_WS = "wss://stream.binance.com:9443/stream?streams="
BYBIT_WS   = "wss://stream.bybit.com/v5/public/linear"

_tg_token: str = ""
_tg_chat:  str = ""


@dataclass
class BookTop:
    exchange: str
    symbol: str
    bid: float
    ask: float
    ts: float   # unix ms когда получено


# ── shared state ───────────────────────────────────────────────────────────────

books: dict[str, BookTop] = {}
_last_saved: dict[str, float] = {}    # symbol → unix ts последней записи
_last_rx: dict[str, float] = {}       # "binance"/"bybit" → unix ts последнего пакета

# Bybit держит in-memory стакан чтобы правильно применять delta-обновления
_bybit_ob: dict[str, dict] = {}       # symbol → {"bids": {price: qty}, "asks": {...}}


# ── spread computation ─────────────────────────────────────────────────────────

def compute_spread(sym: str) -> dict | None:
    """
    Returns dict с полными данными или None.
    Проверяет freshness обоих книг.
    """
    b = books.get(f"binance:{sym}")
    y = books.get(f"bybit:{sym}")
    if not b or not y:
        return None

    now_ms = time.time() * 1000
    b_age = now_ms - b.ts
    y_age = now_ms - y.ts
    book_age = max(b_age, y_age)

    if book_age > MAX_BOOK_AGE_MS:
        return None  # stale данные — игнорируем

    gross = net = 0.0
    buy_ex = sell_ex = ""

    if b.ask < y.bid:
        gross = (y.bid - b.ask) / b.ask
        net   = gross - 0.001
        buy_ex, sell_ex = "binance", "bybit"
    elif y.ask < b.bid:
        gross = (b.bid - y.ask) / y.ask
        net   = gross - 0.001
        buy_ex, sell_ex = "bybit", "binance"
    else:
        return None

    return {
        "gross": gross,
        "net": net,
        "buy_ex": buy_ex,
        "sell_ex": sell_ex,
        "binance_bid": b.bid,
        "binance_ask": b.ask,
        "bybit_bid": y.bid,
        "bybit_ask": y.ask,
        "book_age_ms": round(book_age),
    }


# ── Binance WebSocket ──────────────────────────────────────────────────────────

async def binance_listener(engine) -> None:
    streams = "/".join(f"{s.lower()}@bookTicker" for s in SYMBOLS)
    url = BINANCE_WS + streams
    backoff = 2
    while True:
        try:
            async with websockets.connect(url, ping_interval=None) as ws:
                logger.info("Binance WS connected")
                backoff = 2
                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=WS_SILENCE_SEC)
                    except asyncio.TimeoutError:
                        raise RuntimeError(f"Binance WS silent for {WS_SILENCE_SEC}s — forcing reconnect")
                    _last_rx["binance"] = time.time()
                    msg = json.loads(raw)
                    data = msg.get("data", msg)
                    sym = data.get("s")
                    if sym not in SYMBOLS:
                        continue
                    books[f"binance:{sym}"] = BookTop(
                        exchange="binance", symbol=sym,
                        bid=float(data["b"]), ask=float(data["a"]),
                        ts=time.time() * 1000,
                    )
                    await _check_spread(sym, engine)
        except Exception as e:
            logger.warning(f"Binance WS error: {e}. Reconnect in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


# ── Bybit WebSocket ────────────────────────────────────────────────────────────

def _apply_bybit_delta(sym: str, msg_type: str, bids: list, asks: list) -> tuple[float, float] | None:
    """
    Применяет snapshot или delta к in-memory стакану.
    Возвращает (best_bid, best_ask) или None если стакан пуст.
    """
    if sym not in _bybit_ob:
        _bybit_ob[sym] = {"bids": {}, "asks": {}}

    ob = _bybit_ob[sym]

    if msg_type == "snapshot":
        ob["bids"] = {float(p): float(q) for p, q in bids}
        ob["asks"] = {float(p): float(q) for p, q in asks}
    else:  # delta
        for p, q in bids:
            fp, fq = float(p), float(q)
            if fq == 0:
                ob["bids"].pop(fp, None)
            else:
                ob["bids"][fp] = fq
        for p, q in asks:
            fp, fq = float(p), float(q)
            if fq == 0:
                ob["asks"].pop(fp, None)
            else:
                ob["asks"][fp] = fq

    if not ob["bids"] or not ob["asks"]:
        return None
    best_bid = max(ob["bids"])
    best_ask = min(ob["asks"])
    return best_bid, best_ask


async def bybit_listener(engine) -> None:
    backoff = 2
    while True:
        try:
            async with websockets.connect(BYBIT_WS, ping_interval=None) as ws:
                logger.info("Bybit WS connected")
                backoff = 2
                _bybit_ob.clear()
                sub = {"op": "subscribe", "args": [f"orderbook.1.{s}" for s in SYMBOLS]}
                await ws.send(json.dumps(sub))
                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=WS_SILENCE_SEC)
                    except asyncio.TimeoutError:
                        raise RuntimeError(f"Bybit WS silent for {WS_SILENCE_SEC}s — forcing reconnect")
                    _last_rx["bybit"] = time.time()
                    msg = json.loads(raw)
                    topic = msg.get("topic", "")
                    if not topic.startswith("orderbook"):
                        continue
                    sym = topic.split(".")[-1]
                    if sym not in SYMBOLS:
                        continue
                    msg_type = msg.get("type", "delta")
                    data = msg.get("data", {})
                    bids = data.get("b", [])
                    asks = data.get("a", [])
                    result = _apply_bybit_delta(sym, msg_type, bids, asks)
                    if result is None:
                        continue
                    best_bid, best_ask = result
                    books[f"bybit:{sym}"] = BookTop(
                        exchange="bybit", symbol=sym,
                        bid=best_bid, ask=best_ask,
                        ts=float(msg.get("ts", time.time() * 1000)),
                    )
                    await _check_spread(sym, engine)
        except Exception as e:
            logger.warning(f"Bybit WS error: {e}. Reconnect in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


# ── telegram ───────────────────────────────────────────────────────────────────

async def _tg_send(text: str) -> None:
    if not _tg_token or not _tg_chat:
        return
    try:
        url = f"https://api.telegram.org/bot{_tg_token}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": _tg_chat,
            "text": text,
            "parse_mode": "HTML",
        }).encode()
        await asyncio.get_event_loop().run_in_executor(
            None, lambda: urllib.request.urlopen(url, data, timeout=5)
        )
    except Exception as e:
        logger.warning(f"Telegram send failed: {e}")


# ── spread checker ─────────────────────────────────────────────────────────────

async def _check_spread(sym: str, engine) -> None:
    result = compute_spread(sym)
    if result is None or result["gross"] < MIN_SPREAD_PCT:
        return

    now = time.time()
    if now - _last_saved.get(sym, 0) < SAVE_COOLDOWN:
        return
    _last_saved[sym] = now

    gross = result["gross"]
    net   = result["net"]
    gross_pct = round(gross * 100, 4)
    net_pct   = round(net * 100, 4)
    buy_price  = result["binance_ask"] if result["buy_ex"] == "binance" else result["bybit_ask"]
    sell_price = result["bybit_bid"]   if result["sell_ex"] == "bybit"  else result["binance_bid"]

    event = SpreadEvent(
        symbol=sym,
        buy_exchange=result["buy_ex"],
        sell_exchange=result["sell_ex"],
        buy_price=buy_price,
        sell_price=sell_price,
        spread_pct=gross_pct,
        net_pct=net_pct,
        binance_bid=result["binance_bid"],
        binance_ask=result["binance_ask"],
        bybit_bid=result["bybit_bid"],
        bybit_ask=result["bybit_ask"],
        book_age_ms=result["book_age_ms"],
        ts=utcnow(),
    )

    with Session(engine) as session:
        session.add(event)
        session.commit()

    level = "🚨 ALERT" if gross >= ALERT_SPREAD_PCT else "📊"
    logger.info(
        f"{level} {sym} spread={gross_pct:.3f}% net={net_pct:.3f}% "
        f"buy@{result['buy_ex']} sell@{result['sell_ex']} "
        f"age={result['book_age_ms']}ms"
    )

    if gross < ALERT_SPREAD_PCT:
        return

    pnl_usdt = round(PAPER_SIZE_USDT * net / 1, 4)

    paper = ArbPaperTrade(
        symbol=sym,
        buy_exchange=result["buy_ex"],
        sell_exchange=result["sell_ex"],
        buy_price=buy_price,
        sell_price=sell_price,
        size_usdt=PAPER_SIZE_USDT,
        gross_pct=gross_pct,
        net_pct=net_pct,
        pnl_usdt=pnl_usdt,
        book_age_ms=result["book_age_ms"],
        ts=utcnow(),
    )

    with Session(engine) as session:
        session.add(paper)
        session.commit()

        today_start = utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        day_trades = session.scalar(
            select(func.count(ArbPaperTrade.id))
            .where(ArbPaperTrade.ts >= today_start)
        ) or 0
        day_pnl = session.scalar(
            select(func.sum(ArbPaperTrade.pnl_usdt))
            .where(ArbPaperTrade.ts >= today_start)
        ) or 0.0

    msg = (
        f"💱 <b>SpreadArb PAPER TRADE</b>\n"
        f"<b>{sym}</b>: buy@{result['buy_ex'].upper()} → sell@{result['sell_ex'].upper()}\n"
        f"━━━━━━━━━━━━━━━\n"
        f"Gross: <b>{gross_pct:.3f}%</b>  |  Net: <b>{net_pct:.3f}%</b>\n"
        f"Size: ${PAPER_SIZE_USDT:,.0f}  |  PnL: <b>+${pnl_usdt:.2f}</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📊 Сегодня: {day_trades} сделок  |  +${day_pnl:.2f} USDT"
    )
    await _tg_send(msg)


# ── watchdog ───────────────────────────────────────────────────────────────────

async def watchdog() -> None:
    """Логирует если нет данных от биржи дольше WS_SILENCE_SEC."""
    while True:
        await asyncio.sleep(30)
        now = time.time()
        for ex in ("binance", "bybit"):
            last = _last_rx.get(ex, 0)
            silence = now - last
            if silence > WS_SILENCE_SEC:
                logger.warning(f"⚠️  {ex} WS silent for {silence:.0f}s — reconnect should trigger")


# ── stats printer ──────────────────────────────────────────────────────────────

async def stats_printer(engine) -> None:
    """Hourly summary — только за последний час."""
    while True:
        await asyncio.sleep(3600)
        from sqlalchemy import func, select
        from shared.db import SpreadEvent as SE
        from datetime import timedelta

        hour_ago = utcnow() - timedelta(hours=1)

        with Session(engine) as session:
            rows = session.execute(
                select(SE.symbol, func.count(), func.avg(SE.spread_pct), func.max(SE.spread_pct))
                .where(SE.ts >= hour_ago)
                .where(SE.spread_pct >= ALERT_SPREAD_PCT * 100)
                .group_by(SE.symbol)
            ).all()
            total_hour = session.scalar(
                select(func.count(SE.id)).where(SE.ts >= hour_ago)
            ) or 0
            stale = session.scalar(
                select(func.count(SE.id))
                .where(SE.ts >= hour_ago)
                .where(SE.book_age_ms > 2000)
            ) or 0

        logger.info(f"=== Hourly arbitrage summary | total={total_hour} stale(>2s)={stale} ===")
        if rows:
            for sym, cnt, avg_sp, max_sp in rows:
                logger.info(f"  {sym}: {cnt} alert signals | avg={avg_sp:.3f}% | max={max_sp:.3f}%")
        else:
            logger.info("  No alert-level spreads this hour")


# ── entrypoint ─────────────────────────────────────────────────────────────────

async def main() -> None:
    global _tg_token, _tg_chat
    cfg = load_config()
    _tg_token = cfg.telegram_token
    _tg_chat  = cfg.telegram_chat_id

    logger.remove()
    logger.add(sys.stderr, level="INFO")
    logger.add("logs/arbitrage_monitor.log", rotation="50 MB", retention="90 days")

    engine = init_db(cfg.database_url)
    logger.info(f"SpreadArb monitor started | symbols={SYMBOLS} | paper_size=${PAPER_SIZE_USDT:.0f} | alert≥{ALERT_SPREAD_PCT*100:.2f}% | max_book_age={MAX_BOOK_AGE_MS}ms")
    await _tg_send("🚀 <b>SpreadArb запущен</b>\nМониторю Binance↔Bybit | алерт при спреде ≥0.40%")

    await asyncio.gather(
        binance_listener(engine),
        bybit_listener(engine),
        stats_printer(engine),
        watchdog(),
    )


if __name__ == "__main__":
    asyncio.run(main())

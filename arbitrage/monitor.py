"""SpreadArb — Phase 1: мониторинг + РЕАЛИСТИЧНАЯ бумажная симуляция (v2).

Что изменилось в v2 (честная модель):
  • Глубокий стакан: Binance depth20, Bybit orderbook.50 (а не только топ)
  • VWAP-цена исполнения на заданный объём (учитывает проскальзывание по глубине)
  • Комиссии ×2 (покупка + продажа), раньше считалась только одна
  • Реалистичный размер позиции (PAPER_SIZE_USDT=$100 по умолчанию)
  • Пишем И наивную (топ стакана), И честную (VWAP) цифру — для сравнения

При net-спреде ≥ ALERT_NET_PCT (после комиссий и слиппеджа):
  - записывает SpreadEvent + ArbPaperTrade
  - сигналит в executor
  - шлёт Telegram алерт

Run: python -m arbitrage.monitor
"""
from __future__ import annotations
import asyncio
import json
import time
import urllib.request
import urllib.parse
from dataclasses import dataclass, field

import websockets
from loguru import logger
from sqlalchemy.orm import Session
from sqlalchemy import func, select

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from shared.db import init_db, SpreadEvent, ArbPaperTrade
from shared.config import load_config
from shared.utils import utcnow

# очередь для передачи сигналов в executor (если запущен в одном процессе)
_exec_queue: asyncio.Queue | None = None

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT"]

# Комиссии (taker, спот). Две сделки за арбитраж → платим дважды.
FEE_PER_SIDE   = 0.001              # 0.10% taker
FEE_ROUNDTRIP  = FEE_PER_SIDE * 2   # 0.20% суммарно

# Пороги теперь по ЧИСТОМУ спреду (после комиссий+слиппеджа), а не gross.
MIN_NET_PCT    = -0.001             # логируем даже лёгкий минус (для статистики)
ALERT_NET_PCT  = 0.0005             # 0.05% чистыми → paper trade + Telegram + executor

MAX_BOOK_AGE_MS  = 5000
SAVE_COOLDOWN    = 1.0
WS_SILENCE_SEC   = 30
PAPER_SIZE_USDT  = float(os.environ.get("PAPER_SIZE_USDT", "100.0"))  # реалистичный объём

BINANCE_WS = "wss://stream.binance.com:9443/stream?streams="
BYBIT_WS   = "wss://stream.bybit.com/v5/public/linear"

_tg_token: str = ""
_tg_chat:  str = ""


@dataclass
class Book:
    exchange: str
    symbol: str
    bids: list[tuple[float, float]]   # (price, qty), убыв. по цене
    asks: list[tuple[float, float]]   # (price, qty), возр. по цене
    ts: float                         # unix ms когда получено


# ── shared state ───────────────────────────────────────────────────────────────

books: dict[str, Book] = {}
_last_saved: dict[str, float] = {}
_last_rx: dict[str, float] = {}
_bybit_ob: dict[str, dict] = {}       # symbol → {"bids": {price:qty}, "asks": {...}}


# ── VWAP fill ────────────────────────────────────────────────────────────────────

def vwap_fill(levels: list[tuple[float, float]], size_usdt: float) -> tuple[float, bool]:
    """
    Проходит уровни стакана пока не наберёт size_usdt.
    levels — для покупки asks (возр.), для продажи bids (убыв.).
    Возвращает (средневзвешенная цена, хватило_ли_ликвидности).
    """
    remaining = size_usdt
    cost = 0.0          # сколько USDT потратили/получили
    base = 0.0          # сколько монет набрали
    for price, qty in levels:
        if price <= 0 or qty <= 0:
            continue
        level_usdt = price * qty
        take = min(remaining, level_usdt)
        base += take / price
        cost += take
        remaining -= take
        if remaining <= 1e-9:
            break
    if remaining > 1e-9 or base <= 0:
        return 0.0, False          # глубины не хватило
    return cost / base, True


# ── spread computation (realistic) ───────────────────────────────────────────────

def compute_spread(sym: str, size_usdt: float) -> dict | None:
    b = books.get(f"binance:{sym}")
    y = books.get(f"bybit:{sym}")
    if not b or not y or not b.asks or not b.bids or not y.asks or not y.bids:
        return None

    now_ms = time.time() * 1000
    book_age = max(now_ms - b.ts, now_ms - y.ts)
    if book_age > MAX_BOOK_AGE_MS:
        return None

    b_ask, b_bid = b.asks[0][0], b.bids[0][0]
    y_ask, y_bid = y.asks[0][0], y.bids[0][0]

    # направление по топу стакана
    if b_ask < y_bid:
        buy_ex, sell_ex = "binance", "bybit"
        buy_book, sell_book = b, y
        naive_buy, naive_sell = b_ask, y_bid
    elif y_ask < b_bid:
        buy_ex, sell_ex = "bybit", "binance"
        buy_book, sell_book = y, b
        naive_buy, naive_sell = y_ask, b_bid
    else:
        return None

    # наивный спред (топ стакана, как было раньше)
    naive_gross = (naive_sell - naive_buy) / naive_buy
    naive_net   = naive_gross - FEE_PER_SIDE        # старая формула: одна комиссия

    # реалистичный спред: VWAP на size_usdt по глубине
    vwap_buy,  ok_buy  = vwap_fill(buy_book.asks,  size_usdt)
    vwap_sell, ok_sell = vwap_fill(sell_book.bids, size_usdt)
    fillable = ok_buy and ok_sell

    if fillable:
        real_gross = (vwap_sell - vwap_buy) / vwap_buy
    else:
        real_gross = naive_gross   # не хватило глубины — берём наивный, но пометим fillable=False

    real_net  = real_gross - FEE_ROUNDTRIP          # честно: две комиссии
    slippage  = naive_gross - real_gross            # стоимость глубины

    return {
        "buy_ex": buy_ex, "sell_ex": sell_ex,
        "naive_gross": naive_gross, "naive_net": naive_net,
        "real_gross": real_gross, "real_net": real_net,
        "slippage": slippage, "fillable": fillable,
        "buy_price": vwap_buy if fillable else naive_buy,
        "sell_price": vwap_sell if fillable else naive_sell,
        "binance_bid": b_bid, "binance_ask": b_ask,
        "bybit_bid": y_bid, "bybit_ask": y_ask,
        "book_age_ms": round(book_age),
    }


# ── Binance WebSocket (depth20) ──────────────────────────────────────────────────

async def binance_listener(engine) -> None:
    streams = "/".join(f"{s.lower()}@depth20@100ms" for s in SYMBOLS)
    url = BINANCE_WS + streams
    backoff = 2
    while True:
        try:
            async with websockets.connect(url, ping_interval=None, max_size=2**21) as ws:
                logger.info("Binance WS connected (depth20)")
                backoff = 2
                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=WS_SILENCE_SEC)
                    except asyncio.TimeoutError:
                        raise RuntimeError(f"Binance WS silent for {WS_SILENCE_SEC}s — forcing reconnect")
                    _last_rx["binance"] = time.time()
                    msg = json.loads(raw)
                    stream = msg.get("stream", "")
                    data = msg.get("data", msg)
                    # имя стрима: btcusdt@depth20@100ms
                    sym = stream.split("@")[0].upper() if stream else None
                    if sym not in SYMBOLS:
                        continue
                    bids = [(float(p), float(q)) for p, q in data.get("bids", [])]
                    asks = [(float(p), float(q)) for p, q in data.get("asks", [])]
                    if not bids or not asks:
                        continue
                    books[f"binance:{sym}"] = Book(
                        exchange="binance", symbol=sym,
                        bids=bids, asks=asks, ts=time.time() * 1000,
                    )
                    await _check_spread(sym, engine)
        except Exception as e:
            logger.warning(f"Binance WS error: {e}. Reconnect in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


# ── Bybit WebSocket (orderbook.50) ───────────────────────────────────────────────

def _apply_bybit_delta(sym: str, msg_type: str, bids: list, asks: list) -> bool:
    if sym not in _bybit_ob:
        _bybit_ob[sym] = {"bids": {}, "asks": {}}
    ob = _bybit_ob[sym]
    if msg_type == "snapshot":
        ob["bids"] = {float(p): float(q) for p, q in bids}
        ob["asks"] = {float(p): float(q) for p, q in asks}
    else:
        for p, q in bids:
            fp, fq = float(p), float(q)
            ob["bids"].pop(fp, None) if fq == 0 else ob["bids"].__setitem__(fp, fq)
        for p, q in asks:
            fp, fq = float(p), float(q)
            ob["asks"].pop(fp, None) if fq == 0 else ob["asks"].__setitem__(fp, fq)
    return bool(ob["bids"] and ob["asks"])


def _bybit_sorted(sym: str) -> tuple[list, list]:
    ob = _bybit_ob[sym]
    bids = sorted(ob["bids"].items(), key=lambda x: -x[0])
    asks = sorted(ob["asks"].items(), key=lambda x: x[0])
    return bids, asks


async def bybit_listener(engine) -> None:
    backoff = 2
    while True:
        try:
            async with websockets.connect(BYBIT_WS, ping_interval=None, max_size=2**21) as ws:
                logger.info("Bybit WS connected (orderbook.50)")
                backoff = 2
                _bybit_ob.clear()
                sub = {"op": "subscribe", "args": [f"orderbook.50.{s}" for s in SYMBOLS]}
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
                    ok = _apply_bybit_delta(sym, msg_type, data.get("b", []), data.get("a", []))
                    if not ok:
                        continue
                    bids, asks = _bybit_sorted(sym)
                    books[f"bybit:{sym}"] = Book(
                        exchange="bybit", symbol=sym,
                        bids=bids, asks=asks,
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
            "chat_id": _tg_chat, "text": text, "parse_mode": "HTML",
        }).encode()
        await asyncio.get_event_loop().run_in_executor(
            None, lambda: urllib.request.urlopen(url, data, timeout=5)
        )
    except Exception as e:
        logger.warning(f"Telegram send failed: {e}")


# ── spread checker ─────────────────────────────────────────────────────────────

async def _check_spread(sym: str, engine) -> None:
    r = compute_spread(sym, PAPER_SIZE_USDT)
    if r is None or r["real_net"] < MIN_NET_PCT:
        return

    now = time.time()
    if now - _last_saved.get(sym, 0) < SAVE_COOLDOWN:
        return
    _last_saved[sym] = now

    real_gross_pct = round(r["real_gross"] * 100, 4)
    real_net_pct   = round(r["real_net"] * 100, 4)
    naive_gross_pct = round(r["naive_gross"] * 100, 4)
    naive_net_pct   = round(r["naive_net"] * 100, 4)
    slippage_pct    = round(r["slippage"] * 100, 4)

    event = SpreadEvent(
        symbol=sym, buy_exchange=r["buy_ex"], sell_exchange=r["sell_ex"],
        buy_price=r["buy_price"], sell_price=r["sell_price"],
        spread_pct=real_gross_pct, net_pct=real_net_pct,
        binance_bid=r["binance_bid"], binance_ask=r["binance_ask"],
        bybit_bid=r["bybit_bid"], bybit_ask=r["bybit_ask"],
        book_age_ms=r["book_age_ms"], ts=utcnow(),
    )
    with Session(engine) as session:
        session.add(event)
        session.commit()

    is_alert = r["real_net"] >= ALERT_NET_PCT and r["fillable"]
    level = "🚨 ALERT" if is_alert else ("📊" if r["fillable"] else "🚫 thin")
    logger.info(
        f"{level} {sym} real_net={real_net_pct:.3f}% (naive={naive_net_pct:.3f}% "
        f"slip={slippage_pct:.3f}%) fill={r['fillable']} "
        f"buy@{r['buy_ex']} sell@{r['sell_ex']} age={r['book_age_ms']}ms"
    )

    if not is_alert:
        return

    if _exec_queue is not None:
        try:
            _exec_queue.put_nowait({
                "symbol": sym, "buy_ex": r["buy_ex"], "sell_ex": r["sell_ex"],
                "gross": r["real_gross"], "net": r["real_net"],
                "buy_price": r["buy_price"], "sell_price": r["sell_price"],
                "book_age_ms": r["book_age_ms"],
            })
        except asyncio.QueueFull:
            pass

    pnl_usdt = round(PAPER_SIZE_USDT * r["real_net"], 4)

    paper = ArbPaperTrade(
        symbol=sym, buy_exchange=r["buy_ex"], sell_exchange=r["sell_ex"],
        buy_price=r["buy_price"], sell_price=r["sell_price"],
        size_usdt=PAPER_SIZE_USDT,
        gross_pct=real_gross_pct, net_pct=real_net_pct, pnl_usdt=pnl_usdt,
        naive_gross_pct=naive_gross_pct, naive_net_pct=naive_net_pct,
        slippage_pct=slippage_pct, fillable=r["fillable"],
        book_age_ms=r["book_age_ms"], ts=utcnow(),
    )
    with Session(engine) as session:
        session.add(paper)
        session.commit()
        today_start = utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        day_trades = session.scalar(
            select(func.count(ArbPaperTrade.id)).where(ArbPaperTrade.ts >= today_start)
        ) or 0
        day_pnl = session.scalar(
            select(func.sum(ArbPaperTrade.pnl_usdt)).where(ArbPaperTrade.ts >= today_start)
        ) or 0.0

    msg = (
        f"💱 <b>SpreadArb PAPER (realistic)</b>\n"
        f"<b>{sym}</b>: buy@{r['buy_ex'].upper()} → sell@{r['sell_ex'].upper()}\n"
        f"━━━━━━━━━━━━━━━\n"
        f"Real net: <b>{real_net_pct:.3f}%</b>  (naive {naive_net_pct:.3f}%)\n"
        f"Slippage: {slippage_pct:.3f}%  |  Size: ${PAPER_SIZE_USDT:,.0f}\n"
        f"PnL: <b>+${pnl_usdt:.3f}</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📊 Сегодня: {day_trades} сделок  |  +${day_pnl:.2f} USDT"
    )
    await _tg_send(msg)


# ── watchdog ───────────────────────────────────────────────────────────────────

async def watchdog() -> None:
    while True:
        await asyncio.sleep(30)
        now = time.time()
        for ex in ("binance", "bybit"):
            silence = now - _last_rx.get(ex, 0)
            if silence > WS_SILENCE_SEC:
                logger.warning(f"⚠️  {ex} WS silent for {silence:.0f}s — reconnect should trigger")


# ── stats printer ──────────────────────────────────────────────────────────────

async def stats_printer(engine) -> None:
    while True:
        await asyncio.sleep(3600)
        from datetime import timedelta
        hour_ago = utcnow() - timedelta(hours=1)
        with Session(engine) as session:
            rows = session.execute(
                select(ArbPaperTrade.symbol, func.count(), func.avg(ArbPaperTrade.net_pct),
                       func.sum(ArbPaperTrade.pnl_usdt))
                .where(ArbPaperTrade.ts >= hour_ago)
                .group_by(ArbPaperTrade.symbol)
            ).all()
            total = session.scalar(
                select(func.count(SpreadEvent.id)).where(SpreadEvent.ts >= hour_ago)
            ) or 0
        logger.info(f"=== Hourly summary | spread_events={total} | paper trades: ===")
        if rows:
            for sym, cnt, avg_net, pnl in rows:
                logger.info(f"  {sym}: {cnt} trades | avg_net={avg_net:.3f}% | PnL=${pnl:.2f}")
        else:
            logger.info("  No profitable (net>0) paper trades this hour")


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
    logger.info(
        f"SpreadArb monitor v2 (realistic) | symbols={SYMBOLS} | "
        f"size=${PAPER_SIZE_USDT:.0f} | fees={FEE_ROUNDTRIP*100:.2f}% | alert_net≥{ALERT_NET_PCT*100:.3f}%"
    )
    await _tg_send("🚀 <b>SpreadArb v2 запущен</b>\nЧестная модель: VWAP по глубине + комиссии ×2")

    await asyncio.gather(
        binance_listener(engine),
        bybit_listener(engine),
        stats_printer(engine),
        watchdog(),
    )


if __name__ == "__main__":
    asyncio.run(main())

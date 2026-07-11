"""Liquidation Recorder — пишет ликвидации Bybit в БД для анализа каскадов.

Гипотеза: каскад ликвидаций выносит цену на 2-5% за секунды → отскок.
Неделя данных покажет, есть ли статистически значимый отскок и на каких символах.

Подписка: allLiquidation по топ-N перпов по обороту (+ мажоры всегда).
Цену для анализа отскока потом возьмём из исторических klines (REST) —
рекордеру достаточно писать сами события.

Run: python -m arbitrage.liq_recorder
"""
from __future__ import annotations
import asyncio
import json
import urllib.request
from datetime import datetime, timezone

import websockets
from loguru import logger
from sqlalchemy.orm import Session

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from shared.db import init_db, LiqEvent
from shared.config import load_config

WS_URL   = "wss://stream.bybit.com/v5/public/linear"
TOP_N    = 30          # топ по обороту
ALWAYS   = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "XRPUSDT", "BNBUSDT"]
WS_SILENCE_SEC = 60
BATCH_COMMIT   = 20    # коммитим в БД пачками
STATS_EVERY    = 3600  # часовая сводка в лог


def _top_by_turnover() -> list[str]:
    url = "https://api.bybit.com/v5/market/tickers?category=linear"
    with urllib.request.urlopen(url, timeout=15) as r:
        data = json.loads(r.read())
    rows = []
    for it in data["result"]["list"]:
        s = it.get("symbol", "")
        if s.endswith("USDT"):
            try:
                rows.append((s, float(it.get("turnover24h", 0))))
            except ValueError:
                pass
    rows.sort(key=lambda x: -x[1])
    top = [s for s, _ in rows[:TOP_N]]
    for s in ALWAYS:
        if s not in top:
            top.append(s)
    return top


async def recorder(engine, symbols: list[str]) -> None:
    pending: list[LiqEvent] = []
    total = 0
    backoff = 2
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=None, max_size=2**22) as ws:
                logger.info(f"WS connected, подписка на {len(symbols)} символов")
                for i in range(0, len(symbols), 10):
                    await ws.send(json.dumps({
                        "op": "subscribe",
                        "args": [f"allLiquidation.{s}" for s in symbols[i:i+10]],
                    }))
                    await asyncio.sleep(0.2)
                backoff = 2
                while True:
                    raw = await asyncio.wait_for(ws.recv(), timeout=WS_SILENCE_SEC)
                    m = json.loads(raw)
                    topic = m.get("topic", "")
                    if not topic.startswith("allLiquidation"):
                        continue
                    for d in m.get("data", []):
                        try:
                            price = float(d.get("p", 0))
                            qty   = float(d.get("v", 0))
                            ev = LiqEvent(
                                symbol=d.get("s", topic.split(".")[-1]),
                                side=d.get("S", ""),
                                qty=qty, price=price,
                                value_usdt=round(price * qty, 2),
                                ts=datetime.fromtimestamp(
                                    int(d.get("T", 0)) / 1000, tz=timezone.utc),
                            )
                            pending.append(ev)
                        except (ValueError, TypeError):
                            continue
                    if len(pending) >= BATCH_COMMIT:
                        with Session(engine) as session:
                            session.add_all(pending)
                            session.commit()
                        total += len(pending)
                        pending.clear()
        except asyncio.TimeoutError:
            # тишина = ликвидаций нет; сбросим накопленное и переподключимся
            if pending:
                with Session(engine) as session:
                    session.add_all(pending)
                    session.commit()
                total += len(pending)
                pending.clear()
            logger.debug("WS тихо (нет ликвидаций) — reconnect")
        except Exception as e:
            logger.warning(f"WS error: {e}. Reconnect in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


async def stats(engine) -> None:
    from sqlalchemy import select, func
    from datetime import timedelta
    while True:
        await asyncio.sleep(STATS_EVERY)
        hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
        with Session(engine) as session:
            cnt = session.scalar(
                select(func.count(LiqEvent.id)).where(LiqEvent.ts >= hour_ago)) or 0
            vol = session.scalar(
                select(func.sum(LiqEvent.value_usdt)).where(LiqEvent.ts >= hour_ago)) or 0
        logger.info(f"[liq] за час: {cnt} ликвидаций на ${vol:,.0f}")


async def main() -> None:
    cfg = load_config()
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    engine = init_db(cfg.database_url)

    symbols = await asyncio.get_event_loop().run_in_executor(None, _top_by_turnover)
    logger.info(f"Liq Recorder | {len(symbols)} символов: {symbols[:10]}...")

    await asyncio.gather(recorder(engine, symbols), stats(engine))


if __name__ == "__main__":
    asyncio.run(main())

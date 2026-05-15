"""Funding Rate Monitor.

Каждую минуту проверяет фандинг рейты на Bybit Linear (perp).
Если рейт > ENTRY_THRESHOLD → пушит сигнал в очередь на открытие позиции.
Если позиция открыта и рейт < EXIT_THRESHOLD → сигнал на закрытие.
"""
from __future__ import annotations
import asyncio
import json
import urllib.request
from datetime import datetime, timezone
from loguru import logger

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "BNBUSDT", "XRPUSDT"]

ENTRY_THRESHOLD = 0.0005   # 0.05% per 8h — входим
EXIT_THRESHOLD  = 0.0001   # 0.01% per 8h — выходим

CHECK_INTERVAL  = 60       # секунд между проверками

# заполняется из run.py
signal_queue: asyncio.Queue | None = None
_tg_token: str = ""
_tg_chat:  str = ""


def _fetch_bybit_funding() -> dict[str, float]:
    """Возвращает {symbol: funding_rate} для всех linear перпов."""
    url = "https://api.bybit.com/v5/market/tickers?category=linear"
    with urllib.request.urlopen(url, timeout=10) as r:
        data = json.loads(r.read())
    rates: dict[str, float] = {}
    for item in data.get("result", {}).get("list", []):
        sym = item.get("symbol", "")
        fr  = item.get("fundingRate", "")
        if sym in SYMBOLS and fr:
            rates[sym] = float(fr)
    return rates


async def _send_tg(msg: str) -> None:
    if not _tg_token or not _tg_chat:
        return
    try:
        payload = json.dumps({"chat_id": _tg_chat, "text": msg}).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{_tg_token}/sendMessage",
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception as e:
        logger.warning(f"TG send failed: {e}")


async def rate_monitor(open_positions: dict[str, int]) -> None:
    """
    open_positions: {symbol: position_db_id} — что сейчас открыто.
    Пушит в signal_queue:
      {"action": "open",  "symbol": ..., "funding_rate": ...}
      {"action": "close", "symbol": ..., "position_id": ...}
    """
    logger.info(f"Funding monitor started | symbols={SYMBOLS}")
    while True:
        try:
            rates = await asyncio.get_event_loop().run_in_executor(
                None, _fetch_bybit_funding
            )
            now = datetime.now(timezone.utc).strftime("%H:%M:%S")
            lines = []
            for sym, fr in rates.items():
                annualized = fr * 3 * 365 * 100  # 3 раза в сутки × 365
                lines.append(f"  {sym}: {fr*100:.4f}%/8h ({annualized:.1f}%/год)")

                if sym not in open_positions and fr >= ENTRY_THRESHOLD:
                    logger.info(f"ENTRY signal: {sym} funding={fr*100:.4f}%")
                    await _send_tg(
                        f"📈 Funding Arb СИГНАЛ\n"
                        f"Символ: {sym}\n"
                        f"Фандинг: {fr*100:.4f}% / 8h\n"
                        f"({annualized:.1f}% годовых)\n"
                        f"→ Открываем позицию"
                    )
                    if signal_queue:
                        await signal_queue.put({"action": "open", "symbol": sym, "funding_rate": fr})

                elif sym in open_positions and fr < EXIT_THRESHOLD:
                    pos_id = open_positions[sym]
                    logger.info(f"EXIT signal: {sym} funding={fr*100:.4f}% (pos_id={pos_id})")
                    await _send_tg(
                        f"📉 Funding Arb ЗАКРЫТИЕ\n"
                        f"Символ: {sym}\n"
                        f"Фандинг упал до: {fr*100:.4f}% / 8h\n"
                        f"→ Закрываем позицию"
                    )
                    if signal_queue:
                        await signal_queue.put({"action": "close", "symbol": sym, "position_id": pos_id})

            logger.info(f"[{now}] Funding rates:\n" + "\n".join(lines))

        except Exception as e:
            logger.error(f"Funding monitor error: {e}")

        await asyncio.sleep(CHECK_INTERVAL)


async def stats_printer(open_positions: dict[str, int]) -> None:
    """Каждые 8 часов шлёт сводку в Telegram."""
    while True:
        await asyncio.sleep(8 * 3600)
        if not open_positions:
            await _send_tg("📊 Funding Arb: открытых позиций нет")
            continue
        msg = f"📊 Funding Arb — {len(open_positions)} позиций открыто:\n"
        for sym, pos_id in open_positions.items():
            msg += f"  • {sym} (id={pos_id})\n"
        await _send_tg(msg)

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

# СКАН-РЕЖИМ: сканируем ВСЕ перпы Bybit, а не фиксированный список.
# Оставляем SYMBOLS для обратной совместимости (лог/справка).
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "BNBUSDT", "XRPUSDT"]

ENTRY_THRESHOLD = 0.0005   # 0.05% per 8h — входим (положительный фандинг)
EXIT_THRESHOLD  = 0.0001   # 0.01% per 8h — выходим
MAX_POSITIONS   = 5        # макс. одновременных позиций (под реальный капитал)

CHECK_INTERVAL     = 60       # секунд между проверками фандинга
SPOT_REFRESH_SEC   = 3600     # как часто обновлять список спот-пар

# заполняется из run.py
signal_queue: asyncio.Queue | None = None
_tg_token: str = ""
_tg_chat:  str = ""

# кэш спот-вселенной (для проверки что есть чем хеджить)
_spot_symbols: set[str] = set()
_spot_ts: float = 0.0


def _fetch_all_funding() -> dict[str, float]:
    """{symbol: funding_rate} по ВСЕМ linear-перпам с котировкой USDT."""
    url = "https://api.bybit.com/v5/market/tickers?category=linear"
    with urllib.request.urlopen(url, timeout=10) as r:
        data = json.loads(r.read())
    rates: dict[str, float] = {}
    for item in data.get("result", {}).get("list", []):
        sym = item.get("symbol", "")
        fr  = item.get("fundingRate", "")
        if sym.endswith("USDT") and fr:
            try:
                rates[sym] = float(fr)
            except ValueError:
                pass
    return rates


def _fetch_spot_symbols() -> set[str]:
    """Множество спот-пар Bybit (для хеджа: long spot + short perp)."""
    url = "https://api.bybit.com/v5/market/instruments-info?category=spot"
    with urllib.request.urlopen(url, timeout=10) as r:
        data = json.loads(r.read())
    out: set[str] = set()
    for it in data.get("result", {}).get("list", []):
        if it.get("status") == "Trading" and it.get("quoteCoin") == "USDT":
            out.add(it.get("symbol", ""))
    return out


def _get_spot_symbols() -> set[str]:
    """Спот-вселенная с кэшем (обновляется раз в час)."""
    global _spot_symbols, _spot_ts
    import time
    now = time.time()
    if not _spot_symbols or now - _spot_ts > SPOT_REFRESH_SEC:
        try:
            _spot_symbols = _fetch_spot_symbols()
            _spot_ts = now
        except Exception as e:
            logger.warning(f"spot symbols fetch failed: {e}")
    return _spot_symbols


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
    logger.info(f"Funding monitor [SCAN MODE] started | все перпы Bybit | "
                f"entry≥{ENTRY_THRESHOLD*100:.3f}%/8h | max_pos={MAX_POSITIONS}")
    while True:
        try:
            rates, spot = await asyncio.get_event_loop().run_in_executor(
                None, lambda: (_fetch_all_funding(), _get_spot_symbols())
            )
            now = datetime.now(timezone.utc).strftime("%H:%M:%S")

            # кандидаты: положительный фандинг ≥ порога И есть спот для хеджа
            candidates = sorted(
                ((s, r) for s, r in rates.items()
                 if r >= ENTRY_THRESHOLD and s in spot),
                key=lambda x: -x[1],
            )

            # топ фандинга для лога (независимо от порога)
            top = sorted(rates.items(), key=lambda x: -x[1])[:8]
            top_lines = [f"  {s}: {r*100:.4f}%/8h ({r*3*365*100:.0f}%/год)"
                         + ("  ✅спот" if s in spot else "  ⛔нет спота")
                         for s, r in top]
            logger.info(f"[{now}] перпов={len(rates)} | кандидатов≥порога={len(candidates)} | "
                        f"открыто={len(open_positions)}\nТоп фандинга:\n" + "\n".join(top_lines))

            # ── ЗАКРЫТИЕ: открытые позиции где фандинг упал ──
            for sym in list(open_positions.keys()):
                fr = rates.get(sym, 0.0)
                if fr < EXIT_THRESHOLD:
                    pos_id = open_positions[sym]
                    logger.info(f"EXIT signal: {sym} funding={fr*100:.4f}% (pos_id={pos_id})")
                    await _send_tg(
                        f"📉 Funding ЗАКРЫТИЕ\n{sym}: фандинг упал до {fr*100:.4f}%/8h\n→ закрываем"
                    )
                    if signal_queue:
                        await signal_queue.put({"action": "close", "symbol": sym, "position_id": pos_id})

            # ── ОТКРЫТИЕ: топ-кандидаты, пока не упёрлись в MAX_POSITIONS ──
            # slots — свободные места в этом цикле; executor заполнит open_positions
            # реальными id до следующего цикла, поэтому не мутируем словарь здесь.
            slots = MAX_POSITIONS - len(open_positions)
            for sym, fr in candidates:
                if slots <= 0:
                    break
                if sym in open_positions:
                    continue
                annualized = fr * 3 * 365 * 100
                logger.info(f"ENTRY signal: {sym} funding={fr*100:.4f}% ({annualized:.0f}%/год)")
                await _send_tg(
                    f"📈 Funding СИГНАЛ (скан)\n"
                    f"{sym}: {fr*100:.4f}%/8h  ({annualized:.0f}%/год)\n"
                    f"→ long спот + short перп (delta-neutral)"
                )
                if signal_queue:
                    await signal_queue.put({"action": "open", "symbol": sym, "funding_rate": fr})
                slots -= 1

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

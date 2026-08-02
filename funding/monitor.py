"""Funding Rate Monitor.

Каждую минуту проверяет фандинг рейты на Bybit Linear (perp).
Если рейт > ENTRY_THRESHOLD → пушит сигнал в очередь на открытие позиции.
Если позиция открыта и рейт < EXIT_THRESHOLD → сигнал на закрытие.
"""
from __future__ import annotations
import asyncio
import json
import urllib.request
from datetime import datetime, timedelta, timezone
from loguru import logger

# СКАН-РЕЖИМ: сканируем ВСЕ перпы Bybit, а не фиксированный список.
# Оставляем SYMBOLS для обратной совместимости (лог/справка).
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "BNBUSDT", "XRPUSDT"]

# Порог входа поднят 0.05%→0.20% по persistence-анализу settled-истории (2 мес, 619 перпов):
#   entry=0.05%: E[net]=−0.17%/эпизод, прибыльных 14%  ← старый порог УБЫТОЧЕН
#   entry=0.10%: E[net]=+0.02% (шум)
#   entry=0.20%: E[net]=+0.40%/эпизод, прибыльных 60%, медиана 3 сеттлмента ← работаем тут
ENTRY_THRESHOLD = 0.002    # 0.20% за сеттлмент
EXIT_THRESHOLD  = 0.0001   # 0.01% — выходим
MAX_POSITIONS   = 5        # макс. одновременных позиций (под реальный капитал)

# Анти-churn: фандинг мелькает → раньше вход/выход крутились с нулём собранного
# (в реале это −0.31% комиссий за цикл). Теперь:
ENTRY_CONFIRM   = 10       # фандинг ≥ порога N проверок подряд (N минут)
EXIT_CONFIRM    = 10       # ниже exit-порога N проверок подряд
ENTRY_WINDOW_H  = 3.0      # входим только если до сеттлмента ≤ этого (часов)

# ── ПРАВИЛО МИНИМАЛЬНОГО ХОЛДА (добавлено 02.08) ─────────────────────────────
# Анти-churn выше отсекал вход на мелькании, но НЕ мешал выйти до начисления.
# Замерено на перп-перп (баг №25): 107 из 146 сделок (73%) закрылись, не пережив
# ни одного сеттлмента — заплатили комиссию и не получили выплату, за которой шли.
# Здесь тот же баг проявился живьём: SCRTUSDT, холд 2.8ч, фандинг ровно 0.0000,
# PnL −$0.3489.
# Фикс провалидирован в hold_paper.py на n=29: дожитие до начисления 92-97%.
MIN_SETTLEMENTS   = 1      # не выходим, пока не пережили начисление
MAX_HOLD_HOURS    = 30.0   # предохранитель от вечной позиции
EMERGENCY_RATE    = -0.0010  # если ставка ушла ниже (−0.10%/8ч) — мы ПЛАТИМ,
                             # тогда минимальный холд не держим и выходим

CHECK_INTERVAL     = 60       # секунд между проверками фандинга
SPOT_REFRESH_SEC   = 3600     # как часто обновлять список спот-пар

# заполняется из run.py
signal_queue: asyncio.Queue | None = None
_tg_token: str = ""
_tg_chat:  str = ""

# кэш спот-вселенной (для проверки что есть чем хеджить)
_spot_symbols: set[str] = set()
_spot_ts: float = 0.0

# счётчики устойчивости фандинга (анти-churn)
_above_streak: dict[str, int] = {}   # symbol → подряд циклов ≥ ENTRY_THRESHOLD
_below_streak: dict[str, int] = {}   # symbol → подряд циклов < EXIT_THRESHOLD


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


FUNDING_HOURS = (0, 8, 16)


def _settlements_survived(opened: datetime, now: datetime) -> int:
    """Сколько начислений позиция реально пережила."""
    n = 0
    t = opened.replace(minute=0, second=0, microsecond=0)
    while t <= now:
        if t.hour in FUNDING_HOURS and opened < t <= now:
            n += 1
        t += timedelta(hours=1)
    return n


def _hold_state(engine, pos_id: int) -> tuple[int, float] | None:
    """(пережито начислений, часов в позиции) по данным БД."""
    if engine is None:
        return None
    try:
        from sqlalchemy.orm import Session
        from shared.db import FundingPosition
        with Session(engine) as s:
            p = s.get(FundingPosition, pos_id)
            if not p or not p.opened_at:
                return None
            opened = p.opened_at
            if opened.tzinfo is None:      # SQLite отдаёт naive — форсим UTC
                opened = opened.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return (_settlements_survived(opened, now),
                (now - opened).total_seconds() / 3600)
    except Exception as e:
        logger.warning(f"hold_state({pos_id}): {e}")
        return None


async def rate_monitor(open_positions: dict[str, int], engine=None) -> None:
    """
    open_positions: {symbol: position_db_id} — что сейчас открыто.
    engine: нужен для проверки минимального холда (без него правило отключено).
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

            # обновляем счётчики устойчивости
            for s, r in rates.items():
                if r >= ENTRY_THRESHOLD and s in spot:
                    _above_streak[s] = _above_streak.get(s, 0) + 1
                else:
                    _above_streak.pop(s, None)
                if r < EXIT_THRESHOLD:
                    _below_streak[s] = _below_streak.get(s, 0) + 1
                else:
                    _below_streak.pop(s, None)

            # до сеттлмента (входим только в окне перед начислением)
            from funding.executor import _seconds_to_next_settlement
            hours_to_settle = _seconds_to_next_settlement() / 3600
            in_window = hours_to_settle <= ENTRY_WINDOW_H

            # кандидаты: фандинг устойчив ENTRY_CONFIRM циклов И окно сеттлмента
            candidates = sorted(
                ((s, r) for s, r in rates.items()
                 if _above_streak.get(s, 0) >= ENTRY_CONFIRM and in_window),
                key=lambda x: -x[1],
            )

            # топ фандинга для лога (независимо от порога)
            top = sorted(rates.items(), key=lambda x: -x[1])[:8]
            top_lines = [f"  {s}: {r*100:.4f}%/8h ({r*3*365*100:.0f}%/год)"
                         + ("  ✅спот" if s in spot else "  ⛔нет спота")
                         for s, r in top]
            logger.info(f"[{now}] перпов={len(rates)} | кандидатов={len(candidates)} | "
                        f"открыто={len(open_positions)} | до сеттлмента {hours_to_settle:.1f}ч"
                        f"{' (окно входа)' if in_window else ''}\nТоп фандинга:\n" + "\n".join(top_lines))

            # ── ЗАКРЫТИЕ: фандинг устойчиво ниже exit-порога ──
            # ⚠️ НО не раньше, чем пережили начисление (см. MIN_SETTLEMENTS).
            for sym in list(open_positions.keys()):
                if _below_streak.get(sym, 0) < EXIT_CONFIRM:
                    continue
                fr = rates.get(sym, 0.0)
                pos_id = open_positions[sym]

                st = _hold_state(engine, pos_id)
                reason = "фандинг ниже порога"
                if st is not None:
                    n_sett, hold_h = st
                    if n_sett < MIN_SETTLEMENTS:
                        if fr <= EMERGENCY_RATE:
                            reason = f"АВАРИЯ: ставка {fr*100:.4f}% — платим"
                        elif hold_h > MAX_HOLD_HOURS:
                            reason = f"max_hold {hold_h:.1f}ч без начисления"
                        else:
                            # держим: выйти сейчас = заплатить комиссию за ничто
                            logger.info(
                                f"HOLD {sym}: фандинг {fr*100:.4f}% ниже порога, но "
                                f"начислений {n_sett}, в позиции {hold_h:.1f}ч → ждём выплату")
                            continue

                logger.info(f"EXIT signal: {sym} funding={fr*100:.4f}% "
                            f"(pos_id={pos_id}, {reason})")
                await _send_tg(
                    f"📉 Funding ЗАКРЫТИЕ\n{sym}: фандинг {fr*100:.4f}%/8h\n"
                    f"Причина: {reason}"
                )
                if signal_queue:
                    await signal_queue.put({"action": "close", "symbol": sym,
                                            "position_id": pos_id})

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

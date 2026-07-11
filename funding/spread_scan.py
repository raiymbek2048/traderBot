"""Funding Spread Scanner — спред фандинга Bybit vs Binance по общим перпам.

Идея (вариант А из мозгоштурма): шорт перпа на бирже с высоким фандингом +
лонг того же перпа на бирже с низким = delta-neutral, зарабатываем РАЗНИЦУ фандингов.
Не требует спота, работает на самых жирных ставках.

Каждые CHECK_INTERVAL:
  - тянет фандинги обеих бирж, нормализует в %/день (интервалы у Bybit бывают 1/2/4/8ч)
  - пишет топ-спреды в БД (funding_spread_snaps)
  - Telegram-алерт при спреде ≥ ALERT_DAILY_PCT, повтор не чаще раза в час на символ

Run: python -m funding.spread_scan
"""
from __future__ import annotations
import asyncio
import json
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone

from loguru import logger
from sqlalchemy.orm import Session

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from shared.db import init_db, FundingSpreadSnap
from shared.config import load_config
from shared.utils import utcnow

CHECK_INTERVAL   = 300     # 5 минут
TOP_N_SAVE       = 20      # сколько топ-спредов писать в БД за цикл
ALERT_DAILY_PCT  = 1.0     # алерт при |спреде| ≥ 1%/день (365%/год)
ALERT_COOLDOWN   = 3600    # не спамить по одному символу чаще раза в час

_tg_token = ""
_tg_chat  = ""
_last_alert: dict[str, float] = {}

# интервалы фандинга Bybit (минуты), обновляем раз в час
_by_intervals: dict[str, int] = {}
_by_int_ts = 0.0

# интервалы фандинга Binance (часы) — исключения из стандартных 8ч
_bn_intervals: dict[str, int] = {}
_bn_int_ts = 0.0


def _get(url: str) -> dict | list:
    with urllib.request.urlopen(url, timeout=15) as r:
        return json.loads(r.read())


def _tg(text: str) -> None:
    if not _tg_token or not _tg_chat:
        return
    try:
        data = urllib.parse.urlencode({"chat_id": _tg_chat, "text": text}).encode()
        urllib.request.urlopen(
            f"https://api.telegram.org/bot{_tg_token}/sendMessage", data, timeout=5)
    except Exception as e:
        logger.warning(f"TG failed: {e}")


def _bybit_intervals() -> dict[str, int]:
    global _by_intervals, _by_int_ts
    if not _by_intervals or time.time() - _by_int_ts > 3600:
        try:
            out: dict[str, int] = {}
            cursor = ""
            while True:
                url = "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000"
                if cursor:
                    url += f"&cursor={urllib.parse.quote(cursor)}"
                data = _get(url)
                for it in data["result"]["list"]:
                    out[it["symbol"]] = int(it.get("fundingInterval", 480))
                cursor = data["result"].get("nextPageCursor", "")
                if not cursor:
                    break
            _by_intervals = out
            _by_int_ts = time.time()
        except Exception as e:
            logger.warning(f"intervals fetch failed: {e}")
    return _by_intervals


def _binance_intervals() -> dict[str, int]:
    """Нестандартные интервалы фандинга Binance (часы). Стандарт = 8ч."""
    global _bn_intervals, _bn_int_ts
    if time.time() - _bn_int_ts > 3600:
        try:
            data = _get("https://fapi.binance.com/fapi/v1/fundingInfo")
            _bn_intervals = {
                it["symbol"]: int(it.get("fundingIntervalHours", 8))
                for it in data if it.get("symbol")
            }
            _bn_int_ts = time.time()
        except Exception as e:
            logger.warning(f"binance fundingInfo failed: {e}")
            _bn_int_ts = time.time()  # не дёргаем каждый цикл
    return _bn_intervals


def scan_once() -> list[dict]:
    """Возвращает спреды по общим перпам, отсортированные по |спреду|."""
    by = _get("https://api.bybit.com/v5/market/tickers?category=linear")
    by_fr: dict[str, float] = {}
    by_px: dict[str, float] = {}
    by_ba: dict[str, tuple[float, float]] = {}   # (bid, ask)
    for it in by["result"]["list"]:
        s, fr = it.get("symbol", ""), it.get("fundingRate", "")
        if s.endswith("USDT") and fr:
            try:
                by_fr[s] = float(fr)
                by_px[s] = float(it.get("lastPrice", 0))
                by_ba[s] = (float(it.get("bid1Price", 0)), float(it.get("ask1Price", 0)))
            except ValueError:
                pass

    bn = _get("https://fapi.binance.com/fapi/v1/premiumIndex")
    bn_fr: dict[str, float] = {}
    bn_px: dict[str, float] = {}
    for it in bn:
        s, fr = it.get("symbol", ""), it.get("lastFundingRate", "")
        if s.endswith("USDT") and fr:
            try:
                bn_fr[s] = float(fr)
                bn_px[s] = float(it.get("markPrice", 0))
            except ValueError:
                pass

    # bid/ask Binance-перпов — исполнимые цены (один вызов на все символы)
    bn_ba: dict[str, tuple[float, float]] = {}
    try:
        book = _get("https://fapi.binance.com/fapi/v1/ticker/bookTicker")
        for it in book:
            s = it.get("symbol", "")
            if s in bn_fr:
                bn_ba[s] = (float(it.get("bidPrice", 0)), float(it.get("askPrice", 0)))
    except Exception as e:
        logger.warning(f"binance bookTicker failed: {e}")

    by_int = _bybit_intervals()
    bn_int = _binance_intervals()
    rows = []
    for s in set(by_fr) & set(bn_fr):
        by_daily = by_fr[s] * (1440 / by_int.get(s, 480))
        bn_daily = bn_fr[s] * (24 / bn_int.get(s, 8))
        spread_daily = by_daily - bn_daily
        bp, np_ = by_px.get(s, 0), bn_px.get(s, 0)
        gap = round((bp - np_) / np_ * 100, 4) if np_ else None

        # исполнимый вход по bid/ask в направлении сделки (+ = конвергенция за нас)
        exec_edge = None
        bb, ba_ = by_ba.get(s, (0, 0))
        nb, na_ = bn_ba.get(s, (0, 0))
        if bb and ba_ and nb and na_:
            if spread_daily > 0:   # SHORT Bybit (sell@bid) + LONG Binance (buy@ask)
                exec_edge = round((bb - na_) / na_ * 100, 4)
            else:                  # LONG Bybit (buy@ask) + SHORT Binance (sell@bid)
                exec_edge = round((nb - ba_) / ba_ * 100, 4)

        rows.append({
            "symbol": s,
            "bybit_fr": by_fr[s], "binance_fr": bn_fr[s],
            "bybit_daily_pct": round(by_daily * 100, 4),
            "binance_daily_pct": round(bn_daily * 100, 4),
            "spread_daily_pct": round(spread_daily * 100, 4),
            "bybit_price": bp, "binance_price": np_,
            "price_gap_pct": gap,
            "exec_edge_pct": exec_edge,
        })
    rows.sort(key=lambda r: -abs(r["spread_daily_pct"]))
    return rows


async def main() -> None:
    global _tg_token, _tg_chat
    cfg = load_config()
    _tg_token, _tg_chat = cfg.telegram_token, cfg.telegram_chat_id

    logger.remove()
    logger.add(sys.stderr, level="INFO")

    engine = init_db(cfg.database_url)
    logger.info(f"Funding Spread Scanner | Bybit vs Binance | каждые {CHECK_INTERVAL}с | "
                f"алерт ≥{ALERT_DAILY_PCT}%/день")
    _tg("🔀 Funding Spread Scanner запущен\nBybit vs Binance, все общие перпы\n"
        f"Алерт при спреде ≥{ALERT_DAILY_PCT}%/день")

    while True:
        try:
            rows = await asyncio.get_event_loop().run_in_executor(None, scan_once)
            now = utcnow()

            with Session(engine) as session:
                for r in rows[:TOP_N_SAVE]:
                    session.add(FundingSpreadSnap(ts=now, **r))
                session.commit()

            top5 = "\n".join(
                f"  {r['symbol']}: {r['spread_daily_pct']:+.3f}%/д "
                f"(By {r['bybit_daily_pct']:+.2f} / Bn {r['binance_daily_pct']:+.2f})"
                for r in rows[:5])
            logger.info(f"общих={len(rows)} | топ-5 спредов:\n{top5}")

            for r in rows:
                sp = r["spread_daily_pct"]
                if abs(sp) < ALERT_DAILY_PCT:
                    break
                sym = r["symbol"]
                if time.time() - _last_alert.get(sym, 0) < ALERT_COOLDOWN:
                    continue
                _last_alert[sym] = time.time()
                # положительный спред: шорт Bybit + лонг Binance; отрицательный — наоборот
                plan = "SHORT Bybit + LONG Binance" if sp > 0 else "LONG Bybit + SHORT Binance"
                # исполнимый вход по bid/ask (fallback на mid-гэп)
                gap = r["price_gap_pct"] or 0.0
                entry_edge = r["exec_edge_pct"]
                if entry_edge is None:
                    entry_edge = gap if sp > 0 else -gap
                aligned = "✅ aligned" if entry_edge >= 0 else "⚠️ adverse"
                fees = 0.21  # перп-тейкер ~0.05%×4 сделки (вход+выход обеих ног)
                cost = fees + max(0.0, -entry_edge)
                be_hours = cost / abs(sp) * 24 if sp else 0
                _tg(
                    f"🔀 Funding Spread ≥{ALERT_DAILY_PCT}%/день\n"
                    f"{sym}: {sp:+.3f}%/день (≈{sp*365:+.0f}%/год)\n"
                    f"Bybit {r['bybit_daily_pct']:+.2f}%/д | Binance {r['binance_daily_pct']:+.2f}%/д\n"
                    f"Вход по bid/ask: {entry_edge:+.2f}% {aligned}\n"
                    f"Break-even ≈ {be_hours:.1f}ч удержания (комиссии+вход)\n"
                    f"Delta-neutral: {plan}"
                )
        except Exception as e:
            logger.error(f"scan error: {e}")

        await asyncio.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())

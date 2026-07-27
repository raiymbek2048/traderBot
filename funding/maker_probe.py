"""Maker Fill Probe — замер исполнимости лимитных ордеров по реальной ленте.

═══ ЗАЧЕМ ═══
Единственный лом, доказанный на наших данных дважды:
    liq-momentum, 36 сделок: taker 0.11% → −$0.74  |  0% комиссий → +$1.24
    перп-перп,  146 сделок:  taker 0.22% → −$31.19 |  maker 0.08% → −$20.97
Край существует и примерно равен комиссии.

Но перед тем как строить мейкер-экзекьютор, надо честно узнать ДВЕ вещи,
которые репрайсинг журнала знать не мог:

  1. НАЛИВАЕТСЯ ли лимитник вообще (и как быстро)?
  2. Не наливается ли он преимущественно КОГДА ЦЕНА ПОШЛА ПРОТИВ НАС?
     (adverse selection — именно это убивает maker в momentum-стратегиях)

Плюс для delta-neutral критично третье: JOINT-FILL — обе ноги в одном окне.
Одна нога = голая направленная позиция.

═══ МЕТОД (честный, без реальных ордеров) ═══
Ставим ВИРТУАЛЬНЫЙ лимитник на текущем bid (buy) / ask (sell) и слушаем
реальную ленту сделок обеих бирж.

Консервативное правило залива: цена должна пройти СТРОГО через нашу —
для buy@bid нужна сделка НИЖЕ bid. Обоснование: на своём уровне мы в конце
очереди, залив «по нашей цене» не гарантирован. Это занижает fill-rate,
то есть ошибается в безопасную сторону.

После залива ждём ещё 30с и смотрим, куда ушёл mid → adverse_bps.
    adverse_bps > 0  = цена ушла ПРОТИВ нас (плохой залив)
    adverse_bps < 0  = цена ушла за нас (хороший залив)

═══ ЧТО СЧИТАТЬ УСПЕХОМ (зафиксировано заранее, 27.07.2026) ═══
Мейкер стоит строить, если ОБА условия:
  A. P(joint-fill в 300с) ≥ 50%   — обе ноги наливаются в разумном окне
  B. средний adverse_bps < +2 bps — заливы не отравлены отбором
Иначе мейкер вреден и таки надо остаться тейкером.

Run: python -m funding.maker_probe
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
from collections import defaultdict, deque
from datetime import datetime, timezone

import websockets
from loguru import logger
from sqlalchemy import select
from sqlalchemy.orm import Session

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from shared.config import load_config
from shared.db import init_db, MakerFillProbe

BY_WS = "wss://stream.bybit.com/v5/public/linear"
BN_WS = "wss://fstream.binance.com/stream"

PROBE_EVERY_S = 300        # новый замер каждые 5 мин
FILL_WINDOW_S = 300        # сколько ждём залива
ADVERSE_LAG_S = 30         # через сколько после залива смотрим mid
N_SYMBOLS = 5              # сколько символов держать под замером
REFRESH_SYMBOLS_S = 3600
BASELINE = "ETHUSDT"       # эталон ликвидности, всегда в наборе

# ⚠️ один тикер ≠ один актив (проверено 27.07: 4 из 587 общих тикеров).
# ONUSDT $88.20 vs $0.177, SNTUSDT 257%, WAVESUSDT 214%, VINEUSDT 95%.
MAX_PRICE_DISLOCATION = 10.0

# последние сделки: symbol → deque[(ts, price)]
_trades: dict[str, deque] = defaultdict(lambda: deque(maxlen=4000))
# книги: (exchange, symbol) → (bid, ask, ts)
_books: dict[tuple[str, str], tuple[float, float, float]] = {}
_symbols: list[str] = []


def _get(url: str, tries: int = 3):
    for i in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=20) as r:
                return json.loads(r.read())
        except Exception:
            if i == tries - 1:
                raise
            time.sleep(0.5 * (i + 1))


def pick_symbols() -> list[str]:
    """Символы под замер: те, что реально могли бы попасть в hold-стратегию.

    Берём общие перпы Bybit/Binance с ненулевым спредом фандинга и приличным
    оборотом, плюс ETHUSDT как эталон ликвидности.
    """
    try:
        by = _get("https://api.bybit.com/v5/market/tickers?category=linear")
        byd = {}
        for it in by["result"]["list"]:
            s = it.get("symbol", "")
            if s.endswith("USDT") and it.get("fundingRate"):
                try:
                    byd[s] = {"fr": float(it["fundingRate"]),
                              "turn": float(it.get("turnover24h") or 0),
                              "px": float(it.get("lastPrice") or 0)}
                except ValueError:
                    pass
        bn = {}
        for it in _get("https://fapi.binance.com/fapi/v1/premiumIndex"):
            s = it.get("symbol", "")
            if s.endswith("USDT") and it.get("markPrice"):
                try:
                    bn[s] = float(it["markPrice"])
                except ValueError:
                    pass
        # ⚠️ один тикер ≠ один актив: ONUSDT $88.20 (Bybit) vs $0.177 (Binance).
        # Замерять исполнимость на разных активах бессмысленно.
        common = []
        for s, d in byd.items():
            if s not in bn:
                continue
            bp, np_ = d.get("px", 0), bn[s]
            if bp > 0 and np_ > 0 and \
                    abs(bp - np_) / min(bp, np_) * 100 > MAX_PRICE_DISLOCATION:
                logger.warning(f"пропуск {s}: цены расходятся "
                               f"{bp:g}/{np_:g} — разные активы")
                continue
            common.append((s, d))
        # средний оборот: не мажоры (там ловить нечего), но и не dust
        mid = [s for s, d in common if 2e6 <= d["turn"] <= 2e8]
        mid.sort(key=lambda s: -abs(byd[s]["fr"]))
        out = mid[:N_SYMBOLS - 1]
        if BASELINE not in out and BASELINE in byd and BASELINE in bn:
            out.append(BASELINE)
        return out or [BASELINE]
    except Exception as e:
        logger.warning(f"pick_symbols failed: {e}")
        return _symbols or [BASELINE]


async def bybit_stream() -> None:
    backoff = 2
    while True:
        try:
            syms = list(_symbols)
            if not syms:
                await asyncio.sleep(5)
                continue
            async with websockets.connect(BY_WS, ping_interval=None,
                                          max_size=2 ** 22) as ws:
                args = [f"publicTrade.{s}" for s in syms] + \
                       [f"tickers.{s}" for s in syms]
                for i in range(0, len(args), 10):
                    await ws.send(json.dumps({"op": "subscribe",
                                              "args": args[i:i + 10]}))
                    await asyncio.sleep(0.2)
                logger.info(f"[bybit] подписан на {len(syms)} символов")
                backoff = 2
                silent = 0
                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=20)
                    except asyncio.TimeoutError:
                        silent += 1
                        if silent > 4:
                            raise RuntimeError("no data, reconnect")
                        await ws.send(json.dumps({"op": "ping"}))
                        continue
                    silent = 0
                    m = json.loads(raw)
                    topic = m.get("topic", "")
                    if topic.startswith("publicTrade"):
                        for d in m.get("data", []):
                            try:
                                _trades[f"bybit:{d['s']}"].append(
                                    (int(d["T"]) / 1000, float(d["p"])))
                            except (KeyError, ValueError):
                                pass
                    elif topic.startswith("tickers"):
                        # Bybit v5 присылает snapshot, затем ДЕЛЬТЫ (только
                        # изменённые поля). Мержим в последнее известное,
                        # иначе дельта без bid1Price теряла бы всю книгу.
                        d = m.get("data") or {}
                        s = d.get("symbol", "")
                        if not s:
                            continue
                        prev = _books.get(("bybit", s))
                        try:
                            b = float(d["bid1Price"]) if d.get("bid1Price") else (prev[0] if prev else 0)
                            a = float(d["ask1Price"]) if d.get("ask1Price") else (prev[1] if prev else 0)
                        except (TypeError, ValueError):
                            continue
                        if b and a:
                            _books[("bybit", s)] = (b, a, time.time())
                    # переподписка при смене набора
                    if set(_symbols) != set(syms):
                        raise RuntimeError("symbol set changed")
        except Exception as e:
            logger.warning(f"[bybit] {e}; reconnect in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


async def binance_stream() -> None:
    backoff = 2
    while True:
        try:
            syms = list(_symbols)
            if not syms:
                await asyncio.sleep(5)
                continue
            # ⚠️ @aggTrade на USDⓈ-M фьючерсах Binance НЕ отдаёт данные:
            # замерено 27.07 — 3436 bookTicker и 0 aggTrade за 20с по ETHUSDT,
            # ни в комби-, ни в одиночном стриме, ни в нижнем регистре.
            # @trade работает (618 сообщений за 12с). Из-за этого лента Binance
            # была пуста и ВСЕ ноги Binance показывали trades_seen=0.
            streams = "/".join(
                [f"{s.lower()}@trade" for s in syms] +
                [f"{s.lower()}@bookTicker" for s in syms])
            url = f"{BN_WS}?streams={streams}"
            async with websockets.connect(url, ping_interval=20,
                                          max_size=2 ** 22) as ws:
                logger.info(f"[binance] подписан на {len(syms)} символов")
                backoff = 2
                while True:
                    raw = await asyncio.wait_for(ws.recv(), timeout=60)
                    m = json.loads(raw)
                    d = m.get("data") or {}
                    ev = d.get("e", "")
                    if ev in ("trade", "aggTrade"):
                        try:
                            _trades[f"binance:{d['s']}"].append(
                                (int(d["T"]) / 1000, float(d["p"])))
                        except (KeyError, ValueError):
                            pass
                    elif d.get("b") and d.get("a"):
                        try:
                            _books[("binance", d["s"])] = (
                                float(d["b"]), float(d["a"]), time.time())
                        except (TypeError, ValueError):
                            pass
                    if set(_symbols) != set(syms):
                        raise RuntimeError("symbol set changed")
        except Exception as e:
            logger.warning(f"[binance] {e}; reconnect in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


def _mid(ex: str, sym: str) -> float | None:
    v = _books.get((ex, sym))
    if not v:
        return None
    return (v[0] + v[1]) / 2


def _crossed(ex: str, sym: str, side: str, price: float,
             t_from: float) -> float | None:
    """Прошла ли лента СТРОГО через нашу цену после t_from → ts сделки."""
    key = f"{ex}:{sym}"
    for ts, p in _trades[key]:
        if ts < t_from:
            continue
        if side == "buy@bid" and p < price:
            return ts
        if side == "sell@ask" and p > price:
            return ts
    return None


async def one_probe(engine, sym: str, turnover: float) -> None:
    """Один замер: обе ноги (шорт Bybit / лонг Binance) одновременно."""
    by = _books.get(("bybit", sym))
    bn = _books.get(("binance", sym))
    if not by or not bn:
        logger.debug(f"[probe] {sym}: пропуск — нет книги "
                     f"(bybit={'да' if by else 'нет'} binance={'да' if bn else 'нет'})")
        return
    t0 = time.time()
    if t0 - by[2] > 30 or t0 - bn[2] > 30:
        logger.debug(f"[probe] {sym}: пропуск — книга устарела "
                     f"(by {t0-by[2]:.0f}с, bn {t0-bn[2]:.0f}с)")
        return

    group = f"{sym}-{int(t0)}"
    legs = [
        ("bybit", "sell@ask", by[1], (by[0] + by[1]) / 2,
         (by[1] - by[0]) / ((by[1] + by[0]) / 2) * 100),
        ("binance", "buy@bid", bn[0], (bn[0] + bn[1]) / 2,
         (bn[1] - bn[0]) / ((bn[1] + bn[0]) / 2) * 100),
    ]

    ids = {}
    with Session(engine) as s:
        for ex, side, px, mid, width in legs:
            r = MakerFillProbe(
                exchange=ex, symbol=sym, side=side, probe_group=group,
                limit_price=px, mid_at_place=mid, book_width_pct=round(width, 5),
                turnover24h=turnover, window_secs=FILL_WINDOW_S,
                placed_at=datetime.now(timezone.utc))
            s.add(r); s.flush(); ids[(ex, side)] = r.id
        s.commit()

    fills: dict[tuple[str, str], float] = {}
    deadline = t0 + FILL_WINDOW_S
    while time.time() < deadline and len(fills) < len(legs):
        await asyncio.sleep(1)
        for ex, side, px, _, _ in legs:
            if (ex, side) in fills:
                continue
            ts = _crossed(ex, sym, side, px, t0)
            if ts:
                fills[(ex, side)] = ts

    # adverse selection: mid через 30с после залива
    await asyncio.sleep(ADVERSE_LAG_S)

    with Session(engine) as s:
        for ex, side, px, mid0, _ in legs:
            r = s.get(MakerFillProbe, ids[(ex, side)])
            ts = fills.get((ex, side))
            r.trades_seen = sum(1 for tt, _ in _trades[f"{ex}:{sym}"] if tt >= t0)
            r.resolved_at = datetime.now(timezone.utc)
            if ts:
                m_now = _mid(ex, sym)
                r.filled = True
                r.secs_to_fill = round(ts - t0, 2)
                r.mid_at_fill = px
                r.mid_after_30s = m_now
                if m_now and px:
                    # шортим: цена вверх = против нас; лонгуем: вниз = против нас
                    d = (m_now - px) / px * 10_000
                    r.adverse_bps = round(d if side == "sell@ask" else -d, 2)
            else:
                r.filled = False
        s.commit()

    got = len(fills)
    logger.info(f"[probe] {sym}: залилось {got}/2 "
                f"({'JOINT' if got == 2 else 'partial' if got else 'none'})"
                + (f" by={fills.get(('bybit','sell@ask'), 0) and round(fills[('bybit','sell@ask')]-t0,1)}с"
                   if ('bybit', 'sell@ask') in fills else "")
                + (f" bn={round(fills[('binance','buy@bid')]-t0,1)}с"
                   if ('binance', 'buy@bid') in fills else ""))


async def _wait_books_ready(timeout: float = 90.0) -> bool:
    """Ждём наполнения книг обеих бирж перед первым замером.

    Без этого первый круг гонялся со стартом WS: книги пусты → все замеры
    выходили молча → сон 300с впустую.
    """
    t0 = time.time()
    while time.time() - t0 < timeout:
        ready = [s for s in _symbols
                 if ("bybit", s) in _books and ("binance", s) in _books]
        if len(ready) >= max(1, len(_symbols) // 2):
            logger.info(f"книги готовы у {len(ready)}/{len(_symbols)} символов "
                        f"за {time.time()-t0:.0f}с → начинаю замеры")
            return True
        await asyncio.sleep(2)
    logger.warning(f"книги не наполнились за {timeout:.0f}с "
                   f"(есть: {list(_books.keys())[:6]})")
    return False


async def prober(engine) -> None:
    turnovers: dict[str, float] = {}
    last_refresh = 0.0
    await _wait_books_ready()
    while True:
        try:
            if time.time() - last_refresh > REFRESH_SYMBOLS_S:
                new = pick_symbols()
                if new:
                    _symbols[:] = new
                    logger.info(f"набор символов: {new}")
                    try:
                        d = _get("https://api.bybit.com/v5/market/tickers?category=linear")
                        turnovers = {it["symbol"]: float(it.get("turnover24h") or 0)
                                     for it in d["result"]["list"]}
                    except Exception:
                        pass
                last_refresh = time.time()

            live = [s for s in _symbols
                    if ("bybit", s) in _books and ("binance", s) in _books]
            if not live:
                logger.warning("нет символов с книгами обеих бирж, ждём 30с")
                await asyncio.sleep(30)
                continue

            # return_exceptions=True удобен (одна нога не валит круг), но он
            # ГЛОТАЕТ ошибки: из-за этого опечатка в _crossed() три круга
            # молча писала неразрешённые записи. Теперь исключения логируем.
            res = await asyncio.gather(*[
                one_probe(engine, s, turnovers.get(s, 0.0)) for s in live
            ], return_exceptions=True)
            for sym, r in zip(live, res):
                if isinstance(r, BaseException):
                    logger.error(f"[probe] {sym} упал: {type(r).__name__}: {r}")
        except Exception as e:
            logger.error(f"prober: {e}")
        await asyncio.sleep(PROBE_EVERY_S)


async def report(engine) -> None:
    """Сводка против ЗАРАНЕЕ зафиксированных критериев A и B."""
    while True:
        await asyncio.sleep(3600)
        try:
            with Session(engine) as s:
                rows = s.execute(select(
                    MakerFillProbe.probe_group, MakerFillProbe.exchange,
                    MakerFillProbe.symbol, MakerFillProbe.filled,
                    MakerFillProbe.secs_to_fill, MakerFillProbe.adverse_bps,
                    MakerFillProbe.book_width_pct, MakerFillProbe.trades_seen
                ).where(MakerFillProbe.resolved_at.isnot(None))).all()
            if len(rows) < 4:
                continue

            groups = defaultdict(list)
            for g, ex, sym, f, secs, adv, w, tr in rows:
                groups[g].append({"ex": ex, "sym": sym, "f": bool(f),
                                  "secs": secs, "adv": adv, "w": w,
                                  "tr": tr or 0})
            # ⚠️ Нога с нулевой лентой — это НЕ «не залилось», это НЕ ИЗМЕРЕНО.
            # Так баг Binance @aggTrade дал бы вывод «мейкер не работает на
            # Binance» вместо «канал данных пуст». Такие группы отбрасываем.
            complete = [v for v in groups.values()
                        if len(v) == 2 and all(x["tr"] > 0 for x in v)]
            no_tape = sum(1 for v in groups.values()
                          if len(v) == 2 and any(x["tr"] == 0 for x in v))
            if not complete:
                logger.warning(f"нет валидных замеров "
                               f"(отброшено без ленты: {no_tape})")
                continue

            joint = sum(1 for v in complete if all(x["f"] for x in v))
            partial = sum(1 for v in complete
                          if any(x["f"] for x in v) and not all(x["f"] for x in v))
            none_ = len(complete) - joint - partial
            advs = [x["adv"] for v in complete for x in v
                    if x["f"] and x["adv"] is not None]
            secs = [x["secs"] for v in complete for x in v
                    if x["f"] and x["secs"] is not None]

            jr = joint / len(complete) * 100
            mean_adv = statistics.mean(advs) if advs else 0.0
            ok_a = jr >= 50
            ok_b = mean_adv < 2.0

            # по биржам отдельно
            per_ex = defaultdict(lambda: [0, 0])
            for v in complete:
                for x in v:
                    per_ex[x["ex"]][1] += 1
                    if x["f"]:
                        per_ex[x["ex"]][0] += 1

            logger.info(f"[report] замеров={len(complete)} joint={jr:.0f}% "
                        f"adverse={mean_adv:+.2f}bps A={ok_a} B={ok_b}")

            msg = (f"🧪 MAKER PROBE (валидных замеров: {len(complete)}"
                   + (f", отброшено без ленты: {no_tape}" if no_tape else "")
                   + ")\n\n"
                   f"Обе ноги залились: {joint} ({jr:.0f}%)\n"
                   f"Только одна: {partial} | Ни одной: {none_}\n"
                   f"Медиана времени залива: "
                   f"{statistics.median(secs):.0f}с\n" if secs else "")
            msg += (f"Adverse selection: {mean_adv:+.2f} bps\n\n"
                    f"По биржам (fill-rate ноги):\n")
            for ex, (f_, t_) in per_ex.items():
                msg += f"  {ex}: {f_}/{t_} ({f_/t_*100:.0f}%)\n"
            msg += (f"\nКритерии (заданы заранее):\n"
                    f"{'✅' if ok_a else '❌'} A: joint-fill ≥50% ({jr:.0f}%)\n"
                    f"{'✅' if ok_b else '❌'} B: adverse <+2bps ({mean_adv:+.2f})\n\n"
                    f"{'→ мейкер стоит строить' if ok_a and ok_b else '→ мейкер вреден, остаёмся тейкером'}")

            cfg = load_config()
            if cfg.telegram_token and cfg.telegram_chat_id:
                try:
                    data = urllib.parse.urlencode(
                        {"chat_id": cfg.telegram_chat_id, "text": msg}).encode()
                    urllib.request.urlopen(
                        f"https://api.telegram.org/bot{cfg.telegram_token}/sendMessage",
                        data, timeout=8)
                except Exception as e:
                    logger.warning(f"TG: {e}")
        except Exception as e:
            logger.warning(f"report: {e}")


async def main() -> None:
    cfg = load_config()
    logger.remove(); logger.add(sys.stderr, level="INFO")
    engine = init_db(cfg.database_url)

    _symbols[:] = pick_symbols()
    logger.info(f"Maker Probe | символы={_symbols} | окно залива={FILL_WINDOW_S}с "
                f"| замер каждые {PROBE_EVERY_S}с")
    logger.info("Правило залива КОНСЕРВАТИВНОЕ: лента должна пройти СТРОГО "
                "через нашу цену (в очереди мы последние)")

    if cfg.telegram_token and cfg.telegram_chat_id:
        try:
            txt = (f"🧪 Maker Probe запущен\n"
                   f"Символы: {', '.join(_symbols)}\n"
                   f"Замер: виртуальный лимитник на bid/ask, окно {FILL_WINDOW_S}с\n"
                   f"Критерии заранее: joint-fill ≥50%, adverse <+2bps\n"
                   f"Сводка каждый час")
            data = urllib.parse.urlencode(
                {"chat_id": cfg.telegram_chat_id, "text": txt}).encode()
            urllib.request.urlopen(
                f"https://api.telegram.org/bot{cfg.telegram_token}/sendMessage",
                data, timeout=8)
        except Exception:
            pass

    await asyncio.gather(bybit_stream(), binance_stream(),
                         prober(engine), report(engine))


if __name__ == "__main__":
    asyncio.run(main())

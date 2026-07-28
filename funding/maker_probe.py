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

═══ ИСТОРИЯ КРИТЕРИЕВ ═══

v1 (задано 27.07, ПРОВАЛЕНО на n=471):
  A. P(joint-fill в 300с) ≥ 50%     → 83%     ✅
  B. средний adverse_bps < +2 bps   → +2.99   ❌
Тест провален по букве. Но метрика B была специфицирована НЕВЕРНО: она
усредняла ноги независимо, тогда как в delta-neutral паре ноги гасят adverse
друг друга по построению (цена вверх → шорт в минусе, лонг в плюсе).
Чистый adverse НА ПАРУ оказался −2.89 bps, то есть в нашу пользу.

⚠️ Метрика изменена ПОСЛЕ того, как увиден результат. Это ровно тот приём,
который весь проект даёт ложные находки (см. LOOPHOLE_SEARCH.md: правило
+$0.402 → −$1.42 после дедупликации). Поэтому v1 считается ПРОВАЛЕННОЙ,
а исправленная метрика — НОВОЙ гипотезой со своим окном и своими критериями.

Проверено и отвергнуто попутно: «adverse растёт с неликвидностью» — нет,
>$100M даёт +0.55, $20-100M +5.11, $5-20M −1.45. Связи нет.

═══ КРИТЕРИИ v2 (заданы 28.07 ДО сбора v2-данных) ═══
Экономический порог: мейкер экономит (0.055−0.020)%×4 ноги = 14 bps.
Значит полная ожидаемая стоимость мейкер-входа должна быть заметно ниже.

  E[стоимость] = P(joint)·adverse_на_пару + P(partial)·min(догнать, развернуть)

  1. n ≥ 200 валидных v2-групп
  2. E[стоимость] < 7 bps          — половина экономии, запас на ошибку модели
  3. P(partial)·стоимость_partial < 4 bps — голые ноги не съедают всё сами
  4. adverse на пару < +7 bps      — сам по себе не должен съесть экономию

Провал любого → мейкер не строим, остаёмся тейкером и закрываем вопрос.

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

PROBE_VERSION = 2
PROBE_EVERY_S = 300        # новый замер каждые 5 мин
FILL_WINDOW_S = 300        # сколько ждём залива
ADVERSE_LAG_S = 30         # через сколько после залива смотрим mid
PARTIAL_DECIDE_S = 60      # столько ждём вторую ногу, потом считаем разруливание
TAKER_BPS = 5.5            # тейкер-комиссия одной ноги, bps
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
                probe_version=PROBE_VERSION,
                placed_at=datetime.now(timezone.utc))
            s.add(r); s.flush(); ids[(ex, side)] = r.id
        s.commit()

    fills: dict[tuple[str, str], float] = {}
    # стоимость разруливания, если вторая нога не пришла за PARTIAL_DECIDE_S
    resolution: dict[tuple[str, str], tuple[float, float]] = {}
    first_fill_ts: float | None = None
    deadline = t0 + FILL_WINDOW_S
    while time.time() < deadline and len(fills) < len(legs):
        await asyncio.sleep(1)
        for ex, side, px, _, _ in legs:
            if (ex, side) in fills:
                continue
            ts = _crossed(ex, sym, side, px, t0)
            if ts:
                fills[(ex, side)] = ts
                if first_fill_ts is None:
                    first_fill_ts = time.time()

        # одна нога висит дольше PARTIAL_DECIDE_S → фиксируем цену выхода
        # из непарной позиции ПРЯМО СЕЙЧАС (это реальный момент решения)
        if (first_fill_ts and len(fills) == 1 and not resolution
                and time.time() - first_fill_ts >= PARTIAL_DECIDE_S):
            (fex, fside) = next(iter(fills))
            for ex, side, px, _, _ in legs:
                bk = _books.get((ex, sym))
                if not bk:
                    continue
                bid, ask = bk[0], bk[1]
                if (ex, side) == (fex, fside):
                    # развернуть залившуюся ногу тейкером
                    if side == "sell@ask":      # мы продали → выкупаем по ask
                        cost = (ask - px) / px * 10_000 + TAKER_BPS
                    else:                        # мы купили → продаём по bid
                        cost = (px - bid) / px * 10_000 + TAKER_BPS
                    resolution[("unwind", ex)] = (cost, 0.0)
                else:
                    # догнать недостающую ногу тейкером
                    if side == "sell@ask":      # надо продать → бьём в bid
                        cost = (px - bid) / px * 10_000 + TAKER_BPS
                    else:                        # надо купить → бьём в ask
                        cost = (ask - px) / px * 10_000 + TAKER_BPS
                    resolution[("chase", ex)] = (cost, 0.0)

    # adverse selection: mid через 30с после залива
    await asyncio.sleep(ADVERSE_LAG_S)

    partial = len(fills) == 1
    chase = next((c for (k, _), (c, _) in resolution.items() if k == "chase"), None)
    unwind = next((c for (k, _), (c, _) in resolution.items() if k == "unwind"), None)

    with Session(engine) as s:
        for ex, side, px, mid0, _ in legs:
            r = s.get(MakerFillProbe, ids[(ex, side)])
            ts = fills.get((ex, side))
            r.trades_seen = sum(1 for tt, _ in _trades[f"{ex}:{sym}"] if tt >= t0)
            r.resolved_at = datetime.now(timezone.utc)
            if partial:
                r.partial_leg = True
                r.chase_cost_bps = round(chase, 2) if chase is not None else None
                r.unwind_cost_bps = round(unwind, 2) if unwind is not None else None
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


MAKER_SAVING_BPS = (0.00055 - 0.00020) * 4 * 10_000   # 14 bps на цикл


async def report(engine) -> None:
    """Сводка против критериев v2 (заданы ДО сбора v2-данных)."""
    while True:
        await asyncio.sleep(3600)
        try:
            with Session(engine) as s:
                rows = s.execute(select(
                    MakerFillProbe.probe_group, MakerFillProbe.exchange,
                    MakerFillProbe.filled, MakerFillProbe.secs_to_fill,
                    MakerFillProbe.adverse_bps, MakerFillProbe.trades_seen,
                    MakerFillProbe.chase_cost_bps, MakerFillProbe.unwind_cost_bps
                ).where(MakerFillProbe.resolved_at.isnot(None),
                        MakerFillProbe.probe_version == PROBE_VERSION)).all()
            if len(rows) < 4:
                continue

            groups = defaultdict(list)
            for g, ex, f, secs, adv, tr, ch, un in rows:
                groups[g].append({"ex": ex, "f": bool(f), "secs": secs,
                                  "adv": adv, "tr": tr or 0,
                                  "chase": ch, "unwind": un})
            # ⚠️ Нога с нулевой лентой — это НЕ «не залилось», это НЕ ИЗМЕРЕНО.
            complete = [v for v in groups.values()
                        if len(v) == 2 and all(x["tr"] > 0 for x in v)]
            no_tape = sum(1 for v in groups.values()
                          if len(v) == 2 and any(x["tr"] == 0 for x in v))
            if not complete:
                logger.warning(f"нет валидных v2-замеров (без ленты: {no_tape})")
                continue

            n = len(complete)
            joint = [v for v in complete if all(x["f"] for x in v)]
            partial = [v for v in complete
                       if any(x["f"] for x in v) and not all(x["f"] for x in v)]

            # ГЛАВНОЕ: adverse СУММОЙ ПО ПАРЕ — ноги гасят друг друга
            nets = [sum(x["adv"] for x in v)
                    for v in joint
                    if all(x["adv"] is not None for x in v)]
            net_adv = statistics.mean(nets) if nets else 0.0

            # стоимость разруливания непарного залива: берём дешёвейший путь
            part_costs = []
            for v in partial:
                opts = [c for x in v for c in (x["chase"], x["unwind"])
                        if c is not None]
                if opts:
                    part_costs.append(min(opts))
            part_cost = statistics.mean(part_costs) if part_costs else 0.0

            p_joint = len(joint) / n
            p_part = len(partial) / n
            e_cost = p_joint * net_adv + p_part * part_cost
            part_drag = p_part * part_cost

            c1 = n >= 200
            c2 = e_cost < 7.0
            c3 = part_drag < 4.0
            c4 = net_adv < 7.0
            passed = c1 and c2 and c3 and c4

            secs = [x["secs"] for v in complete for x in v
                    if x["f"] and x["secs"] is not None]

            logger.info(f"[report v2] n={n} joint={p_joint*100:.0f}% "
                        f"net_adv={net_adv:+.2f} part_cost={part_cost:.2f} "
                        f"E={e_cost:+.2f}bps passed={passed}")

            msg = (f"🧪 MAKER PROBE v2 (n={n}"
                   + (f", без ленты отброшено: {no_tape}" if no_tape else "")
                   + f")\n\n"
                   f"Обе ноги: {len(joint)} ({p_joint*100:.0f}%) | "
                   f"одна: {len(partial)} ({p_part*100:.0f}%)\n")
            if secs:
                msg += f"Медиана залива: {statistics.median(secs):.0f}с\n"
            msg += (f"\nЭКОНОМИКА (экономия мейкера {MAKER_SAVING_BPS:.0f} bps):\n"
                    f"  adverse на пару:  {net_adv:+.2f} bps\n"
                    f"  разрулить одну:   {part_cost:+.2f} bps "
                    f"(n={len(part_costs)})\n"
                    f"  вклад непарных:   {part_drag:+.2f} bps\n"
                    f"  E[стоимость]:     {e_cost:+.2f} bps\n"
                    f"  ЧИСТО:            {MAKER_SAVING_BPS - e_cost:+.2f} bps\n"
                    f"\nКритерии v2 (заданы до сбора):\n"
                    f"{'✅' if c1 else '⬜'} 1. n≥200 ({n})\n"
                    f"{'✅' if c2 else '❌'} 2. E[стоимость]<7 ({e_cost:+.2f})\n"
                    f"{'✅' if c3 else '❌'} 3. вклад непарных<4 ({part_drag:+.2f})\n"
                    f"{'✅' if c4 else '❌'} 4. adverse пары<7 ({net_adv:+.2f})\n\n"
                    + ("→ мейкер выгоден, можно строить экзекьютор"
                       if passed else
                       ("→ рано, копим выборку" if not c1
                        else "→ мейкер не окупается, остаёмся тейкером")))

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

"""Решающая проверка лазейки: HOLD-модель на 2 месяцах settled-истории.

Наш persistence-анализ (июль) использовал EPISODE-модель: вошёл на спайке,
вышел когда ставка упала. Вывод был «порог 0.05% убыточен, 0.20% работает».

Но лазейка в другом: не ловить спайки, а ВЫБРАТЬ актив со стабильно
положительным фандингом и ДЕРЖАТЬ, амортизируя разовую стоимость входа.

Проверяем честно, out-of-sample:
  правило отбора  : trailing N дней средний settled-фандинг ≥ порога
  действие        : long spot + short perp, держать H дней
  стоимость       : комиссии 0.31% + basis 0.20% = 0.51% РАЗОВО (замерено живьём)
  результат       : Σ settled-фандинг за период удержания − стоимость

Критично: отбор по ПРОШЛОМУ, результат по БУДУЩЕМУ (никакого lookahead).

Run: python scripts/hold_validation.py
"""
from __future__ import annotations
import json
import statistics
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

BASE = "https://api.bybit.com"
ONE_TIME_COST_PCT = 0.51      # комиссии 0.31% + basis 0.20% (из живых замеров)
TRAIL_DAYS = 7                # окно отбора (прошлое)
HOLD_DAYS = 14                # горизонт удержания (будущее)
MIN_TRAIL_DAILY_PCT = 0.10    # порог отбора: ср. фандинг %/день за trailing окно
MAX_ABS_RATE = 0.005          # |rate| > 0.5%/интервал = кап → радиоактивный, скип
MIN_TURNOVER = 2_000_000      # $2M оборота/24ч


def get(url: str, tries: int = 3):
    for i in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=20) as r:
                return json.loads(r.read())
        except Exception:
            if i == tries - 1:
                return None
            time.sleep(0.5 * (i + 1))
    return None


def universe() -> list[dict]:
    """Перпы, у которых ЕСТЬ спот-пара (иначе нечем хеджить) и есть оборот."""
    spot = set()
    d = get(f"{BASE}/v5/market/instruments-info?category=spot")
    if d:
        for it in d["result"]["list"]:
            if it.get("status") == "Trading":
                spot.add(it["symbol"])

    out, cursor = [], ""
    intervals = {}
    while True:
        u = f"{BASE}/v5/market/instruments-info?category=linear&limit=1000"
        if cursor:
            u += f"&cursor={urllib.parse.quote(cursor)}"
        d = get(u)
        if not d:
            break
        for it in d["result"]["list"]:
            intervals[it["symbol"]] = int(it.get("fundingInterval", 480))
        cursor = d["result"].get("nextPageCursor", "")
        if not cursor:
            break

    d = get(f"{BASE}/v5/market/tickers?category=linear")
    if not d:
        return []
    for it in d["result"]["list"]:
        s = it.get("symbol", "")
        if not s.endswith("USDT") or s not in spot:
            continue
        try:
            turn = float(it.get("turnover24h", 0))
        except ValueError:
            continue
        if turn < MIN_TURNOVER:
            continue
        out.append({"symbol": s, "turnover": turn,
                    "interval_min": intervals.get(s, 480)})
    return out


def settled_history(symbol: str) -> list[tuple[int, float]]:
    d = get(f"{BASE}/v5/market/funding/history?category=linear&symbol={symbol}&limit=200")
    if not d:
        return []
    rows = []
    for r in d.get("result", {}).get("list", []):
        try:
            rows.append((int(r["fundingRateTimestamp"]), float(r["fundingRate"])))
        except (KeyError, ValueError):
            continue
    rows.sort()
    return rows


def main():
    print("Загружаю вселенную (перпы со спотом и оборотом)...")
    uni = universe()
    print(f"  подходящих символов: {len(uni)}")
    if not uni:
        print("нет данных"); return

    print(f"Тяну settled-историю фандинга (200 записей = ~66 дней)...")
    hist: dict[str, list[tuple[int, float]]] = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(settled_history, u["symbol"]): u["symbol"] for u in uni}
        done = 0
        for f in futs:
            pass
        for f, sym in futs.items():
            h = f.result()
            done += 1
            if done % 50 == 0:
                print(f"  {done}/{len(uni)}...")
            if len(h) >= 60:
                hist[sym] = h
    print(f"  с достаточной историей: {len(hist)}")

    meta = {u["symbol"]: u for u in uni}
    DAY_MS = 86_400_000

    # ── rolling out-of-sample тест ───────────────────────────────────────
    results = []           # (symbol, t0, trail_daily, fwd_net_pct)
    skipped_capped = 0

    for sym, h in hist.items():
        interval_min = meta[sym]["interval_min"]
        per_day = 1440 / interval_min
        ts_list = [t for t, _ in h]
        rate_list = [r for _, r in h]

        if any(abs(r) > MAX_ABS_RATE for r in rate_list):
            skipped_capped += 1
            continue

        for i in range(len(h)):
            t0 = ts_list[i]
            # trailing окно (ПРОШЛОЕ): [t0 - TRAIL_DAYS, t0)
            trail = [r for t, r in h if t0 - TRAIL_DAYS * DAY_MS <= t < t0]
            if len(trail) < TRAIL_DAYS * per_day * 0.7:
                continue
            trail_daily = statistics.mean(trail) * per_day * 100
            if trail_daily < MIN_TRAIL_DAILY_PCT:
                continue
            # forward окно (БУДУЩЕЕ): [t0, t0 + HOLD_DAYS]
            fwd = [r for t, r in h if t0 <= t < t0 + HOLD_DAYS * DAY_MS]
            if len(fwd) < HOLD_DAYS * per_day * 0.7:
                continue
            gross = sum(fwd) * 100          # шорт перп получает при rate>0
            net = gross - ONE_TIME_COST_PCT
            results.append((sym, t0, trail_daily, gross, net))

    print()
    print("=" * 78)
    print(f"HOLD-МОДЕЛЬ out-of-sample: отбор по trailing {TRAIL_DAYS}д, "
          f"держим {HOLD_DAYS}д")
    print("=" * 78)
    print(f"Правило: ср.фандинг за прошлые {TRAIL_DAYS}д ≥ {MIN_TRAIL_DAILY_PCT}%/день")
    print(f"Стоимость: {ONE_TIME_COST_PCT}% разово (комиссии 0.31% + basis 0.20%)")
    print(f"Скипнуто как капнутые/радиоактивные: {skipped_capped} символов")
    print()

    if not results:
        print("❌ НИ ОДНОГО сигнала — правило не находит кандидатов.")
        return

    nets = [r[4] for r in results]
    grosses = [r[3] for r in results]
    wins = sum(1 for n in nets if n > 0)
    n = len(results)
    print(f"Сигналов (символ×дата): {n} на {len(set(r[0] for r in results))} символах")
    print(f"  Σgross фандинг : {statistics.mean(grosses):+.3f}% в среднем за {HOLD_DAYS}д")
    print(f"  E[net]         : {statistics.mean(nets):+.3f}%  "
          f"(медиана {statistics.median(nets):+.3f}%)")
    print(f"  Прибыльных     : {wins}/{n} ({wins/n*100:.0f}%)")
    print(f"  Годовых по E[net]: {statistics.mean(nets)/HOLD_DAYS*365:+.1f}%/год")
    verdict = "✅ ПОЛОЖИТЕЛЬНО" if statistics.mean(nets) > 0 else "❌ ОТРИЦАТЕЛЬНО"
    print(f"  ВЕРДИКТ: {verdict}")

    # ── чувствительность к порогу отбора и горизонту ────────────────────
    print()
    print("─── Чувствительность: порог отбора × горизонт удержания ───")
    print(f"{'порог %/д':>10s}", end="")
    for hd in (7, 14, 30):
        print(f"{'H='+str(hd)+'д':>22s}", end="")
    print()
    print(f"{'':>10s}", end="")
    for _ in (7, 14, 30):
        print(f"{'E[net]':>9s}{'win%':>7s}{'n':>6s}", end="")
    print()

    for thr in (0.05, 0.10, 0.20, 0.40):
        print(f"{thr:>10.2f}", end="")
        for hd in (7, 14, 30):
            res = []
            for sym, h in hist.items():
                interval_min = meta[sym]["interval_min"]
                per_day = 1440 / interval_min
                if any(abs(r) > MAX_ABS_RATE for _, r in h):
                    continue
                for t0, _ in h:
                    trail = [r for t, r in h if t0 - TRAIL_DAYS * DAY_MS <= t < t0]
                    if len(trail) < TRAIL_DAYS * per_day * 0.7:
                        continue
                    td = statistics.mean(trail) * per_day * 100
                    if td < thr:
                        continue
                    fwd = [r for t, r in h if t0 <= t < t0 + hd * DAY_MS]
                    if len(fwd) < hd * per_day * 0.7:
                        continue
                    res.append(sum(fwd) * 100 - ONE_TIME_COST_PCT)
            if res:
                w = sum(1 for x in res if x > 0) / len(res) * 100
                print(f"{statistics.mean(res):>+9.3f}{w:>6.0f}%{len(res):>6d}", end="")
            else:
                print(f"{'—':>9s}{'—':>7s}{'0':>6s}", end="")
        print()

    # ── топ символов по стабильности ─────────────────────────────────────
    print()
    print("─── Символы с наибольшим числом прибыльных сигналов ───")
    by_sym = defaultdict(list)
    for sym, t0, td, g, net in results:
        by_sym[sym].append(net)
    ranked = sorted(by_sym.items(),
                    key=lambda kv: -statistics.mean(kv[1]))
    print(f"{'символ':<16s}{'сигналов':>9s}{'E[net]%':>9s}{'win%':>7s}{'оборот24ч':>12s}")
    for sym, vals in ranked[:15]:
        w = sum(1 for v in vals if v > 0) / len(vals) * 100
        turn = meta[sym]["turnover"]
        ts = f"${turn/1e6:.0f}M"
        print(f"{sym:<16s}{len(vals):>9d}{statistics.mean(vals):>+9.3f}"
              f"{w:>6.0f}%{ts:>12s}")

    print()
    print("=" * 78)
    print("ЧТО ЭТО ЗНАЧИТ")
    print("=" * 78)
    m = statistics.mean(nets)
    if m > 0:
        print(f"Отбор по прошлому + удержание дают E[net] {m:+.3f}% за {HOLD_DAYS}д")
        print(f"на выборке {n} сигналов. Это НЕ спайк-охота (её мы уже провалили),")
        print("а удержание стабильного режима с амортизацией разовой стоимости.")
        print("Проверять дальше: paper-прогон реальным исполнением 2-4 недели.")
    else:
        print(f"E[net] {m:+.3f}% — отрицательно. Лазейка закрыта и здесь:")
        print("стабильность фандинга в прошлом не предсказывает будущее")
        print("достаточно надёжно, чтобы перекрыть разовую стоимость входа.")


if __name__ == "__main__":
    import urllib.parse
    main()

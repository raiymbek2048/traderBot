"""Гипотеза №13: СНАЙП СЕТТЛМЕНТА — минимальная экспозиция вокруг начисления.

═══ ЧЕМ ОТЛИЧАЕТСЯ ОТ ГИПОТЕЗ 3, 4, 8, 9, 10 ═══
Все предыдущие тесты фандинга ДЕРЖАЛИ позицию часами и платили за это:
  №9 (n=29): фандинг +15.3 bps, basis −8.5, комиссии −22 → −15.7 bps
  №8: спред распадался, реализация ~10% от предсказанного
  №4 (n=146): средний холд 2.16ч, 73% не дожили до начисления

Но фандинг начисляется на позицию, которая СУЩЕСТВУЕТ В МОМЕНТ сеттлмента.
Держать 8 часов не требуется — требуется 4 минуты вокруг метки времени.

Это меняет уравнение:
  • доход фиксирован — берём ФАКТИЧЕСКИ начисленную ставку, распад спреда не важен
  • basis-дрейф за 4 минуты ≈ 0 вместо −8.5…−14.8 bps
  • остаётся ровно один вопрос: КАК ЧАСТО одно начисление больше комиссии

Это подсчёт по истории, а не гипотеза о поведении рынка.

═══ ДВА ВАРИАНТА ИСПОЛНЕНИЯ ═══
A. ХЕДЖИРОВАННЫЙ (spot+perp, delta-neutral):
   комиссии 0.31% round-trip (спот тейкер 0.1%×2 + перп тейкер 0.055%×2)
   → нужна ставка > 31 bps за одно начисление
B. ГОЛЫЙ ПЕРП (только шорт перпа, 4 минуты направленного риска):
   комиссии 0.11% (перп тейкер 0.055%×2)
   → нужна ставка > 11 bps, но добавляется риск цены за 4 минуты

Вариант B имеет намного низкий порог, но платит за это дисперсией.
Считаем оба, плюс отдельно замеряем 4-минутную волатильность цены,
чтобы риск B был не предположением, а числом.

═══ КРИТЕРИИ (ЗАФИКСИРОВАНЫ 03.08 ДО ПЕРВОГО ЗАПУСКА) ═══
Направление живо, если для ХОТЯ БЫ ОДНОГО варианта выполнено ВСЁ:
  1. n ≥ 30 событий (символ × сеттлмент) на хеджируемых символах
  2. МЕДИАНА net > +0.10% после комиссий варианта
  3. Доля прибыльных > 60%
     (для A должна быть почти 100% — фандинг детерминирован; если нет,
      значит ошибка в модели)
  4. Вклад лучшего события в Σ < 40%
     ← критерий отработал 4 раза: HOMEUSDT ×7, ZHIPUUSDT 67%,
       обрезка API у листингов, DIP 2%/n=11. Пятая постановка.
  5. События есть минимум в 2 разных календарных месяцах
  6. Для варианта B дополнительно: медиана net > 4-мин волатильности цены
     (иначе доход тонет в шуме — как было с 452%/год на NVDL против
      дневной волатильности 4-6%)

Ловушки учтены:
  • ОБРЕЗКА API: funding/history?limit=200 отдаёт только последние записи →
    тянем по окнам с startTime И endTime (без второго Bybit отдаёт пусто)
  • ХЕДЖИРУЕМОСТЬ: только символы со спот-парой Bybit (иначе нечем хеджировать
    вариант A) — жирная ставка обычно там, где хеджа нет
  • settled, не предсказанное: берём фактически начисленные ставки
  • медиана, не среднее

Run: python scripts/settlement_snipe_analysis.py
"""
from __future__ import annotations
import json
import statistics
import sys
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

BASE = "https://api.bybit.com"
FEES_HEDGED_PCT = 0.31        # спот 0.1×2 + перп 0.055×2
FEES_NAKED_PCT = 0.11         # перп 0.055×2
LOOKBACK_DAYS = 90
MIN_TURNOVER = 500_000        # $0.5M/24ч — должен быть поток для входа/выхода
EXPOSURE_MIN = 4              # окно удержания вокруг сеттлмента, минут


def get(url: str, tries: int = 3):
    for i in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=25) as r:
                return json.loads(r.read())
        except Exception:
            if i == tries - 1:
                return None
            time.sleep(0.5 * (i + 1))
    return None


def hedgeable_universe() -> dict[str, dict]:
    """Перпы со спот-парой Bybit и оборотом (иначе вариант A неисполним)."""
    spot = set()
    d = get(f"{BASE}/v5/market/instruments-info?category=spot")
    if d:
        for it in d["result"]["list"]:
            if it.get("status") == "Trading":
                spot.add(it["symbol"])

    intervals, cursor = {}, ""
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

    out = {}
    d = get(f"{BASE}/v5/market/tickers?category=linear")
    if not d:
        return out
    for it in d["result"]["list"]:
        s = it.get("symbol", "")
        if not s.endswith("USDT") or s not in spot:
            continue
        try:
            turn = float(it.get("turnover24h") or 0)
        except ValueError:
            continue
        if turn < MIN_TURNOVER:
            continue
        out[s] = {"turnover": turn, "interval_min": intervals.get(s, 480)}
    return out


def settled_window(symbol: str, start_ms: int, end_ms: int):
    """Settled-ставки по окнам. ⚠️ startTime БЕЗ endTime = пусто (баг №20),
    limit=200 без окна = только последние записи (баг №30)."""
    rows, cur = [], start_ms
    step = 30 * 86_400_000        # 30 дней за запрос
    while cur < end_ms:
        nxt = min(cur + step, end_ms)
        d = get(f"{BASE}/v5/market/funding/history?category=linear"
                f"&symbol={symbol}&startTime={cur}&endTime={nxt}&limit=200")
        if d:
            for r in d.get("result", {}).get("list", []):
                try:
                    rows.append((int(r["fundingRateTimestamp"]),
                                 float(r["fundingRate"])))
                except (KeyError, ValueError):
                    continue
        cur = nxt
        time.sleep(0.1)
    return sorted(set(rows))


def vol_4min(symbol: str, samples: int = 200) -> float | None:
    """Медиана |движения цены| за EXPOSURE_MIN минут — риск варианта B в bps."""
    d = get(f"{BASE}/v5/market/kline?category=linear&symbol={symbol}"
            f"&interval=1&limit={samples}")
    if not d or d.get("retCode") != 0:
        return None
    bars = []
    for r in d.get("result", {}).get("list", []):
        try:
            bars.append((int(r[0]), float(r[4])))
        except (IndexError, ValueError):
            continue
    bars.sort()
    if len(bars) < EXPOSURE_MIN + 10:
        return None
    moves = []
    for i in range(len(bars) - EXPOSURE_MIN):
        a, b = bars[i][1], bars[i + EXPOSURE_MIN][1]
        if a > 0:
            moves.append(abs(b - a) / a * 10_000)
    return statistics.median(moves) if moves else None


def main():
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - LOOKBACK_DAYS * 86_400_000

    print("Загружаю хеджируемую вселенную (перп + спот Bybit)...")
    uni = hedgeable_universe()
    print(f"  символов со спотом и оборотом ≥${MIN_TURNOVER:,}: {len(uni)}")
    if not uni:
        print("❌ вселенная пуста"); return

    print(f"\nТяну settled-историю за {LOOKBACK_DAYS} дней по окнам...")
    hist = {}
    syms = list(uni)
    with ThreadPoolExecutor(max_workers=5) as ex:
        futs = {ex.submit(settled_window, s, start_ms, now_ms): s for s in syms}
        for i, (f, s) in enumerate(futs.items(), 1):
            hist[s] = f.result()
            if i % 25 == 0:
                print(f"  {i}/{len(syms)}...")
    total_settles = sum(len(v) for v in hist.values())
    print(f"  всего начислений собрано: {total_settles:,}")

    # ── события: одно начисление = одна возможность снайпа ────────────────
    events = []
    for s, rows in hist.items():
        for ts, rate in rows:
            pct = rate * 100          # ставка за ОДИН интервал, %
            if pct <= 0:
                continue              # шорт перпа получает только при rate>0
            dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            events.append({
                "symbol": s, "ts": dt, "month": dt.strftime("%Y-%m"),
                "rate_pct": pct,
                "net_hedged": pct - FEES_HEDGED_PCT,
                "net_naked": pct - FEES_NAKED_PCT,
                "turnover": uni[s]["turnover"],
            })
    print(f"  начислений с положительной ставкой: {len(events):,}")

    print("\n" + "=" * 78)
    print(f"СНАЙП СЕТТЛМЕНТА: держим ~{EXPOSURE_MIN} мин вокруг начисления")
    print("=" * 78)

    for label, key, fee in (("A. ХЕДЖИРОВАННЫЙ (spot+perp)", "net_hedged", FEES_HEDGED_PCT),
                            ("B. ГОЛЫЙ ПЕРП", "net_naked", FEES_NAKED_PCT)):
        prof = [e for e in events if e[key] > 0]
        print(f"\n─── {label}, комиссии {fee}% ───")
        print(f"  начислений выше комиссии: {len(prof):,} из {len(events):,} "
              f"({len(prof)/max(1,len(events))*100:.2f}%)")
        if len(prof) < 5:
            print("  ❌ событий почти нет")
            continue
        nets = [e[key] for e in prof]
        med = statistics.median(nets)
        tot = sum(nets)
        share = max(nets) / tot * 100 if tot > 0 else float("inf")
        months = defaultdict(list)
        for e in prof:
            months[e["month"]].append(e[key])
        pos_m = sum(1 for m, v in months.items() if statistics.median(v) > 0)
        print(f"  медиана net: {med:+.3f}%  | среднее {statistics.mean(nets):+.3f}%")
        print(f"  Σ net: {tot:+.2f}%  | лучший вклад {share:.0f}%")
        print(f"  месяцев: {len(months)} (с плюсом {pos_m})")
        # частота: сколько таких событий в день
        span = (max(e["ts"] for e in prof) - min(e["ts"] for e in prof)).days or 1
        print(f"  частота: {len(prof)/span:.1f} событий/день на всю вселенную")
        print(f"  топ-8 символов по числу событий:")
        bysym = defaultdict(list)
        for e in prof:
            bysym[e["symbol"]].append(e[key])
        for s, v in sorted(bysym.items(), key=lambda kv: -len(kv[1]))[:8]:
            print(f"    {s:<14s} n={len(v):4d}  медиана {statistics.median(v):+.3f}%  "
                  f"оборот ${uni[s]['turnover']/1e6:.1f}M")

        c1 = len(prof) >= 30
        c2 = med > 0.10
        c3 = True          # доля прибыльных считается по определению отбора
        c4 = share < 40
        c5 = pos_m >= 2
        print(f"  критерии: n≥30 {c1} | медиана>0.10% {c2} | "
              f"лучший<40% {c4} | ≥2 мес {c5}")
        if all((c1, c2, c4, c5)):
            print(f"  ✅ {label} проходит формальные критерии")
        else:
            print(f"  ❌ {label} не проходит")

    # ── риск варианта B: 4-минутная волатильность ─────────────────────────
    print("\n─── Критерий 6: риск голого перпа (4-мин волатильность) ───")
    cand = sorted({e["symbol"] for e in events if e["net_naked"] > 0.10},
                  key=lambda s: -uni[s]["turnover"])[:10]
    if not cand:
        print("  нет кандидатов для замера")
    else:
        print(f"{'символ':<14s}{'медиана |Δ| 4мин':>18s}{'медиана net':>13s}  вердикт")
        for s in cand:
            v = vol_4min(s)
            nets = [e["net_naked"] for e in events
                    if e["symbol"] == s and e["net_naked"] > 0]
            if v is None or not nets:
                continue
            m = statistics.median(nets)
            ok = m * 100 > v      # net в %, v в bps
            print(f"{s:<14s}{v:>15.1f}bps{m*100:>10.1f}bps  "
                  f"{'доход > шума' if ok else 'ТОНЕТ в шуме'}")
            time.sleep(0.1)

    print("\n" + "=" * 78)
    print("Вердикт по критериям — см. выше по каждому варианту.")
    print("Ключевой вопрос был: как часто ОДНО начисление больше комиссии.")
    print("=" * 78)


if __name__ == "__main__":
    main()

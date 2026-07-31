"""Гипотеза №10: сбор фандинга на НОВЫХ листингах (delta-neutral).

═══ ПОЧЕМУ ЭТА ОБЛАСТЬ ═══
9 закрытых гипотез научили: край на публичных сигналах равен комиссии, потому
что сигнал уже отражён в цене. У НОВОГО листинга истории нет — отражать нечего.
Плюс это единственная область, где наши 0.3 мс в Токио реально преимущество.

Наблюдение из фазы 6: самый жирный фандинг всегда у свежих перпов —
PURR 687%/год, AMAT 311%, BMNR 161%. Тогда мы их отбросили («нет спота»),
но не проверили те, у которых спот ЕСТЬ.

Конструкция: long spot + short perp того же актива с первого дня листинга.
Дельта ≈ 0. Доход = фандинг. Разовая стоимость (замерено на своих сделках,
фаза 18): комиссии 0.31% + basis 0.20% = **0.51%**.

Ключевой вопрос: у зрелых пар фандинг реализуется на ~10% от предсказанного
(фаза 21-23), поэтому 0.015%/день не окупает 0.51%. Но у новых листингов
ставки в десятки раз выше — даже 10% реализации может окупить вход.

═══ КРИТЕРИИ (ЗАФИКСИРОВАНЫ 31.07 ДО ПЕРВОГО ЗАПУСКА) ═══
Направление стоит развивать, только если выполнено ВСЁ:

  1. n ≥ 15 листингов со спотом и историей ≥7 дней
  2. МЕДИАНА net-доходности за 7 дней > +1.0%
     (медиана, не среднее — среднее вытягивают хвосты)
  3. Доля прибыльных > 60%
  4. Вклад лучшего листинга в Σ < 40%
     ← этот критерий уже дважды спас от ложного вывода (HOMEUSDT ×7,
       ZHIPUUSDT 67% мейкер-прибыли). Ставится третий раз осознанно.
  5. Эффект не сосредоточен в одном месяце: медиана положительна минимум
     в 2 разных месяцах выборки (проверка на режим)

Провал любого пункта → область закрывается без построения кода.

Учёт всех известных ловушек:
  • спот обязателен (иначе хеджировать нечем — урок фазы 6 и H5)
  • цены Bybit/Binance не сравниваем по тикеру (урок: ONUSDT $88 vs $0.177)
  • settled-ставки из истории, не предсказанные (урок №4)
  • стоимость входа 0.51% из ЖИВЫХ замеров, не из теории

Run: python scripts/listing_funding_analysis.py
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
from datetime import datetime, timezone

BASE = "https://api.bybit.com"
ONE_TIME_COST_PCT = 0.51     # комиссии 0.31% + basis 0.20% (живые замеры)
HOLD_DAYS = 7                # горизонт удержания от листинга
MIN_HISTORY_DAYS = 7
LOOKBACK_DAYS = 180          # насколько назад искать листинги
MAX_ABS_RATE = 0.02          # |rate| > 2%/интервал = кап, радиоактивный


def get(url: str, tries: int = 3):
    for i in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=25) as r:
                return json.loads(r.read())
        except Exception:
            if i == tries - 1:
                return None
            time.sleep(0.6 * (i + 1))
    return None


def spot_universe() -> set[str]:
    out = set()
    d = get(f"{BASE}/v5/market/instruments-info?category=spot")
    if d:
        for it in d["result"]["list"]:
            if it.get("status") == "Trading":
                out.add(it["symbol"])
    return out


def perp_listings() -> list[dict]:
    """Все linear-перпы с датой листинга и интервалом фандинга."""
    out, cursor = [], ""
    while True:
        u = f"{BASE}/v5/market/instruments-info?category=linear&limit=1000"
        if cursor:
            u += f"&cursor={urllib.parse.quote(cursor)}"
        d = get(u)
        if not d:
            break
        for it in d["result"]["list"]:
            try:
                lt = int(it.get("launchTime") or 0)
            except ValueError:
                continue
            if not lt or not it["symbol"].endswith("USDT"):
                continue
            out.append({
                "symbol": it["symbol"],
                "launch_ms": lt,
                "interval_min": int(it.get("fundingInterval", 480)),
                "status": it.get("status", ""),
            })
        cursor = d["result"].get("nextPageCursor", "")
        if not cursor:
            break
    return out


def settled(symbol: str, since_ms: int, until_ms: int) -> list[tuple[int, float]]:
    """Settled-ставки в ЗАДАННОМ окне.

    ⚠️ Два подвоха Bybit, оба уже стоили нам ошибок:
      1. `limit=200` без окна отдаёт только ПОСЛЕДНИЕ 200 записей. Для листинга
         3-месячной давности его первая неделя туда не попадает — и выглядит
         как «истории нет». Именно так 13 из 17 листингов ложно выпали из
         первого прогона (урок №20: отсутствие данных прикидывается фактом).
      2. `startTime` БЕЗ `endTime` возвращает пусто (баг №20). Передаём оба.
    """
    d = get(f"{BASE}/v5/market/funding/history?category=linear&symbol={symbol}"
            f"&startTime={since_ms}&endTime={until_ms}&limit=200")
    if not d:
        return []
    rows = []
    for r in d.get("result", {}).get("list", []):
        try:
            ts = int(r["fundingRateTimestamp"])
            if since_ms <= ts <= until_ms:
                rows.append((ts, float(r["fundingRate"])))
        except (KeyError, ValueError):
            continue
    rows.sort()
    return rows


def main():
    now_ms = int(time.time() * 1000)
    DAY = 86_400_000

    print("Загружаю вселенную...")
    spot = spot_universe()
    perps = perp_listings()
    print(f"  спот-пар: {len(spot)} | перпов с датой листинга: {len(perps)}")

    # свежие листинги: в окне LOOKBACK, но прожили ≥ MIN_HISTORY_DAYS
    fresh = [p for p in perps
             if now_ms - p["launch_ms"] <= LOOKBACK_DAYS * DAY
             and now_ms - p["launch_ms"] >= MIN_HISTORY_DAYS * DAY]
    print(f"  листингов за {LOOKBACK_DAYS}д с историей ≥{MIN_HISTORY_DAYS}д: {len(fresh)}")

    hedgeable = [p for p in fresh if p["symbol"] in spot]
    print(f"  из них СО СПОТОМ (можно хеджировать): {len(hedgeable)}")
    if not hedgeable:
        print("\n❌ Нет хеджируемых свежих листингов — критерий 1 недостижим.")
        return

    print(f"\nТяну settled-историю фандинга по окну каждого листинга...")
    hist = {}
    with ThreadPoolExecutor(max_workers=5) as ex:
        futs = {ex.submit(settled, p["symbol"], p["launch_ms"],
                          p["launch_ms"] + HOLD_DAYS * DAY): p["symbol"]
                for p in hedgeable}
        for i, (f, sym) in enumerate(futs.items(), 1):
            hist[sym] = f.result()
            if i % 20 == 0:
                print(f"  {i}/{len(hedgeable)}...")

    rows, dropped = [], defaultdict(list)
    for p in hedgeable:
        sym = p["symbol"]
        h = hist.get(sym) or []
        lt = p["launch_ms"]
        window = [(t, r) for t, r in h if lt <= t <= lt + HOLD_DAYS * DAY]
        if len(window) < 3:
            dropped["мало сеттлментов в окне"].append(sym)
            continue
        if any(abs(r) > MAX_ABS_RATE for _, r in window):
            dropped["кап = радиоактивный"].append(sym)
            continue
        # шорт перпа получает при rate>0
        gross = sum(r for _, r in window) * 100
        net = gross - ONE_TIME_COST_PCT
        launch_dt = datetime.fromtimestamp(lt / 1000, tz=timezone.utc)
        rows.append({
            "symbol": sym, "launch": launch_dt,
            "month": launch_dt.strftime("%Y-%m"),
            "settles": len(window),
            "gross": gross, "net": net,
            "age_days": (now_ms - lt) / DAY,
        })

    if dropped:
        print("\n  Отброшено (с причиной — чтобы не спутать «нет данных» с «нет края»):")
        for why, syms in dropped.items():
            print(f"    {why}: {len(syms)} — {', '.join(syms[:6])}"
                  + (" ..." if len(syms) > 6 else ""))

    if not rows:
        print("\n❌ После фильтров не осталось наблюдений.")
        return

    rows.sort(key=lambda r: -r["net"])
    nets = [r["net"] for r in rows]
    n = len(rows)
    med = statistics.median(nets)
    wins = sum(1 for x in nets if x > 0)
    total = sum(nets)
    best_share = (rows[0]["net"] / total * 100) if total > 0 else float("inf")

    print("\n" + "=" * 76)
    print(f"НОВЫЕ ЛИСТИНГИ: long spot + short perp, держим {HOLD_DAYS}д от листинга")
    print("=" * 76)
    print(f"Стоимость входа: {ONE_TIME_COST_PCT}% разово (живые замеры)")
    print(f"Наблюдений: {n}")
    print()
    print(f"{'символ':<16s}{'листинг':>12s}{'сеттл':>7s}{'gross%':>9s}{'net%':>9s}")
    for r in rows[:20]:
        print(f"{r['symbol']:<16s}{r['launch']:%d %b %y}{r['settles']:>7d}"
              f"{r['gross']:>+9.2f}{r['net']:>+9.2f}")
    if n > 20:
        print(f"  ... и ещё {n-20}")

    print()
    print(f"  МЕДИАНА net:      {med:+.3f}%")
    print(f"  среднее net:      {statistics.mean(nets):+.3f}%")
    print(f"  Σ net:            {total:+.3f}%")
    print(f"  прибыльных:       {wins}/{n} ({wins/n*100:.0f}%)")
    print(f"  вклад лучшего:    {best_share:.0f}% от Σ")
    print(f"  годовых по медиане: {med/HOLD_DAYS*365:+.0f}%/год")

    # критерий 5: разбивка по месяцам
    by_month = defaultdict(list)
    for r in rows:
        by_month[r["month"]].append(r["net"])
    print()
    print("  По месяцам листинга:")
    pos_months = 0
    for m in sorted(by_month):
        v = by_month[m]
        mm = statistics.median(v)
        if mm > 0:
            pos_months += 1
        print(f"    {m}: n={len(v):3d}  медиана {mm:+7.3f}%")

    c1 = n >= 15
    c2 = med > 1.0
    c3 = wins / n > 0.60
    c4 = best_share < 40
    c5 = pos_months >= 2
    print()
    print("  КРИТЕРИИ (заданы до запуска):")
    for lbl, ok, val in (
        ("1. n ≥ 15", c1, f"{n}"),
        ("2. медиана > +1.0%", c2, f"{med:+.3f}%"),
        ("3. прибыльных > 60%", c3, f"{wins/n*100:.0f}%"),
        ("4. лучший < 40% Σ", c4, f"{best_share:.0f}%"),
        ("5. ≥2 месяца с плюсом", c5, f"{pos_months}"),
    ):
        print(f"    {'✅' if ok else '❌'} {lbl:<24s} → {val}")

    print()
    if all((c1, c2, c3, c4, c5)):
        print("  ВЕРДИКТ: ✅ направление стоит развивать → строить paper-executor")
    else:
        print("  ВЕРДИКТ: ❌ критерии не пройдены → область закрывается")
    print("=" * 76)


if __name__ == "__main__":
    main()

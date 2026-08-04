"""Гипотеза №14: дифференциал фандинга RISEx vs Bybit (носитель для фарма поинтов).

═══ ЧЕМ ЭТО ОТЛИЧАЕТСЯ ОТ ГИПОТЕЗ 3, 4, 8, 9, 10, 13 ═══
Все они искали ДОХОД ОТ ФАНДИНГА как самоцель и упёрлись в то, что ставки на
хеджируемых символах ниже издержек (гипотеза 13: 0 из 51 810 начислений выше
комиссии хеджа).

Здесь фандинг — не цель, а вопрос СТОИМОСТИ УДЕРЖАНИЯ позиции, которая нужна
для другого: копить поинты RISEx Ignite (начисляются за open interest × время,
а не за оборот — проверено по анонсу программы).

Логика: если дифференциал фандинга положителен, позиция не стоит денег, а
приносит, и поинты идут бесплатным приложением. Если отрицателен — это цена
лотерейного билета, и её надо знать заранее.

═══ КОНСТРУКЦИЯ ═══
Дельта-нейтрально, один актив, две площадки:
  лонг RISEx + шорт Bybit   → нетто = (фандинг Bybit) − (фандинг RISEx)
  шорт RISEx + лонг Bybit   → нетто = (фандинг RISEx) − (фандинг Bybit)
Выбираем направление с положительным нетто. Итого нетто = |разница|.

Ставки нормализуются в %/год: RISEx платит ПОЧАСОВО (interval 1ч),
Bybit — по своему интервалу (обычно 8ч, бывает 1/2/4ч).

Издержки (наши ФАКТИЧЕСКИЕ ставки, проверены через API 3 авг):
  RISEx Tier 1:  taker 3.0 bps / maker 1.0 bps, газ спонсируется
  Bybit:         taker 10.0 bps / maker 3.6 bps  ← ВЫШЕ публичного тарифа 5.5!
  разовый цикл тейкером: 0.06% + 0.20% = 0.26%
  разовый цикл мейкером: 0.02% + 0.072% = 0.092%

═══ КРИТЕРИИ (ЗАФИКСИРОВАНЫ 4 АВГ ДО ПЕРВОГО ЗАПУСКА) ═══
Носитель годится, если выполнено ВСЁ:
  1. Пересекающейся истории ≥ 14 дней на символ
  2. МЕДИАНА дифференциала > +0.02%/день (= +7.3%/год)
     порог выбран так, чтобы бить оба наших замеренных бенчмарка с запасом:
     Bybit USDT savings 1.7%/год и cash-and-carry 3.0%/год
  3. ЗНАК дифференциала устойчив: одно направление в ≥70% дней
     (иначе придётся переворачиваться и платить 0.26% каждый раз)
  4. Квалифицируются ≥3 символа (не одна везучая пара)
  5. Open interest RISEx по символу > $200k (есть куда войти и выйти)

⚠️ Отдельно фиксирую: снимок 4 авг показывал BTC RISEx −3.31%/год против
Bybit +3.0%/год, то есть +6.31% нетто. Но снимок — это НЕ замер. Проект дважды
поймал ровно эту ошибку: FHE давал +0.454%/день и обвалился в 30 раз; NVDL
показывал предсказанные 0.4255%, а начислялось 0.0000%. Поэтому смотрим историю.

Run: python scripts/risex_funding_diff.py
"""
from __future__ import annotations
import json
import statistics
import sys
import time
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone

RISEX = "https://api.rise.trade"
BYBIT = "https://api.bybit.com"
LOOKBACK_DAYS = 90
MIN_DAYS = 14
MIN_MEDIAN_DAILY = 0.02      # %/день
MIN_SIGN_STABILITY = 0.70
MIN_SYMBOLS = 3
MIN_OI_USD = 200_000
FEE_CYCLE_TAKER = 0.26       # % разово
FEE_CYCLE_MAKER = 0.092


# ⚠️ RISEx отдаёт 403 на дефолтный User-Agent urllib (curl при этом работает).
# Без заголовка функция молча возвращала None → «активных рынков: 0», что
# выглядело как «API не ответил», а не как «нас заблокировали по UA».
_UA = {"User-Agent": "Mozilla/5.0 (traderbot research)"}


def get(url: str, tries: int = 3):
    last = None
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers=_UA)
            with urllib.request.urlopen(req, timeout=25) as r:
                return json.loads(r.read())
        except Exception as e:
            last = e
            if i == tries - 1:
                print(f"    [get] {url[:70]}… → {type(e).__name__}: {str(e)[:60]}")
                return None
            time.sleep(0.5 * (i + 1))
    return None


def risex_markets() -> dict[str, dict]:
    """{BASE: {market_id, oi_usd, funding_interval_h, ...}} по активным рынкам."""
    d = get(f"{RISEX}/v1/markets")
    if not d:
        return {}
    out = {}
    for m in d.get("data", {}).get("markets", []):
        if not m.get("active"):
            continue
        name = (m.get("display_name") or "")
        if "deprecated" in name or "/" not in name:
            continue
        base = name.split("/")[0]
        try:
            mark = float(m.get("mark_price") or 0)
            oi = float(m.get("open_interest") or 0)
            iv_ns = float(m.get("funding_interval") or 3.6e12)
        except (TypeError, ValueError):
            continue
        out[base] = {
            "market_id": str(m.get("market_id")),
            "oi_usd": oi * mark,
            "interval_h": iv_ns / 3.6e12,
            "mark": mark,
            "vol24h": float(m.get("quote_volume_24h") or 0),
        }
    return out


def risex_history(market_id: str, start_ns: int, end_ns: int):
    """Часовые начисления. Пагинация по 1000."""
    rows, page = [], 1
    while page <= 20:
        d = get(f"{RISEX}/v1/markets/id/{market_id}/funding-rate-history"
                f"?page={page}&limit=1000&start_time={start_ns}&end_time={end_ns}")
        if not d:
            break
        recs = (d.get("data") or {}).get("records") or d.get("records") or []
        if not recs:
            break
        for r in recs:
            try:
                rows.append((int(r["start_time"]) // 1_000_000,   # → ms
                             float(r["funding_rate"])))
            except (KeyError, ValueError):
                continue
        if len(recs) < 1000:
            break
        page += 1
        time.sleep(0.12)
    return sorted(set(rows))


def bybit_history(symbol: str, start_ms: int, end_ms: int):
    """settled-ставки Bybit. ⚠️ startTime без endTime = пусто (баг №20),
    limit=200 без окна = только последние (баг №30)."""
    rows, cur = [], start_ms
    step = 30 * 86_400_000
    while cur < end_ms:
        nxt = min(cur + step, end_ms)
        d = get(f"{BYBIT}/v5/market/funding/history?category=linear"
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


def bybit_intervals() -> dict[str, float]:
    out, cursor = {}, ""
    while True:
        u = f"{BYBIT}/v5/market/instruments-info?category=linear&limit=1000"
        if cursor:
            u += f"&cursor={cursor}"
        d = get(u)
        if not d:
            break
        for it in d["result"]["list"]:
            out[it["symbol"]] = int(it.get("fundingInterval", 480)) / 60
        cursor = d["result"].get("nextPageCursor", "")
        if not cursor:
            break
    return out


def daily_series(rows, interval_h: float) -> dict[str, float]:
    """Суммарный фандинг за календарный день, в % за день."""
    by_day = defaultdict(float)
    for ms, rate in rows:
        day = datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        by_day[day] += rate * 100
    return dict(by_day)


def main():
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - LOOKBACK_DAYS * 86_400_000

    print("Загружаю рынки RISEx...")
    rx = risex_markets()
    print(f"  активных рынков: {len(rx)}")
    if not rx:
        print("❌ API не ответил"); return

    by_iv = bybit_intervals()
    # сопоставление: BTC → BTCUSDT
    pairs = []
    for base, info in rx.items():
        sym = f"{base}USDT"
        if sym in by_iv:
            pairs.append((base, sym, info))
    print(f"  есть и на Bybit: {len(pairs)} — {', '.join(p[0] for p in pairs)}")
    print(f"  только на RISEx (нечем хеджировать): "
          f"{', '.join(sorted(set(rx) - {p[0] for p in pairs}))}")

    print(f"\nТяну историю за {LOOKBACK_DAYS} дней...")
    results = []
    for base, sym, info in pairs:
        rxh = risex_history(info["market_id"], start_ms * 1_000_000,
                            now_ms * 1_000_000)
        byh = bybit_history(sym, start_ms, now_ms)
        if not rxh or not byh:
            print(f"  {base:<6s} нет данных (RISEx {len(rxh)}, Bybit {len(byh)})")
            continue

        rx_daily = daily_series(rxh, info["interval_h"])
        by_daily = daily_series(byh, by_iv.get(sym, 8))
        common = sorted(set(rx_daily) & set(by_daily))
        if len(common) < 3:
            print(f"  {base:<6s} пересечение {len(common)} дней — мало")
            continue

        # дифференциал за день: |Bybit − RISEx|, направление выбираем
        diffs = [by_daily[d] - rx_daily[d] for d in common]
        med = statistics.median(diffs)
        direction = "лонг RISEx + шорт Bybit" if med > 0 else "шорт RISEx + лонг Bybit"
        # устойчивость знака в выбранном направлении
        sign_ok = sum(1 for x in diffs if (x > 0) == (med > 0)) / len(diffs)
        results.append({
            "base": base, "days": len(common),
            "med_daily": abs(med), "annual": abs(med) * 365,
            "sign_stab": sign_ok, "direction": direction,
            "rx_med": statistics.median(list(rx_daily.values())),
            "by_med": statistics.median(list(by_daily.values())),
            "oi": info["oi_usd"], "vol": info["vol24h"],
            "first": common[0], "last": common[-1],
        })
        print(f"  {base:<6s} {len(common):>3d} дней  дифф.медиана {abs(med):+.4f}%/д "
              f"({abs(med)*365:+.1f}%/год)  знак {sign_ok*100:.0f}%")

    if not results:
        print("\n❌ Нет ни одного символа с достаточной историей.")
        return

    results.sort(key=lambda r: -r["annual"])
    print("\n" + "=" * 84)
    print("ДИФФЕРЕНЦИАЛ ФАНДИНГА RISEx vs BYBIT (носитель для фарма поинтов)")
    print("=" * 84)
    print(f"История: {results[0]['first']} → {results[0]['last']}")
    print(f"Разовые издержки: тейкером {FEE_CYCLE_TAKER}% | мейкером {FEE_CYCLE_MAKER}%")
    print()
    print(f"{'символ':<8s}{'дней':>5s}{'RISEx/д':>10s}{'Bybit/д':>10s}"
          f"{'дифф/д':>10s}{'%/год':>9s}{'знак':>7s}{'OI':>10s}  направление")
    for r in results:
        print(f"{r['base']:<8s}{r['days']:>5d}{r['rx_med']:>+10.4f}{r['by_med']:>+10.4f}"
              f"{r['med_daily']:>+10.4f}{r['annual']:>+9.1f}{r['sign_stab']*100:>6.0f}%"
              f"{r['oi']/1e6:>9.2f}M  {r['direction']}")

    # ── критерии ──────────────────────────────────────────────────────────
    qual = [r for r in results
            if r["days"] >= MIN_DAYS
            and r["med_daily"] > MIN_MEDIAN_DAILY
            and r["sign_stab"] >= MIN_SIGN_STABILITY
            and r["oi"] >= MIN_OI_USD]

    print()
    print("КРИТЕРИИ (заданы до запуска):")
    print(f"  1. история ≥{MIN_DAYS} дней")
    print(f"  2. медиана дифференциала > {MIN_MEDIAN_DAILY}%/день (={MIN_MEDIAN_DAILY*365:.1f}%/год)")
    print(f"  3. знак устойчив ≥{MIN_SIGN_STABILITY*100:.0f}% дней")
    print(f"  4. квалифицируются ≥{MIN_SYMBOLS} символа")
    print(f"  5. OI RISEx > ${MIN_OI_USD:,}")
    print()
    print(f"  Прошли символы: {len(qual)} — "
          f"{', '.join(r['base'] for r in qual) if qual else 'нет'}")
    for r in qual:
        days_to_be = FEE_CYCLE_TAKER / r["med_daily"] if r["med_daily"] else 999
        print(f"    {r['base']:<7s} {r['annual']:+.1f}%/год, окупает вход за "
              f"{days_to_be:.1f} дней, знак {r['sign_stab']*100:.0f}%")

    ok = len(qual) >= MIN_SYMBOLS
    print()
    print("=" * 84)
    if ok:
        print("✅ НОСИТЕЛЬ ГОДИТСЯ: дифференциал платит за удержание позиции,")
        print("   поинты Ignite идут бесплатным приложением.")
        print("   Следующий шаг: paper-прогон на реальном исполнении.")
    else:
        print("❌ Критерии не пройдены. Удержание позиции стоит денег —")
        print("   это цена лотерейного билета, и теперь она известна.")
    print("=" * 84)


if __name__ == "__main__":
    main()

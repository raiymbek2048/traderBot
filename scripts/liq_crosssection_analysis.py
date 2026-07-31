"""Гипотеза №12: кросс-секционный сдвиг после каскада на BTC.

═══ ЧЕМ ОТЛИЧАЕТСЯ ОТ ГИПОТЕЗ №5 и №11 ═══
Оба предыдущих теста на ликвидациях мерили АБСОЛЮТНОЕ движение того же символа:
  №5  (фаза 13-14): каскад → вход по рынку → следуем импульсу     → провал
  №11 (фаза 25):    лимитка ниже рынка → каскад заливает          → провал

Здесь мерим ОТНОСИТЕЛЬНОЕ движение: после крупного каскада на BTC альты
систематически отстают или опережают BTC? Сделка парная и нейтральная к рынку:
long alt + short BTC (или наоборот), обе ноги перп.

Почему МИНУТНЫЙ масштаб может выжить там, где секундный мёртв: HFT выбирают
субсекундные лаги (фаза 3: наш ордер 60-130мс против их 0.001-0.005мс — разрыв
×20000). Минутный дрейф — другое явление: медленное перераспределение капитала,
где скорость исполнения не решает.

═══ МЕТОД ═══
Триггер: каскад Sell-ликвидаций на BTCUSDT ≥ MIN_CASCADE (наши данные).
В момент t0 = конец каскада:
  ret_btc = (close(t0+H) − close(t0)) / close(t0)
  ret_alt = то же для альта
  spread  = ret_alt − ret_btc          ← это и есть PnL парной сделки
Если spread систематически положителен → long alt + short BTC.
Если систематически отрицателен → обратная сделка (симметрично, знак учтён).

Комиссии: 4 ноги тейкером = 0.22% (мейкер не берём — makerprobe показал,
что двусторонний пассивный вход даёт 17% непарных заливов ценой +34 bps).

═══ КРИТЕРИИ (ЗАФИКСИРОВАНЫ 31.07 ДО ПЕРВОГО ЗАПУСКА) ═══
  1. n ≥ 30 наблюдений на паре (символ × каскад)
  2. |МЕДИАНА| net > +0.10%  (после 0.22% комиссий)
  3. Доля прибыльных в направлении медианы > 55%
  4. Вклад лучшего наблюдения в Σ < 40%
     ← критерий срабатывал 4 раза: HOMEUSDT ×7, ZHIPUUSDT 67%,
       обрезка API у листингов, DIP 2%/n=11. Пятая постановка.
  5. Знак медианы совпадает минимум в 2 разных календарных неделях
     (иначе это режим, а не эффект — урок liq-momentum)

Провал любого → гипотеза закрывается.

Ловушки учтены:
  • тикер ≠ актив: работаем только внутри Bybit, одна биржа, сравнение цен не нужно
  • lookahead: t0 — конец каскада, доходности считаются строго после
  • режим: критерий 5 требует устойчивости знака по неделям
  • толстый хвост: смотрим медиану, а не среднее (урок №11: среднее +0.22%
    при медиане −0.10%)

Run: python scripts/liq_crosssection_analysis.py
"""
from __future__ import annotations
import json
import sqlite3
import statistics
import sys
import time
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone

DB = ("/private/tmp/claude-501/-Users-raiymbekdaniiaruulu-IdealProjects-StartTups-claud/"
      "ca35c44a-7b50-4c62-b973-d2f29e7a1363/scratchpad/fresh.db")
BASE = "https://api.bybit.com"

TRIGGER = "BTCUSDT"
CASCADE_WINDOW_S = 15
MIN_CASCADE_USD = 300_000        # крупные каскады на BTC
HORIZONS_MIN = (5, 15, 60)
FEE_PAIR_PCT = 0.22              # 4 ноги тейкером
ALTS = ["ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT",
        "NEARUSDT", "ZECUSDT", "HYPEUSDT", "1000PEPEUSDT", "LINKUSDT"]


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


def klines(symbol: str, start_ms: int, end_ms: int):
    out, cur = [], start_ms
    while cur < end_ms:
        nxt = min(cur + 1000 * 60_000, end_ms)
        d = get(f"{BASE}/v5/market/kline?category=linear&symbol={symbol}"
                f"&interval=1&start={cur}&end={nxt}&limit=1000")
        if d and d.get("retCode") == 0:
            for r in d.get("result", {}).get("list", []):
                try:
                    out.append((int(r[0]), float(r[4])))
                except (IndexError, ValueError):
                    continue
        cur = nxt
        time.sleep(0.12)
    return sorted(set(out))


def pts(s):
    for f in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, f).replace(tzinfo=timezone.utc)
        except ValueError:
            continue


def btc_cascades() -> list[dict]:
    conn = sqlite3.connect(DB)
    rows = conn.execute(
        "SELECT price, value_usdt, ts FROM liq_events "
        "WHERE symbol=? AND side='Sell' ORDER BY ts", (TRIGGER,)).fetchall()
    conn.close()
    out, cur = [], None
    for px, val, ts in rows:
        t = pts(ts)
        if not t:
            continue
        if cur and (t - cur["end"]).total_seconds() <= CASCADE_WINDOW_S:
            cur["end"] = t
            cur["value"] += (val or 0)
            cur["count"] += 1
        else:
            if cur and cur["value"] >= MIN_CASCADE_USD:
                out.append(cur)
            cur = {"end": t, "value": val or 0, "count": 1}
    if cur and cur["value"] >= MIN_CASCADE_USD:
        out.append(cur)
    return out


def px_at(bars, ts_ms):
    """close первой свечи с ts >= ts_ms"""
    for t, c in bars:
        if t >= ts_ms:
            return c
    return None


def main():
    print("Читаю каскады BTC из своего датасета...")
    cas = btc_cascades()
    print(f"  Sell-каскадов BTC ≥${MIN_CASCADE_USD:,}: {len(cas)}")
    if len(cas) < 30:
        print(f"\n❌ Каскадов меньше 30 — критерий 1 недостижим по построению.")
        if not cas:
            return

    lo = min(c["end"] for c in cas) - timedelta(minutes=5)
    hi = max(c["end"] for c in cas) + timedelta(minutes=max(HORIZONS_MIN) + 10)
    lo_ms, hi_ms = int(lo.timestamp() * 1000), int(hi.timestamp() * 1000)

    print(f"\nТяну свечи {TRIGGER} + {len(ALTS)} альтов...")
    bars = {}
    for i, s in enumerate([TRIGGER] + ALTS, 1):
        b = klines(s, lo_ms, hi_ms)
        if len(b) > 100:
            bars[s] = b
        print(f"  {i}/{len(ALTS)+1} {s}: {len(b)} свечей"
              + ("" if len(b) > 100 else "  ← мало, исключён"))

    if TRIGGER not in bars:
        print("нет свечей BTC"); return

    # ── сбор наблюдений ───────────────────────────────────────────────────
    obs: dict[tuple[str, int], list[dict]] = defaultdict(list)
    for c in cas:
        t0 = int(c["end"].timestamp() * 1000)
        week = c["end"].strftime("%G-W%V")
        b0 = px_at(bars[TRIGGER], t0)
        if not b0:
            continue
        for h in HORIZONS_MIN:
            b1 = px_at(bars[TRIGGER], t0 + h * 60_000)
            if not b1:
                continue
            ret_btc = (b1 - b0) / b0 * 100
            for alt in ALTS:
                ab = bars.get(alt)
                if not ab:
                    continue
                a0 = px_at(ab, t0)
                a1 = px_at(ab, t0 + h * 60_000)
                if not a0 or not a1:
                    continue
                ret_alt = (a1 - a0) / a0 * 100
                obs[(alt, h)].append({
                    "spread": ret_alt - ret_btc, "week": week,
                    "ret_alt": ret_alt, "ret_btc": ret_btc,
                })

    print("\n" + "=" * 80)
    print(f"КРОСС-СЕКЦИЯ ПОСЛЕ КАСКАДА BTC: spread = ret_alt − ret_btc")
    print("=" * 80)
    print(f"Комиссии парной сделки {FEE_PAIR_PCT}% (4 ноги тейкером)")
    print()
    print(f"{'альт':<14s}{'H':>4s}{'n':>5s}{'медиана':>10s}{'среднее':>10s}"
          f"{'|net|':>9s}{'win%':>7s}{'лучш':>7s}{'нед±':>6s}")

    passed = []
    for alt in ALTS:
        for h in HORIZONS_MIN:
            r = obs.get((alt, h)) or []
            if len(r) < 5:
                continue
            sp = [x["spread"] for x in r]
            med = statistics.median(sp)
            direction = 1 if med >= 0 else -1
            # PnL в направлении медианы, за вычетом комиссий
            nets = [direction * x["spread"] - FEE_PAIR_PCT for x in r]
            med_net = statistics.median(nets)
            wins = sum(1 for x in nets if x > 0) / len(nets) * 100
            tot = sum(nets)
            share = (max(nets) / tot * 100) if tot > 0 else float("inf")
            by_w = defaultdict(list)
            for x in r:
                by_w[x["week"]].append(direction * x["spread"])
            same = sum(1 for w, v in by_w.items() if statistics.median(v) > 0)
            sh = f"{share:.0f}%" if share != float("inf") else "n/a"
            print(f"{alt:<14s}{h:>4d}{len(r):>5d}{med:>+10.3f}"
                  f"{statistics.mean(sp):>+10.3f}{med_net:>+9.3f}"
                  f"{wins:>6.0f}%{sh:>7s}{same:>6d}")
            if (len(r) >= 30 and med_net > 0.10 and wins > 55
                    and share < 40 and same >= 2):
                passed.append((alt, h, med_net, len(r), wins, share, same))

    print()
    print("КРИТЕРИИ (заданы до запуска): n≥30, медиана net>+0.10%, win>55%,")
    print("                              лучшая<40%Σ, знак в ≥2 неделях")
    print()
    if passed:
        print("✅ ПРОШЛИ:")
        for alt, h, mn, n, w, sh, sw in sorted(passed, key=lambda x: -x[2]):
            print(f"   {alt} H={h}м: медиана net {mn:+.3f}%, n={n}, "
                  f"win {w:.0f}%, лучшая {sh:.0f}%, недель {sw}")
        print("   → гипотеза жива, следующий шаг: paper-executor")
    else:
        print("❌ НИ ОДНА пара альт×горизонт не прошла все критерии.")
        print("   → гипотеза закрывается")
    print("=" * 80)


if __name__ == "__main__":
    main()

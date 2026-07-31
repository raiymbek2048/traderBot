"""Гипотеза №11: ПОГЛОЩЕНИЕ ликвидаций — стоять лимиткой ниже рынка.

═══ ЧЕМ ЭТО ОТЛИЧАЕТСЯ ОТ ПРОВАЛИВШЕЙСЯ ГИПОТЕЗЫ №5 ═══
Фаза 13-14 тестировала: каскад → ждём 60с → входим ПО РЫНКУ → следуем импульсу.
Провал: режимно-зависимо, −$0.74 на 36 сделках.

Здесь наоборот. Лимитная заявка стоит НИЖЕ рынка ЗАРАНЕЕ. Каскад ликвидаций
сам сметает стакан до нашей цены — мы покупаем у форсированных продавцов
по панической цене, которую выбрали сами. Три отличия:

  1. Цена входа НАША, а не рыночная (вход на 1-3% лучше)
  2. Мы мейкер, не тейкер (комиссия 0.02% против 0.055%)
  3. Не нужны две ноги одновременно — та проблема, что убила makerprobe
     (17% непарных заливов ценой +34 bps), здесь отсутствует: нога одна

Замер makerprobe подтвердил, что лимитники наливаются (85% joint, медиана 8с).

═══ МЕТОД ═══
Для каждого каскада Sell-ликвидаций (лонги выносят, цена вниз):
  reference = цена в начале каскада (из самой ликвидации)
  наша заявка = reference × (1 − DIP)
  залив = если минимум klines в окне каскада ушёл СТРОГО ниже нашей цены
  выход = close на T+HOLD минут после залива
  комиссии = maker вход 0.02% + taker выход 0.055% = 0.075% round-trip

Данные: наши 112k ликвидаций + 1-мин klines Bybit (REST).

═══ КРИТЕРИИ (ЗАФИКСИРОВАНЫ 31.07 ДО ПЕРВОГО ЗАПУСКА) ═══
  1. n ≥ 30 заливов
  2. МЕДИАНА net > +0.15%  (2× стоимости round-trip 0.075%)
  3. Доля прибыльных > 55%
  4. Вклад лучшей сделки в Σ < 40%
     ← критерий уже трижды спасал от ложного вывода (HOMEUSDT ×7,
       ZHIPUUSDT 67%, listing-анализ). Ставится четвёртый раз.
  5. Медиана положительна минимум в 2 разных календарных неделях

Провал любого → гипотеза закрывается без построения кода.

Известные ловушки учтены:
  • «слишком хорошо = ловушка» — глубокие DIP заливаются только на обвалах,
    поэтому смотрим ВСЕ уровни DIP, а не только лучший
  • lookahead: залив определяется по low ПОСЛЕ начала каскада, выход — строго
    позже залива
  • режим: критерий 5 требует плюса в разных неделях
  • выживание: считаем и те заливы, что ушли в минус, без отбора

Run: python scripts/liq_absorption_analysis.py
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

CASCADE_WINDOW_S = 15
MIN_CASCADE_USD = 100_000
DIP_LEVELS = (0.5, 1.0, 2.0, 3.0)     # % ниже reference
HOLD_MIN = (15, 60, 240)               # горизонты выхода
FEE_ROUNDTRIP_PCT = 0.075              # maker 0.02 + taker 0.055
FILL_WINDOW_MIN = 10                   # сколько минут после каскада ждём залива
TRADEABLE_MIN_VOL = 2_000_000          # $2M+ ликвидаций за период — есть поток


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


def klines(symbol: str, start_ms: int, end_ms: int) -> list[tuple[int, float, float, float]]:
    """1-мин свечи: [(ts, high, low, close)]"""
    d = get(f"{BASE}/v5/market/kline?category=linear&symbol={symbol}"
            f"&interval=1&start={start_ms}&end={end_ms}&limit=1000")
    if not d or d.get("retCode") != 0:
        return []
    out = []
    for r in d.get("result", {}).get("list", []):
        try:
            out.append((int(r[0]), float(r[2]), float(r[3]), float(r[4])))
        except (IndexError, ValueError):
            continue
    out.sort()
    return out


def load_cascades() -> list[dict]:
    conn = sqlite3.connect(DB)
    # только символы с реальным потоком ликвидаций
    syms = [r[0] for r in conn.execute(
        "SELECT symbol FROM liq_events GROUP BY symbol "
        "HAVING SUM(value_usdt) >= ?", (TRADEABLE_MIN_VOL,)).fetchall()]
    rows = conn.execute(
        "SELECT symbol, side, price, value_usdt, ts FROM liq_events "
        f"WHERE symbol IN ({','.join('?'*len(syms))}) AND side='Sell' "
        "ORDER BY symbol, ts", syms).fetchall()
    conn.close()

    def pts(s):
        for f in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(s, f).replace(tzinfo=timezone.utc)
            except ValueError:
                continue

    by_sym = defaultdict(list)
    for sym, side, px, val, ts in rows:
        t = pts(ts)
        if t and px and px > 0:
            by_sym[sym].append((t, px, val or 0))

    cascades = []
    for sym, evs in by_sym.items():
        cur = None
        for t, px, val in evs:
            if cur and (t - cur["end"]).total_seconds() <= CASCADE_WINDOW_S:
                cur["end"] = t
                cur["value"] += val
                cur["count"] += 1
            else:
                if cur and cur["value"] >= MIN_CASCADE_USD:
                    cascades.append(cur)
                cur = {"symbol": sym, "start": t, "end": t,
                       "ref_price": px, "value": val, "count": 1}
        if cur and cur["value"] >= MIN_CASCADE_USD:
            cascades.append(cur)
    return cascades


def main():
    print("Читаю каскады из своего датасета...")
    cascades = load_cascades()
    print(f"  каскадов Sell ≥${MIN_CASCADE_USD:,}: {len(cascades)}")
    if not cascades:
        print("нет данных"); return
    syms = sorted({c["symbol"] for c in cascades})
    print(f"  символов: {len(syms)} — {', '.join(syms[:10])}"
          + (" ..." if len(syms) > 10 else ""))

    # свечи тянем блоками по символу, чтобы не долбить API на каждый каскад
    print("\nТяну 1-мин свечи по окнам каскадов...")
    need = defaultdict(list)
    for c in cascades:
        need[c["symbol"]].append(c)
    kl: dict[str, list] = {}
    for i, (sym, cs) in enumerate(need.items(), 1):
        lo = min(c["start"] for c in cs) - timedelta(minutes=5)
        hi = max(c["end"] for c in cs) + timedelta(minutes=max(HOLD_MIN) + 20)
        chunks, cur = [], int(lo.timestamp() * 1000)
        hi_ms = int(hi.timestamp() * 1000)
        while cur < hi_ms:
            nxt = min(cur + 1000 * 60_000, hi_ms)
            k = klines(sym, cur, nxt)
            chunks.extend(k)
            cur = nxt
            time.sleep(0.12)
        kl[sym] = sorted(set(chunks))
        print(f"  {i}/{len(need)} {sym}: {len(kl[sym])} свечей")

    # ── прогон по сетке DIP × HOLD ────────────────────────────────────────
    results: dict[tuple[float, int], list[dict]] = defaultdict(list)
    for c in cascades:
        bars = kl.get(c["symbol"]) or []
        if not bars:
            continue
        t0 = int(c["end"].timestamp() * 1000)
        fill_hi = t0 + FILL_WINDOW_MIN * 60_000
        win = [b for b in bars if t0 <= b[0] <= fill_hi]
        if not win:
            continue
        ref = c["ref_price"]
        for dip in DIP_LEVELS:
            limit = ref * (1 - dip / 100)
            hit = next((b for b in win if b[2] < limit), None)   # low СТРОГО ниже
            if not hit:
                continue
            for hold in HOLD_MIN:
                exit_ts = hit[0] + hold * 60_000
                nxt = [b for b in bars if b[0] >= exit_ts]
                if not nxt:
                    continue
                exit_px = nxt[0][3]
                raw = (exit_px - limit) / limit * 100
                results[(dip, hold)].append({
                    "symbol": c["symbol"], "week": c["end"].strftime("%G-W%V"),
                    "raw": raw, "net": raw - FEE_ROUNDTRIP_PCT,
                    "value": c["value"],
                })

    print("\n" + "=" * 78)
    print("ПОГЛОЩЕНИЕ ЛИКВИДАЦИЙ: лимитка на DIP% ниже, выход через HOLD мин")
    print("=" * 78)
    print(f"Комиссии {FEE_ROUNDTRIP_PCT}% round-trip (maker вход + taker выход)")
    print()
    print(f"{'DIP%':>5s}{'HOLD':>6s}{'заливов':>9s}{'медиана':>10s}"
          f"{'среднее':>10s}{'win%':>7s}{'лучш.вклад':>12s}{'недель+':>9s}")

    best = None
    for dip in DIP_LEVELS:
        for hold in HOLD_MIN:
            r = results.get((dip, hold)) or []
            if len(r) < 5:
                print(f"{dip:>5.1f}{hold:>6d}{len(r):>9d}{'—':>10s}"
                      f"{'—':>10s}{'—':>7s}{'—':>12s}{'—':>9s}")
                continue
            nets = [x["net"] for x in r]
            med = statistics.median(nets)
            wins = sum(1 for x in nets if x > 0) / len(nets) * 100
            tot = sum(nets)
            share = (max(nets) / tot * 100) if tot > 0 else float("inf")
            by_w = defaultdict(list)
            for x in r:
                by_w[x["week"]].append(x["net"])
            pos_w = sum(1 for w, v in by_w.items() if statistics.median(v) > 0)
            sh = f"{share:.0f}%" if share != float("inf") else "n/a"
            print(f"{dip:>5.1f}{hold:>6d}{len(r):>9d}{med:>+10.3f}"
                  f"{statistics.mean(nets):>+10.3f}{wins:>6.0f}%{sh:>12s}{pos_w:>9d}")
            ok = (len(r) >= 30 and med > 0.15 and wins > 55
                  and share < 40 and pos_w >= 2)
            if ok and (best is None or med > best[1]):
                best = ((dip, hold), med, len(r), wins, share, pos_w)

    print()
    print("КРИТЕРИИ (заданы до запуска): n≥30, медиана>+0.15%, win>55%,")
    print("                              лучшая<40%Σ, ≥2 недель с плюсом")
    print()
    if best:
        (dip, hold), med, n, wins, share, pos_w = best
        print(f"✅ ПРОЙДЕНО: DIP={dip}% HOLD={hold}м → медиана {med:+.3f}%, "
              f"n={n}, win {wins:.0f}%, лучшая {share:.0f}%, недель+ {pos_w}")
        print("   → гипотеза жива, следующий шаг: paper-executor")
    else:
        print("❌ НИ ОДНА комбинация DIP×HOLD не прошла все критерии.")
        print("   → гипотеза закрывается")
    print("=" * 78)


if __name__ == "__main__":
    main()

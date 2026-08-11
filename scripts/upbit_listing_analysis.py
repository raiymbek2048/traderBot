"""Гипотеза №15: реакция на анонс листинга Upbit.

═══ ПОЧЕМУ ЭТА ОБЛАСТЬ ═══
14 гипотез упёрлись в «доход = доходность × капитал», и при $500 любой честный
край даёт копейки. Нужен доход, НЕ пропорциональный капиталу.

Листинг — как раз такой случай: ловится не спред (пропорционален размеру), а
ПРОЦЕНТ движения. $500 × 20% = $100 за событие, независимо от того, сколько
капитала у соседа.

Плюс это единственная область, где наш реальный актив — 0.3 мс до бирж из
Токио — может иметь значение. Для арбитража и фандинга он оказался бесполезен
(гипотезы 1, 4, 13), потому что там мы конкурировали с HFT на их поле.

═══ МЕТОД ═══
Upbit — биржа с самым выраженным «эффектом листинга» (корейская премия).
Берём анонсы «신규 거래지원 안내» (поддержка новой торговли), извлекаем тикеры,
и смотрим цену этого токена на Bybit вокруг момента анонса.

Момент отсчёта — `first_listed_at` (ПЕРВАЯ публикация). Поле `listed_at`
меняется при правках уведомления и для замера непригодно.

═══ ГЛАВНЫЙ ДИАГНОСТИЧЕСКИЙ ВОПРОС ═══
Какая доля часового движения происходит в ПЕРВУЮ минуту?
  >70% в первой минуте → решает скорость, конкурируем с ботами
  <30% в первой минуте → есть время войти, край доступен

Это не критерий прохождения, а то, ради чего замер и делается.

═══ КРИТЕРИИ (ЗАФИКСИРОВАНЫ 11 АВГ ДО ПЕРВОГО ЗАПУСКА) ═══
  1. n ≥ 20 событий с котировками
  2. МЕДИАНА движения анонс→+1ч > +3%  (пампа вообще существует)
  3. МЕДИАНА реализуемого входа (вход +60с, выход по лучшему из +5/+15/+60м)
     > +1% ПОСЛЕ комиссий 0.2%
  4. Вклад лучшего события в Σ < 40%
     ← критерий срабатывал 4 раза за проект: HOMEUSDT ×7, ZHIPUUSDT 67%,
       обрезка API у листингов, DIP 2%/n=11. Шестая постановка.
  5. Эффект есть минимум в 2 разных месяцах (не режим одного периода)

Провал любого → область закрывается.

Ловушки учтены:
  • вход через +60с, а не в момент анонса — мы физически не можем быть
    мгновенными; замер по цене анонса был бы подглядыванием в будущее
  • медиана, не среднее (урок: гипотеза 11 дала среднее +0.22% при медиане −0.10%)
  • токен должен УЖЕ торговаться на Bybit до анонса, иначе ловить нечего
  • UA-заголовок: и Upbit, и RISEx отдают 403 на дефолтный urllib

Run: python scripts/upbit_listing_analysis.py
"""
from __future__ import annotations
import json
import re
import statistics
import sys
import time
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone

UPBIT = "https://api-manager.upbit.com/api/v1/announcements"
BYBIT = "https://api.bybit.com"
_UA = {"User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 Chrome/120 Safari/537.36")}

PAGES = 25                  # 25 × 30 = 750 анонсов ≈ вся доступная история
ENTRY_DELAY_S = 60          # мы не мгновенные — входим через минуту
HORIZONS_MIN = (5, 15, 60)
FEE_ROUNDTRIP_PCT = 0.2     # спот тейкер 0.1% × 2
LISTING_MARK = "신규 거래지원"    # «поддержка новой торговли»


def get(url: str, tries: int = 3):
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers=_UA)
            with urllib.request.urlopen(req, timeout=25) as r:
                return json.loads(r.read())
        except Exception as e:
            if i == tries - 1:
                print(f"    [get] {url[:60]}… → {type(e).__name__}")
                return None
            time.sleep(0.6 * (i + 1))
    return None


def upbit_listings() -> list[dict]:
    """Анонсы новых листингов: (тикеры, момент ПЕРВОЙ публикации)."""
    out = []
    for page in range(1, PAGES + 1):
        d = get(f"{UPBIT}?os=web&page={page}&per_page=30&category=trade")
        if not d:
            break
        notices = ((d.get("data") or {}).get("notices")) or []
        if not notices:
            break
        for a in notices:
            title = a.get("title") or ""
            if LISTING_MARK not in title:
                continue
            raw = a.get("first_listed_at") or a.get("listed_at")
            if not raw:
                continue
            try:
                ts = datetime.fromisoformat(raw)
            except ValueError:
                continue
            # тикеры латиницей в скобках: (DOS) или (CYS, ICNT, XAN)
            tickers = []
            for grp in re.findall(r"\(([^)]*)\)", title):
                for tk in re.split(r"[,\s/]+", grp):
                    tk = tk.strip().upper()
                    if re.fullmatch(r"[A-Z0-9]{2,12}", tk) and tk not in (
                            "KRW", "BTC", "USDT", "USD", "BNB"):
                        tickers.append(tk)
            if tickers:
                out.append({"ts": ts.astimezone(timezone.utc),
                            "tickers": sorted(set(tickers)),
                            "title": title[:70]})
        time.sleep(0.25)
    return out


def bybit_klines(symbol: str, start_ms: int, end_ms: int):
    d = get(f"{BYBIT}/v5/market/kline?category=spot&symbol={symbol}"
            f"&interval=1&start={start_ms}&end={end_ms}&limit=500")
    if not d or d.get("retCode") != 0:
        return []
    out = []
    for r in d.get("result", {}).get("list", []):
        try:
            out.append((int(r[0]), float(r[2]), float(r[3]), float(r[4])))
        except (IndexError, ValueError):
            continue
    return sorted(out)


def main():
    print("Тяну анонсы Upbit...")
    ann = upbit_listings()
    print(f"  анонсов новых листингов: {len(ann)}")
    if not ann:
        print("❌ анонсы не получены"); return
    print(f"  период: {min(a['ts'] for a in ann):%d %b %Y} → "
          f"{max(a['ts'] for a in ann):%d %b %Y}")

    print("\nСобираю котировки Bybit вокруг анонсов...")
    events, skipped = [], defaultdict(int)
    DAY = 86_400_000
    for i, a in enumerate(ann, 1):
        t0 = int(a["ts"].timestamp() * 1000)
        if time.time() * 1000 - t0 < 2 * 3_600_000:
            skipped["слишком свежий"] += 1
            continue
        for tk in a["tickers"]:
            sym = f"{tk}USDT"
            bars = bybit_klines(sym, t0 - 30 * 60_000, t0 + 90 * 60_000)
            time.sleep(0.1)
            if len(bars) < 30:
                skipped["нет котировок на Bybit"] += 1
                continue
            pre = [b for b in bars if b[0] < t0]
            if not pre:
                skipped["нет цены ДО анонса"] += 1
                continue
            entry_bars = [b for b in bars if b[0] >= t0 + ENTRY_DELAY_S * 1000]
            if not entry_bars:
                skipped["нет цены после входа"] += 1
                continue
            p_ann = pre[-1][3]                   # close последней свечи до анонса
            p_entry = entry_bars[0][3]           # наш реальный вход через 60с
            if p_ann <= 0 or p_entry <= 0:
                skipped["нулевая цена"] += 1
                continue

            ev = {"ticker": tk, "ts": a["ts"],
                  "month": a["ts"].strftime("%Y-%m"),
                  "p_ann": p_ann, "p_entry": p_entry,
                  "move_1m": (p_entry - p_ann) / p_ann * 100}
            for h in HORIZONS_MIN:
                seg = [b for b in bars if t0 <= b[0] <= t0 + h * 60_000]
                ev[f"max_{h}"] = ((max(b[1] for b in seg) - p_ann) / p_ann * 100
                                  if seg else 0.0)
                ex = [b for b in bars if b[0] >= t0 + h * 60_000]
                ev[f"exit_{h}"] = (((ex[0][3] - p_entry) / p_entry * 100
                                    - FEE_ROUNDTRIP_PCT) if ex else None)
            events.append(ev)
        if i % 25 == 0:
            print(f"  {i}/{len(ann)} анонсов, событий {len(events)}")

    print(f"\n  собрано событий: {len(events)}")
    if skipped:
        print("  отброшено (с причиной — «нет данных» ≠ «нет края»):")
        for k, v in skipped.items():
            print(f"    {k}: {v}")
    if len(events) < 5:
        print("\n❌ Событий слишком мало для выводов."); return

    print("\n" + "=" * 80)
    print("РЕАКЦИЯ НА АНОНС ЛИСТИНГА UPBIT (цена токена на Bybit)")
    print("=" * 80)
    print(f"Вход через {ENTRY_DELAY_S}с после публикации, комиссии {FEE_ROUNDTRIP_PCT}%")
    print()

    m1 = [e["move_1m"] for e in events]
    print(f"Движение за первую минуту:  медиана {statistics.median(m1):+.3f}%  "
          f"среднее {statistics.mean(m1):+.3f}%")
    for h in HORIZONS_MIN:
        mx = [e[f"max_{h}"] for e in events]
        print(f"Максимум к +{h:>2d}м:            медиана {statistics.median(mx):+.3f}%  "
              f"среднее {statistics.mean(mx):+.3f}%")

    # ГЛАВНЫЙ ВОПРОС: доля часового движения в первой минуте
    shares = [e["move_1m"] / e["max_60"] * 100
              for e in events if e["max_60"] > 0.5]
    if shares:
        print()
        print(f"ДОЛЯ ЧАСОВОГО ДВИЖЕНИЯ В ПЕРВОЙ МИНУТЕ: "
              f"медиана {statistics.median(shares):.0f}%  (n={len(shares)})")
        med_share = statistics.median(shares)
        if med_share > 70:
            print("  → решает скорость, окно закрывается за секунды")
        elif med_share < 30:
            print("  → движение растянуто, время войти ЕСТЬ")
        else:
            print("  → смешанный режим")

    print()
    print("РЕАЛИЗУЕМЫЙ РЕЗУЛЬТАТ (вход +60с, выход по горизонту, за вычетом комиссий):")
    best = None
    for h in HORIZONS_MIN:
        vals = [e[f"exit_{h}"] for e in events if e[f"exit_{h}"] is not None]
        if not vals:
            continue
        med = statistics.median(vals)
        wins = sum(1 for v in vals if v > 0) / len(vals) * 100
        tot = sum(vals)
        share = max(vals) / tot * 100 if tot > 0 else float("inf")
        months = defaultdict(list)
        for e in events:
            if e[f"exit_{h}"] is not None:
                months[e["month"]].append(e[f"exit_{h}"])
        pos_m = sum(1 for mm, v in months.items() if statistics.median(v) > 0)
        sh = f"{share:.0f}%" if share != float("inf") else "n/a"
        print(f"  выход +{h:>2d}м: n={len(vals):>3d}  медиана {med:+.3f}%  "
              f"среднее {statistics.mean(vals):+.3f}%  win {wins:>3.0f}%  "
              f"лучший {sh:>5s}  мес+ {pos_m}")
        ok = (len(vals) >= 20 and med > 1.0 and share < 40 and pos_m >= 2)
        if ok and (best is None or med > best[1]):
            best = (h, med, len(vals), wins, share, pos_m)

    print()
    print("ТОП-10 событий по движению за час:")
    for e in sorted(events, key=lambda x: -x["max_60"])[:10]:
        print(f"  {e['ts']:%d %b %y} {e['ticker']:<8s} 1мин {e['move_1m']:+7.2f}%  "
              f"max1ч {e['max_60']:+8.2f}%")

    print()
    print("КРИТЕРИИ (заданы до запуска): n≥20, медиана движения 1ч>+3%,")
    print("  медиана реализуемого>+1%, лучший<40%Σ, ≥2 месяцев")
    print()
    med_1h = statistics.median([e["max_60"] for e in events])
    print(f"  движение 1ч медиана: {med_1h:+.3f}%  "
          f"{'✅' if med_1h > 3 else '❌'}")
    if best:
        h, med, n, wins, share, pos_m = best
        print(f"  ✅ ПРОШЛО: выход +{h}м, медиана {med:+.3f}%, n={n}, "
              f"win {wins:.0f}%, лучший {share:.0f}%, месяцев {pos_m}")
        print("\n→ область живая, следующий шаг: paper-исполнение в реальном времени")
    else:
        print("  ❌ ни один горизонт не прошёл все критерии")
        print("\n→ область закрывается")
    print("=" * 80)


if __name__ == "__main__":
    main()

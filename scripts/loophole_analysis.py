"""Поиск лазейки: репрайсинг реального журнала под непроверенные режимы исполнения.

Мы всегда были ТЕЙКЕРОМ и всегда ТОРГОВАЛИ (churn). Никогда не были МЕЙКЕРОМ
и никогда просто не ДЕРЖАЛИ. Проверяем на своих же 146 перп-перп сделках и
16 днях фандинга, что меняется в этих режимах.

Гипотезы:
  H1 maker-исполнение: perp maker 0.02%×4 = 0.08% вместо taker 0.055%×4 = 0.21%
  H2 no-cross: мейкер не пересекает спред → часть basis-убытка исчезает
  H3 aligned-only: входить только когда exec_edge > 0 (конвергенция за нас)
  H4 hold-through: держать через N сеттлментов вместо 2.2ч
  H5 buy&hold carry: купить и держать 16 дней вместо churn

Run: python scripts/loophole_analysis.py
"""
import sqlite3
import statistics
from collections import defaultdict
from datetime import datetime, timedelta

DB = ("/private/tmp/claude-501/-Users-raiymbekdaniiaruulu-IdealProjects-StartTups-claud/"
      "ca35c44a-7b50-4c62-b973-d2f29e7a1363/scratchpad/traderbot.db")

# Реальные ставки Bybit/Binance linear perp (retail, без VIP)
TAKER = 0.00055
MAKER = 0.00020
LEGS = 4                      # вход+выход, две биржи
FEE_TAKER_CYCLE = TAKER * LEGS   # 0.220%
FEE_MAKER_CYCLE = MAKER * LEGS   # 0.080%
# Смешанный: вход мейкером (не спешим), выход тейкером (иногда надо срочно)
FEE_MIXED_CYCLE = (MAKER * 2 + TAKER * 2)  # 0.150%


def parse_ts(s):
    if not s:
        return None
    for f in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, f)
        except ValueError:
            continue
    return None


def settlements_crossed(opened, closed):
    """Сколько сеттлментов (00/08/16 UTC) позиция реально пережила."""
    if not opened or not closed:
        return 0
    n = 0
    t = opened.replace(minute=0, second=0, microsecond=0)
    while t <= closed:
        if t.hour in (0, 8, 16) and opened < t <= closed:
            n += 1
        t += timedelta(hours=1)
    return n


def load_trades():
    conn = sqlite3.connect(DB)
    rows = conn.execute("""
        SELECT symbol, direction, size_usdt, entry_spread_daily_pct,
               entry_exec_edge_pct, funding_collected_usdt, basis_pnl_usdt,
               fees_usdt, pnl_usdt, opened_at, closed_at
        FROM spread_positions WHERE status='closed'
    """).fetchall()
    conn.close()
    out = []
    for (sym, d, sz, spread, edge, fund, basis, fees, pnl, op, cl) in rows:
        o, c = parse_ts(op), parse_ts(cl)
        out.append({
            "symbol": sym, "direction": d, "size": sz or 50.0,
            "spread": spread or 0.0, "edge": edge or 0.0,
            "funding": fund or 0.0, "basis": basis or 0.0,
            "fees": fees or 0.0, "pnl": pnl or 0.0,
            "opened": o, "closed": c,
            "hours": ((c - o).total_seconds() / 3600) if (o and c) else 0.0,
            "settles": settlements_crossed(o, c),
        })
    return out


def hdr(t):
    print("\n" + "=" * 78)
    print(t)
    print("=" * 78)


def summarize(name, trades, pnl_key="pnl"):
    if not trades:
        print(f"  {name:38s}  n=0")
        return None
    n = len(trades)
    tot = sum(t[pnl_key] for t in trades)
    wins = sum(1 for t in trades if t[pnl_key] > 0)
    avg = tot / n
    mark = "✅" if tot > 0 else "❌"
    print(f"  {name:38s}  n={n:3d}  Σ={tot:+8.3f}  avg={avg:+.4f}  "
          f"win={wins:3d}/{n:<3d} ({wins/n*100:2.0f}%) {mark}")
    return {"n": n, "total": tot, "avg": avg, "wins": wins}


def main():
    trades = load_trades()

    # ────────────────────────────────────────────────────────────────────
    hdr("БАЗА: что реально произошло (перп-перп A/B журнал)")
    n = len(trades)
    f_sum = sum(t["funding"] for t in trades)
    b_sum = sum(t["basis"] for t in trades)
    fee_sum = sum(t["fees"] for t in trades)
    p_sum = sum(t["pnl"] for t in trades)
    notional = sum(t["size"] for t in trades)

    print(f"Сделок: {n} | нотионал/нога: ${trades[0]['size']:.0f} | "
          f"суммарный оборот: ${notional:,.0f}")
    print(f"Средний холд: {statistics.mean(t['hours'] for t in trades):.2f}ч | "
          f"медиана: {statistics.median(t['hours'] for t in trades):.2f}ч")
    print()
    print(f"  Фандинг собран : {f_sum:+8.3f}  ({f_sum/notional*100:+.3f}% от нотионала)")
    print(f"  Basis          : {b_sum:+8.3f}  ({b_sum/notional*100:+.3f}%)")
    print(f"  Комиссии       : {-fee_sum:+8.3f}  ({-fee_sum/notional*100:+.3f}%)")
    print(f"  ─────────────────────────────")
    print(f"  ИТОГО          : {p_sum:+8.3f}  ({p_sum/notional*100:+.3f}%)")

    # ────────────────────────────────────────────────────────────────────
    hdr("ГЛАВНАЯ УЛИКА: сколько сеттлментов мы реально пережили")
    by_settle = defaultdict(list)
    for t in trades:
        by_settle[t["settles"]].append(t)
    print(f"  {'сеттлментов':<14s} {'сделок':>7s} {'Σфандинг':>10s} {'Σbasis':>9s} "
          f"{'Σкомис':>8s} {'ΣPnL':>9s}")
    for s in sorted(by_settle):
        g = by_settle[s]
        print(f"  {s:<14d} {len(g):>7d} {sum(x['funding'] for x in g):>+10.3f} "
              f"{sum(x['basis'] for x in g):>+9.3f} "
              f"{-sum(x['fees'] for x in g):>+8.3f} "
              f"{sum(x['pnl'] for x in g):>+9.3f}")
    zero = len(by_settle.get(0, []))
    print(f"\n  ⚠️  {zero}/{n} сделок ({zero/n*100:.0f}%) закрылись НЕ ПЕРЕЖИВ НИ ОДНОГО "
          f"сеттлмента —\n      то есть заплатили комиссию и не получили фандинг вообще.")

    # ────────────────────────────────────────────────────────────────────
    hdr("H1+H2: репрайсинг под maker-исполнение")
    print("Мейкер не платит taker-комиссию И не пересекает спред при входе.")
    print("Консервативно: убираем только комиссию, basis оставляем как был.\n")

    for label, fee_rate in (("taker 0.220% (как торговали)", FEE_TAKER_CYCLE),
                            ("mixed 0.150% (вход maker)",    FEE_MIXED_CYCLE),
                            ("maker 0.080% (все 4 ноги)",    FEE_MAKER_CYCLE)):
        rep = []
        for t in trades:
            new_fee = t["size"] * fee_rate
            rep.append({**t, "rp": t["funding"] + t["basis"] - new_fee})
        summarize(label, rep, "rp")

    print("\nЕсли ДОПОЛНИТЕЛЬНО не пересекать спред (мейкер = мы ставим цену).")
    print("Оценка экономии: |adverse-часть exec_edge| при входе.\n")
    for label, fee_rate in (("maker + no-cross (оценка)", FEE_MAKER_CYCLE),):
        rep = []
        for t in trades:
            new_fee = t["size"] * fee_rate
            # adverse-вход (edge<0) мейкером не платится: возвращаем половину
            saved = t["size"] * (abs(min(0.0, t["edge"])) / 100) * 0.5
            rep.append({**t, "rp": t["funding"] + t["basis"] - new_fee + saved})
        summarize(label, rep, "rp")

    # ────────────────────────────────────────────────────────────────────
    hdr("H3: только aligned-вход (exec_edge > 0 — конвергенция за нас)")
    aligned = [t for t in trades if t["edge"] > 0]
    adverse = [t for t in trades if t["edge"] <= 0]
    summarize("aligned, taker (как было)", aligned)
    summarize("adverse, taker (как было)", adverse)
    print()
    for label, fee_rate in (("aligned + maker 0.080%", FEE_MAKER_CYCLE),):
        rep = [{**t, "rp": t["funding"] + t["basis"] - t["size"] * fee_rate}
               for t in aligned]
        summarize(label, rep, "rp")

    # ────────────────────────────────────────────────────────────────────
    hdr("H4: только сделки, пережившие ≥1 сеттлмент (не churn)")
    held = [t for t in trades if t["settles"] >= 1]
    summarize("held≥1 settle, taker", held)
    for label, fee_rate in (("held≥1 + maker 0.080%", FEE_MAKER_CYCLE),):
        rep = [{**t, "rp": t["funding"] + t["basis"] - t["size"] * fee_rate}
               for t in held]
        summarize(label, rep, "rp")

    # ────────────────────────────────────────────────────────────────────
    hdr("H1+H3+H4 КОМБО: aligned + держим сеттлмент + maker")
    combo = [t for t in trades if t["edge"] > 0 and t["settles"] >= 1]
    summarize("КОМБО, taker (как было)", combo)
    for label, fee_rate in (("КОМБО + mixed 0.150%", FEE_MIXED_CYCLE),
                            ("КОМБО + maker 0.080%", FEE_MAKER_CYCLE)):
        rep = [{**t, "rp": t["funding"] + t["basis"] - t["size"] * fee_rate}
               for t in combo]
        summarize(label, rep, "rp")
    if combo:
        print(f"\n  Детализация КОМБО ({len(combo)} сделок):")
        print(f"  {'символ':<14s} {'spread%/д':>9s} {'edge%':>7s} {'часов':>6s} "
              f"{'сеттл':>5s} {'фандинг':>8s} {'basis':>8s}")
        for t in sorted(combo, key=lambda x: -x["funding"]):
            print(f"  {t['symbol']:<14s} {t['spread']:>+9.2f} {t['edge']:>+7.2f} "
                  f"{t['hours']:>6.1f} {t['settles']:>5d} "
                  f"{t['funding']:>+8.3f} {t['basis']:>+8.3f}")

    # ────────────────────────────────────────────────────────────────────
    hdr("H5: buy & hold — держать 16 дней вместо churn")
    conn = sqlite3.connect(DB)
    rows = conn.execute("""
        SELECT symbol, ts, bybit_daily_pct, bybit_price
        FROM funding_spread_snaps
        WHERE bybit_daily_pct IS NOT NULL AND bybit_price > 0.001
        ORDER BY ts
    """).fetchall()
    conn.close()

    per_sym = defaultdict(list)
    for sym, ts, daily, px in rows:
        t = parse_ts(ts)
        if t:
            per_sym[sym].append((t, daily, px))

    print("Держим ОДНУ ногу-хедж (long spot + short perp того же актива).")
    print("Basis такой конструкции → 0 по построению (тот же актив, конвергенция).")
    print("Доход = Σ фандинг за период. Стоимость = 0.31% один раз (spot+perp taker).\n")

    SPOT_PERP_CYCLE = 0.0031
    cands = []
    for sym, seq in per_sym.items():
        if len(seq) < 100:
            continue
        span_days = (seq[-1][0] - seq[0][0]).total_seconds() / 86400
        if span_days < 5:
            continue
        rates = [d for _, d, _ in seq]
        mean_daily = statistics.mean(rates)
        frac_pos = sum(1 for r in rates if r > 0) / len(rates)
        # накопленный фандинг: средняя ставка × дни (шорт перп получает при rate>0)
        gross = mean_daily / 100 * span_days
        net = gross - SPOT_PERP_CYCLE
        # фильтр качества: не капнутый, живой
        capped = any(abs(r) > 2.0 for r in rates)
        cands.append({
            "symbol": sym, "mean_daily": mean_daily, "frac_pos": frac_pos,
            "days": span_days, "gross": gross * 100, "net": net * 100,
            "capped": capped, "snaps": len(seq),
            "px": statistics.median(p for _, _, p in seq),
        })

    good = [c for c in cands if not c["capped"] and c["px"] > 0.01]
    good.sort(key=lambda c: -abs(c["net"]))

    print(f"  Символов с достаточной историей: {len(cands)} "
          f"(после фильтра качества: {len(good)})\n")
    print(f"  {'символ':<14s} {'ср.фандинг':>11s} {'%времени+':>10s} {'дней':>6s} "
          f"{'gross%':>8s} {'net%':>8s} {'год.%':>8s}")
    shown = 0
    for c in good:
        if shown >= 15:
            break
        annual = c["net"] / c["days"] * 365 if c["days"] else 0
        print(f"  {c['symbol']:<14s} {c['mean_daily']:>+11.3f} {c['frac_pos']*100:>9.0f}% "
              f"{c['days']:>6.1f} {c['gross']:>+8.3f} {c['net']:>+8.3f} {annual:>+8.1f}")
        shown += 1

    profitable = [c for c in good if c["net"] > 0]
    print(f"\n  Прибыльных buy&hold за период: {len(profitable)}/{len(good)}")
    if profitable:
        best = max(profitable, key=lambda c: c["net"] / c["days"])
        print(f"  Лучший: {best['symbol']} net {best['net']:+.3f}% за "
              f"{best['days']:.1f}д = {best['net']/best['days']*365:+.1f}%/год")
        med = statistics.median(c["net"] / c["days"] * 365 for c in profitable)
        print(f"  Медиана годовых по прибыльным: {med:+.1f}%/год")

    # стабильные: фандинг одного знака ≥80% времени
    stable = [c for c in good if (c["frac_pos"] > 0.8 or c["frac_pos"] < 0.2)
              and abs(c["mean_daily"]) > 0.05]
    stable.sort(key=lambda c: -abs(c["net"]))
    print(f"\n  ─── Стабильные (знак фандинга держится >80% времени): {len(stable)} ───")
    for c in stable[:10]:
        annual = c["net"] / c["days"] * 365 if c["days"] else 0
        side = "short perp" if c["mean_daily"] > 0 else "long perp"
        print(f"  {c['symbol']:<14s} {c['mean_daily']:>+8.3f}%/д  {c['frac_pos']*100:>3.0f}%+  "
              f"net {c['net']:>+7.3f}%  ({annual:>+7.1f}%/год)  {side}")

    hdr("ВЫВОД")
    print("Смотри цифры выше. Ключевые вопросы:")
    print("  1. Переворачивает ли maker знак на перп-перп? (H1)")
    print("  2. Спасает ли aligned+hold? (КОМБО)")
    print("  3. Есть ли символы со стабильным фандингом для buy&hold? (H5)")


if __name__ == "__main__":
    main()

"""Свежий угол: предсказуема ли микроструктура RISEx↔Bybit?
Три теста, которых не было ни в одной из 15 гипотез:

A. ЛИД-ЛАГ: движение Bybit предсказывает следующий бар RISEx? (новая тонкая
   площадка отстаёт от крупной). Если да — направленная сделка на RISEx.
B. РЕВЕРСИЯ BASIS: когда гэп цен площадок расширяется, откатывает ли он?
   (basis всегда был затратой — а вдруг это торгуемый сигнал).
C. ФАНДИНГ→ЦЕНА: экстремальный фандинг RISEx предсказывает разворот?
   (толпа лонгов на новой бирже → сквиз).

Данные: 5-мин снимки mark-цен обеих площадок, 6 символов, 39 дней (бэкап).
Всё сравнивается с издержками RISEx: taker 3bps, round-trip 6bps.
"""
import sqlite3
import numpy as np
from collections import defaultdict
from datetime import datetime

DB = "/Users/raiymbekdaniiaruulu/traderbot_archive/traderbot_20260911.db"
SYMS = ["HYPE", "BNB", "XRP", "ETH", "SOL", "DOGE"]
RT_COST_BPS = 6.0   # RISEx round-trip taker

conn = sqlite3.connect(DB)

def load_aligned(sym):
    """Возвращает (rx[], by[], funding_rx[]) выровненные по ts."""
    rows = conn.execute(
        "SELECT ts, venue, mark_price, funding_rate FROM venue_funding_snaps "
        "WHERE symbol=? ORDER BY ts", (sym,)).fetchall()
    at = defaultdict(dict)
    for ts, venue, price, fr in rows:
        if price and price > 0:
            at[ts][venue] = (price, fr)
    rx, by, frx = [], [], []
    for ts in sorted(at):
        if "risex" in at[ts] and "bybit" in at[ts]:
            rx.append(at[ts]["risex"][0])
            by.append(at[ts]["bybit"][0])
            frx.append(at[ts]["risex"][1] or 0)
    return np.array(rx), np.array(by), np.array(frx)

def rets(p):
    return np.diff(p) / p[:-1] * 1e4  # в bps

print("="*76)
print("A. ЛИД-ЛАГ RISEx↔Bybit (5-мин бары, bps)")
print("="*76)
print(f"{'символ':<7s}{'contemp':>9s}{'By→RX лаг':>11s}{'RX→By лаг':>11s}{'вывод':>16s}")
poolA = {"c":[], "bl":[], "rl":[]}
for sym in SYMS:
    rx, by, _ = load_aligned(sym)
    rr, br = rets(rx), rets(by)
    n = min(len(rr), len(br))
    rr, br = rr[:n], br[:n]
    c = np.corrcoef(rr, br)[0,1]                      # одновременная
    by_leads = np.corrcoef(rr[1:], br[:-1])[0,1]      # RX[t] ~ By[t-1]
    rx_leads = np.corrcoef(br[1:], rr[:-1])[0,1]      # By[t] ~ RX[t-1]
    poolA["c"].append(c); poolA["bl"].append(by_leads); poolA["rl"].append(rx_leads)
    who = "Bybit ведёт" if by_leads>rx_leads+0.02 else ("RISEx ведёт" if rx_leads>by_leads+0.02 else "паритет")
    print(f"{sym:<7s}{c:>9.3f}{by_leads:>+11.3f}{rx_leads:>+11.3f}{who:>16s}")
print(f"{'СРЕДНЕЕ':<7s}{np.mean(poolA['c']):>9.3f}{np.mean(poolA['bl']):>+11.3f}{np.mean(poolA['rl']):>+11.3f}")

print("\n"+"="*76)
print("B. РЕВЕРСИЯ BASIS: широкий гэп откатывает следующий бар?")
print("="*76)
print(f"{'символ':<7s}{'σ gap bps':>10s}{'autocorr':>10s}{'|Δ| после шир.гэпа':>20s}")
for sym in SYMS:
    rx, by, _ = load_aligned(sym)
    gap = (rx - by)/by * 1e4
    ac = np.corrcoef(gap[1:], gap[:-1])[0,1]
    # после |gap| в топ-10% — насколько откатывает к 0 за след.бар
    thr = np.percentile(np.abs(gap), 90)
    wide = np.where(np.abs(gap[:-1]) > thr)[0]
    revert = np.mean(np.abs(gap[wide]) - np.abs(gap[wide+1])) if len(wide) else 0
    print(f"{sym:<7s}{np.std(gap):>10.1f}{ac:>10.3f}{revert:>+18.1f}bps")

print("\n"+"="*76)
print("C. ФАНДИНГ RISEx → форвардная цена (контрарный сквиз?)")
print("="*76)
print(f"{'символ':<7s}{'corr(fund,fwd1h)':>17s}{'топ-фанд→ср.движ 1ч':>22s}")
for sym in SYMS:
    rx, by, frx = load_aligned(sym)
    H = 12  # 12 баров × 5мин = 1 час
    fwd = (rx[H:] - rx[:-H])/rx[:-H] * 1e4
    f = frx[:-H]
    c = np.corrcoef(f, fwd)[0,1] if np.std(f)>0 else 0
    thr = np.percentile(f, 90)
    hi = np.where(f > thr)[0]
    mv = np.mean(fwd[hi]) if len(hi) else 0
    print(f"{sym:<7s}{c:>+17.3f}{mv:>+20.1f}bps")

print("\n"+"="*76)
print(f"Порог значимости: движение должно бить RT-издержки {RT_COST_BPS} bps.")
conn.close()

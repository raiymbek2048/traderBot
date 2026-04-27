"""Gate effectiveness analysis — run after 30+ days shadow mode.

Metrics:
  gate_effectiveness = avg_pnl_approved / avg_pnl_all - 1  (needs >= 10%)
  win_rate_approved >= win_rate_all + 5%

Usage:
  python scripts/gate_effectiveness.py
"""
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from sqlalchemy.orm import Session
from sqlalchemy import select
from shared.config import load_config
from shared.db import init_db, GateDecision, Trade


def main() -> None:
    cfg = load_config()
    engine = init_db(cfg.database_url)

    with Session(engine) as session:
        decisions = session.execute(
            select(GateDecision).where(GateDecision.shadow_mode == True)
        ).scalars().all()

        trades = session.execute(
            select(Trade).where(Trade.status == "closed")
        ).scalars().all()

    if not decisions:
        print("No gate decisions yet. Run shadow mode for 30 days first.")
        return

    # Map signal_id → trade pnl_pct
    signal_pnl: dict[int, float] = {}
    for t in trades:
        if t.signal_id and t.pnl_pct is not None:
            signal_pnl[t.signal_id] = t.pnl_pct

    all_pnls = []
    approved_pnls = []
    blocked_pnls = []

    for d in decisions:
        if d.signal_id not in signal_pnl:
            continue
        pnl = signal_pnl[d.signal_id]
        all_pnls.append(pnl)
        if d.gate_decision == "approve":
            approved_pnls.append(pnl)
        else:
            blocked_pnls.append(pnl)

    if not all_pnls:
        print("No closed trades matched to gate decisions yet.")
        return

    def stats(pnls: list[float], label: str) -> None:
        if not pnls:
            print(f"{label}: no data")
            return
        avg = sum(pnls) / len(pnls)
        wr = sum(1 for p in pnls if p > 0) / len(pnls)
        print(f"{label}: n={len(pnls)}  avg_pnl={avg:.2%}  win_rate={wr:.1%}")

    print(f"\n{'='*50}")
    print("GATE SHADOW MODE EFFECTIVENESS REPORT")
    print(f"{'='*50}")
    print(f"Total decisions with outcomes: {len(all_pnls)}")
    print()
    stats(all_pnls, "All signals      ")
    stats(approved_pnls, "Gate APPROVED    ")
    stats(blocked_pnls, "Gate BLOCKED     ")

    if approved_pnls and all_pnls:
        avg_all = sum(all_pnls) / len(all_pnls)
        avg_approved = sum(approved_pnls) / len(approved_pnls)
        wr_all = sum(1 for p in all_pnls if p > 0) / len(all_pnls)
        wr_approved = sum(1 for p in approved_pnls if p > 0) / len(approved_pnls)

        effectiveness = avg_approved / avg_all - 1 if avg_all != 0 else 0
        wr_delta = wr_approved - wr_all

        print(f"\ngate_effectiveness = {effectiveness:.1%}  (need >= 10%)")
        print(f"win_rate_delta     = {wr_delta:+.1%}  (need >= +5%)")
        print()

        go_live = effectiveness >= 0.10 and wr_delta >= 0.05
        if go_live:
            print("✅ GATE PASSES CRITERIA — ready to enable live blocking")
        else:
            criteria = []
            if effectiveness < 0.10:
                criteria.append(f"gate_effectiveness {effectiveness:.1%} < 10%")
            if wr_delta < 0.05:
                criteria.append(f"win_rate_delta {wr_delta:+.1%} < +5%")
            print(f"❌ NOT READY: {', '.join(criteria)}")

    print(f"\nScore distribution (approved signals):")
    if approved_pnls:
        scores = [d.composite_score for d in decisions if d.gate_decision == "approve" and d.composite_score]
        if scores:
            print(f"  min={min(scores):.3f}  avg={sum(scores)/len(scores):.3f}  max={max(scores):.3f}")
    print()


if __name__ == "__main__":
    main()

"""Composite gate score calculation with source degradation.

Formula (APM ADR Selective Alpha Gate v2):
  score = 0.5 * liq_score + 0.3 * div_score + 0.2 * onchain_score

When a source is unavailable its weight is redistributed proportionally.
On-chain (CryptoQuant) disabled by default in shadow mode (no paid API).
"""
from __future__ import annotations
from dataclasses import dataclass

GATE_THRESHOLD_INITIAL = 0.6  # calibrate after 30-day shadow mode

WEIGHTS_DEFAULT = {
    "liq": 0.5,
    "div": 0.3,
    "onchain": 0.2,
}


@dataclass
class GateScore:
    composite: float
    liq: float
    div: float
    onchain: float | None
    decision: str        # "approve" | "block" | "blocked_macro"
    threshold: float
    macro_blocked: bool
    reason: str


def compute_gate_score(
    liq_score: float,
    div_score: float,
    onchain_score: float | None = None,  # None = disabled
    macro_blocked: bool = False,
    threshold: float = GATE_THRESHOLD_INITIAL,
) -> GateScore:
    if macro_blocked:
        return GateScore(
            composite=0.0,
            liq=liq_score,
            div=div_score,
            onchain=onchain_score,
            decision="blocked_macro",
            threshold=threshold,
            macro_blocked=True,
            reason="macro event blocker active",
        )

    # Build active weights (redistribute if source missing)
    active: dict[str, float] = {
        "liq": WEIGHTS_DEFAULT["liq"],
        "div": WEIGHTS_DEFAULT["div"],
    }
    scores: dict[str, float] = {
        "liq": liq_score,
        "div": div_score,
    }

    if onchain_score is not None:
        active["onchain"] = WEIGHTS_DEFAULT["onchain"]
        scores["onchain"] = onchain_score
    # else: redistribute onchain weight proportionally
    else:
        total_without = sum(active.values())
        for k in active:
            active[k] = active[k] / total_without  # normalise to 1.0

    composite = sum(active[k] * scores[k] for k in active)

    decision = "approve" if composite >= threshold else "block"
    parts = [f"liq={liq_score:.2f}", f"div={div_score:.2f}"]
    if onchain_score is not None:
        parts.append(f"onchain={onchain_score:.2f}")
    reason = f"composite={composite:.3f} ({', '.join(parts)})"

    return GateScore(
        composite=composite,
        liq=liq_score,
        div=div_score,
        onchain=onchain_score,
        decision=decision,
        threshold=threshold,
        macro_blocked=False,
        reason=reason,
    )

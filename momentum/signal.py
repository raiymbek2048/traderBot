"""5m signal generation — two branches:

ETH branch: VWAP mean reversion — fade deviation > threshold on weak volume
BTC branch: BTC lead-lag — ETH follows BTC move within 3 bars
Regime: ADX(14) on 1h. Ranging OK for reversion; trending OK for lead-lag.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Literal
import numpy as np

Direction = Literal["long", "short"]
Regime = Literal["trending", "transition", "ranging"]


@dataclass
class MomentumSignal:
    direction: Direction
    branch: str
    regime: Regime
    adx: float
    size_multiplier: float
    mark_price: float
    sl_price: float
    tp_price: float
    reason: str


# ── indicators ────────────────────────────────────────────────────────────────

def _ema(values: list[float], period: int) -> list[float]:
    result = []
    k = 2 / (period + 1)
    for i, v in enumerate(values):
        if i == 0:
            result.append(v)
        else:
            result.append(v * k + result[-1] * (1 - k))
    return result


def compute_rsi(closes: list[float], period: int = 2) -> float:
    if len(closes) < period + 1:
        return 50.0
    deltas = np.diff(closes[-(period * 3):])
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_gain = np.mean(gains[-period:])
    avg_loss = np.mean(losses[-period:])
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def compute_vwap(ohlcv: list[dict]) -> float:
    total_pv = sum((c["high"] + c["low"] + c["close"]) / 3 * c["volume"] for c in ohlcv)
    total_v = sum(c["volume"] for c in ohlcv)
    return total_pv / total_v if total_v > 0 else 0.0


def compute_adx(ohlcv: list[dict], period: int = 14) -> float:
    if len(ohlcv) < period * 2:
        return 25.0
    highs  = np.array([c["high"]  for c in ohlcv])
    lows   = np.array([c["low"]   for c in ohlcv])
    closes = np.array([c["close"] for c in ohlcv])
    tr = np.maximum(highs[1:] - lows[1:],
         np.maximum(np.abs(highs[1:] - closes[:-1]),
                    np.abs(lows[1:]  - closes[:-1])))
    up = np.clip(highs[1:] - highs[:-1], 0, None)
    dn = np.clip(lows[:-1] - lows[1:],  0, None)
    up[up < dn] = 0
    dn[dn < up] = 0
    atr = np.convolve(tr, np.ones(period)/period, mode='valid')
    pdi = np.convolve(up, np.ones(period)/period, mode='valid') / (atr + 1e-9) * 100
    ndi = np.convolve(dn, np.ones(period)/period, mode='valid') / (atr + 1e-9) * 100
    dx  = np.abs(pdi - ndi) / (pdi + ndi + 1e-9) * 100
    return float(np.mean(dx[-period:]))


def compute_atr(ohlcv: list[dict], period: int = 14) -> float:
    if len(ohlcv) < period + 1:
        return 0.0
    highs  = np.array([c["high"]  for c in ohlcv])
    lows   = np.array([c["low"]   for c in ohlcv])
    closes = np.array([c["close"] for c in ohlcv])
    tr = np.maximum(highs[1:] - lows[1:],
         np.maximum(np.abs(highs[1:] - closes[:-1]),
                    np.abs(lows[1:]  - closes[:-1])))
    return float(np.mean(tr[-period:]))


def compute_oi_delta_pct(oi_history: list[dict], lookback: int = 12) -> float:
    if len(oi_history) < lookback + 1:
        return 0.0
    curr = oi_history[-1]["oi"]
    past = oi_history[-1 - lookback]["oi"]
    return (curr - past) / (past + 1e-9)


# ── regime classifier ─────────────────────────────────────────────────────────

def classify_regime(ohlcv_1h: list[dict]) -> tuple[Regime, float, float]:
    adx = compute_adx(ohlcv_1h, 14)
    atr = compute_atr(ohlcv_1h, 14)
    if adx > 25:
        return "trending", adx, atr
    elif adx < 20:
        return "ranging", adx, atr
    else:
        return "transition", adx, atr


def regime_size_multiplier(regime: Regime) -> float:
    return {"trending": 1.0, "transition": 0.75, "ranging": 0.25}[regime]


# ── signal generators ─────────────────────────────────────────────────────────

def _eth_branch(
    eth_5m: list[dict],
    vwap_window: int = 48,       # ~4h rolling VWAP
    deviation_threshold: float = 0.005,  # 0.5% from VWAP
    vol_multiplier: float = 1.2, # current bar volume must be > N×avg to confirm move
) -> Direction | None:
    """
    VWAP mean reversion: price deviated > threshold from 4h VWAP on a
    volume spike → expect reversion. Enter AGAINST the deviation.

    High-volume spike away from VWAP = aggressive but temporary imbalance.
    Works best in ranging/transition markets.
    """
    if len(eth_5m) < vwap_window + 5:
        return None

    session = eth_5m[-vwap_window:]
    vwap = compute_vwap(session)
    price = eth_5m[-1]["close"]
    deviation = (price - vwap) / (vwap + 1e-9)

    # Volume spike confirms the push (overextension, not a true breakout)
    volumes = [c["volume"] for c in eth_5m[-vwap_window:]]
    vol_avg = np.mean(volumes[:-1])
    vol_current = volumes[-1]
    vol_spike = vol_current > vol_avg * vol_multiplier

    if not vol_spike:
        return None

    # Candle close should be in the direction of the deviation
    # (confirms we're at an extreme, not mid-move)
    candle = eth_5m[-1]
    candle_range = candle["high"] - candle["low"]
    if candle_range == 0:
        return None
    bullish_close = (price - candle["low"]) / candle_range > 0.6
    bearish_close = (candle["high"] - price) / candle_range > 0.6

    # RSI(2) confirmation — only enter at extreme readings
    closes = [c["close"] for c in eth_5m[-20:]]
    rsi2 = compute_rsi(closes, period=2)

    if deviation > deviation_threshold and bearish_close and rsi2 > 80:
        return "short"
    if deviation < -deviation_threshold and bullish_close and rsi2 < 20:
        return "long"
    return None


def _btc_branch(
    btc_5m: list[dict],
    eth_5m: list[dict],
    btc_threshold: float = 0.0020,
    eth_max_move: float = 0.0010,
) -> Direction | None:
    """
    BTC lead-lag: BTC moved 0.20%+ over 3 bars, ETH hasn't reacted yet.
    Enter ETH in direction of BTC move.
    """
    if len(btc_5m) < 5 or len(eth_5m) < 5:
        return None

    btc_ret = (btc_5m[-1]["close"] - btc_5m[-4]["close"]) / (btc_5m[-4]["close"] + 1e-9)
    eth_ret = abs((eth_5m[-1]["close"] - eth_5m[-4]["close"]) / (eth_5m[-4]["close"] + 1e-9))

    if abs(btc_ret) < btc_threshold:
        return None
    if eth_ret > eth_max_move:
        return None  # ETH already caught up

    if len(btc_5m) >= 10:
        vol_avg = np.mean([c["volume"] for c in btc_5m[-10:-1]])
        if btc_5m[-1]["volume"] < vol_avg * 1.3:
            return None

    return "long" if btc_ret > 0 else "short"


# ── main entry ────────────────────────────────────────────────────────────────

def generate_momentum_signal(
    eth_5m: list[dict],
    btc_5m: list[dict] | None,
    eth_oi_5m: list[dict],
    ohlcv_1h: list[dict],
    existing_funding_direction: str | None = None,
    sl_pct: float = 0.0035,
    tp_pct: float = 0.0070,
    ema_fast: int = 8,
    ema_slow: int = 21,
    vwap_threshold: float = 0.005,
    btc_threshold: float = 0.0020,
) -> MomentumSignal | None:
    if not eth_5m:
        return None

    regime, adx, atr = classify_regime(ohlcv_1h)
    size_mult = regime_size_multiplier(regime)
    mark_price = eth_5m[-1]["close"]

    # ETH branch: VWAP reversion (works in ranging + transition, skip strong trends)
    direction = None
    branch = "eth"
    if regime != "trending":
        direction = _eth_branch(eth_5m, deviation_threshold=vwap_threshold)

    # BTC branch: lead-lag (works in trending + transition)
    if direction is None and btc_5m and regime != "ranging":
        direction = _btc_branch(btc_5m, eth_5m, btc_threshold)
        branch = "btc"

    if direction is None:
        return None

    if existing_funding_direction and existing_funding_direction != direction:
        return None

    sl_price = mark_price * (1 - sl_pct) if direction == "long" else mark_price * (1 + sl_pct)
    tp_price = mark_price * (1 + tp_pct) if direction == "long" else mark_price * (1 - tp_pct)

    reason = (
        f"branch={branch} regime={regime}(ADX={adx:.1f}) "
        f"price={mark_price:.2f} sl={sl_price:.2f} tp={tp_price:.2f}"
    )

    return MomentumSignal(
        direction=direction,
        branch=branch,
        regime=regime,
        adx=adx,
        size_multiplier=size_mult,
        mark_price=mark_price,
        sl_price=sl_price,
        tp_price=tp_price,
        reason=reason,
    )

"""Phase 1 + Hybrid: три стратегии с DFA режимным классификатором.

Стратегии (APM ADR e602d90ac716):
  A — funding rate MR + OI delta фильтр + SMA тренд (fallback, ~50% времени)
  B — BTC lead-lag: BTC_return_15m > ±0.35% при ETH_return_15m < 0.15% (~30%)
  C — Liquidation cascade: OI_drop_1h > 3% + volume_spike + cascade confirmation (~20%)

Режимный классификатор: DFA exponent на 500h rolling — устойчивее Hurst на 168h.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Literal
import numpy as np


Direction = Literal["long", "short"]
Strategy = Literal["A", "B", "C"]


@dataclass
class Signal:
    direction: Direction
    strategy: Strategy
    funding_rate: float
    mark_price: float
    confidence: float
    reason: str


# ── математика ─────────────────────────────────────────────────────────────

def compute_rsi(closes: list[float], period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    deltas = np.diff(closes[-(period + 1):])
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_gain = gains.mean()
    avg_loss = losses.mean()
    if avg_loss == 0:
        return 100.0
    return 100 - (100 / (1 + avg_gain / avg_loss))


def compute_sma(closes: list[float], period: int = 20) -> float | None:
    if len(closes) < period:
        return None
    return float(np.mean(closes[-period:]))


def compute_dfa(prices: list[float], min_scale: int = 4, max_scale: int = 50) -> float:
    """DFA (Detrended Fluctuation Analysis) exponent.
    0.5 = random walk; < 0.5 = mean-reverting; > 0.5 = trending.
    Надёжен при n >= 200 точках.
    """
    n = len(prices)
    if n < max_scale * 2:
        return 0.5  # недостаточно данных
    y = np.cumsum(np.array(prices) - np.mean(prices))
    scales = np.unique(np.logspace(np.log10(min_scale), np.log10(max_scale), 15).astype(int))
    fluctuations = []
    for s in scales:
        n_seg = n // s
        if n_seg < 2:
            continue
        fluct = []
        for i in range(n_seg):
            seg = y[i * s:(i + 1) * s]
            x = np.arange(len(seg))
            coef = np.polyfit(x, seg, 1)
            fluct.append(np.mean((seg - np.polyval(coef, x)) ** 2))
        fluctuations.append(np.sqrt(np.mean(fluct)))
    if len(fluctuations) < 3:
        return 0.5
    log_s = np.log(scales[:len(fluctuations)])
    log_f = np.log(fluctuations)
    alpha, _ = np.polyfit(log_s, log_f, 1)
    return float(np.clip(alpha, 0.1, 0.9))


def classify_regime(closes_500h: list[float]) -> tuple[str, float]:
    """Возвращает (regime, dfa_exponent).
    regime: 'mean_reverting' | 'trending' | 'transition'
    """
    dfa = compute_dfa(closes_500h)
    if dfa < 0.42:
        return "mean_reverting", dfa
    elif dfa > 0.58:
        return "trending", dfa
    else:
        return "transition", dfa


def compute_oi_delta(oi_history: list[dict], lookback_h: int = 4) -> float | None:
    """Вычисляет OI delta за lookback_h часов. oi_history — список {'ts', 'oi'}, сортирован по времени."""
    if len(oi_history) <= lookback_h:
        return None
    current = oi_history[-1]["oi"]
    past = oi_history[-1 - lookback_h]["oi"]
    if past == 0:
        return None
    return (current - past) / past


def compute_oi_drop_1h(oi_history: list[dict]) -> float:
    """OI drop за последний час (положительное = падение)."""
    if len(oi_history) < 2:
        return 0.0
    prev = oi_history[-2]["oi"]
    curr = oi_history[-1]["oi"]
    if prev == 0:
        return 0.0
    return (prev - curr) / prev  # positive when OI drops


# ── генераторы сигналов ────────────────────────────────────────────────────

def generate_signal_a(
    funding_rate: float,
    mark_price: float,
    ohlcv_1h: list[dict],
    ohlcv_4h: list[dict] | None,
    oi_history: list[dict] | None,
    threshold: float = 0.0001,
    leverage_multiplier: float = 1.0,
) -> Signal | None:
    """Strategy A: funding rate MR + 4h OI delta + SMA тренд."""
    if abs(funding_rate) < threshold:
        return None

    direction: Direction = "short" if funding_rate > 0 else "long"
    confidence = 0.5

    closes_1h = [c["close"] for c in ohlcv_1h]
    rsi = compute_rsi(closes_1h)
    if direction == "short" and rsi < 45:
        return None
    if direction == "long" and rsi > 55:
        return None

    # 4h SMA тренд-фильтр
    if ohlcv_4h and len(ohlcv_4h) >= 20:
        sma_20 = compute_sma([c["close"] for c in ohlcv_4h], 20)
        if sma_20:
            if direction == "short" and mark_price > sma_20:
                return None
            if direction == "long" and mark_price < sma_20:
                return None

    # OI delta фильтр (APM улучшение Strategy A)
    if oi_history:
        oi_delta = compute_oi_delta(oi_history, lookback_h=4)
        if oi_delta is not None:
            if direction == "long" and oi_delta < 0.005:
                return None  # лонг требует OI > +0.5%
            if direction == "short" and oi_delta > -0.01:
                return None  # шорт требует OI < -1.0%
            confidence += 0.10

    if rsi > 60 and direction == "short" or rsi < 40 and direction == "long":
        confidence += 0.15
    if abs(funding_rate) > threshold * 2:
        confidence += 0.05

    return Signal(
        direction=direction,
        strategy="A",
        funding_rate=funding_rate,
        mark_price=mark_price,
        confidence=min(confidence * leverage_multiplier, 1.0),
        reason=f"funding={funding_rate:.4%} rsi={rsi:.1f} oi_delta={compute_oi_delta(oi_history or [], 4) or 0:.2%}",
    )


def generate_signal_b(
    btc_ohlcv_15m: list[dict],
    eth_ohlcv_15m: list[dict],
    mark_price: float,
    btc_threshold: float = 0.0035,
    eth_max_move: float = 0.0015,
) -> Signal | None:
    """Strategy B: BTC lead-lag. BTC мов > ±0.35% при ETH не двигался (<0.15%)."""
    if len(btc_ohlcv_15m) < 3 or len(eth_ohlcv_15m) < 3:
        return None

    btc_now = btc_ohlcv_15m[-1]["close"]
    btc_prev = btc_ohlcv_15m[-2]["close"]
    eth_now = eth_ohlcv_15m[-1]["close"]
    eth_prev = eth_ohlcv_15m[-2]["close"]

    if btc_prev == 0 or eth_prev == 0:
        return None

    btc_ret = (btc_now - btc_prev) / btc_prev
    eth_ret = abs((eth_now - eth_prev) / eth_prev)

    if abs(btc_ret) < btc_threshold:
        return None
    if eth_ret > eth_max_move:
        return None  # ETH уже отреагировал

    # volume confirmation
    if len(btc_ohlcv_15m) >= 10:
        btc_vol_ma = np.mean([c["volume"] for c in btc_ohlcv_15m[-10:-1]])
        if btc_ohlcv_15m[-1]["volume"] < btc_vol_ma * 1.5:
            return None  # нет объёмного подтверждения

    direction: Direction = "long" if btc_ret > 0 else "short"

    return Signal(
        direction=direction,
        strategy="B",
        funding_rate=0.0,
        mark_price=mark_price,
        confidence=0.65,
        reason=f"btc_ret={btc_ret:.3%} eth_ret={eth_ret:.3%} lead_lag",
    )


def generate_signal_c(
    oi_history: list[dict],
    ohlcv_1h: list[dict],
    mark_price: float,
    funding_rate: float,
    liquidation_usd: float = 0.0,
    volume_ma_168h: float | None = None,
) -> Signal | None:
    """Strategy C: Liquidation cascade reversal.
    OI drop >3%/1h + volume spike + cascade confirmation.
    """
    if len(oi_history) < 2 or len(ohlcv_1h) < 2:
        return None

    oi_drop_1h = compute_oi_drop_1h(oi_history)
    if oi_drop_1h < 0.03:
        return None

    # Volume spike
    current_vol = ohlcv_1h[-1]["volume"]
    if volume_ma_168h and current_vol < volume_ma_168h * 2.5:
        return None

    # Price move < 1.5% (не паника, а принудительные ликвидации)
    price_prev = ohlcv_1h[-2]["close"]
    price_move = abs(mark_price - price_prev) / price_prev
    if price_move > 0.015:
        return None

    # Liquidation confirmation ($5M threshold, если данные есть)
    if liquidation_usd > 0 and liquidation_usd < 5_000_000:
        return None

    # Funding confirmation: нейтральный funding блокирует вход
    if abs(funding_rate) < 0.0001:
        return None

    # Направление против ликвидаций (ликвидировали лонгов → цена упала → LONG)
    price_direction = "up" if ohlcv_1h[-1]["close"] < ohlcv_1h[-2]["close"] else "down"
    direction: Direction = "long" if price_direction == "up" else "short"

    return Signal(
        direction=direction,
        strategy="C",
        funding_rate=funding_rate,
        mark_price=mark_price,
        confidence=0.70,
        reason=f"oi_drop={oi_drop_1h:.2%} vol_spike price_move={price_move:.2%} cascade",
    )


# ── главная функция (обратная совместимость) ───────────────────────────────

def generate_signal(
    funding_rate: float,
    mark_price: float,
    funding_history: list[float],
    ohlcv_1h: list[dict],
    oi_current: float | None,
    oi_prev: float | None,
    ohlcv_4h: list[dict] | None = None,
    oi_history: list[dict] | None = None,
    btc_ohlcv_15m: list[dict] | None = None,
    eth_ohlcv_15m: list[dict] | None = None,
    closes_500h: list[float] | None = None,
    threshold: float = 0.0001,
) -> Signal | None:
    """Главный диспетчер: классифицирует режим, выбирает стратегию."""

    # ATR volatility spike filter: не входим если текущая свеча > 2.5x 24h ATR
    if len(ohlcv_1h) >= 25:
        highs  = np.array([c["high"]  for c in ohlcv_1h[-26:]])
        lows   = np.array([c["low"]   for c in ohlcv_1h[-26:]])
        closes = np.array([c["close"] for c in ohlcv_1h[-26:]])
        tr = np.maximum(highs[1:] - lows[1:],
             np.maximum(np.abs(highs[1:] - closes[:-1]),
                        np.abs(lows[1:]  - closes[:-1])))
        atr_24h = tr[-24:].mean()
        tr_now  = tr[-1]
        if atr_24h > 0 and tr_now > atr_24h * 2.5:
            return None  # volatility spike — no-trade

    # Приоритет 1: Strategy C — liquidation cascade (независимо от режима)
    if oi_history:
        sig_c = generate_signal_c(
            oi_history=oi_history,
            ohlcv_1h=ohlcv_1h,
            mark_price=mark_price,
            funding_rate=funding_rate,
        )
        if sig_c:
            return sig_c

    # Режимный классификатор
    regime = "transition"
    dfa = 0.5
    if closes_500h and len(closes_500h) >= 100:
        regime, dfa = classify_regime(closes_500h)

    leverage_multiplier = 0.5 if regime == "transition" else 1.0

    # Приоритет 2: Strategy B — BTC lead-lag (trending режим)
    if regime in ("trending", "transition") and btc_ohlcv_15m and eth_ohlcv_15m:
        sig_b = generate_signal_b(btc_ohlcv_15m, eth_ohlcv_15m, mark_price)
        if sig_b:
            return sig_b

    # Приоритет 3: Strategy A — funding rate MR (всегда как fallback)
    return generate_signal_a(
        funding_rate=funding_rate,
        mark_price=mark_price,
        ohlcv_1h=ohlcv_1h,
        ohlcv_4h=ohlcv_4h,
        oi_history=oi_history,
        threshold=threshold,
        leverage_multiplier=leverage_multiplier,
    )

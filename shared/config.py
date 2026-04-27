from __future__ import annotations
import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

def _required(key: str) -> str:
    val = os.environ.get(key)
    if not val:
        raise RuntimeError(f"Missing required env var: {key}")
    return val

def _optional(key: str, default: str) -> str:
    return os.environ.get(key, default)

def _float(key: str, default: float) -> float:
    return float(os.environ.get(key, default))

def _bool(key: str, default: bool) -> bool:
    return os.environ.get(key, str(default)).lower() in ("true", "1", "yes")


@dataclass
class Config:
    # Bybit
    bybit_api_key: str
    bybit_api_secret: str
    bybit_testnet: bool

    # Telegram
    telegram_token: str
    telegram_chat_id: str

    # DB
    database_url: str

    # Strategy
    symbol: str
    leverage: int
    risk_per_trade: float
    funding_threshold: float
    stop_loss_pct: float
    take_profit_pct: float

    # Mode
    paper_trading: bool
    log_level: str

    # Momentum strategy
    momentum_enabled: bool
    momentum_sl_pct: float
    momentum_tp_pct: float
    momentum_btc_threshold: float
    momentum_ema_fast: int
    momentum_ema_slow: int
    momentum_vwap_threshold: float
    momentum_max_hold_bars: int


def load_config() -> Config:
    return Config(
        bybit_api_key=_optional("BYBIT_API_KEY", ""),
        bybit_api_secret=_optional("BYBIT_API_SECRET", ""),
        bybit_testnet=_bool("BYBIT_TESTNET", True),
        telegram_token=_optional("TELEGRAM_BOT_TOKEN", ""),
        telegram_chat_id=_optional("TELEGRAM_CHAT_ID", ""),
        database_url=_optional("DATABASE_URL", "sqlite:///traderbot.db"),
        symbol=_optional("SYMBOL", "ETHUSDT"),
        leverage=int(_optional("LEVERAGE", "2")),
        risk_per_trade=_float("RISK_PER_TRADE", 0.015),
        funding_threshold=_float("FUNDING_THRESHOLD", 0.0001),
        stop_loss_pct=_float("STOP_LOSS_PCT", 0.01),
        take_profit_pct=_float("TAKE_PROFIT_PCT", 0.02),
        paper_trading=_bool("PAPER_TRADING", True),
        log_level=_optional("LOG_LEVEL", "INFO"),
        momentum_enabled=_bool("MOMENTUM_ENABLED", True),
        momentum_sl_pct=_float("MOMENTUM_SL_PCT", 0.0020),
        momentum_tp_pct=_float("MOMENTUM_TP_PCT", 0.0040),
        momentum_btc_threshold=_float("MOMENTUM_BTC_THRESHOLD", 0.0020),
        momentum_ema_fast=int(_optional("MOMENTUM_EMA_FAST", "8")),
        momentum_ema_slow=int(_optional("MOMENTUM_EMA_SLOW", "21")),
        momentum_vwap_threshold=_float("MOMENTUM_VWAP_THRESHOLD", 0.005),
        momentum_max_hold_bars=int(_optional("MOMENTUM_MAX_HOLD_BARS", "24")),
    )

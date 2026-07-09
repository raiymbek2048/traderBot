"""Получение funding rate, OHLCV, OI с Bybit."""
from __future__ import annotations
from datetime import datetime, timezone, timezone
import ccxt
import httpx
from loguru import logger

_BYBIT_TICKERS = "https://api.bybit.com/v5/market/tickers"


def _fetch_ticker_direct(symbol: str) -> dict | None:
    """Прямой HTTP запрос к Bybit v5 — обходит баги ccxt-парсера."""
    try:
        r = httpx.get(
            _BYBIT_TICKERS,
            params={"symbol": symbol, "category": "linear"},
            timeout=8.0,
        )
        r.raise_for_status()
        data = r.json()
        if data.get("retCode") == 0:
            items = data.get("result", {}).get("list", [])
            if items:
                return items[0]
    except Exception as e:
        logger.warning(f"Direct ticker fetch failed: {e}")
    return None


class BybitFetcher:
    def __init__(self, api_key: str, api_secret: str, testnet: bool = False):
        self._exchange = ccxt.bybit({
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,   # ccxt сам выдерживает паузы между запросами
            "options": {"defaultType": "linear"},
        })
        if testnet:
            self._exchange.set_sandbox_mode(True)

    def get_funding_rate(self, symbol: str) -> dict:
        mark_price = 0.0
        current_rate = 0.0
        next_funding_time = None

        # Пробуем ccxt; при ошибке — прямой httpx запрос
        try:
            ticker = self._exchange.fetch_ticker(symbol)
            mark_price = ticker["last"]
        except Exception as e:
            logger.warning(f"fetch_ticker (ccxt) failed, using direct API: {e}")
            raw = _fetch_ticker_direct(symbol)
            if raw:
                mark_price = float(raw.get("markPrice") or raw.get("lastPrice") or 0)
                fr = raw.get("fundingRate")
                if fr:
                    current_rate = float(fr)
            else:
                raise RuntimeError(f"Cannot fetch ticker for {symbol}")

        # Funding rate через ccxt
        if current_rate == 0.0:
            try:
                funding = self._exchange.fetch_funding_rate(symbol)
                current_rate = funding["fundingRate"]
                next_funding_time = funding.get("nextFundingDatetime")
            except Exception as e:
                logger.warning(f"fetch_funding_rate (ccxt) failed, using direct API: {e}")
                raw = _fetch_ticker_direct(symbol)
                if raw:
                    fr = raw.get("fundingRate")
                    if fr:
                        current_rate = float(fr)

        # История funding rates
        history = []
        try:
            raw_history = self._exchange.fetch_funding_rate_history(symbol, limit=10)
            history = [
                {"rate": h["fundingRate"], "time": h["datetime"]}
                for h in raw_history
            ]
        except Exception as e:
            logger.warning(f"fetch_funding_rate_history failed: {e}")

        return {
            "symbol": symbol,
            "current_rate": current_rate,
            "next_funding_time": next_funding_time,
            "mark_price": mark_price,
            "history": history,
        }

    def get_ohlcv(self, symbol: str, timeframe: str = "4h", limit: int = 100) -> list[dict]:
        raw = self._exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        return [
            {"ts": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4], "volume": r[5]}
            for r in raw
        ]

    def get_open_interest(self, symbol: str) -> float | None:
        try:
            oi = self._exchange.fetch_open_interest(symbol)
            return oi.get("openInterestAmount")
        except Exception as e:
            logger.warning(f"OI fetch failed: {e}")
            return None

    def get_oi_history(self, symbol: str, timeframe: str = "1h", limit: int = 200) -> list[dict]:
        """OI история для вычисления 4h OI delta и cascade detection."""
        try:
            raw = self._exchange.fetch_open_interest_history(
                symbol, timeframe=timeframe, limit=limit
            )
            return [
                {"ts": r["timestamp"], "oi": r["openInterestAmount"]}
                for r in raw
            ]
        except Exception as e:
            logger.warning(f"OI history fetch failed: {e}")
            return []

    def get_liquidation_volume(self, symbol: str) -> float:
        """Суммарный объём ликвидаций за последний час в USD."""
        try:
            # Bybit предоставляет данные через fetch_liquidations если поддерживается
            liq = self._exchange.fetch_liquidations(symbol, limit=100)
            total = sum(
                abs(l.get("cost") or 0)
                for l in liq
                if l.get("timestamp") and
                   (utcnow().timestamp() * 1000 - l["timestamp"]) < 3_600_000
            )
            return total
        except Exception:
            return 0.0

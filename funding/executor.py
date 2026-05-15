"""Funding Rate Executor.

Открывает delta-neutral позицию:
  Bybit Spot BUY  (покупаем актив)
  Bybit Linear SHORT (шортим перп на ту же сумму)

Закрывает:
  Bybit Spot SELL
  Bybit Linear CLOSE SHORT

Нет ценового риска: рост/падение компенсируют друг друга.
Прибыль = фандинг-рейт каждые 8 часов.
"""
from __future__ import annotations
import asyncio
import hashlib
import hmac
import json
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from loguru import logger
from sqlalchemy.orm import Session
from shared.db import FundingPosition

SIZE_USDT      = float(10)   # переопределяется из env в run.py
PERP_LEVERAGE  = 1           # 1x — без плеча, безопасно
PAPER_TRADING  = True        # переопределяется из run.py

_tg_token: str = ""
_tg_chat:  str = ""

BYBIT_BASE = "https://api.bybit.com"


class BybitClient:
    def __init__(self, api_key: str, api_secret: str) -> None:
        self.api_key    = api_key
        self.api_secret = api_secret
        self.recv_window = "10000"

    def _sign(self, ts: str, payload: str) -> str:
        msg = ts + self.api_key + self.recv_window + payload
        return hmac.new(self.api_secret.encode(), msg.encode(), hashlib.sha256).hexdigest()

    def _headers(self, ts: str, payload: str) -> dict:
        return {
            "X-BAPI-API-KEY":     self.api_key,
            "X-BAPI-TIMESTAMP":   ts,
            "X-BAPI-SIGN":        self._sign(ts, payload),
            "X-BAPI-RECV-WINDOW": self.recv_window,
            "Content-Type":       "application/json",
        }

    def _post(self, path: str, body: dict) -> dict:
        payload = json.dumps(body)
        ts = str(int(time.time() * 1000))
        req = urllib.request.Request(
            BYBIT_BASE + path,
            data=payload.encode(),
            headers=self._headers(ts, payload),
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())

    def _get(self, path: str, params: str = "") -> dict:
        ts = str(int(time.time() * 1000))
        url = BYBIT_BASE + path + (f"?{params}" if params else "")
        req = urllib.request.Request(url, headers=self._headers(ts, params))
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())

    def get_spot_balance(self) -> float:
        data = self._get("/v5/account/wallet-balance", "accountType=UNIFIED")
        coins = data["result"]["list"][0]["coin"]
        usdt = next((c for c in coins if c["coin"] == "USDT"), None)
        return float(usdt["availableToWithdraw"] or 0) if usdt else 0.0

    def get_ticker(self, symbol: str, category: str = "spot") -> dict:
        data = self._get("/v5/market/tickers", f"category={category}&symbol={symbol}")
        return data["result"]["list"][0]

    def set_leverage(self, symbol: str, leverage: int) -> None:
        body = {
            "category": "linear",
            "symbol": symbol,
            "buyLeverage": str(leverage),
            "sellLeverage": str(leverage),
        }
        self._post("/v5/position/set-leverage", body)

    def place_spot_buy(self, symbol: str, size_usdt: float) -> dict:
        """Купить актив в споте на size_usdt USDT."""
        body = {
            "category": "spot",
            "symbol": symbol,
            "side": "Buy",
            "orderType": "Market",
            "qty": str(size_usdt),
            "marketUnit": "quoteCoin",  # qty в USDT
        }
        return self._post("/v5/order/create", body)

    def place_spot_sell(self, symbol: str, qty: float) -> dict:
        """Продать qty монет в споте."""
        body = {
            "category": "spot",
            "symbol": symbol,
            "side": "Sell",
            "orderType": "Market",
            "qty": str(qty),
        }
        return self._post("/v5/order/create", body)

    def place_perp_short(self, symbol: str, qty: float) -> dict:
        """Открыть шорт на qty монет в linear perp."""
        body = {
            "category": "linear",
            "symbol": symbol,
            "side": "Sell",
            "orderType": "Market",
            "qty": str(qty),
            "positionIdx": 0,  # one-way mode
        }
        return self._post("/v5/order/create", body)

    def place_perp_close(self, symbol: str, qty: float) -> dict:
        """Закрыть шорт (купить qty монет в perp)."""
        body = {
            "category": "linear",
            "symbol": symbol,
            "side": "Buy",
            "orderType": "Market",
            "qty": str(qty),
            "positionIdx": 0,
            "reduceOnly": True,
        }
        return self._post("/v5/order/create", body)

    def get_order(self, category: str, symbol: str, order_id: str) -> dict:
        params = f"category={category}&symbol={symbol}&orderId={order_id}"
        data = self._get("/v5/order/realtime", params)
        orders = data.get("result", {}).get("list", [])
        return orders[0] if orders else {}


async def _send_tg(msg: str) -> None:
    if not _tg_token or not _tg_chat:
        return
    try:
        payload = json.dumps({"chat_id": _tg_chat, "text": msg}).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{_tg_token}/sendMessage",
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception as e:
        logger.warning(f"TG send failed: {e}")


def _round_qty(qty: float, symbol: str) -> float:
    """Минимальные шаги количества по символу."""
    steps = {
        "BTCUSDT": 0.001,
        "ETHUSDT": 0.01,
        "SOLUSDT": 0.1,
        "DOGEUSDT": 1.0,
        "BNBUSDT": 0.01,
        "XRPUSDT": 1.0,
    }
    step = steps.get(symbol, 1.0)
    return round(int(qty / step) * step, 8)


async def open_position(
    engine,
    bybit: BybitClient,
    symbol: str,
    funding_rate: float,
    size_usdt: float,
    paper: bool,
) -> int | None:
    """Открыть delta-neutral позицию. Возвращает position_id или None при ошибке."""
    try:
        # Текущая цена
        ticker = bybit.get_ticker(symbol, "spot")
        price = float(ticker["lastPrice"])
        qty = _round_qty(size_usdt / price, symbol)

        if qty <= 0:
            logger.warning(f"qty=0 для {symbol}, пропускаем")
            return None

        balance = bybit.get_spot_balance()
        if balance < size_usdt * 2:  # нужен спот + маржа перпа
            await _send_tg(
                f"⚠️ Недостаточно баланса для {symbol}\n"
                f"Нужно: ${size_usdt*2:.0f}, есть: ${balance:.2f}"
            )
            logger.warning(f"Insufficient balance: need ${size_usdt*2:.0f}, have ${balance:.2f}")
            return None

        logger.info(
            f"Opening position: {symbol} | price={price} | qty={qty} | "
            f"size=${size_usdt} | paper={paper}"
        )

        spot_order_id = "paper_spot"
        perp_order_id = "paper_perp"

        if not paper:
            # Устанавливаем 1x плечо
            try:
                bybit.set_leverage(symbol, PERP_LEVERAGE)
            except Exception as e:
                logger.warning(f"set_leverage failed: {e}")

            # Одновременно: купить спот + шорт перп
            spot_res, perp_res = await asyncio.gather(
                asyncio.get_event_loop().run_in_executor(
                    None, lambda: bybit.place_spot_buy(symbol, size_usdt)
                ),
                asyncio.get_event_loop().run_in_executor(
                    None, lambda: bybit.place_perp_short(symbol, qty)
                ),
            )

            if spot_res.get("retCode") != 0:
                raise RuntimeError(f"Spot buy failed: {spot_res}")
            if perp_res.get("retCode") != 0:
                raise RuntimeError(f"Perp short failed: {perp_res}")

            spot_order_id = spot_res["result"]["orderId"]
            perp_order_id = perp_res["result"]["orderId"]

        # Записать в БД
        with Session(engine) as session:
            pos = FundingPosition(
                symbol=symbol,
                spot_exchange="bybit",
                perp_exchange="bybit",
                spot_order_id=spot_order_id,
                perp_order_id=perp_order_id,
                size_usdt=size_usdt,
                spot_qty=qty,
                perp_qty=qty,
                spot_entry_price=price,
                perp_entry_price=price,
                funding_rate_open=funding_rate,
                funding_collected_usdt=0.0,
                status="open",
                paper=paper,
                opened_at=datetime.now(timezone.utc),
            )
            session.add(pos)
            session.commit()
            pos_id = pos.id

        mode = "PAPER" if paper else "LIVE"
        annualized = funding_rate * 3 * 365 * 100
        await _send_tg(
            f"✅ [{mode}] Позиция открыта\n"
            f"Символ: {symbol}\n"
            f"Спот BUY: {qty} @ ${price:.4f}\n"
            f"Перп SHORT: {qty} @ ${price:.4f}\n"
            f"Размер: ${size_usdt}\n"
            f"Фандинг: {funding_rate*100:.4f}%/8h ({annualized:.1f}%/год)\n"
            f"ID: {pos_id}"
        )
        logger.info(f"Position opened: id={pos_id} {symbol}")
        return pos_id

    except Exception as e:
        logger.error(f"open_position error: {e}")
        await _send_tg(f"❌ Ошибка открытия {symbol}: {e}")
        return None


async def close_position(
    engine,
    bybit: BybitClient,
    symbol: str,
    position_id: int,
    paper: bool,
) -> None:
    """Закрыть delta-neutral позицию."""
    try:
        with Session(engine) as session:
            pos = session.get(FundingPosition, position_id)
            if not pos or pos.status != "open":
                logger.warning(f"Position {position_id} not found or already closed")
                return

            qty = pos.spot_qty
            entry_price = pos.spot_entry_price

        ticker = bybit.get_ticker(symbol, "spot")
        exit_price = float(ticker["lastPrice"])

        if not paper:
            spot_res, perp_res = await asyncio.gather(
                asyncio.get_event_loop().run_in_executor(
                    None, lambda: bybit.place_spot_sell(symbol, qty)
                ),
                asyncio.get_event_loop().run_in_executor(
                    None, lambda: bybit.place_perp_close(symbol, qty)
                ),
            )
            if spot_res.get("retCode") != 0:
                raise RuntimeError(f"Spot sell failed: {spot_res}")
            if perp_res.get("retCode") != 0:
                raise RuntimeError(f"Perp close failed: {perp_res}")

        # PnL от спота и перпа взаимно компенсируются (delta neutral)
        # Реальная прибыль = funding_collected
        with Session(engine) as session:
            pos = session.get(FundingPosition, position_id)
            pos.spot_exit_price = exit_price
            pos.perp_exit_price = exit_price
            pos.funding_rate_close = 0.0
            pos.pnl_usdt = pos.funding_collected_usdt
            pos.status = "closed"
            pos.closed_at = datetime.now(timezone.utc)
            session.commit()
            collected = pos.funding_collected_usdt

        mode = "PAPER" if paper else "LIVE"
        await _send_tg(
            f"🔒 [{mode}] Позиция закрыта\n"
            f"Символ: {symbol}\n"
            f"Вход: ${entry_price:.4f} → Выход: ${exit_price:.4f}\n"
            f"Фандинг собрано: ${collected:.4f}\n"
            f"ID: {position_id}"
        )
        logger.info(f"Position closed: id={position_id} {symbol}, funding=${collected:.4f}")

    except Exception as e:
        logger.error(f"close_position error: {e}")
        await _send_tg(f"❌ Ошибка закрытия {symbol} id={position_id}: {e}")


signal_queue: asyncio.Queue = asyncio.Queue()


async def run(engine, bybit: BybitClient, paper: bool, size_usdt: float) -> None:
    """Главный loop executor: читает сигналы из очереди."""
    open_positions: dict[str, int] = {}  # {symbol: position_id}

    while True:
        signal = await signal_queue.get()
        action = signal["action"]
        symbol = signal["symbol"]

        if action == "open" and symbol not in open_positions:
            pos_id = await open_position(
                engine, bybit, symbol,
                signal["funding_rate"], size_usdt, paper,
            )
            if pos_id:
                open_positions[symbol] = pos_id

        elif action == "close" and symbol in open_positions:
            pos_id = open_positions.pop(symbol)
            await close_position(engine, bybit, symbol, pos_id, paper)

"""SpreadArb Executor — реальное исполнение арбитражных сделок.

Запускается параллельно с monitor.py.
Слушает очередь сигналов от monitor и:
  1. Проверяет глубину стакана
  2. Проверяет балансы на обеих биржах
  3. Одновременно размещает market order на buy и sell
  4. Записывает результат в arb_real_trades
  5. Шлёт Telegram отчёт

Режимы:
  PAPER_TRADING=true  — только логирует, не торгует
  PAPER_TRADING=false — реальные ордера

Run: python -m arbitrage.executor
"""
from __future__ import annotations

import asyncio
import time
import urllib.request
import urllib.parse
import json
import hashlib
import hmac
from dataclasses import dataclass
from typing import Optional

from loguru import logger
from sqlalchemy.orm import Session
from sqlalchemy import select, func

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.db import init_db, ArbRealTrade, ArbPaperTrade
from shared.config import load_config
from shared.utils import utcnow

# ── параметры ──────────────────────────────────────────────────────────────────

MIN_DEPTH_USDT   = 200.0    # минимальная ликвидность на уровне ($)
MIN_BALANCE_USDT = 5.0      # минимальный баланс на бирже чтобы торговать
MAX_DAILY_LOSS   = 20.0     # стоп на день если потеряли больше ($)
COOLDOWN_SEC     = 3.0      # пауза между сделками по одному символу

# очередь сигналов от monitor.py → executor.py
signal_queue: asyncio.Queue = asyncio.Queue(maxsize=100)

_last_trade: dict[str, float] = {}   # symbol → unix ts последней сделки
_daily_pnl: float = 0.0


# ── REST helpers ───────────────────────────────────────────────────────────────

def _sign_binance(params: dict, secret: str) -> str:
    query = urllib.parse.urlencode(params)
    return hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()


def _sign_bybit(params: dict, secret: str, timestamp: int, recv_window: int = 5000) -> str:
    param_str = f"{timestamp}{params.get('api_key','')}{recv_window}"
    sorted_params = "&".join(f"{k}={v}" for k, v in sorted(params.items()) if k not in ("sign", "api_key"))
    param_str += sorted_params
    return hmac.new(secret.encode(), param_str.encode(), hashlib.sha256).hexdigest()


async def _http(method: str, url: str, headers: dict = None, data: bytes = None, timeout: int = 5):
    loop = asyncio.get_event_loop()
    def _call():
        req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            return json.loads(e.read())
    return await loop.run_in_executor(None, _call)


# ── Binance API ────────────────────────────────────────────────────────────────

class BinanceClient:
    BASE = "https://api.binance.com"

    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret

    def _headers(self) -> dict:
        return {"X-MBX-APIKEY": self.api_key}

    async def get_balance(self, asset: str = "USDT") -> float:
        ts = int(time.time() * 1000)
        params = {"timestamp": ts}
        params["signature"] = _sign_binance(params, self.api_secret)
        url = f"{self.BASE}/api/v3/account?" + urllib.parse.urlencode(params)
        data = await _http("GET", url, headers=self._headers())
        for b in data.get("balances", []):
            if b["asset"] == asset:
                return float(b["free"])
        return 0.0

    async def get_depth(self, symbol: str, limit: int = 5) -> dict:
        url = f"{self.BASE}/api/v3/depth?symbol={symbol}&limit={limit}"
        return await _http("GET", url)

    async def place_market_order(self, symbol: str, side: str, quote_qty: float) -> dict:
        """side: BUY или SELL. quote_qty — сумма в USDT."""
        ts = int(time.time() * 1000)
        params = {
            "symbol": symbol,
            "side": side.upper(),
            "type": "MARKET",
            "quoteOrderQty": f"{quote_qty:.2f}",
            "timestamp": ts,
        }
        params["signature"] = _sign_binance(params, self.api_secret)
        url = f"{self.BASE}/api/v3/order"
        data_enc = urllib.parse.urlencode(params).encode()
        return await _http("POST", url, headers=self._headers(), data=data_enc)


# ── Bybit API ──────────────────────────────────────────────────────────────────

class BybitClient:
    BASE = "https://api.bybit.com"

    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret

    def _auth_headers(self, params: dict) -> dict:
        ts = int(time.time() * 1000)
        recv_window = 10000
        param_str = str(ts) + self.api_key + str(recv_window)
        sorted_params = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        param_str += sorted_params
        sign = hmac.new(self.api_secret.encode(), param_str.encode(), hashlib.sha256).hexdigest()
        return {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-SIGN": sign,
            "X-BAPI-TIMESTAMP": str(ts),
            "X-BAPI-RECV-WINDOW": str(recv_window),
            "Content-Type": "application/json",
        }

    async def get_balance(self, asset: str = "USDT") -> float:
        params = {"accountType": "UNIFIED", "coin": asset}
        headers = self._auth_headers(params)
        url = f"{self.BASE}/v5/account/wallet-balance?" + urllib.parse.urlencode(params)
        data = await _http("GET", url, headers=headers)
        try:
            coins = data["result"]["list"][0]["coin"]
            for c in coins:
                if c["coin"] == asset:
                    return float(c["availableToWithdraw"])
        except (KeyError, IndexError):
            pass
        return 0.0

    async def get_depth(self, symbol: str, limit: int = 5) -> dict:
        url = f"{self.BASE}/v5/market/orderbook?category=spot&symbol={symbol}&limit={limit}"
        return await _http("GET", url)

    async def place_market_order(self, symbol: str, side: str, quote_qty: float) -> dict:
        """side: Buy или Sell. quote_qty — сумма в USDT."""
        body = {
            "category": "spot",
            "symbol": symbol,
            "side": side.capitalize(),
            "orderType": "Market",
            "marketUnit": "quoteCoin",
            "qty": f"{quote_qty:.2f}",
        }
        headers = self._auth_headers(body)
        url = f"{self.BASE}/v5/order/create"
        return await _http("POST", url, headers=headers, data=json.dumps(body).encode())


# ── Telegram ───────────────────────────────────────────────────────────────────

_tg_token: str = ""
_tg_chat:  str = ""


async def _tg(text: str) -> None:
    if not _tg_token or not _tg_chat:
        return
    try:
        url = f"https://api.telegram.org/bot{_tg_token}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": _tg_chat, "text": text, "parse_mode": "HTML",
        }).encode()
        await asyncio.get_event_loop().run_in_executor(
            None, lambda: urllib.request.urlopen(url, data, timeout=5)
        )
    except Exception as e:
        logger.warning(f"Telegram failed: {e}")


# ── depth check ────────────────────────────────────────────────────────────────

async def _check_depth(binance: BinanceClient, bybit: BybitClient,
                       symbol: str, buy_ex: str, size_usdt: float) -> tuple[bool, float, float]:
    """Возвращает (ok, buy_price, sell_price)."""
    try:
        db_b, db_y = await asyncio.gather(
            binance.get_depth(symbol),
            bybit.get_depth(symbol),
        )

        if buy_ex == "binance":
            buy_asks = db_b.get("asks", [])
            sell_bids = db_y["result"]["a"] if "result" in db_y else []
        else:
            buy_asks = db_y["result"]["a"] if "result" in db_y else []
            sell_bids = db_b.get("bids", [])

        if not buy_asks or not sell_bids:
            return False, 0, 0

        buy_price  = float(buy_asks[0][0])
        sell_price = float(sell_bids[0][0])

        buy_depth_usdt  = float(buy_asks[0][0]) * float(buy_asks[0][1])
        sell_depth_usdt = float(sell_bids[0][0]) * float(sell_bids[0][1])

        if buy_depth_usdt < size_usdt or sell_depth_usdt < size_usdt:
            logger.info(f"Depth insufficient {symbol}: buy=${buy_depth_usdt:.0f} sell=${sell_depth_usdt:.0f} need=${size_usdt:.0f}")
            return False, 0, 0

        return True, buy_price, sell_price

    except Exception as e:
        logger.warning(f"Depth check error: {e}")
        return False, 0, 0


# ── исполнение сделки ──────────────────────────────────────────────────────────

async def execute_trade(
    signal: dict,
    binance: BinanceClient,
    bybit: BybitClient,
    engine,
    paper: bool,
    size_usdt: float,
) -> None:
    global _daily_pnl

    sym      = signal["symbol"]
    buy_ex   = signal["buy_ex"]
    sell_ex  = signal["sell_ex"]
    gross    = signal["gross"]
    net      = signal["net"]

    # cooldown
    if time.time() - _last_trade.get(sym, 0) < COOLDOWN_SEC:
        return
    _last_trade[sym] = time.time()

    # daily loss guard
    if _daily_pnl <= -MAX_DAILY_LOSS:
        logger.warning(f"Daily loss limit hit: {_daily_pnl:.2f} — остановлен")
        await _tg(f"🛑 <b>Daily loss limit</b>: ${_daily_pnl:.2f}\nТорговля остановлена на сегодня.")
        return

    ts_signal = utcnow()

    if paper:
        pnl = round(size_usdt * net, 4)
        _daily_pnl += pnl

        with Session(engine) as session:
            today = utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
            day_cnt = session.scalar(select(func.count(ArbPaperTrade.id)).where(ArbPaperTrade.ts >= today)) or 0
            day_pnl = session.scalar(select(func.sum(ArbPaperTrade.pnl_usdt)).where(ArbPaperTrade.ts >= today)) or 0.0

        logger.info(f"[PAPER] {sym} {buy_ex}→{sell_ex} gross={gross*100:.3f}% net={net*100:.3f}% pnl=+${pnl:.3f}")
        await _tg(
            f"💱 <b>SpreadArb PAPER</b>\n"
            f"<b>{sym}</b>: buy@{buy_ex.upper()} → sell@{sell_ex.upper()}\n"
            f"━━━━━━━━━━━━━━━\n"
            f"Gross: <b>{gross*100:.3f}%</b>  Net: <b>{net*100:.3f}%</b>\n"
            f"Size: ${size_usdt:,.0f}  PnL: <b>+${pnl:.2f}</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📊 Сегодня: {day_cnt+1} сделок  +${day_pnl+pnl:.2f} USDT"
        )
        return

    # ── РЕАЛЬНОЕ ИСПОЛНЕНИЕ ──────────────────────────────────────────

    # 1. Проверяем глубину прямо сейчас
    ok, buy_price, sell_price = await _check_depth(binance, bybit, sym, buy_ex, size_usdt)
    if not ok:
        return

    # 2. Проверяем балансы
    bin_bal, byb_bal = await asyncio.gather(
        binance.get_balance("USDT"),
        bybit.get_balance("USDT"),
    )
    buy_bal  = bin_bal if buy_ex == "binance" else byb_bal
    sell_bal = byb_bal if sell_ex == "bybit" else bin_bal

    if buy_bal < size_usdt or buy_bal < MIN_BALANCE_USDT:
        logger.warning(f"Insufficient balance on {buy_ex}: ${buy_bal:.2f}")
        await _tg(f"⚠️ Низкий баланс на {buy_ex.upper()}: ${buy_bal:.2f}\nПополни счёт.")
        return

    # 3. Текущий спред ещё выгоден?
    live_gross = (sell_price - buy_price) / buy_price
    if live_gross < 0.002:   # если спред сдулся до <0.2% — пропускаем
        logger.info(f"Spread gone before execution: {live_gross*100:.3f}%")
        return

    # 4. Одновременно размещаем оба ордера
    logger.info(f"[REAL] Placing orders {sym}: buy@{buy_ex} ${size_usdt:.2f}, sell@{sell_ex} ${size_usdt:.2f}")

    t_start = time.monotonic()
    try:
        if buy_ex == "binance":
            buy_task  = binance.place_market_order(sym, "BUY",  size_usdt)
            sell_task = bybit.place_market_order(sym,   "Sell", size_usdt)
        else:
            buy_task  = bybit.place_market_order(sym,   "Buy",  size_usdt)
            sell_task = binance.place_market_order(sym, "SELL", size_usdt)

        buy_resp, sell_resp = await asyncio.gather(buy_task, sell_task, return_exceptions=True)
        exec_ms = (time.monotonic() - t_start) * 1000

    except Exception as e:
        logger.error(f"Order placement exception: {e}")
        await _tg(f"❌ <b>Ошибка ордера</b> {sym}: {e}")
        return

    # 5. Парсим результаты
    buy_filled = sell_filled = buy_price_f = sell_price_f = None
    status = "filled"
    error_msg = None

    try:
        if isinstance(buy_resp, Exception):
            raise buy_resp
        if buy_ex == "binance":
            buy_price_f  = float(buy_resp.get("fills", [{}])[0].get("price", buy_price))
            buy_filled   = float(buy_resp.get("executedQty", 0))
        else:
            buy_price_f  = float(buy_resp.get("result", {}).get("avgPrice", buy_price))
            buy_filled   = float(buy_resp.get("result", {}).get("cumExecQty", 0))
    except Exception as e:
        status = "partial"; error_msg = f"buy error: {e}"

    try:
        if isinstance(sell_resp, Exception):
            raise sell_resp
        if sell_ex == "bybit":
            sell_price_f = float(sell_resp.get("result", {}).get("avgPrice", sell_price))
            sell_filled  = float(sell_resp.get("result", {}).get("cumExecQty", 0))
        else:
            sell_price_f = float(sell_resp.get("fills", [{}])[0].get("price", sell_price))
            sell_filled  = float(sell_resp.get("executedQty", 0))
    except Exception as e:
        status = "partial"; error_msg = (error_msg or "") + f" sell error: {e}"

    # 6. Считаем реальный PnL
    if buy_price_f and sell_price_f and buy_filled:
        real_gross = (sell_price_f - buy_price_f) / buy_price_f
        real_net   = real_gross - 0.001  # 0.1% total комиссии
        pnl_usdt   = round(size_usdt * real_net, 4)
        slippage   = round((gross - real_gross) * 100, 4)
        _daily_pnl += pnl_usdt
    else:
        real_gross = real_net = pnl_usdt = slippage = None

    # 7. Записываем в БД
    with Session(engine) as session:
        trade = ArbRealTrade(
            symbol=sym,
            buy_exchange=buy_ex,
            sell_exchange=sell_ex,
            buy_order_id=str(buy_resp.get("orderId", "")) if isinstance(buy_resp, dict) else None,
            sell_order_id=str(sell_resp.get("result", {}).get("orderId", "")) if isinstance(sell_resp, dict) else None,
            target_size_usdt=size_usdt,
            buy_price_target=buy_price,
            sell_price_target=sell_price,
            buy_price_filled=buy_price_f,
            sell_price_filled=sell_price_f,
            buy_qty_filled=buy_filled,
            sell_qty_filled=sell_filled,
            gross_pct=round(gross * 100, 4),
            net_pct=round(net * 100, 4),
            pnl_usdt=pnl_usdt,
            slippage_pct=slippage,
            status=status,
            error=error_msg,
            ts_signal=ts_signal,
            ts_filled=utcnow(),
        )
        session.add(trade)
        session.commit()

        today = utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        day_cnt = session.scalar(select(func.count(ArbRealTrade.id)).where(ArbRealTrade.ts_signal >= today)) or 0
        day_pnl = session.scalar(select(func.sum(ArbRealTrade.pnl_usdt)).where(ArbRealTrade.ts_signal >= today)) or 0.0

    # 8. Telegram отчёт
    sign = "+" if (pnl_usdt or 0) >= 0 else ""
    pnl_str = f"{sign}${pnl_usdt:.3f}" if pnl_usdt is not None else "?"
    slip_str = f"{slippage:+.3f}%" if slippage is not None else "?"

    status_emoji = "✅" if status == "filled" else "⚠️"
    logger.info(f"[REAL] {sym} {status} pnl={pnl_str} slippage={slip_str} exec={exec_ms:.0f}ms")

    await _tg(
        f"{status_emoji} <b>SpreadArb REAL</b>\n"
        f"<b>{sym}</b>: buy@{buy_ex.upper()} → sell@{sell_ex.upper()}\n"
        f"━━━━━━━━━━━━━━━\n"
        f"Target: {gross*100:.3f}%  |  Filled: {(real_gross or 0)*100:.3f}%\n"
        f"Size: ${size_usdt:,.0f}  |  PnL: <b>{pnl_str}</b>\n"
        f"Slippage: {slip_str}  |  Exec: {exec_ms:.0f}ms\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📊 Сегодня: {day_cnt} сделок  {'+' if day_pnl>=0 else ''}${day_pnl:.2f} USDT"
        + (f"\n⚠️ {error_msg}" if error_msg else "")
    )


# ── main loop ──────────────────────────────────────────────────────────────────

async def run(engine, binance: BinanceClient, bybit: BybitClient, paper: bool, size_usdt: float) -> None:
    logger.info(f"SpreadArb Executor started | paper={paper} | size=${size_usdt:.0f}")

    # проверяем балансы при старте
    bin_bal, byb_bal = await asyncio.gather(
        binance.get_balance("USDT"),
        bybit.get_balance("USDT"),
    )
    logger.info(f"Balances: Binance=${bin_bal:.2f} Bybit=${byb_bal:.2f}")
    await _tg(
        f"🤖 <b>SpreadArb Executor {'PAPER' if paper else 'LIVE'}</b>\n"
        f"💰 Binance: ${bin_bal:.2f} USDT\n"
        f"💰 Bybit: ${byb_bal:.2f} USDT\n"
        f"Size: ${size_usdt:.0f} | Min depth: ${MIN_DEPTH_USDT:.0f}"
    )

    while True:
        try:
            signal = await asyncio.wait_for(signal_queue.get(), timeout=60)
            await execute_trade(signal, binance, bybit, engine, paper, size_usdt)
        except asyncio.TimeoutError:
            continue
        except Exception as e:
            logger.error(f"Executor error: {e}")
            await asyncio.sleep(1)


async def main() -> None:
    global _tg_token, _tg_chat
    import sys
    cfg = load_config()
    _tg_token = cfg.telegram_token
    _tg_chat  = cfg.telegram_chat_id

    logger.remove()
    logger.add(sys.stderr, level="INFO")
    logger.add("logs/arb_executor.log", rotation="50 MB", retention="90 days")

    paper     = cfg.paper_trading
    size_usdt = float(os.environ.get("ARB_SIZE_USDT", "10.0"))

    engine  = init_db(cfg.database_url)
    binance = BinanceClient(cfg.binance_api_key, cfg.binance_api_secret)
    bybit   = BybitClient(cfg.bybit_api_key, cfg.bybit_api_secret)

    await run(engine, binance, bybit, paper, size_usdt)


if __name__ == "__main__":
    asyncio.run(main())

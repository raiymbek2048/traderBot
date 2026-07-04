"""Altcoin spread scanner — ищет пары где cross-exchange спред реально живёт.

Логика: HFT доминируют на топ-ликвидных парах и убивают там спред.
На средних/мелких альткоинах их нет → спред может быть >комиссий.

Сравнение SPOT-SPOT на обеих биржах (честный арбитраж).
Отсекает топ-N самых ликвидных (территория HFT), мониторит остальные.

Пишет:
  logs/scan_results.json — снимок статистики каждый час (переживает краш)
  Telegram — почасовая сводка топ-пар

Run: python -m arbitrage.scan [hours]
"""
from __future__ import annotations
import asyncio, json, time, sys, os, urllib.request, urllib.parse
from collections import defaultdict

import websockets

FEE_ROUNDTRIP = 0.002      # 0.2% taker×2
ALERT_GROSS   = 0.0025     # 0.25% gross = 0.05% net — прибыльно
DROP_TOP_N    = 15         # отбросить N самых ликвидных (HFT-территория)
MAX_SYMBOLS   = 50         # сколько альткоинов мониторить
DURATION_H    = float(sys.argv[1]) if len(sys.argv) > 1 else 10.0

TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT  = os.environ.get("TELEGRAM_CHAT_ID", "")

BINANCE_WS = "wss://stream.binance.com:9443/stream?streams="
BYBIT_WS   = "wss://stream.bybit.com/v5/public/spot"

# состояние
bn: dict[str, tuple[float, float]] = {}   # sym -> (bid, ask)
by: dict[str, tuple[float, float]] = {}
stats: dict[str, dict] = defaultdict(lambda: {
    "samples": 0, "max_gross": -9.0, "n_above_fee": 0, "n_alert": 0, "sum_gross_pos": 0.0,
})
SYMBOLS: list[str] = []
_t0 = time.time()


def _tg(text: str) -> None:
    if not TG_TOKEN or not TG_CHAT:
        return
    try:
        data = urllib.parse.urlencode({"chat_id": TG_CHAT, "text": text}).encode()
        urllib.request.urlopen(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", data, timeout=5
        )
    except Exception:
        pass


def _http_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=15) as r:
        return json.loads(r.read())


def build_universe() -> list[str]:
    """Пары, торгующиеся SPOT на обеих биржах, USDT, средней ликвидности."""
    # Binance: торгуемые USDT-пары + объём
    info = _http_json("https://api.binance.com/api/v3/exchangeInfo")
    bn_ok = {
        s["symbol"] for s in info["symbols"]
        if s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING"
        and s.get("isSpotTradingAllowed")
    }
    vol = _http_json("https://api.binance.com/api/v3/ticker/24hr")
    bn_vol = {v["symbol"]: float(v.get("quoteVolume", 0)) for v in vol if v["symbol"] in bn_ok}

    # Bybit spot
    byb = _http_json("https://api.bybit.com/v5/market/instruments-info?category=spot")
    by_ok = {
        it["symbol"] for it in byb["result"]["list"]
        if it.get("quoteCoin") == "USDT" and it.get("status") == "Trading"
    }

    common = bn_ok & by_ok
    # сортируем по объёму Binance убыв., отбрасываем топ-N (HFT), берём следующие MAX_SYMBOLS
    ranked = sorted(common, key=lambda s: bn_vol.get(s, 0), reverse=True)
    mid = ranked[DROP_TOP_N: DROP_TOP_N + MAX_SYMBOLS]
    return mid


def check(sym: str) -> None:
    if sym not in bn or sym not in by:
        return
    bbid, bask = bn[sym]
    ybid, yask = by[sym]
    g = None
    if bask < ybid:
        g = (ybid - bask) / bask
    elif yask < bbid:
        g = (bbid - yask) / yask
    if g is None:
        return
    st = stats[sym]
    st["samples"] += 1
    if g > st["max_gross"]:
        st["max_gross"] = g
    if g > FEE_ROUNDTRIP:
        st["n_above_fee"] += 1
        st["sum_gross_pos"] += g
    if g >= ALERT_GROSS:
        st["n_alert"] += 1


async def binance_ws() -> None:
    streams = "/".join(f"{s.lower()}@bookTicker" for s in SYMBOLS)
    url = BINANCE_WS + streams
    while True:
        try:
            async with websockets.connect(url, ping_interval=None, max_size=2**22) as ws:
                while True:
                    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=60))
                    d = m.get("data", m)
                    s = d.get("s")
                    if s:
                        bn[s] = (float(d["b"]), float(d["a"]))
                        check(s)
        except Exception:
            await asyncio.sleep(3)


async def bybit_ws() -> None:
    while True:
        try:
            async with websockets.connect(BYBIT_WS, ping_interval=None, max_size=2**22) as ws:
                # подписка батчами по 10
                for i in range(0, len(SYMBOLS), 10):
                    batch = SYMBOLS[i:i+10]
                    await ws.send(json.dumps({"op": "subscribe",
                                              "args": [f"orderbook.1.{s}" for s in batch]}))
                    await asyncio.sleep(0.2)
                while True:
                    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=60))
                    t = m.get("topic", "")
                    if not t.startswith("orderbook"):
                        continue
                    s = t.split(".")[-1]
                    d = m.get("data", {})
                    bids, asks = d.get("b", []), d.get("a", [])
                    if bids and asks:
                        by[s] = (float(bids[0][0]), float(asks[0][0]))
                        check(s)
        except Exception:
            await asyncio.sleep(3)


def dump_report(final: bool = False) -> str:
    rows = []
    for sym, st in stats.items():
        if st["samples"] == 0:
            continue
        avg_pos = (st["sum_gross_pos"] / st["n_above_fee"] * 100) if st["n_above_fee"] else 0
        rows.append({
            "symbol": sym,
            "max_gross_pct": round(st["max_gross"] * 100, 4),
            "max_net_pct": round((st["max_gross"] - FEE_ROUNDTRIP) * 100, 4),
            "n_above_fee": st["n_above_fee"],
            "n_alert": st["n_alert"],
            "avg_profitable_gross_pct": round(avg_pos, 4),
            "samples": st["samples"],
        })
    rows.sort(key=lambda r: r["n_alert"], reverse=True)
    elapsed_h = (time.time() - _t0) / 3600
    payload = {"elapsed_hours": round(elapsed_h, 2), "final": final,
               "symbols_tracked": len(SYMBOLS), "results": rows}
    with open("logs/scan_results.json", "w") as f:
        json.dump(payload, f, indent=2)

    winners = [r for r in rows if r["n_alert"] > 0]
    lines = [f"{'🏁 ФИНАЛ' if final else '⏱'} Скан {elapsed_h:.1f}ч | пар={len(SYMBOLS)}"]
    if winners:
        lines.append(f"Пары с прибыльными окнами (gross≥0.25%): {len(winners)}")
        for r in winners[:10]:
            lines.append(f"  {r['symbol']}: {r['n_alert']} окон, max net={r['max_net_pct']:+.3f}%")
    else:
        lines.append("Прибыльных окон (net>0) НЕТ ни у одной пары.")
    msg = "\n".join(lines)
    _tg(msg)
    return msg


async def hourly_reporter() -> None:
    while True:
        await asyncio.sleep(3600)
        print(dump_report(), flush=True)


async def main() -> None:
    global SYMBOLS
    SYMBOLS = build_universe()
    print(f"Сканирую {len(SYMBOLS)} пар {DURATION_H}ч: {SYMBOLS}", flush=True)
    _tg(f"🔍 Запущен скан альткоинов\nПар: {len(SYMBOLS)} (отброшены топ-{DROP_TOP_N} ликвидных)\nДлительность: {DURATION_H}ч\nИщу спред ≥0.25% (прибыльно после комиссий)")

    tasks = [asyncio.create_task(binance_ws()),
             asyncio.create_task(bybit_ws()),
             asyncio.create_task(hourly_reporter())]
    await asyncio.sleep(DURATION_H * 3600)
    for t in tasks:
        t.cancel()
    print(dump_report(final=True), flush=True)


if __name__ == "__main__":
    asyncio.run(main())

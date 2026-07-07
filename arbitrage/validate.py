"""Depth-aware валидатор кандидатов — честная проверка исполнимости.

Для каждой пары держит ПОЛНЫЙ стакан (Binance depth20 + Bybit orderbook.50, SPOT),
считает VWAP-цену исполнения на SIZE_USDT по обеим ногам → реальный net после комиссий.
Отсеивает «спред на пыли»: если глубины не хватает на SIZE — не считаем окном.

На старте проверяет статус ВЫВОДА монет на обеих биржах (ловушки = вывод закрыт).

Пишет logs/validate_results.json (снимок каждый час) + Telegram сводки.

Run: python -m arbitrage.validate [hours]
"""
from __future__ import annotations
import asyncio, json, time, sys, os, hmac, hashlib, urllib.request, urllib.parse
from collections import defaultdict
import websockets

CANDIDATES = ["HBARUSDT","SUIUSDT","TIAUSDT","BCHUSDT","PENDLEUSDT","CRVUSDT",
              "DYDXUSDT","ETHFIUSDT","XPLUSDT","JTOUSDT","MEGAUSDT","PENGUUSDT"]

FEE_ROUNDTRIP = 0.002
ALERT_NET     = 0.0005          # 0.05% net = прибыльно
SIZE_USDT     = 100.0
DURATION_H    = float(sys.argv[1]) if len(sys.argv) > 1 else 8.0

TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT  = os.environ.get("TELEGRAM_CHAT_ID", "")
BN_KEY   = os.environ.get("BINANCE_API_KEY", "")
BN_SEC   = os.environ.get("BINANCE_API_SECRET", "")
BY_KEY   = os.environ.get("BYBIT_API_KEY", "")
BY_SEC   = os.environ.get("BYBIT_API_SECRET", "")

BINANCE_WS = "wss://stream.binance.com:9443/stream?streams="
BYBIT_WS   = "wss://stream.bybit.com/v5/public/spot"

bn_book: dict[str, dict] = {s: {"bids": [], "asks": []} for s in CANDIDATES}
by_book: dict[str, dict] = {s: {"bids": {}, "asks": {}} for s in CANDIDATES}
stats: dict[str, dict] = defaultdict(lambda: {
    "samples": 0, "fillable_windows": 0, "max_real_net": -9.0, "thin_windows": 0,
})
withdraw_status: dict[str, dict] = {}
_t0 = time.time()


def _tg(text: str) -> None:
    if not TG_TOKEN or not TG_CHAT: return
    try:
        data = urllib.parse.urlencode({"chat_id": TG_CHAT, "text": text}).encode()
        urllib.request.urlopen(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", data, timeout=5)
    except Exception: pass


def vwap_fill(levels, size_usdt):
    """levels: [(price,qty)]. Возвращает (vwap, fillable)."""
    rem, cost, base = size_usdt, 0.0, 0.0
    for price, qty in levels:
        if price <= 0 or qty <= 0: continue
        take = min(rem, price * qty)
        base += take / price; cost += take; rem -= take
        if rem <= 1e-9: break
    if rem > 1e-9 or base <= 0: return 0.0, False
    return cost / base, True


# ── withdrawal status ────────────────────────────────────────────────────────
def check_withdrawals():
    coins = [s.replace("USDT", "") for s in CANDIDATES]
    # Binance
    try:
        ts = int(time.time()*1000)
        q = f"timestamp={ts}"
        sig = hmac.new(BN_SEC.encode(), q.encode(), hashlib.sha256).hexdigest()
        req = urllib.request.Request(
            f"https://api.binance.com/sapi/v1/capital/config/getall?{q}&signature={sig}",
            headers={"X-MBX-APIKEY": BN_KEY})
        data = json.loads(urllib.request.urlopen(req, timeout=15).read())
        bn_w = {c["coin"]: any(n.get("withdrawEnable") for n in c.get("networkList", []))
                for c in data if c["coin"] in coins}
    except Exception as e:
        bn_w = {}; print(f"Binance withdraw check failed: {e}", flush=True)
    # Bybit
    try:
        ts = str(int(time.time()*1000)); rw = "10000"
        pre = ts + BY_KEY + rw
        sig = hmac.new(BY_SEC.encode(), pre.encode(), hashlib.sha256).hexdigest()
        req = urllib.request.Request(
            "https://api.bybit.com/v5/asset/coin/query-info",
            headers={"X-BAPI-API-KEY": BY_KEY, "X-BAPI-SIGN": sig,
                     "X-BAPI-TIMESTAMP": ts, "X-BAPI-RECV-WINDOW": rw})
        data = json.loads(urllib.request.urlopen(req, timeout=15).read())
        rows = data.get("result", {}).get("rows", [])
        by_w = {r["coin"]: any(c.get("chainWithdraw") == "1" for c in r.get("chains", []))
                for r in rows if r["coin"] in coins}
    except Exception as e:
        by_w = {}; print(f"Bybit withdraw check failed: {e}", flush=True)

    for s in CANDIDATES:
        c = s.replace("USDT", "")
        withdraw_status[s] = {"binance": bn_w.get(c), "bybit": by_w.get(c)}


# ── spread check ──────────────────────────────────────────────────────────────
def check(sym):
    b = bn_book[sym]; y = by_book[sym]
    if not b["bids"] or not b["asks"] or not y["bids"] or not y["asks"]: return
    bbid, bask = b["bids"][0][0], b["asks"][0][0]
    ybids = sorted(y["bids"].items(), key=lambda x:-x[0])
    yasks = sorted(y["asks"].items(), key=lambda x:x[0])
    ybid, yask = ybids[0][0], yasks[0][0]

    if bask < ybid:      # buy binance, sell bybit
        buy_levels, sell_levels = b["asks"], ybids
    elif yask < bbid:    # buy bybit, sell binance
        buy_levels, sell_levels = yasks, b["bids"]
    else:
        return
    st = stats[sym]; st["samples"] += 1
    vbuy, ok_b = vwap_fill(buy_levels, SIZE_USDT)
    vsell, ok_s = vwap_fill(sell_levels, SIZE_USDT)
    if not (ok_b and ok_s):
        st["thin_windows"] += 1; return   # спред есть, но глубины на $100 нет = пыль
    real_net = (vsell - vbuy)/vbuy - FEE_ROUNDTRIP
    if real_net > st["max_real_net"]: st["max_real_net"] = real_net
    if real_net >= ALERT_NET: st["fillable_windows"] += 1


async def binance_ws():
    streams = "/".join(f"{s.lower()}@depth20@100ms" for s in CANDIDATES)
    while True:
        try:
            async with websockets.connect(BINANCE_WS+streams, ping_interval=None, max_size=2**22) as ws:
                while True:
                    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=60))
                    d = m.get("data", {}); s = m.get("stream","").split("@")[0].upper()
                    if s in bn_book and d.get("bids") and d.get("asks"):
                        bn_book[s]["bids"] = [(float(p),float(q)) for p,q in d["bids"]]
                        bn_book[s]["asks"] = [(float(p),float(q)) for p,q in d["asks"]]
                        check(s)
        except Exception: await asyncio.sleep(3)


async def bybit_ws():
    while True:
        try:
            async with websockets.connect(BYBIT_WS, ping_interval=None, max_size=2**22) as ws:
                for i in range(0, len(CANDIDATES), 10):
                    await ws.send(json.dumps({"op":"subscribe",
                        "args":[f"orderbook.50.{s}" for s in CANDIDATES[i:i+10]]}))
                    await asyncio.sleep(0.2)
                while True:
                    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=60))
                    t = m.get("topic","")
                    if not t.startswith("orderbook"): continue
                    s = t.split(".")[-1]; d = m.get("data",{}); ob = by_book[s]
                    if m.get("type")=="snapshot":
                        ob["bids"]={float(p):float(q) for p,q in d.get("b",[])}
                        ob["asks"]={float(p):float(q) for p,q in d.get("a",[])}
                    else:
                        for p,q in d.get("b",[]):
                            fp,fq=float(p),float(q); ob["bids"].pop(fp,None) if fq==0 else ob["bids"].__setitem__(fp,fq)
                        for p,q in d.get("a",[]):
                            fp,fq=float(p),float(q); ob["asks"].pop(fp,None) if fq==0 else ob["asks"].__setitem__(fp,fq)
                    check(s)
        except Exception: await asyncio.sleep(3)


def dump_report(final=False):
    rows=[]
    for s in CANDIDATES:
        st=stats[s]
        if st["samples"]==0 and st["thin_windows"]==0: continue
        w = withdraw_status.get(s, {})
        rows.append({"symbol":s,
            "fillable_windows":st["fillable_windows"],
            "max_real_net_pct":round(st["max_real_net"]*100,4) if st["max_real_net"]>-9 else None,
            "thin_windows":st["thin_windows"], "cross_samples":st["samples"],
            "withdraw_binance":w.get("binance"), "withdraw_bybit":w.get("bybit")})
    rows.sort(key=lambda r:r["fillable_windows"], reverse=True)
    el=(time.time()-_t0)/3600
    json.dump({"elapsed_hours":round(el,2),"final":final,"size_usdt":SIZE_USDT,"results":rows},
              open("logs/validate_results.json","w"), indent=2)
    lines=[f"{'🏁 ФИНАЛ' if final else '⏱'} Валидатор {el:.1f}ч | size=${SIZE_USDT:.0f}"]
    real=[r for r in rows if r["fillable_windows"]>0 and r["withdraw_binance"] and r["withdraw_bybit"]]
    if real:
        lines.append(f"РЕАЛЬНЫЕ пары (исполнимо на ${SIZE_USDT:.0f} + вывод открыт):")
        for r in real[:10]:
            lines.append(f"  {r['symbol']}: {r['fillable_windows']} окон, max net={r['max_real_net_pct']:+.3f}%")
    else:
        lines.append("Исполнимых на $100 пар с открытым выводом НЕТ.")
    _tg("\n".join(lines)); return "\n".join(lines)


async def hourly():
    while True:
        await asyncio.sleep(3600); print(dump_report(), flush=True)


async def main():
    print(f"Проверка вывода монет...", flush=True)
    check_withdrawals()
    for s in CANDIDATES:
        w=withdraw_status.get(s,{})
        print(f"  {s}: Binance_withdraw={w.get('binance')} Bybit_withdraw={w.get('bybit')}", flush=True)
    _tg(f"🔬 Валидатор глубины запущен\nКандидатов: {len(CANDIDATES)} | size=${SIZE_USDT:.0f} | {DURATION_H}ч\nПроверяю VWAP-исполнимость + статус вывода")
    tasks=[asyncio.create_task(binance_ws()),asyncio.create_task(bybit_ws()),asyncio.create_task(hourly())]
    await asyncio.sleep(DURATION_H*3600)
    for t in tasks: t.cancel()
    print(dump_report(final=True), flush=True)


if __name__ == "__main__":
    asyncio.run(main())

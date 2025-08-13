# services/markets.py
import asyncio, datetime
import httpx
from loguru import logger

COINGECKO_SIMPLE = "https://api.coingecko.com/api/v3/simple/price"
BYBIT_TICKERS = "https://api.bybit.com/v5/market/tickers"
BYBIT_KLINE = "https://api.bybit.com/v5/market/kline"

SYMBOLS = {
    "ETH": {"coingecko":"ethereum","bybit_symbol":"ETHUSDT","category":"spot"},
    "BTC": {"coingecko":"bitcoin","bybit_symbol":"BTCUSDT","category":"spot"},
}

def _levels_from_price(p: float) -> dict:
    if not isinstance(p,(int,float)) or p <= 0:
        return {"supports": [], "resistances": []}
    # Dynamic rounder
    step = 50 if p > 5000 else 25 if p > 1000 else 10
    supports = [p - 3*step, p - 2*step, p - step]
    resists = [p + step, p + 2*step, p + 3*step]
    return {"supports":[round(x/step)*step for x in supports],
            "resistances":[round(x/step)*step for x in resists]}

async def _bybit_price(asset: str) -> dict|None:
    meta = SYMBOLS[asset]
    params = {"category": meta["category"], "symbol": meta["bybit_symbol"]}
    async with httpx.AsyncClient(timeout=10) as cli:
        r = await cli.get(BYBIT_TICKERS, params=params)
        if r.status_code != 200:
            return None
        data = r.json().get("result",{}).get("list",[])
        if not data:
            return None
        row = data[0]
        price = float(row.get("lastPrice") or 0.0)
        chg = float(row.get("price24hPcnt") or 0.0) * 100.0
        high = float(row.get("highPrice") or 0.0)
        low = float(row.get("lowPrice") or 0.0)
        return {"price": price, "change_24h": chg, "high": high, "low": low}

async def _coingecko_price(asset: str) -> dict|None:
    meta = SYMBOLS[asset]
    params = {"ids": meta["coingecko"], "vs_currencies":"usd", "include_24hr_change":"true"}
    async with httpx.AsyncClient(timeout=10) as cli:
        r = await cli.get(COINGECKO_SIMPLE, params=params)
        if r.status_code != 200:
            return None
        obj = r.json().get(meta["coingecko"]) or {}
        price = float(obj.get("usd") or 0.0)
        chg = float(obj.get("usd_24h_change") or 0.0)
        return {"price": price, "change_24h": chg}

async def price(asset: str) -> dict:
    # Try Bybit, fallback to Coingecko
    for fn in (_bybit_price, _coingecko_price):
        try:
            out = await fn(asset)
            if out and out.get("price",0)>0:
                return out
        except Exception as e:
            logger.warning("price source failed {}: {}", fn.__name__, e)
    return {"price": None, "change_24h": None}

async def series_24h(asset: str) -> dict:
    meta = SYMBOLS[asset]
    params = {"category": meta["category"], "symbol": meta["bybit_symbol"], "interval":"60", "limit":"24"}
    series = []
    async with httpx.AsyncClient(timeout=15) as cli:
        try:
            r = await cli.get(BYBIT_KLINE, params=params)
            data = r.json().get("result",{}).get("list",[])
            # list of [start, open, high, low, close, volume, turnover]
            for row in data:
                close = float(row[4])
                ts = int(row[0])
                series.append([ts, close])
        except Exception as e:
            logger.warning("bybit kline failed: {}", e)
    return {"series": series}

async def snapshot(asset: str) -> dict:
    p = await price(asset)
    ser = await series_24h(asset)
    lv = _levels_from_price(p.get("price") or 0)
    return {"asset": asset, **p, **ser, "levels": lv}

async def pair_change(a: str, b: str) -> float:
    # approximate via 24h change
    A = await price(a)
    B = await price(b)
    ca, cb = A.get("change_24h"), B.get("change_24h")
    if isinstance(ca,(int,float)) and isinstance(cb,(int,float)):
        return ca - cb
    return 0.0

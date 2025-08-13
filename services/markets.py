
import os, asyncio
from typing import Dict, Any, List, Optional, Tuple
import httpx
from loguru import logger

COINGECKO_KEY = os.getenv("COINGECKO_KEY","").strip()
UA = "StarkRadarBot/0.20"
TIMEOUT = 10.0

async def _get_json(url: str, params: Dict[str,Any]=None, headers: Dict[str,str]=None) -> Optional[Dict]:
    headers = {"User-Agent": UA, **(headers or {})}
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        try:
            r = await client.get(url, params=params, headers=headers)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.warning("GET fail: %s %s", url, e); return None

async def price_bybit(symbol: str) -> Optional[float]:
    d = await _get_json("https://api.bybit.com/v5/market/tickers", params={"category":"spot","symbol":f"{symbol}USDT"})
    try: return float(d["result"]["list"][0]["lastPrice"])
    except Exception: return None

async def price_kraken(symbol: str) -> Optional[float]:
    pair = {"BTC":"XXBTZUSD","ETH":"XETHZUSD"}.get(symbol, symbol+"USD")
    d = await _get_json("https://api.kraken.com/0/public/Ticker", params={"pair":pair})
    try: return float(list(d["result"].values())[0]["c"][0])
    except Exception: return None

async def price_coinbase(symbol: str) -> Optional[float]:
    d = await _get_json(f"https://api.exchange.coinbase.com/products/{symbol}-USD/ticker")
    try: return float(d["price"])
    except Exception: return None

async def price_coingecko(symbol: str) -> Optional[float]:
    headers = {"x-cg-demo-api-key": COINGECKO_KEY} if COINGECKO_KEY else None
    d = await _get_json("https://api.coingecko.com/api/v3/simple/price",
                        params={"ids":"bitcoin,ethereum","vs_currencies":"usd"},
                        headers=headers)
    try: return float((d["bitcoin" if symbol=="BTC" else "ethereum"])["usd"])
    except Exception: return None

async def get_price(symbol: str) -> Optional[float]:
    for fn in (price_bybit, price_kraken, price_coinbase, price_coingecko):
        v = await fn(symbol)
        if v: return v
    return None

async def series_bybit(symbol: str):
    d = await _get_json("https://api.bybit.com/v5/market/kline",
                        params={"category":"linear","symbol":f"{symbol}USDT","interval":"60","limit":"24"})
    try:
        arr = d["result"]["list"]
        out = [(int(k[0]), float(k[4])) for k in arr]
        out.sort(key=lambda x:x[0]); return out
    except Exception: return None

async def series_kraken(symbol: str):
    pair = "XETHZUSD" if symbol=="ETH" else "XXBTZUSD"
    d = await _get_json("https://api.kraken.com/0/public/OHLC", params={"pair":pair,"interval":60})
    try:
        arr = list(d["result"].values())[0]
        return [(int(c[0])*1000, float(c[4])) for c in arr][-24:]
    except Exception: return None

async def series_coinbase(symbol: str):
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        try:
            r = await client.get(f"https://api.exchange.coinbase.com/products/{symbol}-USD/candles",
                                 params={"granularity":3600})
            r.raise_for_status()
            arr = r.json()[:24]
            out = [(int(c[0])*1000, float(c[4])) for c in arr]
            out.sort(key=lambda x:x[0]); return out
        except Exception: return None

async def get_series_24h(symbol: str):
    for fn in (series_bybit, series_kraken, series_coinbase):
        s = await fn(symbol)
        if s: return s
    return None

def levels_from_series(series):
    if not series: return {}
    vals = [p for _,p in series]
    hi, lo = max(vals), min(vals)
    mid = (hi+lo)/2
    return {"S1":round(lo,2),"S2":round((lo*0.985+mid*0.015),2),"R1":round(hi,2),
            "R2":round((hi*1.015-mid*0.015),2),"MID":round(mid,2)}

async def snapshot(symbol: str):
    px, series = await asyncio.gather(get_price(symbol), get_series_24h(symbol))
    res = {"symbol":symbol, "price":px, "series":series or []}
    if series:
        res["levels"] = levels_from_series(series)
        res["hilo"] = {"high": max(p for _,p in series), "low": min(p for _,p in series)}
        res["change_24h_pct"] = ((series[-1][1]/series[0][1])-1.0)*100.0
    return res

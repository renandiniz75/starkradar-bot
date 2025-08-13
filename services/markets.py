# services/markets.py
# Foco: fontes públicas com fallback + retry (Coinbase -> Kraken -> Bitstamp)
import asyncio, time
from typing import Dict, Any, List, Optional, Tuple
from loguru import logger
import httpx
from datetime import datetime, timezone

HTTP_TIMEOUT = 8.0
RETRIES = 2
BACKOFF = 0.8

# --------- Utils HTTP ---------
async def _get_json(url: str, params: Dict[str, Any] = None, headers: Dict[str, str] = None) -> Any:
    params = params or {}
    headers = headers or {"User-Agent": "StarkRadarBot/0.17"}
    for attempt in range(RETRIES + 1):
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
                r = await client.get(url, params=params, headers=headers)
                r.raise_for_status()
                return r.json()
        except Exception as e:
            if attempt < RETRIES:
                await asyncio.sleep(BACKOFF * (attempt + 1))
                continue
            logger.warning("GET fail: %s %s", url, str(e))
            raise

# --------- Sources ---------
async def _coinbase_ticker(symbol: str) -> Optional[float]:
    # symbol: "ETH-USD" | "BTC-USD"
    url = f"https://api.exchange.coinbase.com/products/{symbol}/ticker"
    try:
        data = await _get_json(url)
        return float(data.get("price"))
    except Exception:
        return None

async def _coinbase_candles(symbol: str, granularity: int = 3600, limit: int = 24) -> List[Tuple[int, float]]:
    # returns [(ts_sec, close), ...] ascending
    url = f"https://api.exchange.coinbase.com/products/{symbol}/candles"
    params = {"granularity": granularity}
    try:
        data = await _get_json(url, params=params)
        # Coinbase returns [[time, low, high, open, close, volume], ...] (time in seconds), DESC order
        rows = sorted(data, key=lambda x: x[0])
        if limit:
            rows = rows[-limit:]
        series = [(int(r[0]), float(r[4])) for r in rows]
        return series
    except Exception:
        return []

async def _kraken_ticker(pair: str) -> Optional[float]:
    # pair: "ETHUSD" | "BTCUSD"
    url = "https://api.kraken.com/0/public/Ticker"
    params = {"pair": pair}
    try:
        data = await _get_json(url, params=params)
        result = data.get("result") or {}
        key = next(iter(result.keys()), None)
        if not key:
            return None
        last = result[key]["c"][0]
        return float(last)
    except Exception:
        return None

async def _kraken_ohlc(pair: str, interval: int = 60) -> List[Tuple[int, float]]:
    # returns ascending [(ts, close)]
    url = "https://api.kraken.com/0/public/OHLC"
    params = {"pair": pair, "interval": interval}
    try:
        data = await _get_json(url, params=params)
        result = data.get("result") or {}
        # ignore "last"
        keys = [k for k in result.keys() if k != "last"]
        if not keys:
            return []
        arr = result[keys[0]]
        series = [(int(row[0]), float(row[4])) for row in arr][-24:]
        return series
    except Exception:
        return []

async def _bitstamp_ticker(symbol: str) -> Optional[float]:
    # symbol: "ethusd" | "btcusd"
    url = f"https://www.bitstamp.net/api/v2/ticker/{symbol}/"
    try:
        data = await _get_json(url)
        return float(data.get("last"))
    except Exception:
        return None

async def _bitstamp_ohlc(symbol: str, step: int = 3600, limit: int = 24) -> List[Tuple[int, float]]:
    url = f"https://www.bitstamp.net/api/v2/ohlc/{symbol}/"
    params = {"step": step, "limit": limit}
    try:
        data = await _get_json(url, params=params)
        data = (data or {}).get("data", {}).get("ohlc", [])
        series = [(int(x["timestamp"]), float(x["close"])) for x in data]
        return series
    except Exception:
        return []

# --------- Public API ---------
_SYMBOLS = {
    "ETH": {"cb": "ETH-USD", "kr": "ETHUSD", "bs": "ethusd"},
    "BTC": {"cb": "BTC-USD", "kr": "BTCUSD", "bs": "btcusd"},
}

async def price_now(asset: str) -> Optional[float]:
    m = _SYMBOLS.get(asset.upper())
    if not m:
        return None
    # Coinbase
    px = await _coinbase_ticker(m["cb"])
    if px is not None:
        return px
    # Kraken
    px = await _kraken_ticker(m["kr"])
    if px is not None:
        return px
    # Bitstamp
    px = await _bitstamp_ticker(m["bs"])
    return px

async def series_24h(asset: str) -> List[Tuple[int, float]]:
    m = _SYMBOLS.get(asset.upper())
    if not m: return []
    series = await _coinbase_candles(m["cb"], 3600, 24)
    if series: return series
    series = await _kraken_ohlc(m["kr"], 60)
    if series: return series
    series = await _bitstamp_ohlc(m["bs"], 3600, 24)
    return series or []

def _hi_lo_change(series: List[Tuple[int,float]]) -> Dict[str, Optional[float]]:
    if not series:
        return {"high": None, "low": None, "change_24h": None}
    closes = [p for _, p in series]
    high = max(closes)
    low = min(closes)
    change = None
    if len(closes) >= 2:
        change = (closes[-1] / closes[0] - 1) * 100.0
    return {"high": high, "low": low, "change_24h": change}

def _levels(closes: List[float]) -> Dict[str, str]:
    if not closes:
        return {"S": "- / -", "R": "- / -"}
    last = closes[-1]
    # níveis simples: % bands + extremos
    s1 = min(last * 0.97, min(closes))
    s2 = min(last * 0.95, sorted(closes)[max(0, int(len(closes)*0.1)-1)])
    r1 = max(last * 1.03, max(closes))
    r2 = max(last * 1.05, sorted(closes)[int(len(closes)*0.9)])
    def fmt(x): 
        try: return f"{x:,.2f}"
        except: return "-"
    return {"S": f"{fmt(s2)} / {fmt(s1)}", "R": f"{fmt(r1)} / {fmt(r2)}"}

async def snapshot(asset: str) -> Dict[str, Any]:
    ser = await series_24h(asset)
    px = await price_now(asset)
    hlc = _hi_lo_change(ser)
    closes = [p for _, p in ser]
    lv = _levels(closes)
    return {
        "asset": asset,
        "price": px,
        "high_24h": hlc["high"],
        "low_24h": hlc["low"],
        "change_24h_pct": hlc["change_24h"],
        "series_24h": ser,
        "levels": lv,
        "ts": int(time.time())
    }

async def health_check() -> Dict[str, Any]:
    ok_eth = await price_now("ETH") is not None
    ok_btc = await price_now("BTC") is not None
    return {"eth": ok_eth, "btc": ok_btc}

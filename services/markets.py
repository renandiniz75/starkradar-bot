
import os
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Tuple, Optional

import aiohttp
from loguru import logger

# Simple in-memory cache
_CACHE: Dict[str, Tuple[datetime, Any]] = {}
_CACHE_TTL = int(os.getenv("CACHE_TTL_S", "30"))  # seconds
_HTTP_TIMEOUT = aiohttp.ClientTimeout(total=15)

COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY", "")

_session: Optional[aiohttp.ClientSession] = None

def _now() -> datetime:
    return datetime.now(timezone.utc)

def cache_stats() -> Dict[str, Any]:
    return {
        "entries": len(_CACHE),
        "ttl_s": _CACHE_TTL
    }

async def _session_get() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession(timeout=_HTTP_TIMEOUT, trust_env=True)
    return _session

async def close():
    global _session
    if _session and not _session.closed:
        await _session.close()

def _cache_get(key: str):
    data = _CACHE.get(key)
    if not data:
        return None
    ts, val = data
    if (_now() - ts).total_seconds() > _CACHE_TTL:
        return None
    return val

def _cache_set(key: str, val: Any):
    _CACHE[key] = (_now(), val)

async def _get_json(url: str, params: Dict[str, Any] | None = None, headers: Dict[str, str] | None = None) -> Any:
    ses = await _session_get()
    # Simple retry
    for i in range(3):
        try:
            async with ses.get(url, params=params, headers=headers) as r:
                r.raise_for_status()
                return await r.json()
        except Exception as e:
            if i == 2:
                logger.warning(f"GET fail: {url} {e}")
                raise
            await asyncio.sleep(0.5 * (i + 1))

# ---------------- Sources ----------------

async def price_from_coinbase(symbol: str) -> Optional[float]:
    # symbol: "BTC" or "ETH"
    try:
        data = await _get_json(f"https://api.coinbase.com/v2/prices/{symbol}-USD/spot")
        return float(data["data"]["amount"])
    except Exception:
        return None

async def price_from_bitstamp(symbol: str) -> Optional[float]:
    pair = f"{symbol.lower()}usd"
    try:
        data = await _get_json(f"https://www.bitstamp.net/api/v2/ticker/{pair}")
        return float(data["last"])
    except Exception:
        return None

async def price_from_coingecko_simple(symbols: List[str]) -> Dict[str, Any]:
    # symbols in ["bitcoin","ethereum"]
    params = {
        "ids": ",".join(symbols),
        "vs_currencies": "usd,btc",
        "include_24hr_change": "true"
    }
    headers = {}
    if COINGECKO_API_KEY:
        headers["x-cg-pro-api-key"] = COINGECKO_API_KEY
    try:
        data = await _get_json("https://api.coingecko.com/api/v3/simple/price", params=params, headers=headers)
        return data
    except Exception:
        return {}

async def series_from_bybit(symbol: str) -> List[Tuple[int, float]]:
    # symbol: "ETHUSDT" or "BTCUSDT", returns [(ts, price), ...] last 24 hours hourly
    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": "60",
        "limit": "24"
    }
    try:
        data = await _get_json("https://api.bybit.com/v5/market/kline", params=params)
        arr = data.get("result", {}).get("list", [])
        out = []
        for row in arr:
            # bybit returns [startTime, open, high, low, close, volume, turnover]
            ts_ms = int(row[0])
            close = float(row[4])
            out.append((ts_ms, close))
        out.sort(key=lambda x: x[0])
        return out
    except Exception as e:
        logger.warning("bybit kline failed: %s", e)
        return []

# ---------------- Public API ----------------

async def get_prices() -> Dict[str, Any]:
    """Return dict with USD prices for BTC, ETH + 24h change and eth_btc ratio."""
    key = "prices"
    cached = _cache_get(key)
    if cached:
        return cached

    # Try CoinGecko first (if not rate-limited), then fallbacks
    cg = await price_from_coingecko_simple(["bitcoin", "ethereum"])
    btc = cg.get("bitcoin", {}).get("usd")
    eth = cg.get("ethereum", {}).get("usd")
    btc_ch = cg.get("bitcoin", {}).get("usd_24h_change")
    eth_ch = cg.get("ethereum", {}).get("usd_24h_change")

    if btc is None:
        btc = await price_from_coinbase("BTC") or await price_from_bitstamp("BTC")
    if eth is None:
        eth = await price_from_coinbase("ETH") or await price_from_bitstamp("ETH")

    eth_btc = None
    if eth and btc:
        eth_btc = eth / btc

    out = {
        "btc": btc,
        "eth": eth,
        "btc_24h_change": btc_ch,
        "eth_24h_change": eth_ch,
        "eth_btc": eth_btc
    }
    _cache_set(key, out)
    return out

async def series_24h(asset: str) -> List[Tuple[int, float]]:
    """Hourly series for last 24h. asset: 'BTC' or 'ETH'."""
    key = f"series:{asset}"
    cached = _cache_get(key)
    if cached:
        return cached

    symbol = f"{asset}USDT"
    data = await series_from_bybit(symbol)

    # Fallback: build synthetic series with last price if needed
    if not data:
        prices = await get_prices()
        last = prices.get(asset.lower())
        if last:
            now = int(_now().timestamp()) * 1000
            data = [(now - i*3600_000, float(last)) for i in reversed(range(24))]

    _cache_set(key, data)
    return data

async def snapshot(asset: str) -> Dict[str, Any]:
    """Return current price, intraday high/low derived from series, simple levels."""
    prices = await get_prices()
    px = prices.get(asset.lower())
    series = await series_24h(asset)

    intraday_high = max([p for _, p in series], default=px or 0.0)
    intraday_low = min([p for _, p in series], default=px or 0.0)

    # Simple supports/resistances based on quartiles between low/high
    s1 = intraday_low + (intraday_high - intraday_low) * 0.25
    s2 = intraday_low
    r1 = intraday_low + (intraday_high - intraday_low) * 0.75
    r2 = intraday_high

    return {
        "asset": asset,
        "price": px,
        "series_24h": series,
        "intraday_high": intraday_high,
        "intraday_low": intraday_low,
        "supports": [round(s2, 2), round(s1, 2)],
        "resistances": [round(r1, 2), round(r2, 2)]
    }

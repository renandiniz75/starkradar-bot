# services/markets.py
# Coleta de dados com múltiplos fallbacks + cache TTL

import time
import math
from typing import Dict, Any, List, Optional, Tuple

import httpx
from loguru import logger

CACHE: Dict[str, Tuple[float, Any]] = {}
TTL_SHORT = 30       # segundos (preços)
TTL_MEDIUM = 90      # candles
USER_AGENT = "StarkRadar/0.17 (+https://starkradar-bot)"

HTTP_TIMEOUT = 8.0
client = httpx.AsyncClient(timeout=HTTP_TIMEOUT, headers={"User-Agent": USER_AGENT})

# ----------------------------- utils cache ----------------------------- #

def _cache_get(key: str, ttl: int) -> Optional[Any]:
    now = time.time()
    it = CACHE.get(key)
    if not it:
        return None
    ts, val = it
    if now - ts <= ttl:
        return val
    return None

def _cache_set(key: str, val: Any):
    CACHE[key] = (time.time(), val)

# ----------------------------- helpers ----------------------------- #

def _pair(asset: str) -> str:
    asset = asset.upper()
    if asset not in ("ETH", "BTC"):
        raise ValueError("asset inválido")
    return f"{asset}USDT"

def _pct(a: float, b: float) -> float:
    if b == 0 or b is None or a is None:
        return 0.0
    return (a - b) / b * 100.0

# ----------------------------- fontes ----------------------------- #

async def _bybit_price(symbol: str) -> Optional[float]:
    # Ticker último — Bybit
    # Público, mas pode 403. Trate como opcional.
    url = f"https://api.bybit.com/v5/market/tickers?category=linear&symbol={symbol}"
    try:
        r = await client.get(url)
        r.raise_for_status()
        data = r.json()
        items = (data.get("result") or {}).get("list") or []
        if items:
            return float(items[0]["lastPrice"])
    except Exception as e:
        logger.warning("Bybit price fail: {}", e)
    return None

async def _kraken_price(asset: str) -> Optional[float]:
    # Kraken: usa par USD (não USDT). Ajuste simples.
    pair = "ETHUSD" if asset == "ETH" else "XBTUSD"
    url = f"https://api.kraken.com/0/public/Ticker?pair={pair}"
    try:
        r = await client.get(url)
        r.raise_for_status()
        data = r.json()
        res = data.get("result") or {}
        # chave dinâmica (ex: XETHZUSD ou XXBTZUSD)
        key = next(iter(res.keys()))
        return float(res[key]["c"][0])
    except Exception as e:
        logger.warning("Kraken price fail: {}", e)
    return None

async def _coinbase_price(asset: str) -> Optional[float]:
    pair = f"{asset}-USD"
    url = f"https://api.exchange.coinbase.com/products/{pair}/ticker"
    try:
        r = await client.get(url)
        r.raise_for_status()
        data = r.json()
        return float(data["price"])
    except Exception as e:
        logger.warning("Coinbase price fail: {}", e)
    return None

async def _coingecko_price(asset: str) -> Optional[float]:
    ids = {"ETH": "ethereum", "BTC": "bitcoin"}[asset]
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=usd"
    try:
        r = await client.get(url)
        r.raise_for_status()
        data = r.json()
        return float(data[ids]["usd"])
    except Exception as e:
        logger.warning("CoinGecko price fail: {}", e)
    return None

async def get_spot_price(asset: str) -> Optional[float]:
    """
    Preço spot com fallback agressivo.
    """
    cache_key = f"px:{asset}"
    v = _cache_get(cache_key, TTL_SHORT)
    if v is not None:
        return v

    symbol = _pair(asset)
    for fn in (
        lambda: _bybit_price(symbol),
        lambda: _kraken_price(asset),
        lambda: _coinbase_price(asset),
        lambda: _coingecko_price(asset),
    ):
        px = await fn()
        if px:
            _cache_set(cache_key, px)
            return px
    return None

# ---- candles 24h (lista de (ts_ms, open, high, low, close)) ---- #

async def _bybit_candles(symbol: str, interval="60", limit=24) -> Optional[List[List[float]]]:
    url = (
        "https://api.bybit.com/v5/market/kline"
        f"?category=linear&symbol={symbol}&interval={interval}&limit={limit}"
    )
    try:
        r = await client.get(url)
        r.raise_for_status()
        js = r.json()
        rows = (js.get("result") or {}).get("list") or []
        # Bybit retorna [start, open, high, low, close, volume, turnover]
        rows = sorted(rows, key=lambda x: int(x[0]))
        out = []
        for it in rows:
            out.append([
                int(it[0]), float(it[1]), float(it[2]), float(it[3]), float(it[4])
            ])
        return out
    except Exception as e:
        logger.warning("bybit kline fail: {}", e)
    return None

async def _coinbase_candles(asset: str, granularity=3600, limit=24) -> Optional[List[List[float]]]:
    # Coinbase retorna [[time, low, high, open, close, volume], ...]
    pair = f"{asset}-USD"
    url = f"https://api.exchange.coinbase.com/products/{pair}/candles?granularity={granularity}"
    try:
        r = await client.get(url)
        r.raise_for_status()
        data = r.json()
        rows = sorted(data, key=lambda x: x[0])[-limit:]
        out = []
        for t, low, high, opn, cls, _vol in rows:
            out.append([int(t)*1000, float(opn), float(high), float(low), float(cls)])
        return out
    except Exception as e:
        logger.warning("coinbase candles fail: {}", e)
    return None

async def _coingecko_candles(asset: str, days=1) -> Optional[List[List[float]]]:
    # market_chart: prices [[ts_ms, price], ...] — reconstruímos OHLC aproximado em 1h
    ids = {"ETH": "ethereum", "BTC": "bitcoin"}[asset]
    url = f"https://api.coingecko.com/api/v3/coins/{ids}/market_chart?vs_currency=usd&days={days}"
    try:
        r = await client.get(url)
        r.raise_for_status()
        data = r.json()
        prices = data.get("prices") or []
        # bucketiza por hora
        buckets: Dict[int, List[float]] = {}
        for ts, px in prices:
            h = (ts // 3600000) * 3600000
            buckets.setdefault(h, []).append(px)
        out = []
        for h in sorted(buckets.keys())[-24:]:
            arr = buckets[h]
            opn = arr[0]; cls = arr[-1]; high = max(arr); low = min(arr)
            out.append([h, opn, high, low, cls])
        return out
    except Exception as e:
        logger.warning("coingecko candles fail: {}", e)
    return None

async def series_24h(asset: str) -> Optional[List[List[float]]]:
    cache_key = f"kl:24h:{asset}"
    v = _cache_get(cache_key, TTL_MEDIUM)
    if v is not None:
        return v
    symbol = _pair(asset)
    for fn in (
        lambda: _bybit_candles(symbol, "60", 24),
        lambda: _coinbase_candles(asset, 3600, 24),
        lambda: _coingecko_candles(asset, 1),
    ):
        rows = await fn()
        if rows:
            _cache_set(cache_key, rows)
            return rows
    return None

# ---- funding & open interest (Bybit) ---- #

async def get_bybit_funding(symbol: str) -> Optional[float]:
    url = f"https://api.bybit.com/v5/market/funding/history?category=linear&symbol={symbol}&limit=1"
    try:
        r = await client.get(url)
        r.raise_for_status()
        js = r.json()
        lst = (js.get("result") or {}).get("list") or []
        if lst:
            return float(lst[0]["fundingRate"])
    except Exception as e:
        logger.warning("funding fail: {}", e)
    return None

async def get_bybit_oi(symbol: str) -> Optional[float]:
    # Open interest notional (USD) – agregamos 5min último
    url = f"https://api.bybit.com/v5/market/open-interest?category=linear&symbol={symbol}&interval=5min&limit=1"
    try:
        r = await client.get(url)
        r.raise_for_status()
        js = r.json()
        lst = (js.get("result") or {}).get("list") or []
        if lst:
            return float(lst[0]["openInterestUsd"])
    except Exception as e:
        logger.warning("OI fail: {}", e)
    return None

# ---- snapshot consolidado ---- #

async def snapshot(asset: str) -> Dict[str, Any]:
    """
    Retorna dicionário com:
    price, chg24, high, low, open, close, series (closes),
    funding, oi, rel (ETH/BTC), levels {S1,S2,R1,R2}, rsi, ma
    """
    asset = asset.upper()
    px = await get_spot_price(asset)
    candles = await series_24h(asset)

    open_, close_, high_, low_ = None, None, None, None
    chg24 = None
    series = []

    if candles:
        open_ = candles[0][1]
        high_ = max(c[2] for c in candles)
        low_  = min(c[3] for c in candles)
        close_= candles[-1][4]
        series = [c[4] for c in candles]
        if open_ and close_:
            chg24 = _pct(close_, open_)

    price = px or close_  # garante um preço

    # Funding & OI (Bybit – se falhar, “None”)
    symbol = _pair(asset)
    funding = await get_bybit_funding(symbol)
    oi = await get_bybit_oi(symbol)

    # ETH/BTC relativo
    rel = None
    if asset in ("ETH", "BTC"):
        pe = await get_spot_price("ETH")
        pb = await get_spot_price("BTC")
        if pe and pb:
            rel = pe / pb

    # S/R (pivots clássicos)
    levels = {}
    if high_ and low_ and close_:
        P = (high_ + low_ + close_) / 3.0
        R1 = 2*P - low_
        S1 = 2*P - high_
        R2 = P + (high_ - low_)
        S2 = P - (high_ - low_)
        levels = {"S1": S1, "S2": S2, "R1": R1, "R2": R2}

    # RSI (14) e MAs simples (7, 21)
    rsi = None
    ma = {}
    if len(series) >= 21:
        deltas = [series[i] - series[i-1] for i in range(1, len(series))]
        ups = [max(d, 0) for d in deltas]
        downs = [abs(min(d, 0)) for d in deltas]
        n = 14
        avg_up = sum(ups[-n:]) / n
        avg_dn = sum(downs[-n:]) / n
        rs = (avg_up / avg_dn) if avg_dn != 0 else math.inf
        rsi = 100 - (100 / (1 + rs))
        ma["ma7"] = sum(series[-7:]) / 7
        ma["ma21"] = sum(series[-21:]) / 21

    return {
        "asset": asset,
        "price": price,
        "chg24": chg24,
        "open": open_, "close": close_, "high": high_, "low": low_,
        "series": series,
        "funding": funding,
        "open_interest_usd": oi,
        "rel_eth_btc": rel,
        "levels": levels,
        "rsi": rsi,
        "ma": ma,
    }

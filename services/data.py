from __future__ import annotations
import asyncio, time
from typing import Any, Dict, Tuple, Optional
from loguru import logger
from . import config

# CCXT async
if config.USE_CCXT:
    import ccxt.async_support as ccxt_async

CACHE: Dict[str, Tuple[float, Any]] = {}
TTL = 25  # seconds for price cache

class ExchangePool:
    """Context manager that opens/closes Bybit and Binance safely."""
    def __init__(self):
        self.bybit = None
        self.binance = None
    async def __aenter__(self):
        if config.USE_CCXT:
            self.bybit = ccxt_async.bybit({'enableRateLimit': True})
            self.binance = ccxt_async.binance({'enableRateLimit': True})
        return self
    async def __aexit__(self, exc_type, exc, tb):
        for ex in (self.bybit, self.binance):
            if ex:
                try:
                    await ex.close()
                except Exception:
                    pass

async def _fetch_ticker(ex, symbol: str) -> Optional[dict]:
    try:
        return await ex.fetch_ticker(symbol)
    except Exception as e:
        logger.warning(f"fetch_ticker fail {getattr(ex,'id','?')} {symbol}: {e}")
        return None

async def prices_snapshot() -> dict:
    """Return dict with ETHUSDT, BTCUSDT tickers and ETH/BTC ratio."""
    key = "prices"
    now = time.time()
    if key in CACHE and now - CACHE[key][0] < TTL:
        return CACHE[key][1]
    out = {"ETHUSDT": None, "BTCUSDT": None, "ETHBTC": None}
    if not config.USE_CCXT:
        CACHE[key] = (now, out); return out
    async with ExchangePool() as ep:
        eth = await _fetch_ticker(ep.bybit, "ETH/USDT") or await _fetch_ticker(ep.binance, "ETH/USDT")
        btc = await _fetch_ticker(ep.bybit, "BTC/USDT") or await _fetch_ticker(ep.binance, "BTC/USDT")
        ethbtc = await _fetch_ticker(ep.bybit, "ETH/BTC") or await _fetch_ticker(ep.binance, "ETH/BTC")
    def norm(t):
        if not t: return None
        return {
            "symbol": t.get("symbol"),
            "last": t.get("last") or t.get("close"),
            "high": t.get("high"),
            "low": t.get("low"),
            "percentage": t.get("percentage"),
            "info": t.get("info", {})
        }
    out["ETHUSDT"] = norm(eth)
    out["BTCUSDT"] = norm(btc)
    out["ETHBTC"] = norm(ethbtc)
    CACHE[key] = (now, out)
    return out

async def ohlcv_lookback(symbol: str, since_ms: int, timeframe: str = "1h") -> list:
    """Return OHLCV candles from Bybit or Binance, safe close."""
    if not config.USE_CCXT: return []
    async with ExchangePool() as ep:
        for ex in (ep.bybit, ep.binance):
            try:
                data = await ex.fetch_ohlcv(symbol.replace("USDT", "/USDT"), timeframe=timeframe, since=since_ms, limit=400)
                if data: return data
            except Exception as e:
                logger.warning(f"ohlcv fail {getattr(ex,'id','?')} {symbol}: {e}")
    return []

async def funding_and_oi_snapshot() -> dict:
    """Best-effort funding & OI; if not available, returns None fields."""
    snap = {"ETH": {"funding": None, "oi": None}, "BTC": {"funding": None, "oi": None}}
    if not config.USE_CCXT: return snap
    async with ExchangePool() as ep:
        # Many exchanges require futures markets/keys; keep graceful
        for asset in ("ETH", "BTC"):
            try:
                # Placeholder: attempt funding rate via binance delivery if available
                # Not all CCXT builds expose straightforward calls without market loading
                await ep.binance.load_markets()
                market = f"{asset}/USDT"
                fr = None
                try:
                    # Some ccxt versions have fetch_funding_rate for futures symbols; wrap
                    fr = await ep.binance.fetch_funding_rate(market + ":USDT", params={})
                except Exception:
                    fr = None
                snap[asset]["funding"] = (fr or {}).get("fundingRate") if isinstance(fr, dict) else None
            except Exception:
                pass
        # OI often needs API keys/derivatives endpoint; leave None if unavailable
    return snap

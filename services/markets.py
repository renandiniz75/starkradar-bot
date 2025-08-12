from __future__ import annotations
import asyncio, os, math
from loguru import logger
import httpx
import ccxt.async_support as ccxt

# Módulo responsável por: preço spot, OHLCV curto (para sparkline), funding e OI (best-effort).

DEFAULT_EXCH = os.getenv("EXCH", "binance")
TIMEOUT = 15

async def _with_client(exchange_id: str):
    ex = getattr(ccxt, exchange_id)({"enableRateLimit": True})
    return ex

async def get_price_24h_series(symbol: str = "BTC/USDT"):
    ex_id = DEFAULT_EXCH
    ex = await _with_client(ex_id)
    try:
        ohlcv = await ex.fetch_ohlcv(symbol, timeframe="1h", limit=24)
        ticker = await ex.fetch_ticker(symbol)
        price = ticker.get("last")
        series = [c[4] for c in ohlcv] if ohlcv else []
        return {"price": price, "series_24h": series}
    finally:
        await ex.close()

async def get_levels(asset: str):
    # Placeholder simples: níveis redondos próximos ao preço
    pair = "ETH/USDT" if asset.upper() == "ETH" else "BTC/USDT"
    data = await get_price_24h_series(pair)
    px = data.get("price")
    if not px:
        return data | {"supports": [], "resists": []}
    step = 50 if asset.upper() == "ETH" else 1000
    supports = [round(px - step, 2), round(px - 2*step, 2)]
    resists = [round(px + step, 2), round(px + 2*step, 2)]
    return data | {"supports": supports, "resists": resists}

async def get_funding_oi(symbol: str = "BTC/USDT"):
    # Melhores esforços via binance futures se disponível
    try:
        ex = await _with_client("binanceusdm")
        try:
            # funding
            markets = await ex.load_markets()
            m = markets.get(symbol.replace("/USDT", "USDT"))
            # Binance USDM usa par "BTCUSDT"
            inst = (symbol.split("/")[0] + "USDT")
            fr = await ex.fapiPublicGetPremiumIndex(params={"symbol": inst})
            funding = float(fr[0].get("lastFundingRate", 0.0))
            # OI aproximado
            oi = await ex.fapiPublicGetOpenInterest(params={"symbol": inst})
            oi_val = float(oi[0].get("openInterest", 0.0))
            return {"funding": funding, "open_interest": oi_val}
        finally:
            await ex.close()
    except Exception:
        return {"funding": None, "open_interest": None}

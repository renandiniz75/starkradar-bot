# services/markets.py
# Data providers with graceful fallbacks (CoinGecko primary; Coinbase and Binance public as best-effort)
from loguru import logger
import httpx, asyncio, time

HEADERS = {"User-Agent": "starkradar-bot/0.18"}

COINGECKO = "https://api.coingecko.com/api/v3"
COINBASE = "https://api.coinbase.com/v2"

# Map assets to coingecko ids and symbols for convenience
ASSETS = {
    "ETH": {"cg_id": "ethereum", "symbol": "ETHUSDT"},
    "BTC": {"cg_id": "bitcoin",  "symbol": "BTCUSDT"},
}

async def _get_json(client: httpx.AsyncClient, url: str, params=None):
    try:
        r = await client.get(url, params=params, headers=HEADERS, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.warning("GET fail: {} {}", url, e)
        return None

async def spot_snapshot(asset: str):
    """Return dict: price, change_24h, high_24h, low_24h, series (list of floats)."""
    a = ASSETS.get(asset.upper())
    if not a:
        return None
    cg_id = a["cg_id"]
    async with httpx.AsyncClient() as client:
        # 1) price + change + hi/lo
        mkt = await _get_json(client, f"{COINGECKO}/coins/{cg_id}", params={"localization":"false","tickers":"false","market_data":"true","community_data":"false","developer_data":"false","sparkline":"false"})
        price = change = high = low = None
        if mkt and mkt.get("market_data"):
            md = mkt["market_data"]
            price = md["current_price"].get("usd")
            change = md["price_change_percentage_24h"]
            high = md["high_24h"].get("usd")
            low  = md["low_24h"].get("usd")

        # 2) intraday series (last 24h hourly)
        chart = await _get_json(client, f"{COINGECKO}/coins/{cg_id}/market_chart", params={"vs_currency":"usd","days":"1","interval":"hourly"})
        series = []
        if chart and chart.get("prices"):
            # prices is [[ts, price], ...]
            series = [float(p[1]) for p in chart["prices"] if isinstance(p, list) and len(p) >= 2]

        return {"asset": asset, "price": price, "change_24h": change, "high_24h": high, "low_24h": low, "series_24h": series}

async def pair_snapshot():
    """ETH/BTC pair via CoinGecko (relative performance)."""
    async with httpx.AsyncClient() as client:
        data = await _get_json(client, f"{COINGECKO}/simple/price", params={"ids":"ethereum,bitcoin","vs_currencies":"btc,usd","include_24hr_change":"true"})
        if not data: 
            return None
        eth_btc = data.get("ethereum", {}).get("btc")
        eth_usd = data.get("ethereum", {}).get("usd")
        btc_usd = data.get("bitcoin",  {}).get("usd")
        eth_chg = data.get("ethereum", {}).get("usd_24h_change")
        btc_chg = data.get("bitcoin",  {}).get("usd_24h_change")
        return {"eth_btc": eth_btc, "eth_usd": eth_usd, "btc_usd": btc_usd, "eth_chg": eth_chg, "btc_chg": btc_chg}

async def funding_open_interest_stub():
    """Placeholder returning None. Can be expanded with Glassnode/Coinglass APIs (need keys)."""
    return {"funding_rate": None, "open_interest": None}

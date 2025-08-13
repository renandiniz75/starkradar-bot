# v0.17.1-hotfix • services/markets.py
import math, statistics, datetime as dt, io
from typing import Dict, List, Tuple, Optional
import httpx
from loguru import logger
import matplotlib.pyplot as plt

HTTP_TIMEOUT = 8.0
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

# ---- Helpers HTTP

async def _get_json(url: str, params: dict = None, headers: dict = None) -> dict:
    headers = {"User-Agent": UA, **(headers or {})}
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, headers=headers) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        return r.json()

# ---- BYBIT (primário) -------------------------------------------------------

async def bybit_ticker(symbol: str, category: str = "linear") -> Optional[float]:
    # Preço last
    url = "https://api.bybit.com/v5/market/tickers"
    params = {"category": category, "symbol": symbol}
    try:
        data = await _get_json(url, params)
        lst = ((data or {}).get("result") or {}).get("list") or []
        if not lst: return None
        return float(lst[0]["lastPrice"])
    except Exception as e:
        logger.warning("bybit ticker fail: {}", e)
        return None

async def bybit_kline_24h(symbol: str, category: str = "linear") -> Optional[List[Tuple[int,float]]]:
    # 24 velas 1h
    url = "https://api.bybit.com/v5/market/kline"
    params = {"category": category, "symbol": symbol, "interval": "60", "limit": 24}
    try:
        data = await _get_json(url, params)
        lst = ((data or {}).get("result") or {}).get("list") or []
        series = []
        for row in lst:
            # Bybit: [start, open, high, low, close, volume, turnover]
            ts, _o, _h, _l, c = int(row[0]), float(row[1]), float(row[2]), float(row[3]), float(row[4])
            series.append((ts, c))
        series.sort(key=lambda x: x[0])
        return series
    except Exception as e:
        logger.warning("bybit kline fail: {}", e)
        return None

# ---- COINGECKO (fallback) ---------------------------------------------------

ID_MAP = {"ETH": "ethereum", "BTC": "bitcoin"}

async def gecko_price(asset: str) -> Tuple[Optional[float], Optional[float]]:
    """retorna (price_usd, change_24h_pct)"""
    cid = ID_MAP.get(asset)
    if not cid: return (None, None)
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {"ids": cid, "vs_currencies": "usd", "include_24hr_change": "true"}
    try:
        j = await _get_json(url, params)
        val = j.get(cid) or {}
        return (float(val.get("usd")), float(val.get("usd_24h_change")))
    except Exception as e:
        logger.warning("gecko price fail: {}", e); return (None, None)

async def gecko_ohlc_24h(asset: str) -> Optional[List[Tuple[int,float]]]:
    cid = ID_MAP.get(asset)
    if not cid: return None
    # OHLC 5-min candles for 1 day → pega fechamento
    url = f"https://api.coingecko.com/api/v3/coins/{cid}/ohlc"
    params = {"vs_currency": "usd", "days": 1}
    try:
        j = await _get_json(url, params)
        # [timestamp, open, high, low, close]
        series = [(int(row[0]), float(row[4])) for row in j]
        series.sort(key=lambda x: x[0])
        # reduz para ~24 pontos (cada 60min) para gráficos leves
        if len(series) > 0:
            bucket = max(1, len(series)//24)
            series = series[::bucket]
        return series
    except Exception as e:
        logger.warning("gecko ohlc fail: {}", e); return None

# ---- Camada de domínio ------------------------------------------------------

async def series_24h(asset: str) -> List[Tuple[int, float]]:
    # tenta Bybit → fallback Gecko
    sym = "ETHUSDT" if asset == "ETH" else "BTCUSDT"
    s = await bybit_kline_24h(sym, "linear")
    if not s:
        s = await gecko_ohlc_24h(asset)
    return s or []

def calc_levels(series: List[Tuple[int,float]]) -> Dict[str, float]:
    if not series: return {"low": None, "high": None, "s1": None, "s2": None, "r1": None, "r2": None}
    prices = [p for _, p in series]
    lo, hi = min(prices), max(prices)
    # percentis para níveis
    s2 = sorted(prices)[max(0, int(0.1*len(prices))-1)]
    s1 = sorted(prices)[max(0, int(0.25*len(prices))-1)]
    r1 = sorted(prices)[min(len(prices)-1, int(0.75*len(prices)))]
    r2 = sorted(prices)[min(len(prices)-1, int(0.9*len(prices)))]
    return {"low": lo, "high": hi, "s1": s1, "s2": s2, "r1": r1, "r2": r2}

async def price_24h(asset: str) -> Tuple[Optional[float], Optional[float]]:
    # tenta Bybit preço → fallback Gecko
    sym = "ETHUSDT" if asset == "ETH" else "BTCUSDT"
    px = await bybit_ticker(sym, "linear")
    chg = None
    if px is None:
        px, chg = await gecko_price(asset)
    else:
        # se veio o preço da Bybit, tentamos variação via Gecko só para o % (leve)
        _, chg = await gecko_price(asset)
    return (px, chg)

async def snapshot(asset: str) -> Dict:
    ser = await series_24h(asset)
    lv = calc_levels(ser)
    px, chg = await price_24h(asset)
    return {
        "asset": asset,
        "price": px,
        "change_24h": chg,  # %
        "series": ser,
        "levels": lv,
        "ts": dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
    }

# ---- Render gráfico ---------------------------------------------------------

def spark_png(series: List[Tuple[int,float]], title: str) -> bytes:
    fig = plt.figure(figsize=(6, 2.3), dpi=160)
    ax = fig.add_subplot(111)
    if series:
        xs = [dt.datetime.utcfromtimestamp(ts/1000 if ts>1e12 else ts) for ts,_ in series]
        ys = [y for _,y in series]
        ax.plot(xs, ys, linewidth=2)
        ax.fill_between(xs, ys, min(ys), alpha=0.1)
    ax.set_title(title, fontsize=9)
    ax.grid(True, alpha=0.2)
    for spine in ["top","right"]:
        ax.spines[spine].set_visible(False)
    fig.autofmt_xdate(rotation=0)
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    return buf.getvalue()

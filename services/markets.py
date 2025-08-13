# v0.17.2-hotfixB • services/markets.py
import math, statistics, datetime as dt, io, asyncio, random
from typing import Dict, List, Tuple, Optional
import httpx
from loguru import logger
import matplotlib.pyplot as plt

HTTP_TIMEOUT = 8.0
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

ID_MAP = {"ETH": "ethereum", "BTC": "bitcoin"}

# ---------------- HTTP helpers com retry ----------------

async def _get_json(url: str, params: dict = None, headers: dict = None, retries: int = 2) -> dict:
    headers = {"User-Agent": UA, **(headers or {})}
    last_err = None
    for i in range(retries + 1):
        try:
            timeout = httpx.Timeout(HTTP_TIMEOUT)
            async with httpx.AsyncClient(timeout=timeout, headers=headers) as client:
                r = await client.get(url, params=params)
                r.raise_for_status()
                return r.json()
        except Exception as e:
            last_err = e
            await asyncio.sleep(0.5 + 0.5 * i)  # backoff curto
    logger.warning("GET fail {} params={} err={}", url, params, last_err)
    return {}

# ---------------- BYBIT (primário) ----------------

async def bybit_ticker(symbol: str, category: str = "linear") -> Optional[float]:
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
    url = "https://api.bybit.com/v5/market/kline"
    params = {"category": category, "symbol": symbol, "interval": "60", "limit": 24}
    try:
        data = await _get_json(url, params)
        lst = ((data or {}).get("result") or {}).get("list") or []
        if not lst: return None
        series = []
        for row in lst:
            ts, close_ = int(row[0]), float(row[4])
            series.append((ts, close_))
        series.sort(key=lambda x: x[0])
        return series
    except Exception as e:
        logger.warning("bybit kline fail: {}", e)
        return None

# ---------------- COINGECKO (fallback A) ----------------

async def gecko_price(asset: str) -> Tuple[Optional[float], Optional[float]]:
    cid = ID_MAP.get(asset)
    if not cid: return (None, None)
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {"ids": cid, "vs_currencies": "usd", "include_24hr_change": "true"}
    j = await _get_json(url, params)
    val = j.get(cid) or {}
    try:
        return (float(val.get("usd")) if val.get("usd") is not None else None,
                float(val.get("usd_24h_change")) if val.get("usd_24h_change") is not None else None)
    except:
        return (None, None)

async def gecko_ohlc_24h(asset: str) -> Optional[List[Tuple[int,float]]]:
    """OHLC oficial; pode rate‑limitar. Retorna close por candle."""
    cid = ID_MAP.get(asset)
    if not cid: return None
    url = f"https://api.coingecko.com/api/v3/coins/{cid}/ohlc"
    params = {"vs_currency": "usd", "days": 1}
    j = await _get_json(url, params)
    if not isinstance(j, list) or not j:
        return None
    series = [(int(row[0]), float(row[4])) for row in j if isinstance(row, list) and len(row) >= 5]
    series.sort(key=lambda x: x[0])
    # downsample ~24 pontos
    if len(series) > 24:
        step = max(1, len(series)//24)
        series = series[::step]
    return series

# ---------------- COINGECKO (fallback B) ----------------

async def gecko_chart_close_24h(asset: str) -> Optional[List[Tuple[int,float]]]:
    """Usa market_chart (prices) como 2º fallback; muito resiliente."""
    cid = ID_MAP.get(asset)
    if not cid: return None
    url = f"https://api.coingecko.com/api/v3/coins/{cid}/market_chart"
    params = {"vs_currency": "usd", "days": 1, "interval": "hourly"}
    j = await _get_json(url, params)
    prices = (j or {}).get("prices") or []
    if not prices: return None
    series = [(int(ts), float(px)) for ts, px in prices]
    series.sort(key=lambda x: x[0])
    return series

# ---------------- Camada de domínio ----------------

async def series_24h(asset: str) -> List[Tuple[int, float]]:
    sym = "ETHUSDT" if asset == "ETH" else "BTCUSDT"
    # 1) Bybit
    s = await bybit_kline_24h(sym, "linear")
    # 2) Gecko OHLC
    if not s:
        s = await gecko_ohlc_24h(asset)
    # 3) Gecko market_chart
    if not s:
        s = await gecko_chart_close_24h(asset)
    return s or []

def calc_levels(series: List[Tuple[int,float]]) -> Dict[str, float]:
    if not series:
        return {"low": None, "high": None, "s1": None, "s2": None, "r1": None, "r2": None}
    prices = [p for _, p in series]
    lo, hi = min(prices), max(prices)
    q = sorted(prices)
    n = len(q) - 1
    def qidx(p): 
        return q[min(n, max(0, int(p*n)))]
    return {"low": lo, "high": hi, "s1": qidx(0.25), "s2": qidx(0.1), "r1": qidx(0.75), "r2": qidx(0.9)}

async def price_24h(asset: str) -> Tuple[Optional[float], Optional[float]]:
    sym = "ETHUSDT" if asset == "ETH" else "BTCUSDT"
    px = await bybit_ticker(sym, "linear")
    chg = None
    if px is None:
        px, chg = await gecko_price(asset)
    else:
        _, chg = await gecko_price(asset)  # só para % 24h
    return (px, chg)

async def snapshot(asset: str) -> Dict:
    ser = await series_24h(asset)
    lv = calc_levels(ser)
    px, chg = await price_24h(asset)
    ts = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    # Se a série vier vazia, desenha flat com o próprio preço (evita gráfico em branco)
    if not ser and px:
        now_ms = int(dt.datetime.utcnow().timestamp()*1000)
        ser = [(now_ms - 23*3600_000 + i*3600_000, float(px)) for i in range(24)]
    return {"asset": asset, "price": px, "change_24h": chg, "series": ser, "levels": lv, "ts": ts}

# ---------------- Render gráfico ----------------

def spark_png(series: List[Tuple[int,float]], title: str) -> bytes:
    fig = plt.figure(figsize=(6, 2.3), dpi=160)
    ax = fig.add_subplot(111)
    if series:
        xs = [dt.datetime.utcfromtimestamp(ts/1000 if ts>1e12 else ts) for ts,_ in series]
        ys = [y for _,y in series]
        ax.plot(xs, ys, linewidth=2)
        base = min(ys) * 0.995
        ax.fill_between(xs, ys, base, alpha=0.12)
    ax.set_title(title, fontsize=9)
    ax.grid(True, alpha=0.25)
    for spine in ["top","right"]:
        ax.spines[spine].set_visible(False)
    fig.autofmt_xdate(rotation=0)
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    return buf.getvalue()

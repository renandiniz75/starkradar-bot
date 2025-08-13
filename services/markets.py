# services/markets.py
# v0.17.3 – Fonte única: Bybit REST v5 (kline/markPrice)
# Notas:
# - Evita CCXT para preços intraday (Binance 451).
# - Headers com User-Agent para não cair em proteção.
# - Fecha sessões httpx corretamente.

from __future__ import annotations
import math, statistics, time
from datetime import datetime, timezone
import httpx
from loguru import logger

BYBIT_BASE = "https://api.bybit.com"
UA = "Mozilla/5.0 (X11; Linux x86_64) StarkRadarBot/0.17"

# Mapa de símbolos padronizados -> símbolo Bybit linear perp
SYMBOL_MAP = {
    "ETH": "ETHUSDT",
    "BTC": "BTCUSDT",
}

def _now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

async def _get_json(client: httpx.AsyncClient, url: str, params: dict) -> dict | None:
    try:
        r = await client.get(url, params=params, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.warning(f"bybit GET fail: {e}")
        return None

async def series_24h(asset: str) -> dict:
    """
    Retorna:
      {
        'asset': 'ETH',
        'price': 4599.12,
        'high': 4680.0,
        'low': 4310.5,
        'series_24h': [(ts_ms, close), ... 24 pts],
        'levels': {'S': [..], 'R': [..]},
        'ts': 'YYYY-MM-DD HH:MM UTC'
      }
    """
    symbol = SYMBOL_MAP.get(asset.upper())
    if not symbol:
        return {"asset": asset, "price": None, "high": None, "low": None, "series_24h": [], "levels": {"S":[],"R":[]}, "ts": _now_utc_str()}

    headers = {"User-Agent": UA, "Accept": "application/json"}
    async with httpx.AsyncClient(headers=headers, base_url=BYBIT_BASE) as client:
        # Kline 1h, últimos 24 candles
        k = await _get_json(
            client,
            "/v5/market/kline",
            {
                "category": "linear",
                "symbol": symbol,
                "interval": "60",
                "limit": 24,
            },
        )
        if not k or k.get("retCode") != 0:
            logger.warning("bybit kline failed: %s", k)
            return {"asset": asset, "price": None, "high": None, "low": None, "series_24h": [], "levels": {"S":[],"R":[]}, "ts": _now_utc_str()}

        rows = k["result"]["list"]  # cada linha: [start, open, high, low, close, volume, turnover]
        # Bybit devolve em ordem decrescente; vamos ordenar crescente por tempo
        rows.sort(key=lambda r: int(r[0]))
        series = [(int(r[0]), float(r[4])) for r in rows]
        highs  = [float(r[2]) for r in rows]
        lows   = [float(r[3]) for r in rows]
        price  = series[-1][1] if series else None
        hi24   = max(highs) if highs else None
        lo24   = min(lows) if lows else None

        # Níveis simples: pivot clássico a partir do último candle fechado
        if rows:
            o,h,l,c = map(float, rows[-1][1:5])
            pp = (h + l + c) / 3.0
            r1 = 2*pp - l
            s1 = 2*pp - h
            r2 = pp + (h - l)
            s2 = pp - (h - l)
            levels = {"S": [round(s1,2), round(s2,2)], "R": [round(r1,2), round(r2,2)]}
        else:
            levels = {"S": [], "R": []}

        return {
            "asset": asset,
            "price": price,
            "high": hi24,
            "low": lo24,
            "series_24h": series,
            "levels": levels,
            "ts": _now_utc_str(),
        }

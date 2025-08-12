from __future__ import annotations
from typing import Tuple, List, Optional
import math, time
from . import data as data_svc

def _safe_levels(high: Optional[float], low: Optional[float]) -> Tuple[List[float], List[float]]:
    if high is None or low is None or not math.isfinite(high) or not math.isfinite(low):
        return [], []
    mid = (high + low) / 2.0
    span = max(1.0, (high - low) / 4.0)
    step = 50.0 if high > 50000 else (10.0 if high > 5000 else 5.0)
    def r(x, step): return round(x / step) * step
    sups = [r(low, step), r(mid - span/2, step)]
    ress = [r(mid + span/2, step), r(high, step)]
    uniq_s = sorted(set(sups))
    uniq_r = sorted(set(ress))
    return uniq_s, uniq_r

async def dynamic_levels_48h() -> dict:
    now_ms = int(time.time() * 1000)
    since = now_ms - 48 * 3600 * 1000
    candles_eth = await data_svc.ohlcv_lookback("ETHUSDT", since, "1h")
    candles_btc = await data_svc.ohlcv_lookback("BTCUSDT", since, "1h")
    def hi_lo(cs):
        if not cs: return (None, None)
        highs = [c[2] for c in cs if c and len(c) >= 5]
        lows  = [c[3] for c in cs if c and len(c) >= 5]
        if not highs or not lows: return (None, None)
        return (max(highs), min(lows))
    hi_eth, lo_eth = hi_lo(candles_eth)
    hi_btc, lo_btc = hi_lo(candles_btc)
    eth_s, eth_r = _safe_levels(hi_eth, lo_eth)
    btc_s, btc_r = _safe_levels(hi_btc, lo_btc)
    return {"ETH": {"S": eth_s, "R": eth_r}, "BTC": {"S": btc_s, "R": btc_r}}

def synth_summary(prices: dict, lvls: dict, fr_oi: dict) -> str:
    e = prices.get("ETHUSDT") or {}
    b = prices.get("BTCUSDT") or {}
    eb = prices.get("ETHBTC") or {}
    eth = e.get("last"); btc = b.get("last")
    parts = []
    parts.append(f"ETH ${eth:,.2f}" if isinstance(eth,(int,float)) else "ETH —")
    parts.append(f"BTC ${btc:,.2f}" if isinstance(btc,(int,float)) else "BTC —")
    if isinstance(eb.get("last"), (int,float)):
        parts.append(f"ETH/BTC {eb['last']:.5f}")
    eth_s = ", ".join([f"{x:,.0f}" for x in lvls.get('ETH',{}).get('S',[])])
    eth_r = ", ".join([f"{x:,.0f}" for x in lvls.get('ETH',{}).get('R',[])])
    btc_s = ", ".join([f"{x:,.0f}" for x in lvls.get('BTC',{}).get('S',[])])
    btc_r = ", ".join([f"{x:,.0f}" for x in lvls.get('BTC',{}).get('R',[])])
    parts.append(f"NÍVEIS ETH S:{eth_s or '-'} | R:{eth_r or '-'}")
    parts.append(f"NÍVEIS BTC S:{btc_s or '-'} | R:{btc_r or '-'}")
    return "\n".join(parts)

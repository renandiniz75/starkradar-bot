from __future__ import annotations
import os, base64, datetime as dt
from typing import Any, Dict, Optional, List
import httpx
from loguru import logger
from . import config, data as data_svc, analysis, charts, db, news as news_svc

TELEGRAM_API = "https://api.telegram.org"

async def tg_send_text(chat_id: int, text: str, parse_mode: str = "HTML"):
    if not config.BOT_TOKEN: 
        raise RuntimeError("BOT_TOKEN missing")
    async with httpx.AsyncClient(timeout=15) as client:
        url = f"{TELEGRAM_API}/bot{config.BOT_TOKEN}/sendMessage"
        payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode, "disable_web_page_preview": True}
        r = await client.post(url, json=payload)
        r.raise_for_status()

async def tg_send_photo(chat_id: int, png_bytes: bytes, caption: Optional[str] = None):
    if not config.BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN missing")
    async with httpx.AsyncClient(timeout=20) as client:
        url = f"{TELEGRAM_API}/bot{config.BOT_TOKEN}/sendPhoto"
        files = {"photo": ("spark.png", png_bytes, "image/png")}
        data = {"chat_id": str(chat_id)}
        if caption: data["caption"] = caption
        r = await client.post(url, data=data, files=files)
        r.raise_for_status()

async def set_webhook(host_url: str) -> Dict[str, Any]:
    if not config.BOT_TOKEN:
        return {"ok": False, "error": "BOT_TOKEN missing"}
    async with httpx.AsyncClient(timeout=15) as client:
        url = f"{TELEGRAM_API}/bot{config.BOT_TOKEN}/setWebhook"
        target = f"{host_url.rstrip('/')}/webhook"
        r = await client.post(url, json={"url": target, "drop_pending_updates": True})
        return r.json()

def _format_sources(rows: List[Any]) -> str:
    out = []
    for r in rows:
        ts = r["ts"].strftime("%H:%M") if r["ts"] else "--:--"
        title = (r["title"] or "")[:120]
        host = (r["source"] or "").replace("www.","")
        url = r["url"] or ""
        out.append(f"• {ts} {host} — {title}")
    return "\n".join(out) if out else "—"

async def build_pulse_text() -> str:
    prices = await data_svc.prices_snapshot()
    lvls = await analysis.dynamic_levels_48h()
    fr_oi = await data_svc.funding_and_oi_snapshot()
    summary = analysis.synth_summary(prices, lvls, fr_oi)
    rows = await db.get_recent_news(hours=12, limit=6) if config.DATABASE_URL else []
    sources = _format_sources(rows)
    now = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    return (
        f"🧭 <b>Pulse</b> — {now}\n"
        f"{summary}\n\n"
        f"<b>FONTES (12h)</b>:\n{sources}"
    )

async def build_asset_text(asset: str) -> str:
    prices = await data_svc.prices_snapshot()
    lvls = await analysis.dynamic_levels_48h()
    t = prices.get(f"{asset}USDT") or {}
    px = t.get("last")
    lv = lvls.get(asset, {})
    s = ", ".join([f"{x:,.0f}" for x in lv.get("S", [])]) or "-"
    r = ", ".join([f"{x:,.0f}" for x in lv.get("R", [])]) or "-"
    now = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    return f"{asset} ${px:,.2f if isinstance(px,(int,float)) else '-'}\nNíveis S:{s} | R:{r}\n{now}"

async def handle_start(chat_id: int):
    # Try a tiny sparkline using ETH 24h candles (fallback empty)
    prices = await data_svc.prices_snapshot()
    series = []
    # Use last/high/low to craft a fake short series if ohlcv not available quickly
    e = prices.get("ETHUSDT") or {}
    base = e.get("last") or 0.0
    series = [max(0.0, base*(0.995 + i*0.0005)) for i in range(40)]
    img = charts.make_sparkline_png(series)
    await tg_send_photo(chat_id, img, caption="Bem-vindo! Use /pulse, /eth, /btc, /panel.")

async def handle_pulse(chat_id: int):
    text = await build_pulse_text()
    await tg_send_text(chat_id, text)

async def handle_asset(chat_id: int, asset: str):
    text = await build_asset_text(asset)
    await tg_send_text(chat_id, text)

async def handle_panel(chat_id: int):
    await handle_pulse(chat_id)

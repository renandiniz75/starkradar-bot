from __future__ import annotations
import os, io, base64, asyncio
from datetime import datetime, timezone
from loguru import logger
import httpx
from utils import fmt_price, utcnow_iso, spark_png
from .markets import get_levels, get_funding_oi
from .news import fetch_feed_items, summarize_items

BOT_TOKEN = os.getenv("BOT_TOKEN")

async def build_asset_text(asset: str):
    info = await get_levels(asset)
    px = info.get("price")
    s = ", ".join(fmt_price(v) for v in info.get("supports", [])) or "-"
    r = ", ".join(fmt_price(v) for v in info.get("resists", [])) or "-"
    now = utcnow_iso()
    return f"{asset} {fmt_price(px)}\nNíveis S:{s} | R:{r}\n{now}"

async def handle_asset(chat_id: int, asset: str):
    txt = await build_asset_text(asset)
    await send_message(chat_id, txt)

async def handle_pulse(chat_id: int):
    # ETH
    eth = await get_levels("ETH")
    btc = await get_levels("BTC")
    eth_fr_oi = await get_funding_oi("ETH/USDT")
    btc_fr_oi = await get_funding_oi("BTC/USDT")
    items = await fetch_feed_items()
    news_txt = summarize_items(items)

    def k(pi): return fmt_price(pi.get("price"))
    def lv(x): 
        return f"S:{', '.join(fmt_price(v) for v in x.get('supports',[])) or '-'} | R:{', '.join(fmt_price(v) for v in x.get('resists',[])) or '-'}"

    txt = (
        f"⚾ Pulse — {utcnow_iso()}\n"
        f"ETH {k(eth)} — funding {eth_fr_oi['funding'] or '-'} | OI {eth_fr_oi['open_interest'] or '-'}\n"
        f"BTC {k(btc)} — funding {btc_fr_oi['funding'] or '-'} | OI {btc_fr_oi['open_interest'] or '-'}\n"
        f"NÍVEIS ETH {lv(eth)}\n"
        f"NÍVEIS BTC {lv(btc)}\n\n"
        f"FONTES (12h):\n{news_txt}"
    )
    await send_message(chat_id, txt)

async def handle_panel(chat_id: int):
    # Monta um PNG simples com as séries 24h
    from .markets import get_price_24h_series
    eth = await get_price_24h_series("ETH/USDT")
    btc = await get_price_24h_series("BTC/USDT")
    import matplotlib.pyplot as plt
    import io
    fig, ax = plt.subplots(figsize=(6,3), dpi=120)
    ax.plot(eth.get("series_24h", []), label="ETH 24h")
    ax.plot(btc.get("series_24h", []), label="BTC 24h")
    ax.legend(loc="upper left")
    ax.set_title("Painel 24h")
    ax.grid(True, alpha=.2)
    buf = io.BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png")
    plt.close(fig); buf.seek(0)
    await send_photo(chat_id, buf.getvalue())

# --- Telegram helpers -------------------------------------------------

async def send_message(chat_id: int, text: str):
    if not BOT_TOKEN: 
        logger.error("BOT_TOKEN ausente")
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=15) as client:
        await client.post(url, json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True})

async def send_photo(chat_id: int, png_bytes: bytes, caption: str|None=None):
    if not BOT_TOKEN: 
        logger.error("BOT_TOKEN ausente")
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
    files = {"photo": ("panel.png", png_bytes, "image/png")}
    data = {"chat_id": chat_id}
    if caption: data["caption"] = caption
    async with httpx.AsyncClient(timeout=20) as client:
        await client.post(url, data=data, files=files)

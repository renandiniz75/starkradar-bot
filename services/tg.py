# services/tg.py
# v0.17.3 – Corrige formatação quando preço não veio, e manda gráfico se houver série.

from __future__ import annotations
from io import BytesIO
from datetime import datetime, timezone
from loguru import logger
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from services.markets import series_24h

def _fmt_price(px):
    return "-" if px is None else f"{px:,.2f}"

async def build_asset_text(asset: str) -> tuple[str, bytes | None]:
    data = await series_24h(asset)
    px, hi, lo = data["price"], data["high"], data["low"]
    lev = data["levels"]; ts = data["ts"]

    text = (
        f"{asset} ${_fmt_price(px)}\n"
        f"H: {_fmt_price(hi)} | L: {_fmt_price(lo)}\n"
        f"Níveis S:{', '.join(map(str, lev.get('S', [])) or ['-'])} | "
        f"R:{', '.join(map(str, lev.get('R', [])) or ['-'])}\n"
        f"{ts}"
    )

    # Sparkline PNG (se tiver série)
    png_bytes = None
    series = data.get("series_24h") or []
    if series:
        xs = [i for i,_ in enumerate(series)]
        ys = [p for _,p in series]
        fig = plt.figure(figsize=(6,2.2), dpi=150)
        ax = fig.add_axes([0.06,0.2,0.9,0.7])
        ax.plot(xs, ys, linewidth=2)
        ax.set_xticks([]); ax.set_yticks([])
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_visible(False)
        ax.spines['bottom'].set_visible(False)
        buf = BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight")
        plt.close(fig)
        png_bytes = buf.getvalue()

    return text, png_bytes

# Telegram helpers
from os import getenv
import httpx

BOT_TOKEN = getenv("BOT_TOKEN")
API = f"https://api.telegram.org/bot{BOT_TOKEN}"

async def send_message(chat_id: int, text: str, reply_to: int | None=None):
    async with httpx.AsyncClient(timeout=15) as client:
        await client.post(f"{API}/sendMessage", json={
            "chat_id": chat_id, "text": text, "parse_mode": "HTML",
            **({"reply_parameters": {"message_id": reply_to}} if reply_to else {})
        })

async def send_photo(chat_id: int, caption: str, png_bytes: bytes):
    files = {"photo": ("chart.png", png_bytes, "image/png")}
    data  = {"chat_id": chat_id, "caption": caption}
    async with httpx.AsyncClient(timeout=20) as client:
        await client.post(f"{API}/sendPhoto", data=data, files=files)

async def handle_asset(chat_id: int, asset: str):
    text, png = await build_asset_text(asset)
    if png:
        await send_photo(chat_id, text, png)
    else:
        await send_message(chat_id, text)

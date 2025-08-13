# v0.17.1-hotfix • services/tg.py
import io, math
from typing import Tuple
from loguru import logger
import httpx
from . import markets

API = "https://api.telegram.org"

async def _post(token: str, method: str, data: dict, files=None):
    url = f"{API}/bot{token}/{method}"
    timeout = httpx.Timeout(10.0)
    async with httpx.AsyncClient(timeout=timeout) as c:
        r = await c.post(url, data=data, files=files)
        r.raise_for_status()
        return r.json()

async def send_text(token: str, chat_id: int, text: str, parse_mode: str|None=None):
    data = {"chat_id": chat_id, "text": text}
    if parse_mode: data["parse_mode"] = parse_mode
    try:
        await _post(token, "sendMessage", data)
    except Exception as e:
        logger.warning("send_text fail: {}", e)

async def send_png(token: str, chat_id: int, caption: str, png_bytes: bytes):
    files = {"photo": ("chart.png", io.BytesIO(png_bytes), "image/png")}
    data = {"chat_id": chat_id, "caption": caption}
    try:
        await _post(token, "sendPhoto", data, files=files)
    except Exception as e:
        logger.warning("send_png fail: {}", e)

def _fmt_money(v):
    return "-" if v is None else f"${v:,.2f}"

def _fmt_pct(v):
    return "-" if v is None else f"{v:+.2f}%"

def _levels_text(lv):
    s = f"Níveis S:{_fmt_money(lv.get('s1'))}, {_fmt_money(lv.get('s2'))} | R:{_fmt_money(lv.get('r1'))}, {_fmt_money(lv.get('r2'))}"
    return s

# ----------------- handlers -----------------

async def handle_asset(token: str, chat_id: int, asset: str):
    snap = await markets.snapshot(asset)  # nunca None
    price = _fmt_money(snap["price"])
    chg = _fmt_pct(snap["change_24h"])
    lv = _levels_text(snap["levels"])
    title = f"{asset} 24h"
    png = markets.spark_png(snap["series"], title)
    caption = f"{asset} {price} ({chg} 24h)\n{lv}\n{snap['ts']}"
    await send_png(token, chat_id, caption, png)

async def handle_pulse(token: str, chat_id: int):
    eth = await markets.snapshot("ETH")
    btc = await markets.snapshot("BTC")
    rel = None
    if eth["price"] and btc["price"]:
        rel = eth["price"]/btc["price"]
    text = (
        f"Pulse 🕒 {eth['ts']}\n"
        f"ETH { _fmt_money(eth['price']) } ({ _fmt_pct(eth['change_24h'])} 24h)\n"
        f"BTC { _fmt_money(btc['price']) } ({ _fmt_pct(btc['change_24h'])} 24h)\n"
        + (f"ETH/BTC {rel:.5f}\n" if rel else "")
        + "Análise: foco nos gatilhos de rompimento e gestão de risco."
    )
    await send_text(token, chat_id, text)

async def handle_panel(token: str, chat_id: int):
    await handle_asset(token, chat_id, "ETH")
    await handle_asset(token, chat_id, "BTC")

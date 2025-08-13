# services/tg.py
# Lado Telegram: montagem de mensagens e envio

import io
import os
from typing import Dict, Any
from loguru import logger
import httpx

from . import markets
from .analysis import asset_analysis, portfolio_hint
from .images import sparkline_png

TELEGRAM_BASE = "https://api.telegram.org"

def _fmt(v, n=2, prefix="$"):
    if v is None:
        return "-"
    return f"{prefix}{v:,.{n}f}"

def _fmt_pct(v, n=2):
    if v is None:
        return "-"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.{n}f}%"

async def send_text(bot_token: str, chat_id: int, text: str, disable_web_page_preview: bool=True):
    url = f"{TELEGRAM_BASE}/bot{bot_token}/sendMessage"
    async with httpx.AsyncClient(timeout=10.0) as c:
        await c.post(url, data={
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": "true" if disable_web_page_preview else "false",
        })

async def send_photo(bot_token: str, chat_id: int, png: bytes, caption: str):
    url = f"{TELEGRAM_BASE}/bot{bot_token}/sendPhoto"
    files = {'photo': ('chart.png', png, 'image/png')}
    data = {'chat_id': str(chat_id), 'caption': caption}
    async with httpx.AsyncClient(timeout=15.0) as c:
        await c.post(url, files=files, data=data)

# -------------------- mensagens -------------------- #

WELCOME = (
    "Bem-vindo! 👋\n"
    "Use /pulse, /eth, /btc, /panel."
)

async def handle_start(bot_token: str, chat_id: int):
    await send_text(bot_token, chat_id, WELCOME)

async def _asset_block(asset: str) -> Dict[str, Any]:
    s = await markets.snapshot(asset)
    head, rec = asset_analysis(s)
    txt = head + "\n" + rec
    png = sparkline_png(s.get("series"), f"{asset} 24h")
    return {"text": txt, "png": png, "snap": s}

async def handle_asset(bot_token: str, chat_id: int, asset: str):
    block = await _asset_block(asset)
    txt = block["text"]
    s = block["snap"]
    extra = portfolio_hint(asset, s)
    if extra:
        txt += "\n" + extra
    await send_photo(bot_token, chat_id, block["png"], txt)

async def handle_pulse(bot_token: str, chat_id: int):
    eth = await markets.snapshot("ETH")
    btc = await markets.snapshot("BTC")

    eth_line, eth_rec = asset_analysis(eth)
    btc_line, btc_rec = asset_analysis(btc)

    rel = eth.get("rel_eth_btc")
    funding = eth.get("funding")
    oi = eth.get("open_interest_usd")

    header = f"Pulse 🕒 " \
             f"{'ETH ' + _fmt(eth.get('price')) + ' (' + _fmt_pct(eth.get('chg24')) + ')' if eth.get('price') else 'ETH -'}\n" \
             f"{'BTC ' + _fmt(btc.get('price')) + ' (' + _fmt_pct(btc.get('chg24')) + ')' if btc.get('price') else 'BTC -'}"

    analysis = []
    if rel:
        analysis.append(f"ETH/BTC {rel:.5f}.")
    if funding is not None:
        analysis.append(f"Funding (ETH perp) {funding:.4%}.")
    if oi is not None:
        analysis.append(f"OI (ETH) {_fmt(oi)}.")

    analysis.append("Foco: gatilhos de rompimento (R1/R2) e defesa em perdas de suportes (S1/S2).")

    text = header + "\n\n" + eth_line + "\n" + btc_line + "\n\n" + " ".join(analysis)
    await send_text(bot_token, chat_id, text)

async def handle_panel(bot_token: str, chat_id: int):
    # ETH
    eth = await _asset_block("ETH")
    await send_photo(bot_token, chat_id, eth["png"], eth["text"])
    # BTC
    btc = await _asset_block("BTC")
    await send_photo(bot_token, chat_id, btc["png"], btc["text"])

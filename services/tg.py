
import os
import io
from datetime import datetime, timezone
from typing import Any, Dict, Tuple, List

import httpx
from loguru import logger

from services import markets
from utils.img import render_sparkline_png

TELEGRAM_API = "https://api.telegram.org"

def extract_chat_id(update: Dict[str, Any]) -> int | None:
    try:
        if "message" in update:
            return update["message"]["chat"]["id"]
        if "edited_message" in update:
            return update["edited_message"]["chat"]["id"]
        if "channel_post" in update:
            return update["channel_post"]["chat"]["id"]
    except Exception:
        return None
    return None

def extract_text(update: Dict[str, Any]) -> str | None:
    try:
        if "message" in update and "text" in update["message"]:
            return update["message"]["text"]
        if "edited_message" in update and "text" in update["edited_message"]:
            return update["edited_message"]["text"]
    except Exception:
        return None
    return None

async def _tg_post(token: str, method: str, data: Dict[str, Any] | None = None, files: Dict[str, Any] | None = None):
    url = f"{TELEGRAM_API}/bot{token}/{method}"
    timeout = httpx.Timeout(15.0, connect=10.0)
    async with httpx.AsyncClient(timeout=timeout, trust_env=True) as client:
        r = await client.post(url, data=data, files=files)
        try:
            r.raise_for_status()
        except Exception as e:
            logger.warning(f"telegram post fail {method}: {e} {r.text}")
        return r.json()

async def send_markdown(token: str, chat_id: int, text: str, reply_markup: Dict[str, Any] | None = None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return await _tg_post(token, "sendMessage", payload)

async def send_photo(token: str, chat_id: int, caption: str, png_bytes: bytes):
    files = {"photo": ("spark.png", png_bytes, "image/png")}
    data = {"chat_id": chat_id, "caption": caption, "parse_mode": "Markdown"}
    return await _tg_post(token, "sendPhoto", data, files)

def _menu():
    return {
        "keyboard": [
            [{"text": "/pulse"}],
            [{"text": "/eth"}, {"text": "/btc"}],
        ],
        "resize_keyboard": True
    }

def _fmt_num(x: Any) -> str:
    if x is None:
        return "-"
    try:
        return f"${float(x):,.2f}"
    except Exception:
        return str(x)

def _levels_text(info: Dict[str, Any]) -> str:
    s = ", ".join([f"{v:,.0f}" for v in info["supports"]])
    r = ", ".join([f"{v:,.0f}" for v in info["resistances"]])
    return f"S: {s} | R: {r}"

def _analysis(asset: str, info: Dict[str, Any], peer_info: Dict[str, Any]) -> str:
    px = info.get("price")
    ph = info.get("intraday_high")
    pl = info.get("intraday_low")
    r1, r2 = info["resistances"]
    s1, s2 = info["supports"]

    # Simple micro-analysis
    bias = []
    if px and r1:
        if px > r1:
            bias.append("impulso acima de R1")
        elif px < s1:
            bias.append("pressão abaixo de S1")
        else:
            bias.append("faixa intermediária")
    rel = ""
    if asset == "ETH" and peer_info.get("price"):
        rel = f" | ETH/BTC: {info['price']/peer_info['price']:.4f}"

    return f"*{asset}* {_fmt_num(px)}  ({_fmt_num(pl)}–{_fmt_num(ph)})\n{_levels_text(info)}{rel}\nTendência curta: " + ", ".join(bias)

async def build_asset_text(asset: str) -> Tuple[str, List[Tuple[int, float]]]:
    snap = await markets.snapshot(asset)
    peer = await markets.snapshot("BTC" if asset == "ETH" else "ETH")
    txt = _analysis(asset, snap, peer)
    return txt, snap["series_24h"]

async def handle_start(token: str, chat_id: int):
    await send_markdown(token, chat_id, "Bem-vindo! 👋\nUse /pulse, /eth, /btc.", _menu())

async def handle_pulse(token: str, chat_id: int):
    prices = await markets.get_prices()
    eth = prices.get("eth"); btc = prices.get("btc")
    eth_ch = prices.get("eth_24h_change"); btc_ch = prices.get("btc_24h_change")
    ratio = prices.get("eth_btc")
    lines = [
        f"*Pulse* 🕒 {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
        f"ETH: {_fmt_num(eth)} ({eth_ch:+.2f}% 24h)" if eth else "ETH: -",
        f"BTC: {_fmt_num(btc)} ({btc_ch:+.2f}% 24h)" if btc else "BTC: -",
        f"ETH/BTC: {ratio:.4f}" if ratio else "ETH/BTC: -",
        "",
        "_Ação_: gatilhos em rompimentos de resistências; gestão defensiva em perdas de suportes."
    ]
    await send_markdown(token, chat_id, "\n".join(lines), _menu())

async def handle_asset(token: str, chat_id: int, asset: str):
    txt, series = await build_asset_text(asset)
    # Render sparkline
    png = render_sparkline_png(series)
    await send_photo(token, chat_id, txt, png)

async def test_send(token: str) -> bool:
    try:
        await send_markdown(token, 0, "ping")  # will 400 but endpoint reachable
        return True
    except Exception:
        return False

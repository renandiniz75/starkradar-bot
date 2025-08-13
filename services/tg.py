# services/tg.py — Telegram helpers
import io, asyncio
from typing import Tuple
from loguru import logger
from datetime import datetime, timezone

import httpx
from PIL import Image

from services import markets
from utils.img import render_sparkline_png

TG_API = "https://api.telegram.org"

def _fmt(v):
    try:
        return f"{v:,.2f}"
    except Exception:
        return "-"

def _now_utc_str():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

async def _send_message(token: str, chat_id: int, text: str, parse_mode: str="HTML"):
    async with httpx.AsyncClient(timeout=12.0) as client:
        r = await client.post(f"{TG_API}/bot{token}/sendMessage", json={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True
        })
        r.raise_for_status()

async def _send_photo(token: str, chat_id: int, caption: str, png: bytes):
    form = {
        "chat_id": (None, str(chat_id)),
        "caption": (None, caption),
        "parse_mode": (None, "HTML"),
        "photo": ("spark.png", png, "image/png")
    }
    async with httpx.AsyncClient(timeout=12.0) as client:
        r = await client.post(f"{TG_API}/bot{token}/sendPhoto", files=form)
        r.raise_for_status()

async def send_start(token: str, chat_id: int):
    text = (
        "Bem-vindo! 👋\n"
        "Comandos: /pulse, /eth, /btc, /strategy.\n"
        "— StarkRadar v0.17.3"
    )
    await _send_message(token, chat_id, text)

async def send_menu(token: str, chat_id: int):
    await _send_message(token, chat_id, "Use /pulse, /eth, /btc, /strategy.")

async def build_asset_text(asset: str) -> Tuple[str, bytes]:
    snap = await markets.snapshot(asset)
    px = snap.get("price")
    h = snap.get("high_24h")
    l = snap.get("low_24h")
    ch = snap.get("change_24h_pct")
    lv = snap.get("levels") or {"S": "- / -", "R": "- / -"}

    ser = snap.get("series_24h") or []
    series_vals = [p for _, p in ser]
    png = render_sparkline_png(series_vals, label=f"{asset} 24h")

    lines = []
    lines.append(f"<b>{asset}</b> ${_fmt(px)}  ({_fmt(ch)}%)")
    lines.append(f"H/L 24h: ${_fmt(h)} / ${_fmt(l)}")
    lines.append(f"Níveis: S {lv.get('S')} | R {lv.get('R')}")
    lines.append(_now_utc_str())
    return "\n".join(lines), png

async def handle_asset(token: str, chat_id: int, asset: str):
    try:
        txt, png = await build_asset_text(asset)
        await _send_photo(token, chat_id, txt, png)
    except Exception as e:
        logger.exception("handle_asset failed")
        await _send_message(token, chat_id, f"{asset}: dados indisponíveis no momento. {_now_utc_str()}")

async def handle_pulse(token: str, chat_id: int):
    # ETH
    try:
        eth_txt, eth_png = await build_asset_text("ETH")
        await _send_photo(token, chat_id, f"Pulse — {eth_txt}", eth_png)
    except Exception:
        await _send_message(token, chat_id, "ETH: dados indisponíveis.")
    # BTC
    try:
        btc_txt, btc_png = await build_asset_text("BTC")
        await _send_photo(token, chat_id, f"Pulse — {btc_txt}", btc_png)
    except Exception:
        await _send_message(token, chat_id, "BTC: dados indisponíveis.")

async def handle_strategy(token: str, chat_id: int):
    # placeholder de análise (pode ser enriquecido depois com seu portfólio)
    try:
        eth = await markets.snapshot("ETH")
        btc = await markets.snapshot("BTC")
        # heurística breve
        txt = (
            "<b>Estratégia</b>\n"
            f"ETH ${_fmt(eth.get('price'))} ({_fmt(eth.get('change_24h_pct'))}%) | "
            f"BTC ${_fmt(btc.get('price'))} ({_fmt(btc.get('change_24h_pct'))}%)\n"
            f"ETH níveis S {eth['levels'].get('S')} / R {eth['levels'].get('R')}\n"
            f"BTC níveis S {btc['levels'].get('S')} / R {btc['levels'].get('R')}\n"
            "Ação: gatilhos em rompimento das resistências; compras escalonadas em suportes. "
            f"{_now_utc_str()}"
        )
        await _send_message(token, chat_id, txt)
    except Exception:
        await _send_message(token, chat_id, "Estratégia indisponível agora.")

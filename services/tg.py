# services/tg.py — Telegram helpers
import os, base64, datetime as dt
import httpx
from loguru import logger
from . import markets, analysis, images

API_BASE = "https://api.telegram.org"

def _api(token: str, method: str):
    return f"{API_BASE}/bot{token}/{method}"

def _fmt_now():
    return dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

async def send_message(chat_id: int, text: str, parse_mode="Markdown"):
    token = os.getenv("BOT_TOKEN")
    if not token:
        logger.error("BOT_TOKEN missing")
        return
    async with httpx.AsyncClient() as client:
        await client.post(_api(token,"sendMessage"), json={"chat_id": chat_id, "text": text, "parse_mode": parse_mode})

async def send_photo(chat_id: int, png_bytes: bytes, caption: str = None):
    token = os.getenv("BOT_TOKEN")
    if not token:
        logger.error("BOT_TOKEN missing")
        return
    files = {"photo": ("chart.png", png_bytes, "image/png")}
    data = {"chat_id": str(chat_id)}
    if caption:
        data["caption"] = caption
        data["parse_mode"] = "Markdown"
    async with httpx.AsyncClient() as client:
        await client.post(_api(token, "sendPhoto"), data=data, files=files, timeout=30)

async def reply_start(chat_id: int):
    text = ("*Bem-vindo!* 👋\n"
            "Use /pulse para visão rápida, /eth e /btc para cada ativo.\n"
            "_Dica:_ mantenho níveis intraday (S/R) e narrativas táticas.")
    await send_message(chat_id, text)

async def reply_help(chat_id: int):
    await send_message(chat_id, "Comandos: /pulse, /eth, /btc")

async def _asset_message(asset: str):
    snap = await markets.spot_snapshot(asset)
    if not snap:
        return "Dados indisponíveis no momento.", None
    levels = analysis.key_levels(snap.get("series_24h") or [])
    txt = analysis.narrative(asset, snap.get("price"), snap.get("change_24h"), levels)
    lines = []
    lines.append(txt)
    if levels.get("high") and levels.get("low"):
        lines.append(f"Hi/Lo 24h • H:{levels['high']}  L:{levels['low']}")
    lines.append(_fmt_now())
    series = snap.get("series_24h") or []
    return "\n".join(lines), series

async def reply_asset(chat_id: int, asset: str):
    text, series = await _asset_message(asset)
    if series:
        png = images.sparkline_png(series)
        await send_photo(chat_id, png, caption=text)
    else:
        await send_message(chat_id, text)

async def reply_pulse(chat_id: int):
    eth = await markets.spot_snapshot("ETH")
    btc = await markets.spot_snapshot("BTC")
    pair = await markets.pair_snapshot() or {}

    def _line(a, s):
        if not s: return f"{a}: -"
        ch = "-" if s.get("change_24h") is None else f"{s['change_24h']:+.2f}%"
        return f"{a} ${s.get('price', '-'):,} ({ch})"

    rel = ""
    if pair.get("eth_btc"):
        rel = f"ETH/BTC: {pair['eth_btc']:.5f} | ETH 24h {pair.get('eth_chg',0):+.2f}% vs BTC {pair.get('btc_chg',0):+.2f}%"

    lines = ["*Pulse* ⚡", _line("ETH", eth), _line("BTC", btc)]
    if rel: lines.append(rel)
    lines.append(_fmt_now())

    # Prefer ETH chart, else BTC
    series = (eth or {}).get("series_24h") or (btc or {}).get("series_24h") or []
    if series:
        png = images.sparkline_png(series)
        await send_photo(chat_id, png, caption="\n".join(lines))
    else:
        await send_message(chat_id, "\n".join(lines))

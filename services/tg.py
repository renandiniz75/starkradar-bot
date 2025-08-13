# v0.17.2-hotfixB • services/tg.py
import io
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

def _fmt_money(v): return "-" if v is None else f"${v:,.2f}"
def _fmt_pct(v):   return "-" if v is None else f"{v:+.2f}%"

def _levels_text(lv):
    s = f"Níveis S:{_fmt_money(lv.get('s1'))}, {_fmt_money(lv.get('s2'))} | R:{_fmt_money(lv.get('r1'))}, {_fmt_money(lv.get('r2'))}"
    return s

def _quick_view(asset_snap: dict) -> str:
    p = _fmt_money(asset_snap["price"])
    c = _fmt_pct(asset_snap["change_24h"])
    lv = _levels_text(asset_snap["levels"])
    return f"{asset_snap['asset']} {p} ({c} 24h)\n{lv}"

def _pulse_comment(eth: dict, btc: dict) -> str:
    # heurística simples baseada em distância a R1/S1 e variação
    lines = []
    for a in (eth, btc):
        if a["price"] and a["levels"]["r1"] and a["levels"]["s1"]:
            px = a["price"]; r1 = a["levels"]["r1"]; s1 = a["levels"]["s1"]
            dist_r = (r1 - px)/px*100
            dist_s = (px - s1)/px*100
            bias = "alta" if (a["change_24h"] or 0) > 0 else "baixa" if (a["change_24h"] or 0) < 0 else "neutra"
            lines.append(f"{a['asset']}: viés {bias}; ~{abs(dist_r):.2f}% de R1 / ~{abs(dist_s):.2f}% de S1.")
    if lines:
        lines.append("Ação: operar rompimentos com confirmação e reduzir risco se perder S1.")
    return "\n".join(lines) or "Ação: foco nos gatilhos e gestão defensiva em perdas de suporte."

# ----------------- handlers -----------------

async def handle_asset(token: str, chat_id: int, asset: str):
    snap = await markets.snapshot(asset)
    title = f"{asset} 24h"
    png = markets.spark_png(snap["series"], title)
    caption = _quick_view(snap) + f"\n{snap['ts']}"
    await send_png(token, chat_id, caption, png)

async def handle_pulse(token: str, chat_id: int):
    eth, btc = await markets.snapshot("ETH"), await markets.snapshot("BTC")
    rel = (eth["price"]/btc["price"]) if (eth["price"] and btc["price"]) else None
    text = (
        f"Pulse 🕒 {eth['ts']}\n"
        f"{_quick_view(eth)}\n"
        f"{_quick_view(btc)}\n"
        + (f"ETH/BTC {rel:.5f}\n" if rel else "")
        + _pulse_comment(eth, btc)
    )
    await send_text(token, chat_id, text)

async def handle_panel(token: str, chat_id: int):
    await handle_asset(token, chat_id, "ETH")
    await handle_asset(token, chat_id, "BTC")

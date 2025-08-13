
import httpx, asyncio
from typing import Tuple
from datetime import datetime, timezone
from loguru import logger
from services import markets
from utils.img import render_sparkline_png

TG_API = "https://api.telegram.org"

async def _post_json(token: str, method: str, payload: dict):
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.post(f"{TG_API}/bot{token}/{method}", json=payload)
        try: r.raise_for_status()
        except Exception as e: logger.warning("tg {} fail: {}", method, e)
        return r.json()

async def _post_multipart(token: str, method: str, data: dict, files: dict):
    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.post(f"{TG_API}/bot{token}/{method}", data=data, files=files)
        try: r.raise_for_status()
        except Exception as e: logger.warning("tg {} fail: {}", method, e)
        return r.json()

def _fmt(v): return "-" if v is None else f"${v:,.2f}"

async def send_menu(token: str, chat_id: str):
    txt = "Bem-vindo! 👋\nUse /pulse, /eth, /btc."
    await _post_json(token, "sendMessage", {"chat_id":chat_id,"text":txt})

async def build_asset_text(asset: str) -> Tuple[str, bytes]:
    s = await markets.snapshot(asset)
    px = s.get("price"); chg = s.get("change_24h_pct")
    chg_s = "-" if chg is None else f"{chg:+.2f}%"
    hilo = s.get("hilo",{}); lv = s.get("levels",{})
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [f"{asset} {_fmt(px)} ({chg_s})",
             f"Hi/Lo 24h: {_fmt(hilo.get('high'))} / {_fmt(hilo.get('low'))}",
             f"S:{lv.get('S1','-')}/{lv.get('S2','-')} | MID:{lv.get('MID','-')} | R:{lv.get('R2','-')}/{lv.get('R1','-')}",
             f"— v0.20 • {ts}"]
    png = render_sparkline_png(s.get("series", []), label=asset)
    return "\n".join(lines), png

async def handle_asset(token: str, chat_id: str, asset: str):
    txt, png = await build_asset_text(asset)
    files = {"photo": ("spark.png", png, "image/png")}
    await _post_multipart(token, "sendPhoto", {"chat_id":chat_id, "caption":txt}, files)

async def send_pulse(token: str, chat_id: str):
    eth, btc = await asyncio.gather(markets.snapshot("ETH"), markets.snapshot("BTC"))
    def line(s):
        px=s.get("price"); chg=s.get("change_24h_pct"); lv=s.get("levels",{})
        chg_s = "-" if chg is None else f"{chg:+.2f}%"
        return f"{s['symbol']}: {_fmt(px)} ({chg_s}) • S:{lv.get('S1','-')} | R:{lv.get('R1','-')}"
    txt = "Pulse 🕒\n"+ "\n".join([line(eth), line(btc)]) + "\n" + datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    png = render_sparkline_png(eth.get("series", []), label="ETH")
    files = {"photo": ("pulse.png", png, "image/png")}
    await _post_multipart(token, "sendPhoto", {"chat_id":chat_id, "caption":txt}, files)

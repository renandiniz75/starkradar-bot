# services/tg.py
import io, datetime, asyncio, httpx
from . import markets, rendering

TG_API = "https://api.telegram.org/bot{token}/{method}"

async def send_text(token: str, chat_id: int, text: str, parse_mode: str | None = None):
    url = TG_API.format(token=token, method="sendMessage")
    async with httpx.AsyncClient(timeout=20) as cli:
        payload = {"chat_id": chat_id, "text": text}
        if parse_mode:
            payload["parse_mode"] = parse_mode
        await cli.post(url, json=payload)

async def send_photo(token: str, chat_id: int, caption: str, png_bytes: bytes):
    url = TG_API.format(token=token, method="sendPhoto")
    files = {"photo": ("spark.png", png_bytes, "image/png")}
    data = {"chat_id": str(chat_id), "caption": caption}
    async with httpx.AsyncClient(timeout=30) as cli:
        await cli.post(url, data=data, files=files)

async def ping(token: str) -> bool:
    url = TG_API.format(token=token, method="getMe")
    async with httpx.AsyncClient(timeout=10) as cli:
        r = await cli.get(url)
        return r.json().get("ok", False)

def _fmt_usd(x):
    try:
        return f"${x:,.2f}"
    except Exception:
        return "-"

def _levels_text(levels):
    s = " / ".join(str(int(v)) for v in levels.get("supports", []))
    r = " / ".join(str(int(v)) for v in levels.get("resistances", []))
    return s or "-", r or "-"

async def handle_start(token: str, chat_id: int):
    spark = await markets.series_24h("ETH")
    png = rendering.sparkline_png(spark.get("series", []), title="ETH 24h")
    await send_photo(token, chat_id, "Bem-vindo! 👋\nUse /pulse, /eth, /btc, /panel.", png)

async def build_asset_text(asset: str) -> tuple[str, list[float]]:
    snap = await markets.snapshot(asset)
    px = snap.get("price")
    lv = snap.get("levels", {})
    s, r = _levels_text(lv)
    now = datetime.datetime.utcnow().strftime("%H:%M UTC")
    txt = f"{asset} {_fmt_usd(px)}\nNíveis S:{s} | R:{r}\n{now}"
    return txt, (snap.get("series") or [])

async def handle_asset(token: str, chat_id: int, asset: str):
    txt, series = await build_asset_text(asset)
    png = rendering.sparkline_png(series, title=f"{asset} 24h")
    await send_photo(token, chat_id, txt, png)

async def handle_pulse(token: str, chat_id: int):
    eth = await markets.snapshot("ETH")
    btc = await markets.snapshot("BTC")
    # Relative performance
    eth_chg = eth.get("change_24h")
    btc_chg = btc.get("change_24h")
    pair = await markets.pair_change("ETH","BTC")

    lines = [
        f"Pulse 🕒 {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
        f"ETH {_fmt_usd(eth.get('price'))} ({eth_chg:+.2f}% 24h)",
        f"BTC {_fmt_usd(btc.get('price'))} ({btc_chg:+.2f}% 24h)",
        f"ETH/BTC {pair:+.2f}% (24h)",
        "",
        "Análise: foco nos gatilhos de rompimento e gestão de risco em perdas de suporte. Use /eth ou /btc."
    ]
    await send_text(token, chat_id, "\n".join(lines))

async def handle_panel(token: str, chat_id: int):
    # Simple panel: two sparklines in two messages to keep it robust
    for asset in ("ETH","BTC"):
        txt, series = await build_asset_text(asset)
        png = rendering.sparkline_png(series, title=f"{asset} 24h")
        await send_photo(token, chat_id, txt, png)

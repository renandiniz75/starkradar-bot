# app.py
import os
import logging
from datetime import time
from zoneinfo import ZoneInfo

import httpx
from aiohttp import web
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# ============ LOG ============
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("starkradar")

# ============ ENV ============
TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
EXTERNAL_URL = (
    os.environ.get("RENDER_EXTERNAL_URL")
    or os.environ.get("EXTERNAL_URL")
    or "https://starkradar-bot.onrender.com"
).rstrip("/")
PORT = int(os.environ.get("PORT", "10000"))
WEBHOOK_PATH = f"/{TOKEN}" if TOKEN else "/webhook"
TZ = ZoneInfo("America/Sao_Paulo")

# ============ HTTP CLIENT ============
COMMON_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126 Safari/537.36",
    "Accept": "application/json",
}
HTTP_TIMEOUT = httpx.Timeout(10.0, read=10.0, connect=10.0)
client = httpx.AsyncClient(timeout=HTTP_TIMEOUT, headers=COMMON_HEADERS)

# ============ FEEDS ============
async def _binance(symbol: str) -> dict:
    url = "https://api.binance.com/api/v3/ticker/24hr"
    r = await client.get(url, params={"symbol": symbol})
    r.raise_for_status()
    j = r.json()
    return {
        "price": float(j["lastPrice"]),
        "high": float(j["highPrice"]),
        "low": float(j["lowPrice"]),
        "pct": float(j["priceChangePercent"]),
        "vol": float(j.get("quoteVolume", 0.0)),
        "src": "Binance",
    }

async def _coinbase(product: str) -> dict:
    # ticker (price) + stats (open/high/low/volume)
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, headers=COMMON_HEADERS) as cb:
        t = await cb.get(f"https://api.exchange.coinbase.com/products/{product}/ticker")
        t.raise_for_status()
        s = await cb.get(f"https://api.exchange.coinbase.com/products/{product}/stats")
        s.raise_for_status()
        tj, sj = t.json(), s.json()
        price = float(tj["price"])
        high = float(sj["high"])
        low = float(sj["low"])
        open_ = float(sj.get("open", price) or price)
        pct = ((price - open_) / open_) * 100 if open_ else 0.0
        vol = float(sj.get("volume", 0.0))
        return {"price": price, "high": high, "low": low, "pct": pct, "vol": vol, "src": "Coinbase"}

async def _kraken(pair: str) -> dict:
    url = "https://api.kraken.com/0/public/Ticker"
    r = await client.get(url, params={"pair": pair})
    r.raise_for_status()
    j = r.json()
    res = j["result"]
    kpair = next(iter(res.keys()))
    d = res[kpair]
    price = float(d["c"][0])
    high = float(d["h"][1])
    low = float(d["l"][1])
    mid = (high + low) / 2 if (high and low) else price
    pct = (price - mid) / mid * 100 if mid else 0.0
    vol = float(d["v"][1])
    return {"price": price, "high": high, "low": low, "pct": pct, "vol": vol, "src": "Kraken"}

async def _coingecko(coin_id: str) -> dict:
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}"
    params = {
        "localization": "false", "tickers": "false", "market_data": "true",
        "community_data": "false", "developer_data": "false", "sparkline": "false",
    }
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, headers=COMMON_HEADERS) as cg:
        r = await cg.get(url, params=params)
        r.raise_for_status()
        md = r.json()["market_data"]
        return {
            "price": float(md["current_price"]["usd"]),
            "high": float(md["high_24h"]["usd"]),
            "low": float(md["low_24h"]["usd"]),
            "pct": float(md["price_change_percentage_24h"]),
            "vol": float(md["total_volume"]["usd"]),
            "src": "CoinGecko",
        }

async def get_stats(asset: str) -> dict:
    """
    asset: 'ETH' ou 'BTC'
    Cadeia de fallback: Binance -> Coinbase -> Kraken -> CoinGecko
    """
    try:
        if asset == "ETH":
            return await _binance("ETHUSDT")
        else:
            return await _binance("BTCUSDT")
    except Exception as e:
        log.warning("Binance %s falhou: %s", asset, e)

    try:
        if asset == "ETH":
            return await _coinbase("ETH-USD")
        else:
            return await _coinbase("BTC-USD")
    except Exception as e:
        log.warning("Coinbase %s falhou: %s", asset, e)

    try:
        if asset == "ETH":
            return await _kraken("ETHUSD")
        else:
            return await _kraken("XBTUSD")
    except Exception as e:
        log.warning("Kraken %s falhou: %s", asset, e)

    # último fallback
    return await _coingecko("ethereum" if asset == "ETH" else "bitcoin")

def fmt_stats(name: str, s: dict) -> str:
    arrow = "🔺" if s["pct"] >= 0 else "🔻"
    return (
        f"📊 {name} — fonte: {s['src']}\n"
        f"• Preço: ${s['price']:,.2f}\n"
        f"• 24h: {arrow} {s['pct']:.2f}%   (Alta: ${s['high']:,.0f} | Baixa: ${s['low']:,.0f})\n"
        f"• Vol (24h): {s['vol']:,.0f}"
    )

# ============ TELEGRAM HANDLERS ============
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("✅ Stark DeFi Brain online. Envie /eth ou /btc.")

async def eth_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        s = await get_stats("ETH")
        await update.message.reply_text(fmt_stats("ETH", s))
    except Exception as e:
        log.exception("ETH handler")
        await update.message.reply_text(f"⚠️ ETH indisponível agora. ({e})")

async def btc_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        s = await get_stats("BTC")
        await update.message.reply_text(fmt_stats("BTC", s))
    except Exception as e:
        log.exception("BTC handler")
        await update.message.reply_text(f"⚠️ BTC indisponível agora. ({e})")

async def alfa_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("🚀 /alfa em desenvolvimento.")

# ============ JOBS ============
async def send_report(context: ContextTypes.DEFAULT_TYPE) -> None:
    if not CHAT_ID:
        log.warning("CHAT_ID não configurado.")
        return
    try:
        eth = await get_stats("ETH")
        btc = await get_stats("BTC")
        txt = "🧠 Stark DeFi Brain — Boletim automático\n\n" + fmt_stats("ETH", eth) + "\n\n" + fmt_stats("BTC", btc)
        await context.bot.send_message(chat_id=int(CHAT_ID), text=txt)
    except Exception as e:
        log.exception("send_report")
        try:
            await context.bot.send_message(chat_id=int(CHAT_ID), text=f"⚠️ Falha no boletim: {e}")
        except Exception:
            pass

def schedule_jobs(app: Application) -> None:
    jq = app.job_queue
    if jq is None:
        log.warning("JobQueue não disponível. Instale: python-telegram-bot[job-queue]")
        return
    for hh, mm in [(8, 0), (12, 0), (17, 0), (19, 0)]:
        jq.run_daily(send_report, time=time(hour=hh, minute=mm, tzinfo=TZ), name=f"rep_{hh:02d}{mm:02d}")
    log.info("Boletins agendados (BRT): 08:00, 12:00, 17:00, 19:00")

# ============ AIOHTTP BOOT ============
async def make_web_app(application: Application) -> web.Application:
    web_app = web.Application()

    async def health(_: web.Request) -> web.Response:
        return web.Response(text="ok", status=200)

    web_app.router.add_get("/", health)
    web_app.router.add_get("/health", health)

    # webhook PTB
    web_app.router.add_post(WEBHOOK_PATH, application.webhook_handler())

    async def on_startup(_: web.Application):
        await application.initialize()
        schedule_jobs(application)
        await application.start()
        full_url = f"{EXTERNAL_URL}{WEBHOOK_PATH}"
        await application.bot.set_webhook(full_url)
        log.info("Webhook registrado em %s", full_url)

    async def on_cleanup(_: web.Application):
        try:
            await application.bot.delete_webhook(drop_pending_updates=False)
        except Exception:
            pass
        await application.stop()
        await application.shutdown()

    web_app.on_startup.append(on_startup)
    web_app.on_cleanup.append(on_cleanup)
    return web_app

def main() -> None:
    if not TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN não definido")
    if not EXTERNAL_URL.startswith("http"):
        raise RuntimeError("EXTERNAL_URL/RENDER_EXTERNAL_URL inválido")

    application = Application.builder().token(TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("eth", eth_cmd))
    application.add_handler(CommandHandler("btc", btc_cmd))
    application.add_handler(CommandHandler("alfa", alfa_cmd))

    web_app = make_web_app(application)
    web.run_app(web_app, host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    try:
        main()
    finally:
        try:
            import anyio
            anyio.run(client.aclose)
        except Exception:
            pass

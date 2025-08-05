# app.py
import os
import logging
from datetime import time
from zoneinfo import ZoneInfo

import httpx
from aiohttp import web
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ---------------- LOGGING ----------------
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("starkradar")

# ---------------- ENV ----------------
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

# ---------------- HTTP client (com headers) ----------------
COMMON_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Accept": "application/json",
}
HTTP_TIMEOUT = httpx.Timeout(10.0, read=10.0, connect=10.0)
client = httpx.AsyncClient(timeout=HTTP_TIMEOUT, headers=COMMON_HEADERS)

# ---------------- FEEDS: PRIMARY + FALLBACKS ----------------
# 1) BINANCE
async def _binance_24h(symbol: str) -> dict:
    url = "https://api.binance.com/api/v3/ticker/24hr"
    try:
        r = await client.get(url, params={"symbol": symbol})
        r.raise_for_status()
        j = r.json()
        return {
            "price": float(j["lastPrice"]),
            "high": float(j["highPrice"]),
            "low": float(j["lowPrice"]),
            "pct": float(j["priceChangePercent"]),
            "vol": float(j.get("quoteVolume") or j.get("volume") or 0.0),
            "src": "Binance",
        }
    except Exception as e:
        log.warning(f"Binance {symbol} falhou: {e}")
        return {}

# 2) COINBASE
async def _coinbase(product: str) -> dict:
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, headers=COMMON_HEADERS) as cb:
            t = await cb.get(f"https://api.exchange.coinbase.com/products/{product}/ticker")
            s = await cb.get(f"https://api.exchange.coinbase.com/products/{product}/stats")
            t.raise_for_status(); s.raise_for_status()
            tj, sj = t.json(), s.json()
            price = float(tj["price"])
            high = float(sj["high"]); low = float(sj["low"])
            open_ = float(sj["open"]) if sj.get("open") not in ("", None) else price
            pct = ((price - open_) / open_) * 100 if open_ else 0.0
            vol = float(sj.get("volume", 0.0))
            return {"price": price, "high": high, "low": low, "pct": pct, "vol": vol, "src": "Coinbase"}
    except Exception as e:
        log.warning(f"Coinbase {product} falhou: {e}")
        return {}

# 3) KRAKEN
async def _kraken(pair: str) -> dict:
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, headers=COMMON_HEADERS) as k:
            r = await k.get("https://api.kraken.com/0/public/Ticker", params={"pair": pair})
            r.raise_for_status()
            j = r.json(); res = j["result"]; kpair = next(iter(res)); d = res[kpair]
            price = float(d["c"][0]); high = float(d["h"][1]); low = float(d["l"][1])
            mid = (high + low) / 2; pct = (price - mid) / mid * 100 if mid else 0.0
            vol = float(d["v"][1])
            return {"price": price, "high": high, "low": low, "pct": pct, "vol": vol, "src": "Kraken"}
    except Exception as e:
        log.warning(f"Kraken {pair} falhou: {e}")
        return {}

# 4) COINGECKO
async def _coingecko(coin_id: str) -> dict:
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, headers=COMMON_HEADERS) as cg:
            r = await cg.get(
                f"https://api.coingecko.com/api/v3/coins/{coin_id}",
                params={
                    "localization": "false", "tickers": "false", "market_data": "true",
                    "community_data": "false", "developer_data": "false", "sparkline": "false",
                }
            )
            r.raise_for_status()
            md = r.json()["market_data"]
            price = float(md["current_price"]["usd"])
            high = float(md["high_24h"]["usd"]); low = float(md["low_24h"]["usd"])
            pct = float(md["price_change_percentage_24h"])
            vol = float(md["total_volume"]["usd"])
            return {"price": price, "high": high, "low": low, "pct": pct, "vol": vol, "src": "CoinGecko"}
    except Exception as e:
        log.warning(f"CoinGecko {coin_id} falhou: {e}")
        return {}

async def get_stats(asset: str) -> dict:
    """
    asset: 'ETH' ou 'BTC'
    Ordem: Binance -> Coinbase -> Kraken -> CoinGecko
    """
    if asset == "ETH":
        for fn in (
            lambda: _binance_24h("ETHUSDT"),
            lambda: _coinbase("ETH-USD"),
            lambda: _kraken("ETHUSD"),
            lambda: _coingecko("ethereum"),
        ):
            d = await fn()
            if d: return d
    else:  # BTC
        for fn in (
            lambda: _binance_24h("BTCUSDT"),
            lambda: _coinbase("BTC-USD"),
            lambda: _kraken("XBTUSD"),
            lambda: _coingecko("bitcoin"),
        ):
            d = await fn()
            if d: return d
    return {}

def fmt_num(n: float, dec: int = 2) -> str:
    try:
        return f"{float(n):,.{dec}f}"
    except Exception:
        return str(n)

def fmt(asset: str, s: dict) -> str:
    if not s:
        return f"⚠️ {asset}: dados indisponíveis agora. Tente em instantes."
    arrow = "🔺" if s["pct"] >= 0 else "🔻"
    return (
        f"📊 {asset} — fonte: {s['src']}\n"
        f"• Preço: ${fmt_num(s['price'], 2)}\n"
        f"• 24h: {arrow} {s['pct']:.2f}%   (Alta: ${fmt_num(s['high'], 0)} | Baixa: ${fmt_num(s['low'], 0)})\n"
        f"• Vol (24h): {fmt_num(s['vol'], 0)}"
    )

# ---------------- HANDLERS ----------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("✅ Stark DeFi Brain online. Envie /eth ou /btc.")

async def eth_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    s = await get_stats("ETH")
    await update.message.reply_text(fmt("ETH", s))

async def btc_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    s = await get_stats("BTC")
    await update.message.reply_text(fmt("BTC", s))

async def alfa_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("🚀 /alfa: varredura em desenvolvimento. Em breve, sinais.")

# ---------------- JOBS ----------------
async def send_report(context: ContextTypes.DEFAULT_TYPE) -> None:
    if not CHAT_ID:
        log.warning("CHAT_ID não configurado. Ignorando envio automático.")
        return
    eth = await get_stats("ETH")
    btc = await get_stats("BTC")
    txt = "🧠 Stark DeFi Brain — Boletim automático\n\n" + fmt("ETH", eth) + "\n\n" + fmt("BTC", btc)
    try:
        await context.bot.send_message(chat_id=int(CHAT_ID), text=txt)
    except Exception as e:
        log.warning(f"Falha ao enviar boletim: {e}")

def schedule_jobs(app: Application) -> None:
    jq = app.job_queue
    if jq is None:
        log.warning("JobQueue não disponível. Instale: python-telegram-bot[job-queue]")
        return
    for hh, mm in [(8, 0), (12, 0), (17, 0), (19, 0)]:
        jq.run_daily(send_report, time=time(hour=hh, minute=mm, tzinfo=TZ), name=f"rep_{hh:02d}{mm:02d}")
    log.info("Boletins agendados (BRT): 08:00, 12:00, 17:00, 19:00")

# ---------------- AIOHTTP + PTB ----------------
async def make_web_app(application: Application) -> web.Application:
    web_app = web.Application()

    async def health(_: web.Request) -> web.Response:
        return web.Response(text="ok", status=200)

    web_app.router.add_get("/", health)
    web_app.router.add_get("/health", health)
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

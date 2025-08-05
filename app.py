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

# =========================
# LOGGING
# =========================
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("starkradar")

# =========================
# ENV & CONSTANTES
# =========================
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

# HTTP client
HTTP_TIMEOUT = httpx.Timeout(10.0, read=10.0, connect=10.0)
client = httpx.AsyncClient(timeout=HTTP_TIMEOUT)

# =========================
# MARKET HELPERS (BINANCE)
# =========================
BINANCE_24H = "https://api.binance.com/api/v3/ticker/24hr"

async def fetch_binance_24h(symbol: str) -> dict:
    try:
        r = await client.get(BINANCE_24H, params={"symbol": symbol})
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning("Erro Binance 24h %s: %s", symbol, e)
        return {}

def fmt_num(n: float, dec: int = 2) -> str:
    try:
        return f"{float(n):,.{dec}f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(n)

async def snapshot_msg(symbol: str, label: str) -> str:
    data = await fetch_binance_24h(symbol)
    if not data:
        return f"⚠️ {label}: dados indisponíveis agora. Tente em instantes."

    last = data.get("lastPrice")
    pct  = data.get("priceChangePercent")
    high = data.get("highPrice")
    low  = data.get("lowPrice")
    vol  = data.get("volume")

    return (
        f"📊 {label}\n"
        f"• Último: **${fmt_num(last, 2)}**\n"
        f"• 24h: {fmt_num(pct, 2)}%\n"
        f"• Máx 24h: ${fmt_num(high, 2)} | Mín 24h: ${fmt_num(low, 2)}\n"
        f"• Vol 24h: {fmt_num(vol, 2)} {label}\n"
    )

# =========================
# TELEGRAM HANDLERS
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("✅ Stark DeFi Brain online. Envie /eth ou /btc.")

async def eth_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = await snapshot_msg("ETHUSDT", "ETH")
    await update.message.reply_markdown(msg)

async def btc_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = await snapshot_msg("BTCUSDT", "BTC")
    await update.message.reply_markdown(msg)

async def alfa_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("🚀 /alfa: varredura em desenvolvimento. Em breve, sinais.")

# =========================
# JOBS (BOLETINS)
# =========================
async def send_report(context: ContextTypes.DEFAULT_TYPE) -> None:
    if not CHAT_ID:
        log.warning("CHAT_ID não configurado. Ignorando envio automático.")
        return

    eth = await snapshot_msg("ETHUSDT", "ETH")
    btc = await snapshot_msg("BTCUSDT", "BTC")
    header = "🧠 Stark DeFi Brain — Boletim automático"
    text = f"{header}\n\n{eth}\n{btc}"
    try:
        await context.bot.send_message(chat_id=int(CHAT_ID), text=text, parse_mode="Markdown")
    except Exception as e:
        log.warning("Falha ao enviar boletim: %s", e)

def schedule_jobs(app: Application) -> None:
    jq = app.job_queue
    if jq is None:
        log.warning("JobQueue não disponível. Instale: python-telegram-bot[job-queue]")
        return

    for hh, mm in [(8, 0), (12, 0), (17, 0), (19, 0)]:
        jq.run_daily(
            send_report,
            time=time(hour=hh, minute=mm, tzinfo=TZ),
            name=f"rep_{hh:02d}{mm:02d}",
        )
    log.info("Boletins agendados (BRT): 08:00, 12:00, 17:00, 19:00")

# =========================
# AIOHTTP APP + PTB WEBHOOK
# =========================
async def make_web_app(application: Application) -> web.Application:
    web_app = web.Application()

    async def health(_: web.Request) -> web.Response:
        return web.Response(text="ok", status=200)

    # health checks
    web_app.router.add_get("/", health)
    web_app.router.add_get("/health", health)

    # webhook (POST)
    web_app.router.add_post(WEBHOOK_PATH, application.webhook_handler())

    async def on_startup(_: web.Application):
        # Inicializa PTB e webhook
        await application.initialize()
        schedule_jobs(application)
        # start PTB (inicia JobQueue e despachante)
        await application.start()
        # registra webhook no Telegram
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
    # roda aiohttp (bloqueante) – Render health-check receberá 200 em "/" e "/health"
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

# app.py
import os
import logging
from datetime import time
from zoneinfo import ZoneInfo

import httpx
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
# URL pública do Render (ou outra). Pode usar RENDER_EXTERNAL_URL ou fallback:
EXTERNAL_URL = (
    os.environ.get("RENDER_EXTERNAL_URL")
    or os.environ.get("EXTERNAL_URL")
    or "https://starkradar-bot.onrender.com"
).rstrip("/")

PORT = int(os.environ.get("PORT", "10000"))
# Segurança: usar o token no path ajuda a evitar scans:
WEBHOOK_PATH = f"/{TOKEN}" if TOKEN else "/webhook"

# Fuso horário do Brasil:
TZ = ZoneInfo("America/Sao_Paulo")

# HTTPX Client (timeout e retry básico)
HTTP_TIMEOUT = httpx.Timeout(10.0, read=10.0, connect=10.0)
client = httpx.AsyncClient(timeout=HTTP_TIMEOUT)

# =========================
# HELPERS DE MERCADO (BINANCE)
# =========================
BINANCE_24H = "https://api.binance.com/api/v3/ticker/24hr"

async def fetch_binance_24h(symbol: str) -> dict:
    """
    Retorna o dicionário da Binance para o ticker 24h do par informado (ex.: ETHUSDT).
    Campos relevantes:
      - lastPrice
      - priceChangePercent
      - volume
      - highPrice
      - lowPrice
    """
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
    """
    Monta mensagem curta de snapshot para um símbolo da Binance (ex.: ETHUSDT -> ETH).
    """
    data = await fetch_binance_24h(symbol)
    if not data:
        return f"⚠️ {label}: dados indisponíveis agora. Tente em instantes."

    last = data.get("lastPrice")
    pct = data.get("priceChangePercent")
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
# HANDLERS
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
    # Placeholder para oportunidades /alfa
    await update.message.reply_text("🚀 /alfa: varredura em desenvolvimento. Em breve, sinais.")

# =========================
# JOBS (BOLETINS AUTOMÁTICOS)
# =========================
async def send_report(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Envia boletim consolidado no CHAT_ID configurado.
    """
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
        # Guard: não interrompe o serviço caso o extra falhe
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
# MAIN (WEBHOOK)
# =========================
def main() -> None:
    if not TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN não definido")
    if not EXTERNAL_URL.startswith("http"):
        raise RuntimeError("EXTERNAL_URL/RENDER_EXTERNAL_URL inválido")

    application = Application.builder().token(TOKEN).build()

    # Handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("eth", eth_cmd))
    application.add_handler(CommandHandler("btc", btc_cmd))
    application.add_handler(CommandHandler("alfa", alfa_cmd))

    # Jobs agendados
    schedule_jobs(application)

    # Inicia webhook (PTB cuida do setWebhook internamente se webhook_url for passado)
    full_webhook_url = f"{EXTERNAL_URL}{WEBHOOK_PATH}"
    log.info("Iniciando webhook em %s", full_webhook_url)

    application.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=WEBHOOK_PATH,
        webhook_url=full_webhook_url,
    )

if __name__ == "__main__":
    try:
        main()
    finally:
        # Fecha o client HTTPX limpo (se o processo encerrar)
        try:
            import anyio
            anyio.run(client.aclose)
        except Exception:
            pass

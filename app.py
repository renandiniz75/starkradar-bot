import os
import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("starkradar")

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
ALLOWED_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")  # opcional para restringir
PORT = int(os.environ.get("PORT", "10000"))

# Base pública do Render. Em runtime, o Render injeta esta variável.
BASE_URL = os.environ.get("WEBHOOK_BASE", os.environ.get("RENDER_EXTERNAL_URL"))
if not BASE_URL:
    raise RuntimeError(
        "Defina WEBHOOK_BASE (ex: https://seu-servico.onrender.com) "
        "ou deixe o Render setar RENDER_EXTERNAL_URL automaticamente em runtime."
    )

WEBHOOK_PATH = "/webhook"               # caminho do webhook
WEBHOOK_URL = f"{BASE_URL}{WEBHOOK_PATH}"


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if ALLOWED_CHAT_ID and str(update.effective_chat.id) != str(ALLOWED_CHAT_ID):
        return
    await update.message.reply_text("✅ Stark DeFi Brain online. Envie /eth ou /btc.")

async def eth_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if ALLOWED_CHAT_ID and str(update.effective_chat.id) != str(ALLOWED_CHAT_ID):
        return
    await update.message.reply_text("📊 ETH: preparando análise… (placeholder)")

async def btc_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if ALLOWED_CHAT_ID and str(update.effective_chat.id) != str(ALLOWED_CHAT_ID):
        return
    await update.message.reply_text("📊 BTC: preparando análise… (placeholder)")


def build_app() -> Application:
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start_cmd))
    application.add_handler(CommandHandler("eth", eth_cmd))
    application.add_handler(CommandHandler("btc", btc_cmd))

    return application


if __name__ == "__main__":
    app = build_app()

    # O PTB levanta um servidor aiohttp e registra o webhook no Telegram.
    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        webhook_url=WEBHOOK_URL,
        webhook_path=WEBHOOK_PATH,   # onde o Telegram vai dar POST
    )

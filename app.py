import os
from telegram import Update
from telegram.ext import (
    Application, CommandHandler, ContextTypes
)

# --- Variáveis de ambiente ---
BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")  # opcional para envios proativos
RENDER_URL = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
PORT = int(os.environ.get("PORT", "10000"))

# Defina um caminho interno p/ o webhook (qualquer string curta)
WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"{RENDER_URL}{WEBHOOK_PATH}" if RENDER_URL else None


# --- Handlers ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("✅ Stark DeFi Brain online. Envie /eth ou /btc.")

async def eth(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📊 ETH: preparando análise… (stub)")

async def btc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📊 BTC: preparando análise… (stub)")


def build_app() -> Application:
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("eth", eth))
    app.add_handler(CommandHandler("btc", btc))

    return app


if __name__ == "__main__":
    # IMPORTANTE: não use asyncio.run aqui
    application = build_app()

    # Somente WEBHOOK (sem polling)
    # O PTB define o webhook automaticamente quando você informa `webhook_url`
    application.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=WEBHOOK_PATH,      # caminho local que o servidor irá escutar
        webhook_url=WEBHOOK_URL,    # URL pública completa do Render
        drop_pending_updates=True,
    )

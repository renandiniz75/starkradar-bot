import os
import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# Logs básicos
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("starkradar")

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")  # podemos usar para enviar alertas proativo

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Stark DeFi Brain online ✅\nComandos: /ping /eth /btc")

async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("pong 🏓")

# stubs simples — depois a gente pluga fontes de preço e análises
async def eth(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("ETH radar ativo. (pré-deploy).")

async def btc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("BTC radar ativo. (pré-deploy).")

def main():
    if not TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN ausente nas variáveis de ambiente.")
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("ping", ping))
    app.add_handler(CommandHandler("eth", eth))
    app.add_handler(CommandHandler("btc", btc))

    logger.info("Iniciando StarkRadar (polling)...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()

import os
import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ---------- Config ----------
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")  # opcional (para push automáticos)
PORT = int(os.getenv("PORT", "10000"))
EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")  # Render injeta isso

# ---------- Log ----------
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("starkradar-bot")

# ---------- Handlers ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("✅ Stark DeFi Brain online. Envie /eth ou /btc.")

async def eth(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Placeholder simples (a análise será plugada depois)
    await update.message.reply_text("📊 ETH: preparando análise… (placeholder)")

async def btc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📊 BTC: preparando análise… (placeholder)")

def build_application() -> Application:
    if not TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN não encontrado nas variáveis de ambiente.")
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("eth", eth))
    app.add_handler(CommandHandler("btc", btc))

    return app

if __name__ == "__main__":
    app = build_application()

    if not EXTERNAL_URL:
        # Fallback: se por algum motivo o Render não injetar a URL, ainda assim subimos o webhook
        log.warning("RENDER_EXTERNAL_URL não definido; tentei continuar mesmo assim.")
        EXTERNAL_URL = "https://starkradar-bot.onrender.com"  # ajuste se necessário

    # url_path precisa ser único (usamos o próprio token)
    url_path = f"{TOKEN}"
    webhook_url = f"{EXTERNAL_URL}/{url_path}"

    log.info("Subindo webhook: %s", webhook_url)

    # No PTB v20, o método correto é run_webhook com 'url_path'
    app.run_polling(drop_pending_updates=True, close_loop=False)

import os
import logging
from datetime import datetime
from typing import Optional

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# ------------------------
# Config & Logging
# ------------------------
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("starkradar")

TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
ALLOWED_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")  # opcional (string)
PORT = int(os.environ.get("PORT", "10000"))

# Render define essa variável automaticamente na Web Service
BASE_URL = os.environ.get("RENDER_EXTERNAL_URL") or os.environ.get("BASE_URL")
if BASE_URL:
    BASE_URL = BASE_URL.rstrip("/")

if not TOKEN:
    raise RuntimeError("Faltou TELEGRAM_BOT_TOKEN no Environment do Render.")

# url path único por segurança
URL_PATH = f"webhook/{TOKEN}"


def _authorized(update: Update) -> bool:
    """Se TELEGRAM_CHAT_ID estiver setado, só aceita esse chat."""
    if not ALLOWED_CHAT_ID:
        return True
    try:
        return str(update.effective_chat.id) == str(ALLOWED_CHAT_ID)
    except Exception:
        return False


# ------------------------
# Handlers
# ------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update):
        return
    msg = (
        "✅ Stark DeFi Brain online.\n"
        "Comandos disponíveis:\n"
        "• /eth – visão rápida do ETH\n"
        "• /btc – visão rápida do BTC\n"
        "• /help – ajuda"
    )
    await update.message.reply_text(msg)


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update):
        return
    await update.message.reply_text(
        "Ajuda:\n"
        "• /eth – snapshot técnico do Ethereum (placeholder)\n"
        "• /btc – snapshot técnico do Bitcoin (placeholder)\n"
        "• /start – status do bot"
    )


async def eth(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update):
        return
    # placeholder – aqui você poderá plugar sua análise real
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    await update.message.reply_text(
        f"📊 *ETH:* preparando análise… (placeholder)\n"
        f"_timestamp: {now}_",
        parse_mode=ParseMode.MARKDOWN,
    )


async def btc(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update):
        return
    # placeholder – aqui você poderá plugar sua análise real
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    await update.message.reply_text(
        f"📊 *BTC:* preparando análise… (placeholder)\n"
        f"_timestamp: {now}_",
        parse_mode=ParseMode.MARKDOWN,
    )


# ------------------------
# Bootstrap
# ------------------------
async def set_my_commands(app: Application) -> None:
    try:
        await app.bot.set_my_commands(
            [
                ("start", "Status do bot"),
                ("eth", "Snapshot do Ethereum"),
                ("btc", "Snapshot do Bitcoin"),
                ("help", "Ajuda e comandos"),
            ]
        )
        logger.info("Comandos do bot registrados.")
    except Exception as e:
        logger.exception("Falha ao registrar comandos: %s", e)


async def main() -> None:
    app = Application.builder().token(TOKEN).concurrent_updates(True).build()

    # Handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("eth", eth))
    app.add_handler(CommandHandler("btc", btc))

    await set_my_commands(app)

    if not BASE_URL:
        raise RuntimeError(
            "Faltou RENDER_EXTERNAL_URL (Render) ou BASE_URL no Environment."
        )

    webhook_url = f"{BASE_URL}/{URL_PATH}"
    logger.info("Iniciando webhook em %s", webhook_url)

    # Configura e sobe o servidor webhook (sem polling)
    await app.bot.set_webhook(
        url=webhook_url,
        drop_pending_updates=True,
        allowed_updates=["message", "edited_message"],
    )

    # Servidor aiohttp embutido do PTB
    await app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=URL_PATH,          # path local
        webhook_url=webhook_url,    # URL pública
        stop_signals=None,
    )


if __name__ == "__main__":
    import asyncio

    try:
        asyncio.run(main())
    except (SystemExit, KeyboardInterrupt):
        logger.info("Bot finalizado.")

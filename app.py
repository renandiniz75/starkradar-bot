import os
import logging
from datetime import datetime, timezone

import httpx
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# -------------------------------------------------------
# Config
# -------------------------------------------------------
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

PORT = int(os.environ.get("PORT", "10000"))
BASE_URL = os.environ.get("RENDER_EXTERNAL_URL", "https://starkradar-bot.onrender.com").rstrip("/")
WEBHOOK_PATH = "/webhook"
WEBHOOK_URL  = f"{BASE_URL}{WEBHOOK_PATH}"

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("starkradar-bybit")

# -------------------------------------------------------
# Bybit public API (v5)
# -------------------------------------------------------
BYBIT_TICKER_URL = "https://api.bybit.com/v5/market/tickers"

async def fetch_bybit_ticker(symbol: str, category: str = "linear") -> dict | None:
    params = {"category": category, "symbol": symbol}
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            r = await client.get(BYBIT_TICKER_URL, params=params)
            r.raise_for_status()
            data = r.json()
    except Exception as e:
        log.warning(f"HTTP error Bybit {symbol}: {e}")
        return None

    try:
        lst = data.get("result", {}).get("list", [])
        if not lst:
            return None
        t = lst[0]
        return {
            "price": float(t.get("lastPrice") or 0.0),
            "change_24h": float(t.get("price24hPcnt") or 0.0) * 100.0,  # fração -> %
            "high_24h": float(t.get("highPrice24h") or 0.0),
            "low_24h": float(t.get("lowPrice24h") or 0.0),
            "turnover_24h": float(t.get("turnover24h") or 0.0),
        }
    except Exception as e:
        log.exception(f"Parse error Bybit snapshot {symbol}: {e}")
        return None

def fmt_money(x: float) -> str:
    return f"${x:,.2f}"

def arrow(pct: float) -> str:
    if pct > 0.3:  return "🟢⬆️"
    if pct < -0.3: return "🔴⬇️"
    return "🟡➡️"

def now_br() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%d/%m %H:%M")

# -------------------------------------------------------
# Handlers
# -------------------------------------------------------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("✅ Stark DeFi Brain online. Envie /eth ou /btc.")

async def cmd_eth(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("📊 ETH: preparando análise…")
    snap = await fetch_bybit_ticker("ETHUSDT", category="linear")
    if not snap:
        await update.message.reply_text("⚠️ Bybit indisponível agora. Tente em instantes.")
        return
    txt = (
        f"📈 **ETH/USDT (Bybit perp)** — {now_br()}\n"
        f"Preço: {fmt_money(snap['price'])}  {arrow(snap['change_24h'])}\n"
        f"24h: {snap['change_24h']:.2f}% | Alta: {fmt_money(snap['high_24h'])} | "
        f"Baixa: {fmt_money(snap['low_24h'])}\n"
        f"Turnover 24h (USD): {snap['turnover_24h']:,.0f}\n"
        f"\n*Fonte:* Bybit v5 Market Tickers."
    )
    # escapa '-' no MarkdownV2
    await update.message.reply_markdown_v2(txt.replace("-", "\\-"))

async def cmd_btc(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("📊 BTC: preparando análise…")
    snap = await fetch_bybit_ticker("BTCUSDT", category="linear")
    if not snap:
        await update.message.reply_text("⚠️ Bybit indisponível agora. Tente em instantes.")
        return
    txt = (
        f"📈 **BTC/USDT (Bybit perp)** — {now_br()}\n"
        f"Preço: {fmt_money(snap['price'])}  {arrow(snap['change_24h'])}\n"
        f"24h: {snap['change_24h']:.2f}% | Alta: {fmt_money(snap['high_24h'])} | "
        f"Baixa: {fmt_money(snap['low_24h'])}\n"
        f"Turnover 24h (USD): {snap['turnover_24h']:,.0f}\n"
        f"\n*Fonte:* Bybit v5 Market Tickers."
    )
    await update.message.reply_markdown_v2(txt.replace("-", "\\-"))

# -------------------------------------------------------
# Bootstrap / Webhook  (SEM asyncio.run)
# -------------------------------------------------------
def build_app() -> Application:
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN ausente")
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("eth", cmd_eth))
    app.add_handler(CommandHandler("btc", cmd_btc))
    return app

if __name__ == "__main__":
    application = build_app()
    # run_webhook gerencia o próprio loop; não usar asyncio.run aqui.
    application.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=WEBHOOK_PATH.lstrip("/"),
        webhook_url=WEBHOOK_URL,
    )

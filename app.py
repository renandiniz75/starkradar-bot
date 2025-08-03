import asyncio, os
import httpx
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", "0"))

ETH_ALERT_LEVELS = [3468, 3420, 3398, 3560]
BTC_ALERT_LEVELS = [113700, 114200, 115300]

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🚀 StarkRadar ativo! Comandos: /eth /btc")

async def eth(update: Update, context: ContextTypes.DEFAULT_TYPE):
    p = await get_price("ETHUSDT")
    await update.message.reply_text(f"📊 ETH: {p:.2f} | níveis: 3.450 / 3.420 / 3.398 / 3.560")

async def btc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    p = await get_price("BTCUSDT")
    await update.message.reply_text(f"📊 BTC: {p:.0f} | níveis: 113.7k / 114.2k / 115.3k")

async def get_price(symbol: str) -> float:
    url = f"https://api.bybit.com/v5/market/tickers?category=linear&symbol={symbol}"
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(url)
        return float(r.json()["result"]["list"][0]["lastPrice"])

async def alert_loop(app):
    last_hit_eth = set()
    last_hit_btc = set()
    while True:
        try:
            eth = await get_price("ETHUSDT")
            btc = await get_price("BTCUSDT")

            for lvl in ETH_ALERT_LEVELS:
                if ((lvl < 3500 and eth <= lvl) or (lvl > 3500 and eth >= lvl)) and lvl not in last_hit_eth:
                    await app.bot.send_message(CHAT_ID, f"⚠️ ETH gatilho {lvl} | preço {eth:.2f}")
                    last_hit_eth.add(lvl)

            for lvl in BTC_ALERT_LEVELS:
                if ((lvl < 114000 and btc <= lvl) or (lvl > 114000 and btc >= lvl)) and lvl not in last_hit_btc:
                    await app.bot.send_message(CHAT_ID, f"⚠️ BTC gatilho {lvl} | preço {btc:.0f}")
                    last_hit_btc.add(lvl)

        except Exception as e:
            print("loop error:", e)
        await asyncio.sleep(30)

async def main():
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("eth", eth))
    app.add_handler(CommandHandler("btc", btc))
    asyncio.create_task(alert_loop(app))
    print("🤖 StarkRadar 24/7 iniciado")
    await app.run_polling()

if __name__ == "__main__":
    asyncio.run(main())

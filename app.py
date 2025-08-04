# app.py
import os
import logging
import asyncio
from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo

import httpx
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ---------------- LOGGING ----------------
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("starkradar-bot")

# ---------------- ENV ----------------
TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = int(os.environ["TELEGRAM_CHAT_ID"])
BASE_URL = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"{BASE_URL}{WEBHOOK_PATH}" if BASE_URL else None

TZ = ZoneInfo("America/Sao_Paulo")  # BRT

# ---------------- HTTP ----------------
COMMON_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/126.0 Safari/537.36",
    "Accept": "application/json",
}
TIMEOUT = httpx.Timeout(10, connect=10)
CLIENT = httpx.AsyncClient(timeout=TIMEOUT, headers=COMMON_HEADERS)

# ------------- FEEDS (PRIMARY + FALLBACKS) -------------
async def _binance_24h(symbol: str) -> dict:
    """
    Binance pública: https://api.binance.com/api/v3/ticker/24hr?symbol=ETHUSDT
    Retorna price/high/low/pct/vol (quoteVolume).
    """
    url = "https://api.binance.com/api/v3/ticker/24hr"
    params = {"symbol": symbol}
    r = await CLIENT.get(url, params=params)
    r.raise_for_status()
    j = r.json()
    price = float(j["lastPrice"])
    high = float(j["highPrice"])
    low = float(j["lowPrice"])
    pct = float(j["priceChangePercent"])
    vol = float(j.get("quoteVolume", 0.0))  # USD notional
    return {"price": price, "high": high, "low": low, "pct": pct, "vol": vol, "src": "Binance"}

async def _coinbase_ticker(product: str) -> dict:
    """
    Coinbase público:
      - ticker -> last price
      - stats  -> high/low/open/volume (24h)
    """
    async with httpx.AsyncClient(timeout=TIMEOUT, headers=COMMON_HEADERS) as cb:
        t_resp = await cb.get(f"https://api.exchange.coinbase.com/products/{product}/ticker")
        t_resp.raise_for_status()
        s_resp = await cb.get(f"https://api.exchange.coinbase.com/products/{product}/stats")
        s_resp.raise_for_status()
        t = t_resp.json()
        s = s_resp.json()
        price = float(t["price"])
        high = float(s["high"])
        low = float(s["low"])
        open_ = float(s["open"]) if s.get("open") not in (None, "") else price
        pct = ((price - open_) / open_) * 100 if open_ else 0.0
        vol = float(s.get("volume", 0.0))
        return {"price": price, "high": high, "low": low, "pct": pct, "vol": vol, "src": "Coinbase"}

async def _kraken_ticker(pair: str) -> dict:
    url = "https://api.kraken.com/0/public/Ticker"
    params = {"pair": pair}
    async with httpx.AsyncClient(timeout=TIMEOUT, headers=COMMON_HEADERS) as k:
        r = await k.get(url, params=params)
        r.raise_for_status()
        j = r.json()
        res = j["result"]
        kpair = next(iter(res))
        d = res[kpair]
        price = float(d["c"][0])
        high = float(d["h"][1])
        low = float(d["l"][1])
        mid = (high + low) / 2
        pct = (price - mid) / mid * 100 if mid else 0.0
        vol = float(d["v"][1])
        return {"price": price, "high": high, "low": low, "pct": pct, "vol": vol, "src": "Kraken"}

async def _coingecko_simple(coin_id: str) -> dict:
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}"
    params = {
        "localization": "false", "tickers": "false", "market_data": "true",
        "community_data": "false", "developer_data": "false", "sparkline": "false",
    }
    async with httpx.AsyncClient(timeout=TIMEOUT, headers=COMMON_HEADERS) as cg:
        r = await cg.get(url, params=params)
        r.raise_for_status()
        j = r.json()
        md = j["market_data"]
        price = float(md["current_price"]["usd"])
        high = float(md["high_24h"]["usd"])
        low = float(md["low_24h"]["usd"])
        pct = float(md["price_change_percentage_24h"])
        vol = float(md["total_volume"]["usd"])
        return {"price": price, "high": high, "low": low, "pct": pct, "vol": vol, "src": "CoinGecko"}

async def get_eth_stats() -> dict:
    # Ordem: Binance -> Coinbase -> Kraken -> CoinGecko
    try:
        return await _binance_24h("ETHUSDT")
    except Exception as e:
        log.warning(f"Binance ETH falhou: {e}")
    try:
        return await _coinbase_ticker("ETH-USD")
    except Exception as e:
        log.warning(f"Coinbase ETH falhou: {e}")
    try:
        return await _kraken_ticker("ETHUSD")
    except Exception as e:
        log.warning(f"Kraken ETH falhou: {e}")
    return await _coingecko_simple("ethereum")

async def get_btc_stats() -> dict:
    try:
        return await _binance_24h("BTCUSDT")
    except Exception as e:
        log.warning(f"Binance BTC falhou: {e}")
    try:
        return await _coinbase_ticker("BTC-USD")
    except Exception as e:
        log.warning(f"Coinbase BTC falhou: {e}")
    try:
        return await _kraken_ticker("XBTUSD")
    except Exception as e:
        log.warning(f"Kraken BTC falhou: {e}")
    return await _coingecko_simple("bitcoin")

# ------------- FORMATADOR -------------
def fmt(name: str, s: dict) -> str:
    arrow = "🔺" if s["pct"] >= 0 else "🔻"
    return (
        f"📊 {name} — fonte: {s['src']}\n"
        f"• Preço: ${s['price']:,.2f}\n"
        f"• 24h: {arrow} {s['pct']:.2f}%   (Alta: ${s['high']:,.0f} | Baixa: ${s['low']:,.0f})\n"
        f"• Vol (24h): {s['vol']:,.0f}\n"
        f"• {datetime.now(timezone.utc).astimezone(TZ).strftime('%d/%m %H:%M %Z')}"
    )

# ------------- HANDLERS -------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("✅ Stark DeFi Brain online. Envie /eth ou /btc.")

async def cmd_eth(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat.id
    await context.bot.send_message(chat_id=chat, text="📊 ETH: preparando análise…")
    try:
        s = await get_eth_stats()
        await context.bot.send_message(chat_id=chat, text=fmt("ETH", s))
    except Exception as e:
        log.exception("ETH handler error")
        await context.bot.send_message(chat_id=chat, text=f"⚠️ Falha ao obter dados. ({e})")

async def cmd_btc(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat.id
    await context.bot.send_message(chat_id=chat, text="📊 BTC: preparando análise…")
    try:
        s = await get_btc_stats()
        await context.bot.send_message(chat_id=chat, text=fmt("BTC", s))
    except Exception as e:
        log.exception("BTC handler error")
        await context.bot.send_message(chat_id=chat, text=f"⚠️ Falha ao obter dados. ({e})")

# ------------- BOLETINS (08:00, 12:00, 17:00, 19:00 BRT) -------------
async def send_report(context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        eth = await get_eth_stats()
        btc = await get_btc_stats()
        txt = "⏱️ Boletim:\n\n" + fmt("ETH", eth) + "\n\n" + fmt("BTC", btc)
        await context.bot.send_message(chat_id=CHAT_ID, text=txt)
    except Exception as e:
        log.exception("send_report error")

def schedule_jobs(app: Application) -> None:
    jq = app.job_queue
    for hh, mm in [(8, 0), (12, 0), (17, 0), (19, 0)]:
        jq.run_daily(send_report, time=time(hour=hh, minute=mm, tzinfo=TZ), name=f"rep_{hh:02d}{mm:02d}")
    log.info("Boletins agendados (BRT): 08:00, 12:00, 17:00, 19:00")

# ------------- BOOTSTRAP -------------
async def main():
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("eth", cmd_eth))
    app.add_handler(CommandHandler("btc", cmd_btc))

    schedule_jobs(app)

    if WEBHOOK_URL:
        log.info(f"Setting webhook to: {WEBHOOK_URL}{WEBHOOK_PATH}")
        await app.bot.set_webhook(url=f"{WEBHOOK_URL}{WEBHOOK_PATH}")
        await app.run_webhook(
            listen="0.0.0.0",
            port=int(os.environ.get("PORT", "10000")),
            url_path="webhook",
            webhook_url=f"{WEBHOOK_URL}{WEBHOOK_PATH}",
        )
    else:
        log.info("WEBHOOK_URL não setado -> polling.")
        await app.run_polling()

if __name__ == "__main__":
    asyncio.run(main())

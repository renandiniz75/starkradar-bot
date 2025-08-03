import os
import asyncio
import logging
from datetime import datetime, timezone

import aiohttp
import pandas as pd
import numpy as np
from ta.momentum import RSIIndicator
from ta.trend import EMAIndicator

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# ---------------------------------------
# Config
# ---------------------------------------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
PORT = int(os.getenv("PORT", "10000"))
BASE_URL = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")  # Render injeta essa URL
WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"{BASE_URL}{WEBHOOK_PATH}" if BASE_URL else None

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("stark-bot")


# ---------------------------------------
# Utils: fetch de dados e análise
# ---------------------------------------
BINANCE_KLINES = "https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"

async def fetch_klines(symbol: str, interval: str = "1h", limit: int = 500) -> pd.DataFrame:
    url = BINANCE_KLINES.format(symbol=symbol, interval=interval, limit=limit)
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url, timeout=20) as r:
            r.raise_for_status()
            data = await r.json()

    cols = [
        "open_time","open","high","low","close","volume",
        "close_time","qav","trades","taker_base","taker_quote","ignore"
    ]
    df = pd.DataFrame(data, columns=cols)
    for c in ["open","high","low","close","volume"]:
        df[c] = df[c].astype(float)

    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    return df


def analyze_symbol(df: pd.DataFrame, tf_label: str) -> dict:
    """Retorna métricas e níveis para compor a mensagem."""
    close = df["close"]
    high = df["high"]
    low = df["low"]
    vol = df["volume"]

    # Indicadores
    rsi_val = float(RSIIndicator(close, window=14).rsi().iloc[-1])
    ema20 = float(EMAIndicator(close, window=20).ema_indicator().iloc[-1])
    ema50 = float(EMAIndicator(close, window=50).ema_indicator().iloc[-1])
    price = float(close.iloc[-1])

    # Tendência simples
    trend = "⬆️ alta" if ema20 > ema50 else "⬇️ baixa"

    # Suportes/resistências simples (últimos 120 candles)
    lookback = min(120, len(df))
    z = df.tail(lookback)
    s1 = float(z["low"].quantile(0.10))
    s2 = float(z["low"].min())
    r1 = float(z["high"].quantile(0.90))
    r2 = float(z["high"].max())

    # Formatação
    def f(x): 
        return f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X",".")

    # Interpretação do RSI
    if rsi_val < 30:
        rsi_msg = "sobrevendido (potencial repique / compra na fraqueza)"
    elif rsi_val < 40:
        rsi_msg = "fraco (ainda vendedor, mas perto de região de possível defesa)"
    elif rsi_val > 70:
        rsi_msg = "sobrecomprado (risco de realização / reduzir risco)"
    elif rsi_val > 60:
        rsi_msg = "forte (tendência favorável, mas sem euforia)"
    else:
        rsi_msg = "neutro"

    return {
        "tf": tf_label,
        "price": f(price),
        "rsi": f"{rsi_val:.1f}",
        "rsi_msg": rsi_msg,
        "ema20": f(ema20),
        "ema50": f(ema50),
        "trend": trend,
        "s1": f(s1), "s2": f(s2),
        "r1": f(r1), "r2": f(r2),
        "vol": f(float(vol.tail(20).mean())),
    }


def compose_message(symbol: str, h1: dict, m15: dict) -> str:
    now = datetime.now(timezone.utc).strftime("%d/%m %H:%M UTC")
    title = "📊 Análise {} — {}".format(
        "Ethereum (ETH)" if symbol == "ETHUSDT" else "Bitcoin (BTC)", now
    )

    def block(d: dict) -> str:
        return (
            f"• **{d['tf']}**\n"
            f"  • Preço: **${d['price']}**\n"
            f"  • RSI(14): **{d['rsi']}** → {d['rsi_msg']}\n"
            f"  • EMA20/EMA50: **${d['ema20']} / ${d['ema50']}** → Tendência: {d['trend']}\n"
            f"  • Vol. médio(20): **{d['vol']}**\n"
            f"  • Suportes: **${d['s1']}** | **${d['s2']}**\n"
            f"  • Resistências: **${d['r1']}** | **${d['r2']}**\n"
        )

    # Gatilhos operacionais simples
    price = float(h1['price'].replace(".", "").replace(",", "."))  # reverter formatação
    s1 = float(h1['s1'].replace(".", "").replace(",", "."))
    r1 = float(h1['r1'].replace(".", "").replace(",", "."))
    if price < s1:
        call = "⚠️ Abaixo do suporte H1 — **evite aumentar risco**; considerar hedge/short tático."
    elif price > r1:
        call = "✅ Acima da 1ª resistência H1 — **tendência favorece compras** em recuos."
    else:
        call = "⏳ Zona intermediária — **esperar confirmação** (perda/recuperação de níveis)."

    msg = (
        f"{title}\n\n"
        f"{block(h1)}"
        f"{block(m15)}"
        f"**Leitura rápida:** {call}\n"
        f"—\n"
        f"ℹ️ Dados: Binance (klines). Indicadores: RSI(14), EMA20/50. Suportes/resistências calculados sobre os últimos 120 candles.\n"
    )
    return msg


# ---------------------------------------
# Handlers Telegram
# ---------------------------------------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("✅ Stark DeFi Brain online. Envie /eth ou /btc.")

async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("pong")

async def analyze_and_reply(symbol: str, update: Update):
    try:
        df_h1 = await fetch_klines(symbol, "1h", 450)
        df_m15 = await fetch_klines(symbol, "15m", 450)

        h1 = analyze_symbol(df_h1, "H1 (1 hora)")
        m15 = analyze_symbol(df_m15, "M15 (15 minutos)")

        msg = compose_message(symbol, h1, m15)
        await update.message.reply_markdown(msg, disable_web_page_preview=True)
    except Exception as e:
        log.exception("Erro ao analisar %s", symbol)
        await update.message.reply_text(f"Erro ao obter análise: {e}")

async def cmd_eth(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await analyze_and_reply("ETHUSDT", update)

async def cmd_btc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await analyze_and_reply("BTCUSDT", update)


# ---------------------------------------
# App/Server (webhook)
# ---------------------------------------
from aiohttp import web

async def handle_health(request):
    return web.json_response({"ok": True, "ts": datetime.utcnow().isoformat()})

async def handle_webhook(request):
    """Endpoint que recebe updates do Telegram."""
    try:
        app: Application = request.app["bot_app"]
        data = await request.json()
        update = Update.de_json(data, app.bot)
        await app.process_update(update)
        return web.Response(text="OK")
    except Exception as e:
        log.exception("Erro no webhook: %s", e)
        return web.Response(status=500, text=str(e))

async def on_startup(app: web.Application):
    """Registra webhook assim que o servidor sobe."""
    tg_app: Application = app["bot_app"]
    if WEBHOOK_URL:
        await tg_app.bot.set_webhook(url=WEBHOOK_URL, drop_pending_updates=True)
        log.info("Webhook set to %s", WEBHOOK_URL)
    else:
        log.warning("RENDER_EXTERNAL_URL ausente. Webhook não registrado.")

def build_telegram_app() -> Application:
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("eth", cmd_eth))
    app.add_handler(CommandHandler("btc", cmd_btc))
    return app

def build_web_app(tg_app: Application) -> web.Application:
    web_app = web.Application()
    web_app["bot_app"] = tg_app
    web_app.router.add_post(WEBHOOK_PATH, handle_webhook)
    web_app.router.add_get("/health", handle_health)
    web_app.on_startup.append(on_startup)
    return web_app

if __name__ == "__main__":
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN não configurado")

    application = build_telegram_app()
    web_app = build_web_app(application)

    log.info("Starting aiohttp app on port %s", PORT)
    web.run_app(web_app, host="0.0.0.0", port=PORT)

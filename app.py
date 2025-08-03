import os
import asyncio
import logging
from datetime import datetime, timezone

import aiohttp
from aiohttp import web
import pandas as pd
import numpy as np

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ---------------------------------------------------------
# Configuração básica
# ---------------------------------------------------------
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("starkradar")

TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]

# Porta para Render (Render injeta PORT). Local default=10000.
PORT = int(os.getenv("PORT", "10000"))

# Base pública do webhook:
# 1) Preferimos RENDER_EXTERNAL_URL (fornecida pelo Render)
# 2) Se não existir, tentamos WEBHOOK_BASE (variável manual)
# 3) Opcionalmente, você pode trocar pelo domínio do Render fixo.
BASE_URL = os.getenv("RENDER_EXTERNAL_URL") or os.getenv("WEBHOOK_BASE") or ""
WEBHOOK_PATH = "/webhook"
if not BASE_URL:
    # fallback; substitua pela sua URL de serviço se quiser fixar
    BASE_URL = "https://starkradar-bot.onrender.com"
WEBHOOK_URL = f"{BASE_URL}{WEBHOOK_PATH}"

# ---------------------------------------------------------
# Funções auxiliares: EMA e RSI sem 'ta'
# ---------------------------------------------------------
def ema(series: pd.Series, window: int) -> pd.Series:
    return series.ewm(span=window, adjust=False).mean()

def rsi(series: pd.Series, window: int = 14) -> pd.Series:
    delta = series.diff()
    up = np.where(delta > 0, delta, 0.0)
    down = np.where(delta < 0, -delta, 0.0)
    roll_up = pd.Series(up, index=series.index).ewm(span=window, adjust=False).mean()
    roll_down = pd.Series(down, index=series.index).ewm(span=window, adjust=False).mean()
    rs = roll_up / (roll_down + 1e-9)
    return 100.0 - (100.0 / (1.0 + rs))

# ---------------------------------------------------------
# Coleta de OHLC da Bybit (categoria linear, par *USDT)
# ---------------------------------------------------------
async def fetch_ohlc(symbol: str, interval: str = "60", limit: int = 300) -> pd.DataFrame:
    """
    symbol: 'ETH' ou 'BTC'
    interval: '60' (1h), '15' (15m), '240' (4h)
    """
    url = "https://api.bybit.com/v5/market/kline"
    params = {
        "category": "linear",
        "symbol": f"{symbol}USDT",
        "interval": interval,
        "limit": str(limit),
    }
    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url, params=params) as resp:
            data = await resp.json()
            if data.get("retCode") != 0:
                raise RuntimeError(f"Bybit error: {data}")
            rows = data["result"]["list"]  # mais recente primeiro
            rows = list(reversed(rows))    # invertendo para cronológico
            # [start, open, high, low, close, volume, turnover]
            df = pd.DataFrame(
                rows,
                columns=["start", "open", "high", "low", "close", "volume", "turnover"],
            )
            df["time"] = pd.to_datetime(df["start"].astype("int64"), unit="ms", utc=True)
            for col in ["open", "high", "low", "close", "volume"]:
                df[col] = df[col].astype(float)
            return df

# ---------------------------------------------------------
# Identificação de suportes e resistências (pivôs simples)
# ---------------------------------------------------------
def get_levels(close: pd.Series, window: int = 12, n_each: int = 3):
    """
    Usa janelas móveis centradas para localizar mínimas/máximas locais.
    Retorna até n_each suportes e resistências próximos.
    """
    min_roll = close.rolling(window, center=True).min()
    max_roll = close.rolling(window, center=True).max()

    support_points = close[(close == min_roll)].dropna()
    resist_points  = close[(close == max_roll)].dropna()

    supports = sorted(support_points.tail(10).values)[:n_each]
    resistances = sorted(resist_points.tail(10).values, reverse=True)[:n_each]
    return supports, resistances

# ---------------------------------------------------------
# Análise consolidada
# ---------------------------------------------------------
async def analyze_symbol(symbol: str) -> str:
    """
    Retorna um texto final da análise (1h) para ETH ou BTC.
    """
    try:
        df = await fetch_ohlc(symbol, interval="60", limit=400)
    except Exception as e:
        logger.exception("Erro ao buscar OHLC: %s", e)
        return f"❌ {symbol}: falha ao obter dados ({e})."

    if len(df) < 60:
        return f"⚠️ {symbol}: dados insuficientes para análise."

    close = df["close"]
    price = float(close.iloc[-1])
    ts = df["time"].iloc[-1].strftime("%Y-%m-%d %H:%M UTC")

    # Variação 24h (24 candles de 1h)
    try:
        change_24h = (price / float(close.iloc[-24]) - 1.0) * 100.0
    except Exception:
        change_24h = np.nan

    # Indicadores
    rsi14 = float(rsi(close, 14).iloc[-1])
    ema20 = float(ema(close, 20).iloc[-1])
    ema50 = float(ema(close, 50).iloc[-1])
    trend = "tendência de alta" if ema20 > ema50 else "tendência de baixa"

    # Suportes e resistências
    supports, resistances = get_levels(close, window=14, n_each=3)

    # Interpretação rápida do RSI
    if rsi14 >= 70:
        rsi_view = "sobrecomprado"
    elif rsi14 <= 30:
        rsi_view = "sobrevendido"
    else:
        rsi_view = "neutro"

    sup_txt = ", ".join([f"${lvl:,.0f}" for lvl in supports]) if supports else "—"
    res_txt = ", ".join([f"${lvl:,.0f}" for lvl in resistances]) if resistances else "—"

    txt = (
        f"📊 **{symbol}/USDT (1h)**\n"
        f"• Preço: **${price:,.2f}**  |  24h: **{change_24h:+.2f}%**\n"
        f"• RSI(14): **{rsi14:.1f}** ({rsi_view})\n"
        f"• EMA20: **${ema20:,.2f}**  |  EMA50: **${ema50:,.2f}**  → **{trend}**\n"
        f"• Suportes: {sup_txt}\n"
        f"• Resistências: {res_txt}\n"
        f"⏱ {ts}"
    )
    return txt

# ---------------------------------------------------------
# Handlers Telegram
# ---------------------------------------------------------
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = (
        "✅ Stark DeFi Brain online.\n"
        "Envie /eth ou /btc para análise técnica (1h)."
    )
    await update.message.reply_text(msg, disable_web_page_preview=True)

async def eth_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("📈 ETH: preparando análise…")
    text = await analyze_symbol("ETH")
    await update.message.reply_text(text, parse_mode="Markdown", disable_web_page_preview=True)

async def btc_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("📈 BTC: preparando análise…")
    text = await analyze_symbol("BTC")
    await update.message.reply_text(text, parse_mode="Markdown", disable_web_page_preview=True)

# ---------------------------------------------------------
# Aiohttp healthcheck (opcional) + webhook do PTB
# ---------------------------------------------------------
async def health(request: web.Request):
    return web.Response(text="ok")

def main():
    application = Application.builder().token(TOKEN).build()

    # Comandos
    application.add_handler(CommandHandler("start", start_handler))
    application.add_handler(CommandHandler("eth", eth_handler))
    application.add_handler(CommandHandler("btc", btc_handler))

    # Aiohttp app para /health
    webapp = web.Application()
    webapp.router.add_get("/health", health)

    logger.info("Iniciando webhook em %s", WEBHOOK_URL)
    application.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        webhook_url=WEBHOOK_URL,   # PTB 20.3: use 'webhook_url' (não 'webhook_path')
        web_app=webapp,            # reutilizamos o servidor para expor /health
    )

if __name__ == "__main__":
    main()

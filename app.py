# app.py — Stark DeFi Agent (Railway)
# FastAPI + APScheduler + asyncpg + httpx
# Rotas: /healthz, /pulse (GET), /webhook (POST para Telegram)

import os
import math
import datetime as dt
from zoneinfo import ZoneInfo
from typing import Optional

import httpx
import asyncpg
from fastapi import FastAPI, Request
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ---------- Config ----------
TZ = ZoneInfo(os.getenv("TZ", "America/Sao_Paulo"))
DB_URL = os.getenv("DATABASE_URL")
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "")
SEND_ENABLED = bool(TG_TOKEN)  # chat pode vir do webhook

ETH_HEDGE_1 = float(os.getenv("ETH_HEDGE_1", "3900"))
ETH_HEDGE_2 = float(os.getenv("ETH_HEDGE_2", "3800"))
ETH_CLOSE   = float(os.getenv("ETH_CLOSE_HEDGE", "3950"))

BYBIT_SPOT = "https://api.bybit.com/v5/market/tickers?category=spot&symbol={sym}"
BYBIT_FUND = "https://api.bybit.com/v5/market/funding/history?category=linear&symbol=ETHUSDT&limit=1"
BYBIT_OI   = "https://api.bybit.com/v5/market/open-interest?category=linear&symbol=ETHUSDT&interval=5min"
TG_SEND    = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"

app = FastAPI(title="stark-defi-agent")
pool: Optional[asyncpg.Pool] = None
scheduler = AsyncIOScheduler(timezone=str(TZ))

# ---------- DB Schema ----------
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS candles_minute(
  ts timestamptz NOT NULL,
  symbol text NOT NULL,
  open numeric, high numeric, low numeric, close numeric, volume numeric,
  PRIMARY KEY (ts, symbol)
);
CREATE TABLE IF NOT EXISTS derivatives_snap(
  ts timestamptz NOT NULL,
  symbol text NOT NULL,
  exchange text NOT NULL,
  funding numeric,
  open_interest numeric,
  PRIMARY KEY (ts, symbol, exchange)
);
CREATE TABLE IF NOT EXISTS market_rel(
  ts timestamptz PRIMARY KEY,
  eth_usd numeric, btc_usd numeric, eth_btc_ratio numeric
);
"""

async def db_init():
    global pool
    if not DB_URL:
        raise RuntimeError("DATABASE_URL não definido no ambiente.")
    pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=5)
    async with pool.acquire() as c:
        await c.execute(CREATE_SQL)

# ---------- Helpers ----------
async def fetch_json(url: str):
    async with httpx.AsyncClient(timeout=12) as s:
        r = await s.get(url)
        r.raise_for_status()
        return r.json()

async def send_tg(text: str, chat_id: Optional[str] = None):
    """Envia mensagem ao Telegram. Usa chat_id do webhook se for passado; senão, TG_CHAT fixo."""
    if not SEND_ENABLED:
        return
    cid = chat_id or TG_CHAT
    if not cid:
        return
    async with httpx.AsyncClient(timeout=12) as s:
        await s.post(TG_SEND, json={"chat_id": cid, "text": text})

def action_line(eth_price: float) -> str:
    if eth_price < ETH_HEDGE_2:
        return f"🚨 ETH < {ETH_HEDGE_2:.0f} → ampliar hedge p/ 20% (29 ETH)."
    if eth_price < ETH_HEDGE_1:
        return f"⚠️ ETH < {ETH_HEDGE_1:.0f} → ativar hedge 15% (22 ETH)."
    if eth_price > ETH_CLOSE:
        return f"↩️ ETH > {ETH_CLOSE:.0f} → avaliar fechar hedge."
    return "✅ Sem gatilho. Suportes: 4.200/4.000 | Resist: 4.300/4.400."

# ---------- Job de ingestão (1m) ----------
async def ingest_1m():
    now = dt.datetime.now(dt.UTC).replace(second=0, microsecond=0)

    # Preços spot ETH/BTC
    try:
        eth = (await fetch_json(BYBIT_SPOT.format(sym="ETHUSDT")))["result"]["list"][0]
        btc = (await fetch_json(BYBIT_SPOT.format(sym="BTCUSDT")))["result"]["list"][0]
        eth_p = float(eth["lastPrice"]); btc_p = float(btc["lastPrice"])
        eth_h = float(eth["highPrice"]);  eth_l = float(eth["lowPrice"])
        btc_h = float(btc["highPrice"]);  btc_l = float(btc["lowPrice"])
        ethbtc = eth_p / btc_p
    except Exception:
        # Fallback simples: CoinGecko
        cg = await fetch_json("https://api.coingecko.com/api/v3/simple/price?ids=ethereum,bitcoin&vs_currencies=usd")
        eth_p = float(cg["ethereum"]["usd"]); btc_p = float(cg["bitcoin"]["usd"])
        eth_h = eth_l = btc_h = btc_l = math.nan
        ethbtc = eth_p / btc_p

    # Funding & OI
    try:
        f = (await fetch_json(BYBIT_FUND))["result"]["list"][0]
        funding = float(f["fundingRate"])
    except Exception:
        funding = None
    try:
        oi = (await fetch_json(BYBIT_OI))["result"]["list"][-1]
        open_interest = float(oi["openInterest"])
    except Exception:
        open_interest = None

    # Grava no banco
    async with pool.acquire() as c:
        await c.execute(
            "INSERT INTO candles_minute(ts,symbol,open,high,low,close,volume) "
            "VALUES($1,$2,NULL,$3,$4,$5,NULL) ON CONFLICT (ts,symbol) DO UPDATE "
            "SET high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close",
            now, "ETHUSDT", eth_h, eth_l, eth_p
        )
        await c.execute(
            "INSERT INTO candles_minute(ts,symbol,open,high,low,close,volume) "
            "VALUES($1,$2,NULL,$3,$4,$5,NULL) ON CONFLICT (ts,symbol) DO UPDATE "
            "SET high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close",
            now, "BTCUSDT", btc_h, btc_l, btc_p
        )
        await c.execute(
            "INSERT INTO market_rel(ts,eth_usd,btc_usd,eth_btc_ratio) "
            "VALUES($1,$2,$3,$4) ON CONFLICT (ts) DO UPDATE "
            "SET eth_usd=EXCLUDED.eth_usd, btc_usd=EXCLUDED.btc_usd, eth_btc_ratio=EXCLUDED.eth_btc_ratio",
            now, eth_p, btc_p, ethbtc
        )
        await c.execute(
            "INSERT INTO derivatives_snap(ts,symbol,exchange,funding,open_interest) "
            "VALUES($1,$2,$3,$4,$5) ON CONFLICT (ts,symbol,exchange) DO UPDATE "
            "SET funding=EXCLUDED.funding, open_interest=EXCLUDED.open_interest",
            now, "ETHUSDT", "bybit", funding, open_interest
        )

# ---------- Texto do panorama ----------
async def latest_pulse_text() -> str:
    async with pool.acquire() as c:
        m = await c.fetchrow("SELECT * FROM market_rel ORDER BY ts DESC LIMIT 1")
        e = await c.fetchrow("SELECT * FROM candles_minute WHERE symbol='ETHUSDT' ORDER BY ts DESC LIMIT 1")
        b = await c.fetchrow("SELECT * FROM candles_minute WHERE symbol='BTCUSDT' ORDER BY ts DESC LIMIT 1")
        d = await c.fetchrow("SELECT * FROM derivatives_snap WHERE symbol='ETHUSDT' AND exchange='bybit' ORDER BY ts DESC LIMIT 1")
    if not (m and e and b):
        return "⏳ Aguardando primeiros dados…"
    now = dt.datetime.now(TZ).strftime("%Y-%m-%d %H:%M")
    eth = float(m["eth_usd"]); btc = float(m["btc_usd"]); ratio = float(m["eth_btc_ratio"])
    eh, el, bh, bl = e["high"], e["low"], b["high"], b["low"]
    funding = d["funding"] if d else None
    oi = d["open_interest"] if d else None
    lines = [
        f"🕒 {now}",
        f"ETH: ${eth:,.2f}" + (f" (H:{eh:,.2f}/L:{el:,.2f})" if eh is not None else ""),
        f"BTC: ${btc:,.2f}" + (f" (H:{bh:,.2f}/L:{bl:,.2f})" if bh is not None else ""),
        f"ETH/BTC: {ratio:.5f}",
    ]
    if funding is not None: lines.append(f"Funding (ETH): {float(funding)*100:.3f}%/8h")
    if oi is not None:      lines.append(f"Open Interest (ETH): {float(oi):,.0f}")
    lines.append(action_line(eth))
    return "\n".join(lines)

# ---------- Rotas ----------
@app.on_event("startup")
async def _startup():
    await db_init()
    scheduler.add_job(ingest_1m, "interval", minutes=1, id="ingest_1m", replace_existing=True)
    scheduler.start()

@app.get("/healthz")
async def healthz():
    return {"ok": True, "time": dt.datetime.now(TZ).isoformat()}

@app.get("/pulse")
async def pulse():
    text = await latest_pulse_text()
    await send_tg(text)  # envia ao TG_CHAT fixo (se definido)
    return {"ok": True, "message": text}

@app.post("/webhook")
async def telegram_webhook(request: Request):
    """Endpoint para receber mensagens do Telegram via webhook."""
    try:
        update = await request.json()
    except Exception:
        return {"ok": True}

    msg = update.get("message") or update.get("edited_message") or update.get("channel_post")
    if not msg:
        return {"ok": True}

    chat_id = str(msg["chat"]["id"])
    text = (msg.get("text") or "").strip().lower()

    if text in ("/start", "start"):
        await send_tg("✅ Bot online. Use /pulse para o panorama agora.", chat_id)
        return {"ok": True}

    if text == "/pulse":
        panorama = await latest_pulse_text()
        await send_tg(panorama, chat_id)
        return {"ok": True}

    await send_tg("Comando não reconhecido. Use /pulse.", chat_id)
    return {"ok": True}

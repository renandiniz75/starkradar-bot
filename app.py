import os, math, datetime as dt
from zoneinfo import ZoneInfo
import httpx, asyncpg
from fastapi import FastAPI
from apscheduler.schedulers.asyncio import AsyncIOScheduler

TZ = ZoneInfo(os.getenv("TZ", "America/Sao_Paulo"))
DB_URL = os.getenv("DATABASE_URL")
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "")
SEND_ENABLED = bool(TG_TOKEN and TG_CHAT)

ETH_HEDGE_1 = float(os.getenv("ETH_HEDGE_1", "3900"))
ETH_HEDGE_2 = float(os.getenv("ETH_HEDGE_2", "3800"))
ETH_CLOSE   = float(os.getenv("ETH_CLOSE_HEDGE", "3950"))

BYBIT_SPOT = "https://api.bybit.com/v5/market/tickers?category=spot&symbol={sym}"
BYBIT_FUND = "https://api.bybit.com/v5/market/funding/history?category=linear&symbol=ETHUSDT&limit=1"
BYBIT_OI   = "https://api.bybit.com/v5/market/open-interest?category=linear&symbol=ETHUSDT&interval=5min"
TG_SEND    = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"

app = FastAPI(title="stark-defi-agent")
pool: asyncpg.Pool | None = None
scheduler = AsyncIOScheduler(timezone=str(TZ))

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
    pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=5)
    async with pool.acquire() as c:
        await c.execute(CREATE_SQL)

async def fetch_json(url: str):
    async with httpx.AsyncClient(timeout=10) as s:
        r = await s.get(url)
        r.raise_for_status()
        return r.json()

async def send_tg(text: str):
    if not SEND_ENABLED: return
    async with httpx.AsyncClient(timeout=10) as s:
        await s.post(TG_SEND, json={"chat_id": TG_CHAT, "text": text})

async def ingest_1m():
    now = dt.datetime.now(dt.UTC).replace(second=0, microsecond=0)
    try:
        eth = (await fetch_json(BYBIT_SPOT.format(sym="ETHUSDT")))["result"]["list"][0]
        btc = (await fetch_json(BYBIT_SPOT.format(sym="BTCUSDT")))["result"]["list"][0]
        eth_p = float(eth["lastPrice"]); btc_p = float(btc["lastPrice"])
        eth_h = float(eth["highPrice"]);  eth_l = float(eth["lowPrice"])
        btc_h = float(btc["highPrice"]);  btc_l = float(btc["lowPrice"])
        ethbtc = eth_p / btc_p
    except Exception:
        cg = await fetch_json("https://api.coingecko.com/api/v3/simple/price?ids=ethereum,bitcoin&vs_currencies=usd")
        eth_p = float(cg["ethereum"]["usd"]); btc_p = float(cg["bitcoin"]["usd"])
        eth_h = eth_l = btc_h = btc_l = math.nan
        ethbtc = eth_p / btc_p

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

    async with pool.acquire() as c:
        await c.execute(
            "INSERT INTO candles_minute(ts,symbol,open,high,low,close,volume) "
            "VALUES($1,$2,NULL,$3,$4,$5,NULL) ON CONFLICT (ts,symbol) DO UPDATE "
            "SET high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close",
            now, "ETHUSDT", eth_h, eth_l, eth_p)
        await c.execute(
            "INSERT INTO candles_minute(ts,symbol,open,high,low,close,volume) "
            "VALUES($1,$2,NULL,$3,$4,$5,NULL) ON CONFLICT (ts,symbol) DO UPDATE "
            "SET high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close",
            now, "BTCUSDT", btc_h, btc_l, btc_p)
        await c.execute(
            "INSERT INTO market_rel(ts,eth_usd,btc_usd,eth_btc_ratio) "
            "VALUES($1,$2,$3,$4) ON CONFLICT (ts) DO UPDATE "
            "SET eth_usd=EXCLUDED.eth_usd, btc_usd=EXCLUDED.btc_usd, eth_btc_ratio=EXCLUDED.eth_btc_ratio",
            now, eth_p, btc_p, ethbtc)
        await c.execute(
            "INSERT INTO derivatives_snap(ts,symbol,exchange,funding,open_interest) "
            "VALUES($1,$2,$3,$4,$5) ON CONFLICT (ts,symbol,exchange) DO UPDATE "
            "SET funding=EXCLUDED.funding, open_interest=EXCLUDED.open_interest",
            now, "ETHUSDT", "bybit", funding, open_interest)

def action_line(eth_price: float) -> str:
    if eth_price < ETH_HEDGE_2:
        return f"🚨 ETH < {ETH_HEDGE_2:.0f} → ampliar hedge p/ 20% (29 ETH)."
    if eth_price < ETH_HEDGE_1:
        return f"⚠️ ETH < {ETH_HEDGE_1:.0f} → ativar hedge 15% (22 ETH)."
    if eth_price > ETH_CLOSE:
        return f"↩️ ETH > {ETH_CLOSE:.0f} → avaliar fechar hedge."
    return "✅ Sem gatilho de defesa. Suportes: 4.200/4.000 | Resist: 4.300/4.400."

async def latest_pulse_text():
    async with pool.acquire() as c:
        m = await c.fetchrow("SELECT * FROM market_rel ORDER BY ts DESC LIMIT 1")
        d = await c.fetchrow("SELECT * FROM derivatives_snap WHERE symbol='ETHUSDT' AND exchange='bybit' ORDER BY ts DESC LIMIT 1")
        e = await c.fetchrow("SELECT * FROM candles_minute WHERE symbol='ETHUSDT' ORDER BY ts DESC LIMIT 1")
        b = await c.fetchrow("SELECT * FROM candles_minute WHERE symbol='BTCUSDT' ORDER BY ts DESC LIMIT 1")
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
    await send_tg(text)
    return {"ok": True, "message": text}

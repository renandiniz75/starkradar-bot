import os, math, datetime as dt
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, Request
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# =======================
# Config
# =======================
TZ = ZoneInfo(os.getenv("TZ", "America/Sao_Paulo"))
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
SEND_ENABLED = bool(TELEGRAM_TOKEN and TELEGRAM_CHAT_ID)

ETH_HEDGE_1 = float(os.getenv("ETH_HEDGE_1", "3900"))
ETH_HEDGE_2 = float(os.getenv("ETH_HEDGE_2", "3800"))
ETH_CLOSE_HEDGE = float(os.getenv("ETH_CLOSE_HEDGE", "3950"))

BYBIT_SPOT = "https://api.bybit.com/v5/market/tickers?category=spot&symbol={sym}"
BYBIT_FUNDING = "https://api.bybit.com/v5/market/funding/history?category=linear&symbol={sym}&limit=1"
BYBIT_OI = "https://api.bybit.com/v5/market/open-interest?category=linear&symbol={sym}&interval=5min"

TG_SEND = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

app = FastAPI(title="starkradar-bot")

# =======================
# Helpers
# =======================
async def fetch_json(url: str):
    async with httpx.AsyncClient(timeout=10) as s:
        r = await s.get(url)
        r.raise_for_status()
        return r.json()

async def get_pair_snapshot():
    # tenta Bybit; se falhar, usa CoinGecko
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
    return eth_p, eth_h, eth_l, btc_p, btc_h, btc_l, ethbtc

async def get_derivatives_snapshot():
    # funding (último), open interest (último ponto)
    try:
        f = (await fetch_json(BYBIT_FUNDING.format(sym="ETHUSDT")))["result"]["list"][0]
        funding = float(f["fundingRate"])  # ex: 0.0001 -> 0.01%
    except Exception:
        funding = None
    try:
        oi = (await fetch_json(BYBIT_OI.format(sym="ETHUSDT")))["result"]["list"][-1]
        open_interest = float(oi["openInterest"])
    except Exception:
        open_interest = None
    return funding, open_interest

async def send_tg(text: str):
    if not SEND_ENABLED:
        return {"sent": False, "reason": "telegram not configured"}
    async with httpx.AsyncClient(timeout=10) as s:
        await s.post(TG_SEND, json={"chat_id": TELEGRAM_CHAT_ID, "text": text})

def format_pulse(eth, eh, el, btc, bh, bl, ethbtc, funding, oi):
    now = dt.datetime.now(TZ).strftime("%Y-%m-%d %H:%M")
    lines = [f"🕒 {now}",
             f"ETH: ${eth:,.2f}" + (f" (H:{eh:,.2f}/L:{el:,.2f})" if not math.isnan(eh) else ""),
             f"BTC: ${btc:,.2f}" + (f" (H:{bh:,.2f}/L:{bl:,.2f})" if not math.isnan(bh) else ""),
             f"ETH/BTC: {ethbtc:.5f}"]
    if funding is not None:
        lines.append(f"Funding (ETH perp): {funding*100:.3f}%/8h")
    if oi is not None:
        lines.append(f"Open Interest (ETH): {oi:,.0f}")
    # regras
    if eth < ETH_HEDGE_2:
        lines.append(f"🚨 ETH < {ETH_HEDGE_2:.0f} → ampliar hedge p/ 20% (29 ETH).")
    elif eth < ETH_HEDGE_1:
        lines.append(f"⚠️ ETH < {ETH_HEDGE_1:.0f} → ativar hedge 15% (22 ETH).")
    elif eth > ETH_CLOSE_HEDGE:
        lines.append(f"↩️ ETH > {ETH_CLOSE_HEDGE:.0f} → avaliar fechar hedge.")
    else:
        lines.append("✅ Sem gatilho de defesa. Suportes: 4.200/4.000 | Resist: 4.300/4.400.")
    return "\n".join(lines)

# =======================
# Endpoints
# =======================
@app.get("/healthz")
async def healthz():
    return {"ok": True, "time": dt.datetime.now(TZ).isoformat()}

@app.get("/pulse")
async def pulse():
    eth, eh, el, btc, bh, bl, ethbtc = await get_pair_snapshot()
    funding, oi = await get_derivatives_snapshot()
    msg = format_pulse(eth, eh, el, btc, bh, bl, ethbtc, funding, oi)
    await send_tg(msg)
    return {"ok": True, "message": msg}

@app.post("/hooks/tradingview")
async def tv_hook(req: Request):
    data = await req.json()
    event = str(data.get("event","")).upper()
    price = data.get("price", "—")
    if event == "ETH_BREAKDOWN_3900":
        text = f"⚠️ TradingView: ETH {price} < 3900 → sugerir hedge 15% (22 ETH)."
    elif event == "ETH_BREAKDOWN_3800":
        text = f"🚨 TradingView: ETH {price} < 3800 → ampliar hedge p/ 20% (29 ETH)."
    elif event == "ETH_RECLAIM_3950":
        text = f"↩️ TradingView: ETH {price} > 3950 → avaliar fechar hedge."
    else:
        text = f"ℹ️ TradingView: {event} @ {price}"
    await send_tg(text)
    return {"ok": True}

# =======================
# Heartbeat opcional (ex.: a cada 30min)
# =======================
scheduler = AsyncIOScheduler(timezone=str(TZ))
def schedule_jobs():
    if os.getenv("HEARTBEAT_MINUTES"):
        minutes = int(os.getenv("HEARTBEAT_MINUTES", "30"))
        scheduler.add_job(lambda: app.router.routes[1].endpoint(), 'interval', minutes=minutes, id="pulse_job", replace_existing=True)
        scheduler.start()

schedule_jobs()

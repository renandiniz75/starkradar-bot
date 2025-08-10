# app.py — Stark DeFi Agent v5.3 (anti-rate-limit + intervals via ENV)
# Mantém: /pulse, /status, /run/accounts, Telegram, Bybit priv., Aave subgraph,
# notes/strats CSV, hedge hints. Corrige 403/429 com retry/backoff.

import os, hmac, hashlib, time, math, csv, io, asyncio, random, traceback
import datetime as dt
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, List

import httpx
import asyncpg
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from apscheduler.schedulers.asyncio import AsyncIOScheduler

APP_NAME = "stark-defi-agent"
APP_VERSION = "5.3"
TZ = ZoneInfo(os.getenv("TZ", "America/Sao_Paulo"))

DB_URL = os.getenv("DATABASE_URL")

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "")
SEND_ENABLED = bool(TG_TOKEN and TG_CHAT)

# Bybit (read-only)
BYBIT_KEY = os.getenv("BYBIT_RO_KEY", "")
BYBIT_SEC = os.getenv("BYBIT_RO_SECRET", "")
BYBIT_ACCOUNT_TYPE = os.getenv("BYBIT_ACCOUNT_TYPE", "UNIFIED")

# Aave / Etherscan
AAVE_ADDR = os.getenv("AAVE_ADDR", "")
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")

# Hedge params
ETH_HEDGE_1 = float(os.getenv("ETH_HEDGE_1", "3900"))
ETH_HEDGE_2 = float(os.getenv("ETH_HEDGE_2", "3800"))
ETH_CLOSE   = float(os.getenv("ETH_CLOSE_HEDGE", "3950"))
WHALE_USD_MIN = float(os.getenv("WHALE_USD_MIN", "500000"))

# intervals via ENV (segundos)
MARKET_INTERVAL_SEC   = int(os.getenv("MARKET_INTERVAL_SEC", "120"))  # mais suave
WHALES_INTERVAL_SEC   = int(os.getenv("WHALES_INTERVAL_SEC", "60"))
ACCOUNTS_INTERVAL_SEC = int(os.getenv("ACCOUNTS_INTERVAL_SEC", "300"))
ONCHAIN_INTERVAL_SEC  = int(os.getenv("ONCHAIN_INTERVAL_SEC", "300"))

# Endpoints
BYBIT_API = "https://api.bybit.com"
BYBIT_SPOT = BYBIT_API + "/v5/market/tickers?category=spot&symbol={sym}"
BYBIT_RECENT_TRADES = BYBIT_API + "/v5/market/recent-trade?category=linear&symbol=ETHUSDT&limit=1000"
BYBIT_FUND = BYBIT_API + "/v5/market/funding/history?category=linear&symbol=ETHUSDT&limit=1"
BYBIT_OI   = BYBIT_API + "/v5/market/open-interest?category=linear&symbol=ETHUSDT&interval=5min"

COINGECKO_SIMPLE = "https://api.coingecko.com/api/v3/simple/price"
ETHERSCAN_TX = "https://api.etherscan.io/api?module=account&action=txlist&address={addr}&startblock=0&endblock=99999999&sort=desc&apikey={key}"
TG_SEND = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"

# -------------------------------------------------------------------------------------
# App, pool e scheduler
# -------------------------------------------------------------------------------------
app = FastAPI(title=APP_NAME, version=APP_VERSION)
pool: Optional[asyncpg.Pool] = None
scheduler = AsyncIOScheduler(timezone=str(TZ))

last_run = { "ingest_1m": None, "ingest_whales": None, "ingest_accounts": None, "ingest_onchain": None }

# -------------------------------------------------------------------------------------
# SQL
# -------------------------------------------------------------------------------------
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
CREATE TABLE IF NOT EXISTS strategy_versions(
  id serial PRIMARY KEY,
  created_at timestamptz NOT NULL DEFAULT now(),
  name text NOT NULL,
  version text NOT NULL,
  note text
);
CREATE TABLE IF NOT EXISTS notes(
  id serial PRIMARY KEY,
  created_at timestamptz NOT NULL DEFAULT now(),
  tag text, text text NOT NULL
);
CREATE TABLE IF NOT EXISTS actions_log(
  id serial PRIMARY KEY,
  ts timestamptz NOT NULL DEFAULT now(),
  action text NOT NULL,
  details jsonb
);
CREATE TABLE IF NOT EXISTS account_snap(
  ts timestamptz NOT NULL DEFAULT now(),
  venue text NOT NULL,
  metric text NOT NULL,
  value numeric
);
CREATE TABLE IF NOT EXISTS whale_events(
  id serial PRIMARY KEY,
  ts timestamptz NOT NULL,
  venue text NOT NULL,
  side text,
  qty numeric,
  usd_value numeric,
  note text
);
"""

# -------------------------------------------------------------------------------------
# Utils
# -------------------------------------------------------------------------------------
DEFAULT_HEADERS = {
    "User-Agent": f"{APP_NAME}/{APP_VERSION} (+bot)",
    "Accept": "application/json",
}

async def send_tg(text: str, chat_id: Optional[str] = None):
    if not SEND_ENABLED: return
    cid = chat_id or TG_CHAT
    if not cid: return
    try:
        async with httpx.AsyncClient(timeout=12) as s:
            await s.post(TG_SEND, json={"chat_id": cid, "text": text})
    except Exception:
        pass

def action_line(eth_price: float) -> str:
    if eth_price < ETH_HEDGE_2:
        return f"🚨 ETH < {ETH_HEDGE_2:.0f} → ampliar hedge p/ 20% (29 ETH)."
    if eth_price < ETH_HEDGE_1:
        return f"⚠️ ETH < {ETH_HEDGE_1:.0f} → ativar hedge 15% (22 ETH)."
    if eth_price > ETH_CLOSE:
        return f"↩️ ETH > {ETH_CLOSE:.0f} → avaliar fechar hedge."
    return "✅ Sem gatilho. Suportes: 4.200/4.000 | Resist: 4.300/4.400."

# Retry/Backoff
async def fetch_json_retry(url: str, *, headers=None, params=None,
                           retries: int = 5, base_delay: float = 0.8, max_delay: float = 10.0):
    hdrs = dict(DEFAULT_HEADERS)
    if headers: hdrs.update(headers)
    delay = base_delay
    last_exc = None
    for attempt in range(1, retries+1):
        try:
            async with httpx.AsyncClient(timeout=20) as s:
                r = await s.get(url, headers=hdrs, params=params)
                # 403/429/5xx: tratar
                if r.status_code in (403, 429) or 500 <= r.status_code < 600:
                    ra = r.headers.get("Retry-After")
                    wait = float(ra) if (ra and ra.isdigit()) else min(max_delay, delay * (1.6 + random.random()*0.4))
                    await asyncio.sleep(wait)
                    delay = min(max_delay, delay * 1.8)
                    last_exc = httpx.HTTPStatusError(f"{r.status_code} {r.reason_phrase}", request=r.request, response=r)
                    continue
                r.raise_for_status()
                return r.json()
        except (httpx.HTTPStatusError, httpx.ConnectError, httpx.ReadTimeout) as e:
            last_exc = e
            await asyncio.sleep(min(max_delay, delay))
            delay = min(max_delay, delay * 1.8)
    if last_exc: raise last_exc
    raise RuntimeError("fetch_json_retry: exhausted retries")

# DB
async def db_init():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL não definido.")
    global pool
    pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=5)
    async with pool.acquire() as c:
        await c.execute(CREATE_SQL)

# -------------------------------------------------------------------------------------
# Market snapshots
# -------------------------------------------------------------------------------------
async def get_spot_snapshot() -> dict:
    # Tenta Bybit com header de key (mesmo público). Se 403/erro, cai para CoinGecko.
    bybit_headers = {
        "X-BAPI-API-KEY": BYBIT_KEY or "",
        "Referer": "https://bybit.com",
    }
    try:
        eth = (await fetch_json_retry(BYBIT_SPOT.format(sym="ETHUSDT"), headers=bybit_headers))["result"]["list"][0]
        btc = (await fetch_json_retry(BYBIT_SPOT.format(sym="BTCUSDT"), headers=bybit_headers))["result"]["list"][0]
        return {
            "eth": {"price": float(eth["lastPrice"]), "high": float(eth["highPrice"]), "low": float(eth["lowPrice"])},
            "btc": {"price": float(btc["lastPrice"]), "high": float(btc["highPrice"]), "low": float(btc["lowPrice"])},
        }
    except Exception:
        # Fallback: CoinGecko (com retry/backoff)
        cg = await fetch_json_retry(
            COINGECKO_SIMPLE,
            params={"ids":"ethereum,bitcoin","vs_currencies":"usd"},
        )
        return {
            "eth": {"price": float(cg["ethereum"]["usd"]), "high": math.nan, "low": math.nan},
            "btc": {"price": float(cg["bitcoin"]["usd"]),  "high": math.nan, "low": math.nan},
        }

async def get_derivatives_snapshot() -> dict:
    funding = None; open_interest = None
    try:
        f = (await fetch_json_retry(BYBIT_FUND))["result"]["list"][0]
        funding = float(f["fundingRate"])
    except Exception:
        pass
    try:
        oi = (await fetch_json_retry(BYBIT_OI))["result"]["list"][-1]
        open_interest = float(oi["openInterest"])
    except Exception:
        pass
    return {"funding": funding, "oi": open_interest}

async def ingest_1m():
    try:
        now = dt.datetime.now(dt.UTC).replace(second=0, microsecond=0)
        spot = await get_spot_snapshot()
        der  = await get_derivatives_snapshot()

        eth_p = spot["eth"]["price"]; btc_p = spot["btc"]["price"]
        eth_h = spot["eth"]["high"];  eth_l = spot["eth"]["low"]
        btc_h = spot["btc"]["high"];  btc_l = spot["btc"]["low"]

        ethbtc = (eth_p / btc_p) if (btc_p and not math.isnan(btc_p)) else math.nan

        async with pool.acquire() as c:
            await c.execute(
                "INSERT INTO candles_minute(ts,symbol,open,high,low,close,volume) VALUES($1,$2,NULL,$3,$4,$5,NULL) "
                "ON CONFLICT (ts,symbol) DO UPDATE SET high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close",
                now, "ETHUSDT", eth_h, eth_l, eth_p
            )
            await c.execute(
                "INSERT INTO candles_minute(ts,symbol,open,high,low,close,volume) VALUES($1,$2,NULL,$3,$4,$5,NULL) "
                "ON CONFLICT (ts,symbol) DO UPDATE SET high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close",
                now, "BTCUSDT", btc_h, btc_l, btc_p
            )
            await c.execute(
                "INSERT INTO market_rel(ts,eth_usd,btc_usd,eth_btc_ratio) VALUES($1,$2,$3,$4) "
                "ON CONFLICT (ts) DO UPDATE SET eth_usd=EXCLUDED.eth_usd, btc_usd=EXCLUDED.btc_usd, eth_btc_ratio=EXCLUDED.eth_btc_ratio",
                now, eth_p, btc_p, ethbtc
            )
            await c.execute(
                "INSERT INTO derivatives_snap(ts,symbol,exchange,funding,open_interest) VALUES($1,$2,$3,$4,$5) "
                "ON CONFLICT (ts,symbol,exchange) DO UPDATE SET funding=EXCLUDED.funding, open_interest=EXCLUDED.open_interest",
                now, "ETHUSDT", "bybit", der["funding"], der["oi"]
            )
    finally:
        last_run["ingest_1m"] = dt.datetime.now(TZ).isoformat(timespec="seconds")

# -------------------------------------------------------------------------------------
# Whales (agg de recentes)
# -------------------------------------------------------------------------------------
_last_trade_time_ms = 0
_ws_lock = asyncio.Lock()

async def ingest_whales():
    global _last_trade_time_ms
    try:
        async with _ws_lock:
            data = (await fetch_json_retry(BYBIT_RECENT_TRADES))["result"]["list"]
            new = [t for t in data if int(t.get("time", 0)) > _last_trade_time_ms]
            if not new: return
            _last_trade_time_ms = max(int(t.get("time", 0)) for t in new)

            buy_usd=sell_usd=0.0; buy_qty=sell_qty=0.0
            for t in new:
                price=float(t["price"]); qty=float(t["qty"])
                side=(t["side"] or "").upper()
                usd=price*qty
                if side=="BUY":  buy_usd+=usd; buy_qty+=qty
                else:            sell_usd+=usd; sell_qty+=qty

            ts = dt.datetime.now(dt.UTC)
            events=[]
            if buy_usd>=WHALE_USD_MIN: events.append(("BUY", buy_qty,  buy_usd,  f"agg {len(new)} trades"))
            if sell_usd>=WHALE_USD_MIN: events.append(("SELL", sell_qty, sell_usd, f"agg {len(new)} trades"))
            if events:
                async with pool.acquire() as c:
                    for side, qty, usd, note in events:
                        await c.execute(
                            "INSERT INTO whale_events(ts,venue,side,qty,usd_value,note) VALUES($1,$2,$3,$4,$5,$6)",
                            ts, "bybit", side, qty, usd, note
                        )
                for side, qty, usd, note in events:
                    await send_tg(f"🐋 {side} ~ ${usd:,.0f} | qty ~ {qty:,.1f} | {note}")
    finally:
        last_run["ingest_whales"] = dt.datetime.now(TZ).isoformat(timespec="seconds")

# -------------------------------------------------------------------------------------
# Bybit private + Aave snapshots (conta)
# -------------------------------------------------------------------------------------
def bybit_sign_qs(secret: str, params: Dict[str, Any]) -> str:
    qs = "&".join([f"{k}={params[k]}" for k in sorted(params)])
    return hmac.new(secret.encode(), qs.encode(), hashlib.sha256).hexdigest()

async def bybit_private_get(path: str, extra: Dict[str, Any]) -> Dict[str, Any]:
    ts = str(int(time.time()*1000))
    base = {"api_key": BYBIT_KEY, "timestamp": ts, "recv_window": "5000"}
    payload = {**base, **extra}
    sign = bybit_sign_qs(BYBIT_SEC, payload)
    payload["sign"] = sign
    url = f"{BYBIT_API}{path}"
    async with httpx.AsyncClient(timeout=20, headers=DEFAULT_HEADERS) as s:
        r = await s.get(url, params=payload)
        r.raise_for_status()
        return r.json()

async def snapshot_bybit() -> List[Dict[str, Any]]:
    if not (BYBIT_KEY and BYBIT_SEC): return []
    out=[]
    try:
        r = await bybit_private_get("/v5/account/wallet-balance", {"accountType": BYBIT_ACCOUNT_TYPE})
        lst = r.get("result",{}).get("list",[])
        if lst:
            out.append({"venue":"bybit","metric":"total_equity","value": float(lst[0].get("totalEquity",0))})
        r2 = await bybit_private_get("/v5/position/list", {"category":"linear","symbol":"ETHUSDT"})
        pos = r2.get("result",{}).get("list",[])
        if pos:
            out += [
                {"venue":"bybit","metric":"ethusdt_perp_size","value": float(pos[0].get("size",0) or 0)},
                {"venue":"bybit","metric":"ethusdt_perp_leverage","value": float(pos[0].get("leverage",0) or 0)},
            ]
    except Exception:
        traceback.print_exc()
    return out

AAVE_SUBGRAPH = "https://api.thegraph.com/subgraphs/name/aave/protocol-v3"
AAVE_QUERY = """
query ($user: String!) {
  userReserves(where: { user: $user }) {
    reserve { symbol, decimals }
    scaledATokenBalance
    scaledVariableDebt
  }
}
"""
async def snapshot_aave() -> List[Dict[str, Any]]:
    if not AAVE_ADDR: return []
    out=[]
    try:
        async with httpx.AsyncClient(timeout=25, headers=DEFAULT_HEADERS) as s:
            r=await s.post(AAVE_SUBGRAPH,json={"query":AAVE_QUERY,"variables":{"user":AAVE_ADDR.lower()}})
            r.raise_for_status()
            data=r.json()
        reserves=data.get("data",{}).get("userReserves",[])
        total_coll=0.0; total_debt=0.0
        for it in reserves:
            dec=int(it["reserve"]["decimals"])
            a=float(it["scaledATokenBalance"] or 0)/(10**dec)
            d=float(it["scaledVariableDebt"] or 0)/(10**dec)
            if a>0: total_coll+=a
            if d>0: total_debt+=d
        out.append({"venue":"aave","metric":"collateral_proxy","value": total_coll})
        out.append({"venue":"aave","metric":"debt_proxy","value": total_debt})
    except Exception:
        traceback.print_exc()
    return out

async def ingest_accounts():
    rows=[]
    rows += await snapshot_bybit()
    rows += await snapshot_aave()
    async with pool.acquire() as c:
        for r in rows:
            await c.execute("INSERT INTO account_snap(venue,metric,value) VALUES($1,$2,$3)", r["venue"], r["metric"], r["value"])
    last_run["ingest_accounts"] = dt.datetime.now(TZ).isoformat(timespec="seconds")

# -------------------------------------------------------------------------------------
# On-chain opcional (Etherscan)
# -------------------------------------------------------------------------------------
async def ingest_onchain_eth(addr: str):
    if not ETHERSCAN_API_KEY: 
        last_run["ingest_onchain"] = dt.datetime.now(TZ).isoformat(timespec="seconds")
        return
    try:
        data = await fetch_json_retry(ETHERSCAN_TX.format(addr=addr, key=ETHERSCAN_API_KEY))
        txs = data.get("result", [])[:10]
        big=[]
        for t in txs:
            val_eth = float(t.get("value","0"))/1e18
            if val_eth >= 3000:
                big.append((val_eth, t.get("from"), t.get("to")))
        if big:
            ts = dt.datetime.now(dt.UTC)
            async with pool.acquire() as c:
                for val_eth, _from, _to in big:
                    await c.execute(
                        "INSERT INTO whale_events(ts,venue,side,qty,usd_value,note) VALUES($1,$2,$3,$4,$5,$6)",
                        ts, "onchain", "ONCHAIN", val_eth, None, f"{_from} -> {_to}"
                    )
    except Exception:
        traceback.print_exc()
    finally:
        last_run["ingest_onchain"] = dt.datetime.now(TZ).isoformat(timespec="seconds")

# -------------------------------------------------------------------------------------
# Comentário 6–8h e Pulse
# -------------------------------------------------------------------------------------
async def build_commentary() -> str:
    end = dt.datetime.now(dt.UTC); start = end - dt.timedelta(hours=8)
    async with pool.acquire() as c:
        rows = await c.fetch("SELECT ts, eth_usd, btc_usd, eth_btc_ratio FROM market_rel WHERE ts BETWEEN $1 AND $2 ORDER BY ts", start, end)
        deriv = await c.fetch("SELECT ts, funding, open_interest FROM derivatives_snap WHERE ts BETWEEN $1 AND $2 ORDER BY ts", start, end)
        whales = await c.fetch("SELECT ts, side, usd_value FROM whale_events WHERE ts BETWEEN $1 AND $2 ORDER BY ts", start, end)
    if not rows: return "⏳ Aguardando histórico para comentário (volte em alguns minutos)."
    eth = [float(r["eth_usd"]) for r in rows]; btc = [float(r["btc_usd"]) for r in rows]; ratio = [float(r["eth_btc_ratio"]) for r in rows]
    def pct(a,b): return 0.0 if a==0 else (b-a)/a*100.0
    eth_chg = pct(eth[0], eth[-1]); btc_chg = pct(btc[0], btc[-1]); ratio_chg = pct(ratio[0], ratio[-1])
    funding = deriv[-1]["funding"] if deriv else None; oi = deriv[-1]["open_interest"] if deriv else None
    buy = sum(float(w["usd_value"] or 0) for w in whales if w["side"]=="BUY")
    sell= sum(float(w["usd_value"] or 0) for w in whales if w["side"]=="SELL")
    flow = "neutra"; 
    if buy>sell*1.3: flow="compradora"
    elif sell>buy*1.3: flow="vendedora"
    lines=[]
    lines.append(f"ETH: {eth_chg:+.2f}% em 8h; " + (f"funding {float(funding)*100:.3f}%/8h, " if funding is not None else "") + (f"OI ~ {float(oi):,.0f}. " if oi is not None else "") + f"Pressão {flow}.")
    lines.append(f"BTC: {btc_chg:+.2f}% em 8h; dinâmica mais {'forte' if btc_chg>0 else 'fraca'}.")
    lines.append(f"ETH/BTC: {ratio_chg:+.2f}% em 8h; {'ETH ganhando beta' if ratio_chg>0 else 'BTC dominante'}.")
    if ratio_chg>0 and (funding is None or funding<0.0005): lines.append("Síntese: pró-ETH (força relativa + funding contido).")
    elif ratio_chg<0 and (funding is not None and funding>0.001): lines.append("Síntese: atenção à euforia (funding alto).")
    else: lines.append("Síntese: equilíbrio tático; usar gatilhos de preço.")
    return "\n".join(lines)

async def latest_pulse_text() -> str:
    async with pool.acquire() as c:
        m = await c.fetchrow("SELECT * FROM market_rel ORDER BY ts DESC LIMIT 1")
        e = await c.fetchrow("SELECT * FROM candles_minute WHERE symbol='ETHUSDT' ORDER BY ts DESC LIMIT 1")
        b = await c.fetchrow("SELECT * FROM candles_minute WHERE symbol='BTCUSDT' ORDER BY ts DESC LIMIT 1")
        d = await c.fetchrow("SELECT * FROM derivatives_snap WHERE symbol='ETHUSDT' AND exchange='bybit' ORDER BY ts DESC LIMIT 1")
    if not (m and e and b): return "⏳ Aguardando primeiros dados…"
    now = dt.datetime.now(TZ).strftime("%Y-%m-%d %H:%M")
    eth = float(m["eth_usd"]); btc = float(m["btc_usd"]); ratio = float(m["eth_btc_ratio"])
    eh, el, bh, bl = e["high"], e["low"], b["high"], b["low"]
    parts=[f"🕒 {now}",
           f"ETH: ${eth:,.2f}" + (f" (H:{eh:,.2f}/L:{el:,.2f})" if isinstance(eh,(int,float)) and isinstance(el,(int,float)) else ""),
           f"BTC: ${btc:,.2f}" + (f" (H:{bh:,.2f}/L:{bl:,.2f})" if isinstance(bh,(int,float)) and isinstance(bl,(int,float)) else ""),
           f"ETH/BTC: {ratio:.5f}"]
    if d and d["funding"] is not None: parts.append(f"Funding (ETH): {float(d['funding'])*100:.3f}%/8h")
    if d and d["open_interest"] is not None: parts.append(f"Open Interest (ETH): {float(d['open_interest']):,.0f}")
    # ação
    if eth < ETH_HEDGE_2: parts.append(f"🚨 ETH < {ETH_HEDGE_2:.0f} → ampliar hedge p/ 20% (29 ETH).")
    elif eth < ETH_HEDGE_1: parts.append(f"⚠️ ETH < {ETH_HEDGE_1:.0f} → ativar hedge 15% (22 ETH).")
    elif eth > ETH_CLOSE: parts.append(f"↩️ ETH > {ETH_CLOSE:.0f} → avaliar fechar hedge.")
    else: parts.append("✅ Sem gatilho imediato.")
    comment = await build_commentary()
    return "\n".join(parts) + "\n\n" + comment

async def send_pulse_to_chat():
    await send_tg(await latest_pulse_text())

# -------------------------------------------------------------------------------------
# FastAPI
# -------------------------------------------------------------------------------------
@app.on_event("startup")
async def _startup():
    await db_init()
    scheduler.add_job(ingest_1m, "interval", seconds=MARKET_INTERVAL_SEC, id="ingest_1m", replace_existing=True)
    scheduler.add_job(ingest_whales, "interval", seconds=WHALES_INTERVAL_SEC, id="ingest_whales", replace_existing=True)
    scheduler.add_job(ingest_accounts, "interval", seconds=ACCOUNTS_INTERVAL_SEC, id="ingest_accounts", replace_existing=True)
    if AAVE_ADDR and ETHERSCAN_API_KEY:
        scheduler.add_job(ingest_onchain_eth, "interval", seconds=ONCHAIN_INTERVAL_SEC, args=[AAVE_ADDR], id="ingest_onchain", replace_existing=True)
    # boletins
    scheduler.add_job(send_pulse_to_chat, "cron", hour=8,  minute=0, id="bulletin_08", replace_existing=True)
    scheduler.add_job(send_pulse_to_chat, "cron", hour=14, minute=0, id="bulletin_14", replace_existing=True)
    scheduler.add_job(send_pulse_to_chat, "cron", hour=20, minute=0, id="bulletin_20", replace_existing=True)
    scheduler.start()

@app.get("/healthz")
async def healthz():
    return {"ok": True, "time": dt.datetime.now(TZ).isoformat()}

@app.get("/status")
async def status():
    async with pool.acquire() as c:
        mr = await c.fetchval("SELECT COUNT(1) FROM market_rel")
        cm_eth = await c.fetchval("SELECT COUNT(1) FROM candles_minute WHERE symbol='ETHUSDT'")
        cm_btc = await c.fetchval("SELECT COUNT(1) FROM candles_minute WHERE symbol='BTCUSDT'")
        ds = await c.fetchval("SELECT COUNT(1) FROM derivatives_snap")
        whales = await c.fetchval("SELECT COUNT(1) FROM whale_events")
        acc = await c.fetchval("SELECT COUNT(1) FROM account_snap")
    return {
        "counts":{
            "market_rel": mr, "candles_eth": cm_eth, "candles_btc": cm_btc,
            "derivatives": ds, "whale_events": whales, "account_snap": acc
        },
        "last_run": last_run
    }

@app.post("/run/accounts")
async def run_accounts():
    await ingest_accounts()
    return {"ok": True, "ran_at": last_run["ingest_accounts"]}

@app.get("/pulse")
async def pulse():
    text = await latest_pulse_text()
    await send_tg(text)
    return {"ok": True, "message": text}

@app.post("/webhook")
async def telegram_webhook(request: Request):
    try: update = await request.json()
    except Exception: return {"ok": True}
    msg = update.get("message") or update.get("edited_message") or update.get("channel_post")
    if not msg: return {"ok": True}
    chat_id = str(msg["chat"]["id"])
    text = (msg.get("text") or "").strip()
    low = text.lower()
    if low in ("/start","start"):
        await send_tg("✅ Bot online. Comandos: /pulse, /note <texto>, /strat new <nome> | <versão> | <nota>, /strat last, /notes", chat_id); return {"ok": True}
    if low == "/pulse":
        await send_tg(await latest_pulse_text(), chat_id); return {"ok": True}
    if low == "/notes":
        async with pool.acquire() as c:
            rows = await c.fetch("SELECT created_at,text FROM notes ORDER BY id DESC LIMIT 5")
        if not rows: await send_tg("Sem notas ainda.", chat_id); return {"ok": True}
        out=["Notas recentes:"]+[f"- {r['created_at']:%m-%d %H:%M} • {r['text']}" for r in rows]
        await send_tg("\n".join(out), chat_id); return {"ok": True}
    if low == "/strat last":
        async with pool.acquire() as c:
            row = await c.fetchrow("SELECT created_at,name,version,note FROM strategy_versions ORDER BY id DESC LIMIT 1")
        if not row: await send_tg("Nenhuma estratégia salva.", chat_id); return {"ok": True}
        await send_tg(f"Última estratégia:\n{row['created_at']:%Y-%m-%d %H:%M}\n{row['name']} v{row['version']}\n{row['note'] or ''}", chat_id); return {"ok": True}
    if low.startswith("/note"):
        note = text[len("/note"):].strip()
        if not note: await send_tg("Uso: /note seu texto aqui", chat_id); return {"ok": True}
        async with pool.acquire() as c:
            await c.execute("INSERT INTO notes(tag,text) VALUES($1,$2)", None, note)
        await send_tg("📝 Nota salva.", chat_id); return {"ok": True}
    if low.startswith("/strat new"):
        try:
            payload = text[len("/strat new"):].strip()
            name, version, note = [p.strip() for p in payload.split("|", 2)]
        except Exception:
            await send_tg("Uso: /strat new <nome> | <versão> | <nota>", chat_id); return {"ok": True}
        async with pool.acquire() as c:
            await c.execute("INSERT INTO strategy_versions(name,version,note) VALUES($1,$2,$3)", name, version, note)
        await send_tg(f"📌 Estratégia salva: {name} v{version}", chat_id); return {"ok": True}
    await send_tg("Comando não reconhecido. Use /pulse, /note, /strat new, /strat last, /notes.", chat_id)
    return {"ok": True}

@app.get("/export/notes.csv")
async def export_notes():
    async with pool.acquire() as c:
        rows = await c.fetch("SELECT created_at, tag, text FROM notes ORDER BY id DESC")
    buf = io.StringIO(); w = csv.writer(buf)
    w.writerow(["created_at","tag","text"])
    for r in rows: w.writerow([r["created_at"].isoformat(), r["tag"] or "", r["text"]])
    return PlainTextResponse(buf.getvalue(), media_type="text/csv")

@app.get("/export/strats.csv")
async def export_strats():
    async with pool.acquire() as c:
        rows = await c.fetch("SELECT created_at, name, version, note FROM strategy_versions ORDER BY id DESC")
    buf = io.StringIO(); w = csv.writer(buf)
    w.writerow(["created_at","name","version","note"])
    for r in rows: w.writerow([r["created_at"].isoformat(), r["name"], r["version"], r["note"] or ""])
    return PlainTextResponse(buf.getvalue(), media_type="text/csv")

@app.get("/accounts/last")
async def accounts_last():
    async with pool.acquire() as c:
        rows = await c.fetch("""
            SELECT * FROM account_snap
            WHERE ts > now() - interval '1 hour'
            ORDER BY ts DESC, venue, metric
        """)
    return JSONResponse({"rows": [dict(r) for r in rows]})

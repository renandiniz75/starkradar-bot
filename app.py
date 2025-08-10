# app.py — Stark DeFi Agent v5.4 (2025-08-10)
# - Webhook Telegram em /webhook e /webhook/{token}
# - Defaults com seu TOKEN e CHAT_ID (sobrescrevíveis por ENV)
# - Headers+backoff (evita 403/429 Bybit/CG), fallbacks (Binance/Coinbase/CG)
# - Jobs: ingest_1m, ingest_whales, ingest_accounts, ingest_onchain (opcional)
# - Boletins 08/14/20; /pulse on-demand com comentário 6–8h
# - Comandos Telegram: /start, /pulse, /eth, /btc, /alfa, /note, /notes, /strat new|last
# - CSV export e endpoints de status/health
# - Bybit private (wallet/position), Aave via subgraph, whales (agg trades)
# - Banco: Railway Postgres via DATABASE_URL

import os, hmac, hashlib, time, math, csv, io, asyncio, traceback, json, logging
import datetime as dt
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, List

import httpx
import asyncpg
from fastapi import FastAPI, Request, Path
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware

APP_VERSION = "5.4"

# =========================
# Defaults (podem ser sobrescritos por ENV)
# =========================
DEFAULT_TG_TOKEN = "8349135220:AAFHKrmSexocLxEhtP0XouE0EBmTxllh9lU"  # seu token (rotacione depois)
DEFAULT_TG_CHAT  = "47045110"                                         # seu chat id

TZ = ZoneInfo(os.getenv("TZ", "America/Sao_Paulo"))
DB_URL = os.getenv("DATABASE_URL")

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", DEFAULT_TG_TOKEN)
TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID",  DEFAULT_TG_CHAT)
SEND_ENABLED = bool(TG_TOKEN)

BYBIT_KEY = os.getenv("BYBIT_RO_KEY", "")
BYBIT_SEC = os.getenv("BYBIT_RO_SECRET", "")
BYBIT_ACCOUNT_TYPE = os.getenv("BYBIT_ACCOUNT_TYPE", "UNIFIED")

AAVE_ADDR = os.getenv("AAVE_ADDR", "")              # EVM (0x...)
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")

ETH_HEDGE_1 = float(os.getenv("ETH_HEDGE_1", "3900"))
ETH_HEDGE_2 = float(os.getenv("ETH_HEDGE_2", "3800"))
ETH_CLOSE   = float(os.getenv("ETH_CLOSE_HEDGE", "3950"))

WHALE_USD_MIN = float(os.getenv("WHALE_USD_MIN", "500000"))
WHALES_ENABLED = os.getenv("WHALES_ENABLED", "true").lower() not in ("0","false","no")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# =========================
# Consts e endpoints
# =========================
BYBIT_API = "https://api.bybit.com"
BYBIT_PUBLIC_DOMS = ("https://api.bybitglobal.com", "https://api.bybit.com")
BYBIT_SPOT_QS = "/v5/market/tickers"                 # params: category, symbol
BYBIT_RECENT_TRADES = "/v5/market/recent-trade"      # params: category, symbol, limit
BYBIT_FUND = "https://api.bybit.com/v5/market/funding/history?category=linear&symbol=ETHUSDT&limit=1"
BYBIT_OI   = "https://api.bybit.com/v5/market/open-interest?category=linear&symbol=ETHUSDT&interval=5min"

BINANCE_24H = "https://api.binance.com/api/v3/ticker/24hr"
BINANCE_AGG = "https://api.binance.com/api/v3/aggTrades"
COINBASE_TICK = "https://api.exchange.coinbase.com/products/{pair}/ticker"
COINGECKO_SIMPLE = "https://api.coingecko.com/api/v3/simple/price"

TG_SEND = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
TG_API_BASE = f"https://api.telegram.org/bot{TG_TOKEN}"

ETHERSCAN_TX = "https://api.etherscan.io/api?module=account&action=txlist&address={addr}&startblock=0&endblock=99999999&sort=desc&apikey={key}"

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

# =========================
# App e agendador
# =========================
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
log = logging.getLogger("stark-defi-agent")

app = FastAPI(title="stark-defi-agent", version=APP_VERSION)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

from apscheduler.schedulers.asyncio import AsyncIOScheduler
scheduler = AsyncIOScheduler(timezone=str(TZ))

pool: Optional[asyncpg.Pool] = None
last_run = {
    "ingest_1m": None,
    "ingest_whales": None,
    "ingest_accounts": None,
    "ingest_onchain": None,
}

# =========================
# DB schema
# =========================
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

async def db_init():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL não definido.")
    global pool
    pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=5)
    async with pool.acquire() as c:
        await c.execute(CREATE_SQL)
    log.info("DB pronto.")

# =========================
# HTTP helpers
# =========================
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Connection": "keep-alive",
}

async def fetch_json_retry(url: str, *, params: Dict[str, str] | None = None,
                           headers: Dict[str, str] | None = None,
                           tries: int = 4, base_delay: float = 0.6):
    hdrs = {**DEFAULT_HEADERS, **(headers or {})}
    last_exc = None
    for i in range(tries):
        try:
            async with httpx.AsyncClient(timeout=25) as s:
                r = await s.get(url, params=params, headers=hdrs)
                if r.status_code in (403, 429, 502, 503):
                    raise httpx.HTTPStatusError("retryable", request=r.request, response=r)
                r.raise_for_status()
                return r.json()
        except Exception as e:
            last_exc = e
            await asyncio.sleep(base_delay * (2 ** i) + 0.1 * i)  # backoff+jitter
    raise last_exc

async def send_tg(text: str, chat_id: Optional[str] = None):
    if not SEND_ENABLED: 
        log.warning("SEND_ENABLED off, skip Telegram send.")
        return
    cid = str(chat_id or TG_CHAT)
    if not cid:
        log.warning("Sem chat_id.")
        return
    async with httpx.AsyncClient(timeout=12) as s:
        r = await s.post(TG_SEND, json={"chat_id": cid, "text": text})
        try:
            r.raise_for_status()
        except Exception:
            log.exception("Falha ao enviar Telegram: %s", r.text)

def action_line(eth_price: float) -> str:
    if eth_price < ETH_HEDGE_2:
        return f"🚨 ETH < {ETH_HEDGE_2:.0f} → ampliar hedge p/ 20% (29 ETH)."
    if eth_price < ETH_HEDGE_1:
        return f"⚠️ ETH < {ETH_HEDGE_1:.0f} → ativar hedge 15% (22 ETH)."
    if eth_price > ETH_CLOSE:
        return f"↩️ ETH > {ETH_CLOSE:.0f} → avaliar fechar hedge."
    return "✅ Sem gatilho. Suportes: 4.200/4.000 | Resist: 4.300/4.400."

# =========================
# Market snapshots
# =========================
async def get_spot_snapshot() -> dict:
    # Bybit (domínios alternativos)
    for dom in BYBIT_PUBLIC_DOMS:
        try:
            eth = (await fetch_json_retry(f"{dom}{BYBIT_SPOT_QS}",
                                          params={"category":"linear","symbol":"ETHUSDT"}))["result"]["list"][0]
            btc = (await fetch_json_retry(f"{dom}{BYBIT_SPOT_QS}",
                                          params={"category":"linear","symbol":"BTCUSDT"}))["result"]["list"][0]
            return {
                "eth": {"price": float(eth["lastPrice"]), "high": float(eth["highPrice"]), "low": float(eth["lowPrice"])},
                "btc": {"price": float(btc["lastPrice"]), "high": float(btc["highPrice"]), "low": float(btc["lowPrice"])},
            }
        except Exception:
            continue
    # Binance
    try:
        be = await fetch_json_retry(BINANCE_24H, params={"symbol":"ETHUSDT"})
        bb = await fetch_json_retry(BINANCE_24H, params={"symbol":"BTCUSDT"})
        return {
            "eth": {"price": float(be["lastPrice"]), "high": float(be["highPrice"]), "low": float(be["lowPrice"])},
            "btc": {"price": float(bb["lastPrice"]), "high": float(bb["highPrice"]), "low": float(bb["lowPrice"])},
        }
    except Exception:
        pass
    # Coinbase
    try:
        ce = await fetch_json_retry(COINBASE_TICK.format(pair="ETH-USD"))
        cb = await fetch_json_retry(COINBASE_TICK.format(pair="BTC-USD"))
        return {
            "eth": {"price": float(ce["price"]), "high": math.nan, "low": math.nan},
            "btc": {"price": float(cb["price"]), "high": math.nan, "low": math.nan},
        }
    except Exception:
        pass
    # Coingecko (último recurso)
    cg = await fetch_json_retry(COINGECKO_SIMPLE, params={"ids":"ethereum,bitcoin","vs_currencies":"usd"})
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
        ethbtc = eth_p / btc_p
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

# =========================
# Whales (agg trades)
# =========================
_last_trade_time_ms = 0
_ws_lock = asyncio.Lock()

async def ingest_whales():
    if not WHALES_ENABLED:
        last_run["ingest_whales"] = dt.datetime.now(TZ).isoformat(timespec="seconds")
        return
    global _last_trade_time_ms
    try:
        async with _ws_lock:
            data = None
            # Bybit recent-trade
            for dom in BYBIT_PUBLIC_DOMS:
                try:
                    r = await fetch_json_retry(f"{dom}{BYBIT_RECENT_TRADES}",
                                               params={"category":"linear","symbol":"ETHUSDT","limit":"1000"})
                    data = r["result"]["list"]; break
                except Exception:
                    continue
            # Binance fallback
            if data is None:
                agg = await fetch_json_retry(BINANCE_AGG, params={"symbol":"ETHUSDT","limit":"1000"})
                data = [{"time": int(t["T"]), "price": t["p"], "qty": t["q"],
                         "side": "BUY" if t.get("m")==False else "SELL"} for t in agg]

            new = [t for t in data if int(t.get("time", 0)) > _last_trade_time_ms]
            if not new: return
            _last_trade_time_ms = max(int(t.get("time", 0)) for t in new)

            buy_usd=sell_usd=0.0; buy_qty=sell_qty=0.0
            for t in new:
                price=float(t["price"]); qty=float(t["qty"])
                side=(t.get("side") or "").upper()
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
                            ts, "perp", side, qty, usd, note
                        )
                for side, qty, usd, note in events:
                    await send_tg(f"🐋 {side} ~ ${usd:,.0f} | qty ~ {qty:,.1f} | {note}")
    finally:
        last_run["ingest_whales"] = dt.datetime.now(TZ).isoformat(timespec="seconds")

# =========================
# Accounts (Bybit priv.) e Aave
# =========================
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

# =========================
# On-chain opcional (Etherscan)
# =========================
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

# =========================
# Comentário 6–8 horas
# =========================
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
    funding = float(deriv[-1]["funding"]) if deriv and deriv[-1]["funding"] is not None else None
    oi = float(deriv[-1]["open_interest"]) if deriv and deriv[-1]["open_interest"] is not None else None
    buy = sum(float(w["usd_value"] or 0) for w in whales if w["side"]=="BUY")
    sell= sum(float(w["usd_value"] or 0) for w in whales if w["side"]=="SELL")
    flow = "neutra"; 
    if buy>sell*1.3: flow="compradora"
    elif sell>buy*1.3: flow="vendedora"
    lines=[]
    lines.append(f"ETH: {eth_chg:+.2f}% em 8h; " + (f"funding {funding*100:.3f}%/8h, " if funding is not None else "") + (f"OI ~ {oi:,.0f}. " if oi is not None else "") + f"Pressão {flow}.")
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
    parts.append(action_line(eth))
    comment = await build_commentary()
    return "\n".join(parts) + "\n".join(["", comment])

async def send_pulse_to_chat():
    await send_tg(await latest_pulse_text())

# =========================
# FastAPI startup
# =========================
@app.on_event("startup")
async def _startup():
    await db_init()
    scheduler.add_job(ingest_1m, "interval", minutes=1, id="ingest_1m", replace_existing=True)
    scheduler.add_job(ingest_whales, "interval", seconds=30, id="ingest_whales", replace_existing=True)
    scheduler.add_job(ingest_accounts, "interval", minutes=5, id="ingest_accounts", replace_existing=True)
    if AAVE_ADDR and ETHERSCAN_API_KEY:
        scheduler.add_job(ingest_onchain_eth, "interval", minutes=5, args=[AAVE_ADDR], id="ingest_onchain", replace_existing=True)
    scheduler.add_job(send_pulse_to_chat, "cron", hour=8,  minute=0, id="bulletin_08", replace_existing=True)
    scheduler.add_job(send_pulse_to_chat, "cron", hour=14, minute=0, id="bulletin_14", replace_existing=True)
    scheduler.add_job(send_pulse_to_chat, "cron", hour=20, minute=0, id="bulletin_20", replace_existing=True)
    scheduler.start()
    log.info("Startup OK. Versão %s", APP_VERSION)

# =========================
# Endpoints públicos
# =========================
@app.get("/")
async def root():
    return {"ok": True, "service": "stark-defi-agent", "version": APP_VERSION}

@app.get("/healthz")
async def healthz():
    return {"ok": True, "time": dt.datetime.now(TZ).isoformat(), "version": APP_VERSION}

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

# =========================
# Telegram Webhook
# =========================
async def handle_update(update: Dict[str, Any]):
    # extrai dados básicos
    msg = update.get("message") or update.get("edited_message") or update.get("channel_post")
    if not msg: 
        return
    chat_id = msg["chat"]["id"]
    text = (msg.get("text") or "").strip()
    low = text.lower()

    # comandos
    if low in ("/start", "start"):
        await send_tg("✅ Bot online.\nComandos: /pulse, /eth, /btc, /alfa, /note <texto>, /strat new <nome> | <versão> | <nota>, /strat last, /notes", chat_id); 
        return

    if low == "/pulse":
        await send_tg(await latest_pulse_text(), chat_id); 
        return

    if low == "/eth" or low == "/btc":
        snap = await get_spot_snapshot()
        eth = snap["eth"]["price"]; btc = snap["btc"]["price"]; ratio = eth/btc
        line = f"ETH: ${eth:,.2f} | BTC: ${btc:,.2f} | ETH/BTC: {ratio:.5f}\n" + action_line(eth)
        await send_tg(line, chat_id); 
        return

    if low == "/alfa":
        await send_tg(await build_commentary(), chat_id); 
        return

    if low == "/notes":
        async with pool.acquire() as c:
            rows = await c.fetch("SELECT created_at,text FROM notes ORDER BY id DESC LIMIT 5")
        if not rows: await send_tg("Sem notas ainda.", chat_id); return
        out=["Notas recentes:"]+[f"- {r['created_at']:%m-%d %H:%M} • {r['text']}" for r in rows]
        await send_tg("\n".join(out), chat_id); 
        return

    if low == "/strat last":
        async with pool.acquire() as c:
            row = await c.fetchrow("SELECT created_at,name,version,note FROM strategy_versions ORDER BY id DESC LIMIT 1")
        if not row: await send_tg("Nenhuma estratégia salva.", chat_id); return
        await send_tg(f"Última estratégia:\n{row['created_at']:%Y-%m-%d %H:%M}\n{row['name']} v{row['version']}\n{row['note'] or ''}", chat_id); 
        return

    if low.startswith("/note"):
        note = text[len("/note"):].strip()
        if not note: 
            await send_tg("Uso: /note seu texto aqui", chat_id); 
            return
        async with pool.acquire() as c:
            await c.execute("INSERT INTO notes(tag,text) VALUES($1,$2)", None, note)
        await send_tg("📝 Nota salva.", chat_id); 
        return

    if low.startswith("/strat new"):
        try:
            payload = text[len("/strat new"):].strip()
            name, version, note = [p.strip() for p in payload.split("|", 2)]
        except Exception:
            await send_tg("Uso: /strat new <nome> | <versão> | <nota>", chat_id); 
            return
        async with pool.acquire() as c:
            await c.execute("INSERT INTO strategy_versions(name,version,note) VALUES($1,$2,$3)", name, version, note)
        await send_tg(f"📌 Estratégia salva: {name} v{version}", chat_id); 
        return

    # fallback
    await send_tg("Comando não reconhecido. Use /pulse, /eth, /btc, /alfa, /note, /strat new, /strat last, /notes.", chat_id)

@app.post("/webhook")
async def telegram_webhook_plain(request: Request):
    try:
        update = await request.json()
        log.info("Webhook (plain) update_id=%s", update.get("update_id"))
        await handle_update(update)
    except Exception:
        log.exception("Erro no webhook (plain).")
    return {"ok": True}

@app.post("/webhook/{token}")
async def telegram_webhook_token(request: Request, token: str = Path(...)):
    # valida token se quiser travar por rota
    if token != TG_TOKEN:
        log.warning("Webhook token mismatch.")
        return JSONResponse({"ok": False, "error": "token mismatch"}, status_code=403)
    try:
        update = await request.json()
        log.info("Webhook (token) update_id=%s", update.get("update_id"))
        await handle_update(update)
    except Exception:
        log.exception("Erro no webhook (token).")
    return {"ok": True}

# =========================
# Exports
# =========================
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

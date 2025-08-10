# app.py — Stark DeFi Agent v6.0.2 (no-Binance)
# - Remove Binance (451/403)
# - Preço/H-L via Coinbase
# - Funding/OI Bybit opcional (silencioso em 403)
# - Whales via Coinbase com cooldown + dedup
# - News RSS + resumo leve para /pulse
# - Webhook aceita /webhook e /webhook/{token}

import os, hmac, hashlib, time, math, csv, io, asyncio, traceback
import datetime as dt
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, List, Tuple
import xml.etree.ElementTree as ET

import httpx
import asyncpg
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse

# -------------------- Config --------------------
TZ = ZoneInfo(os.getenv("TZ", "America/Sao_Paulo"))
DB_URL = os.getenv("DATABASE_URL")

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "")
SEND_ENABLED = bool(TG_TOKEN)

# Bybit (opcional; falhas serão silenciosas)
BYBIT_KEY = os.getenv("BYBIT_RO_KEY", "")
BYBIT_SEC = os.getenv("BYBIT_RO_SECRET", "")
BYBIT_ACCOUNT_TYPE = os.getenv("BYBIT_ACCOUNT_TYPE", "UNIFIED")

AAVE_ADDR = os.getenv("AAVE_ADDR", "")
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")

ETH_HEDGE_1 = float(os.getenv("ETH_HEDGE_1", "3900"))
ETH_HEDGE_2 = float(os.getenv("ETH_HEDGE_2", "3800"))
ETH_CLOSE   = float(os.getenv("ETH_CLOSE_HEDGE", "3950"))

# Whales
WHALE_ENABLED       = os.getenv("WHALE_ENABLED", "true").lower() == "true"
WHALE_USD_MIN       = float(os.getenv("WHALE_USD_MIN", "1000000"))
WHALE_COOLDOWN_SEC  = int(os.getenv("WHALE_COOLDOWN_SEC", "120"))

# Endpoints (Coinbase)
CB_TICKER = "https://api.exchange.coinbase.com/products/{prod}/ticker"   # last, bid/ask, volume
CB_TRADES = "https://api.exchange.coinbase.com/products/{prod}/trades"   # recent trades (size, price, side)
# Bybit (opcional)
BYBIT_FUND = "https://api.bybit.com/v5/market/funding/history?category=linear&symbol=ETHUSDT&limit=1"
BYBIT_OI   = "https://api.bybit.com/v5/market/open-interest?category=linear&symbol=ETHUSDT&interval=5min"

# Telegram
TG_SEND = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"

# News
NEWS_RSS = "https://cointelegraph.com/rss"

app = FastAPI(title="stark-defi-agent")
pool: Optional[asyncpg.Pool] = None

from apscheduler.schedulers.asyncio import AsyncIOScheduler
scheduler = AsyncIOScheduler(timezone=str(TZ))

last_run = {
    "ingest_1m": None,
    "ingest_whales": None,
    "ingest_accounts": None,
    "ingest_onchain": None,
    "news": None,
}

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
CREATE TABLE IF NOT EXISTS news(
  id serial PRIMARY KEY,
  ts timestamptz NOT NULL DEFAULT now(),
  title text NOT NULL,
  url text
);
"""

# -------------------- Utils --------------------
async def db_init():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL não definido.")
    global pool
    pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=5)
    async with pool.acquire() as c:
        await c.execute(CREATE_SQL)

def now_utc_floor_minute() -> dt.datetime:
    return dt.datetime.now(dt.UTC).replace(second=0, microsecond=0)

async def fetch_json(url: str, headers: Dict[str,str]|None=None, params: Dict[str,Any]|None=None, timeout: float=15.0):
    try:
        async with httpx.AsyncClient(timeout=timeout, headers=headers) as s:
            r = await s.get(url, params=params)
            r.raise_for_status()
            return r.json()
    except Exception:
        # silencioso; quem chama lida com fallback
        return None

async def fetch_text(url: str, timeout: float=15.0) -> Optional[str]:
    try:
        async with httpx.AsyncClient(timeout=timeout) as s:
            r = await s.get(url)
            r.raise_for_status()
            return r.text
    except Exception:
        return None

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

# -------------------- Market (Coinbase) --------------------
async def cb_ticker(product: str) -> Optional[Dict[str, float]]:
    js = await fetch_json(CB_TICKER.format(prod=product), headers={"User-Agent":"stark-radar/1.0"})
    if not js: return None
    try:
        price = float(js["price"])
        # Não há H/L direto; mantemos NaN (o comentário usa apenas close)
        return {"price": price, "high": math.nan, "low": math.nan}
    except Exception:
        return None

async def get_spot_snapshot() -> dict:
    # ETH/BTC via Coinbase tickers
    eth = await cb_ticker("ETH-USD")
    btc = await cb_ticker("BTC-USD")
    if not (eth and btc):
        # fallback mínimo: repetir último conhecido -> sinalizar NaN se faltar
        eth = eth or {"price": math.nan, "high": math.nan, "low": math.nan}
        btc = btc or {"price": math.nan, "high": math.nan, "low": math.nan}
    return {"eth": eth, "btc": btc}

async def get_derivatives_snapshot() -> dict:
    # Bybit opcional e silencioso
    funding = None; open_interest = None
    f = await fetch_json(BYBIT_FUND, timeout=10.0)
    if f and f.get("result",{}).get("list"):
        try:
            funding = float(f["result"]["list"][0]["fundingRate"])
        except Exception:
            pass
    oi = await fetch_json(BYBIT_OI, timeout=10.0)
    if oi and oi.get("result",{}).get("list"):
        try:
            open_interest = float(oi["result"]["list"][-1]["openInterest"])
        except Exception:
            pass
    return {"funding": funding, "oi": open_interest}

async def ingest_1m():
    try:
        now = now_utc_floor_minute()
        spot = await get_spot_snapshot()
        der  = await get_derivatives_snapshot()
        eth_p = spot["eth"]["price"]; btc_p = spot["btc"]["price"]
        eth_h = spot["eth"]["high"];  eth_l = spot["eth"]["low"]
        btc_h = spot["btc"]["high"];  btc_l = spot["btc"]["low"]
        ethbtc = (eth_p / btc_p) if (isinstance(eth_p,(int,float)) and isinstance(btc_p,(int,float)) and btc_p>0) else math.nan

        async with pool.acquire() as c:
            await c.execute(
                "INSERT INTO candles_minute(ts,symbol,open,high,low,close,volume) VALUES($1,$2,NULL,$3,$4,$5,NULL) "
                "ON CONFLICT (ts,symbol) DO UPDATE SET high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close",
                now, "ETHUSD", eth_h, eth_l, eth_p
            )
            await c.execute(
                "INSERT INTO candles_minute(ts,symbol,open,high,low,close,volume) VALUES($1,$2,NULL,$3,$4,$5,NULL) "
                "ON CONFLICT (ts,symbol) DO UPDATE SET high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close",
                now, "BTCUSD", btc_h, btc_l, btc_p
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

# -------------------- Whales (Coinbase) --------------------
_last_whale_sent: Dict[str, float] = {"BUY": 0.0, "SELL": 0.0}  # cooldown por lado
_last_trade_id_seen: Optional[int] = None

async def ingest_whales():
    if not WHALE_ENABLED:
        last_run["ingest_whales"] = dt.datetime.now(TZ).isoformat(timespec="seconds")
        return
    global _last_trade_id_seen, _last_whale_sent

    try:
        # Pega trades recentes de ETH-USD
        js = await fetch_json(CB_TRADES.format(prod="ETH-USD"), headers={"User-Agent":"stark-radar/1.0"}, timeout=10.0)
        if not js: 
            last_run["ingest_whales"] = dt.datetime.now(TZ).isoformat(timespec="seconds")
            return
        # Coinbase retorna lista (mais recentes primeiro)
        trades = js if isinstance(js, list) else []
        if not trades:
            last_run["ingest_whales"] = dt.datetime.now(TZ).isoformat(timespec="seconds")
            return

        # dedup por trade_id
        if _last_trade_id_seen is None:
            _last_trade_id_seen = int(trades[0]["trade_id"])
            last_run["ingest_whales"] = dt.datetime.now(TZ).isoformat(timespec="seconds")
            return

        new = []
        for t in trades:
            tid = int(t["trade_id"])
            if tid > _last_trade_id_seen:
                new.append(t)
        if not new:
            last_run["ingest_whales"] = dt.datetime.now(TZ).isoformat(timespec="seconds")
            return

        _last_trade_id_seen = max(int(t["trade_id"]) for t in new)

        # agrega USD por lado
        buy_usd=sell_usd=0.0; buy_qty=sell_qty=0.0
        for t in new:
            price=float(t["price"]); size=float(t["size"])
            side=(t.get("side","") or "").upper()
            usd = price * size
            if side=="BUY":
                buy_usd += usd; buy_qty += size
            elif side=="SELL":
                sell_usd += usd; sell_qty += size

        ts = dt.datetime.now(dt.UTC)

        msgs=[]
        now_s = time.time()
        if buy_usd >= WHALE_USD_MIN and (now_s - _last_whale_sent["BUY"] >= WHALE_COOLDOWN_SEC):
            msgs.append(("BUY", buy_qty, buy_usd, f"coinbase agg {len(new)}"))
            _last_whale_sent["BUY"] = now_s
        if sell_usd >= WHALE_USD_MIN and (now_s - _last_whale_sent["SELL"] >= WHALE_COOLDOWN_SEC):
            msgs.append(("SELL", sell_qty, sell_usd, f"coinbase agg {len(new)}"))
            _last_whale_sent["SELL"] = now_s

        if msgs:
            async with pool.acquire() as c:
                for side, qty, usd, note in msgs:
                    await c.execute(
                        "INSERT INTO whale_events(ts,venue,side,qty,usd_value,note) VALUES($1,$2,$3,$4,$5,$6)",
                        ts, "coinbase", side, qty, usd, note
                    )
            for side, qty, usd, _ in msgs:
                await send_tg(f"🐋 {side} ~ ${usd:,.0f} | qty ~ {qty:,.1f} | coinbase")
    finally:
        last_run["ingest_whales"] = dt.datetime.now(TZ).isoformat(timespec="seconds")

# -------------------- Accounts (opcional) --------------------
BYBIT_API = "https://api.bybit.com"

def bybit_sign_qs(secret: str, params: Dict[str, Any]) -> str:
    qs = "&".join([f"{k}={params[k]}" for k in sorted(params)])
    return hmac.new(secret.encode(), qs.encode(), hashlib.sha256).hexdigest()

async def bybit_private_get(path: str, extra: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not (BYBIT_KEY and BYBIT_SEC):
        return None
    try:
        ts = str(int(time.time()*1000))
        base = {"api_key": BYBIT_KEY, "timestamp": ts, "recv_window": "5000"}
        payload = {**base, **extra}
        payload["sign"] = bybit_sign_qs(BYBIT_SEC, payload)
        url = f"{BYBIT_API}{path}"
        async with httpx.AsyncClient(timeout=20) as s:
            r = await s.get(url, params=payload)
            r.raise_for_status()
            return r.json()
    except Exception:
        return None

async def snapshot_bybit() -> List[Dict[str, Any]]:
    out=[]
    r = await bybit_private_get("/v5/account/wallet-balance", {"accountType": BYBIT_ACCOUNT_TYPE})
    if r:
        try:
            lst = r.get("result",{}).get("list",[])
            if lst:
                out.append({"venue":"bybit","metric":"total_equity","value": float(lst[0].get("totalEquity",0))})
        except Exception:
            pass
    r2 = await bybit_private_get("/v5/position/list", {"category":"linear","symbol":"ETHUSDT"})
    if r2:
        try:
            pos = r2.get("result",{}).get("list",[])
            if pos:
                out += [
                    {"venue":"bybit","metric":"ethusdt_perp_size","value": float(pos[0].get("size",0) or 0)},
                    {"venue":"bybit","metric":"ethusdt_perp_leverage","value": float(pos[0].get("leverage",0) or 0)},
                ]
        except Exception:
            pass
    return out

async def snapshot_aave() -> List[Dict[str, Any]]:
    if not AAVE_ADDR: return []
    out=[]
    try:
        AAVE_SUBGRAPH = "https://api.thegraph.com/subgraphs/name/aave/protocol-v3"
        AAVE_QUERY = """
        query ($user: String!) {
          userReserves(where: { user: $user }) {
            reserve { symbol, decimals }
            scaledATokenBalance
            scaledVariableDebt
          }
        }"""
        async with httpx.AsyncClient(timeout=25) as s:
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
        pass
    return out

async def ingest_accounts():
    rows=[]
    rows += await snapshot_bybit()
    rows += await snapshot_aave()
    if not rows:
        last_run["ingest_accounts"] = dt.datetime.now(TZ).isoformat(timespec="seconds")
        return
    async with pool.acquire() as c:
        for r in rows:
            await c.execute("INSERT INTO account_snap(venue,metric,value) VALUES($1,$2,$3)", r["venue"], r["metric"], r["value"])
    last_run["ingest_accounts"] = dt.datetime.now(TZ).isoformat(timespec="seconds")

# -------------------- News (RSS) --------------------
async def ingest_news(limit: int = 20):
    try:
        txt = await fetch_text(NEWS_RSS, timeout=15.0)
        if not txt:
            last_run["news"] = dt.datetime.now(TZ).isoformat(timespec="seconds")
            return
        root = ET.fromstring(txt)
        items = root.findall(".//item")[:limit]
        rows=[]
        for it in items:
            title = (it.findtext("title") or "").strip()
            link  = (it.findtext("link") or "").strip()
            if not title: continue
            rows.append((title, link))
        if rows:
            async with pool.acquire() as c:
                for title, link in rows:
                    await c.execute("INSERT INTO news(title,url) VALUES($1,$2) ON CONFLICT DO NOTHING", title, link)
    except Exception:
        pass
    finally:
        last_run["news"] = dt.datetime.now(TZ).isoformat(timespec="seconds")

def summarize_titles(titles: List[str], k: int = 5) -> List[str]:
    # resumão leve: top-k por diversidade (heurística simples)
    out=[]
    seen=set()
    for t in titles:
        key = (t.lower().split(" – ")[0][:60]).strip()
        if key in seen: continue
        out.append("• " + t)
        seen.add(key)
        if len(out)>=k: break
    return out

# -------------------- Commentary & Pulse --------------------
async def build_commentary() -> str:
    end = dt.datetime.now(dt.UTC); start = end - dt.timedelta(hours=8)
    async with pool.acquire() as c:
        rows = await c.fetch("SELECT ts, eth_usd, btc_usd, eth_btc_ratio FROM market_rel WHERE ts BETWEEN $1 AND $2 ORDER BY ts", start, end)
        deriv = await c.fetch("SELECT ts, funding, open_interest FROM derivatives_snap WHERE ts BETWEEN $1 AND $2 ORDER BY ts", start, end)
        whales = await c.fetch("SELECT ts, side, usd_value FROM whale_events WHERE ts BETWEEN $1 AND $2 ORDER BY ts", start, end)
        nrows = await c.fetch("SELECT title FROM news ORDER BY id DESC LIMIT 20")
    if not rows: return "⏳ Aguardando histórico para comentário (volte em alguns minutos)."

    eth = [float(r["eth_usd"]) for r in rows if r["eth_usd"] is not None]
    btc = [float(r["btc_usd"]) for r in rows if r["btc_usd"] is not None]
    ratio = [float(r["eth_btc_ratio"]) for r in rows if r["eth_btc_ratio"] is not None]

    def pct(a,b): 
        try:
            return 0.0 if a==0 else (b-a)/a*100.0
        except Exception:
            return 0.0
    eth_chg = pct(eth[0], eth[-1]) if eth else 0.0
    btc_chg = pct(btc[0], btc[-1]) if btc else 0.0
    ratio_chg = pct(ratio[0], ratio[-1]) if ratio else 0.0

    funding = deriv[-1]["funding"] if deriv else None
    oi = deriv[-1]["open_interest"] if deriv else None

    buy = sum(float(w["usd_value"] or 0) for w in whales if (w["side"] or "").upper()=="BUY")
    sell= sum(float(w["usd_value"] or 0) for w in whales if (w["side"] or "").upper()=="SELL")
    flow = "neutra"; 
    if buy>sell*1.3: flow="compradora"
    elif sell>buy*1.3: flow="vendedora"

    titles = [r["title"] for r in nrows]
    themes = summarize_titles(titles, k=4) if titles else ["• Fluxo de notícias sem driver único dominante."]

    lines=[]
    lines.append("🧾 Comentário (últimas 8h)")
    lines.append(f"• ETH: {eth_chg:+.2f}% | BTC: {btc_chg:+.2f}% | ETH/BTC: {ratio_chg:+.2f}%")
    lines.append(f"• Fluxo de baleias: {flow} (BUY ${buy:,.0f} vs SELL ${sell:,.0f})")
    if funding is not None or oi is not None:
        seg=[]
        if funding is not None: seg.append(f"Funding {float(funding)*100:.3f}%/8h")
        if oi is not None: seg.append(f"OI ~ {float(oi):,.0f}")
        if seg: lines.append("• " + " | ".join(seg))
    lines.append("• Temas de notícia:")
    lines += themes
    if ratio_chg>0 and (funding is None or funding<0.0005): sint="pró-ETH (força relativa + funding contido)."
    elif ratio_chg<0 and (funding is not None and funding>0.001): sint="atenção à euforia (funding alto)."
    else: sint="equilíbrio tático; usar gatilhos de preço."
    lines.append(f"🧭 Síntese: {sint}")
    return "\n".join(lines)

async def latest_pulse_text() -> str:
    async with pool.acquire() as c:
        m = await c.fetchrow("SELECT * FROM market_rel ORDER BY ts DESC LIMIT 1")
        e = await c.fetchrow("SELECT * FROM candles_minute WHERE symbol='ETHUSD' ORDER BY ts DESC LIMIT 1")
        b = await c.fetchrow("SELECT * FROM candles_minute WHERE symbol='BTCUSD' ORDER BY ts DESC LIMIT 1")
        d = await c.fetchrow("SELECT * FROM derivatives_snap WHERE symbol='ETHUSDT' AND exchange='bybit' ORDER BY ts DESC LIMIT 1")
    if not (m and e and b): return "⏳ Aguardando primeiros dados…"
    now = dt.datetime.now(TZ).strftime("%Y-%m-%d %H:%M")
    eth = float(m["eth_usd"]); btc = float(m["btc_usd"]); ratio = float(m["eth_btc_ratio"]) if m["eth_btc_ratio"] is not None else math.nan
    eh, el, bh, bl = e["high"], e["low"], b["high"], b["low"]
    parts=[f"🕒 {now}",
           f"ETH: ${eth:,.2f}" + (f" (H:{eh:,.2f}/L:{el:,.2f})" if isinstance(eh,(int,float)) and isinstance(el,(int,float)) else ""),
           f"BTC: ${btc:,.2f}" + (f" (H:{bh:,.2f}/L:{bl:,.2f})" if isinstance(bh,(int,float)) and isinstance(bl,(int,float)) else ""),
           f"ETH/BTC: {ratio:.5f}" if not math.isnan(ratio) else "ETH/BTC: —"]
    if d and d["funding"] is not None: parts.append(f"Funding (ETH): {float(d['funding'])*100:.3f}%/8h")
    if d and d["open_interest"] is not None: parts.append(f"Open Interest (ETH): {float(d['open_interest']):,.0f}")
    parts.append(action_line(eth))
    comment = await build_commentary()
    return "\n".join(parts) + "\n\n" + comment

async def send_pulse_to_chat():
    await send_tg(await latest_pulse_text())

# -------------------- FastAPI --------------------
@app.on_event("startup")
async def _startup():
    await db_init()
    # jobs
    scheduler.add_job(ingest_1m, "interval", minutes=1, id="ingest_1m", replace_existing=True)
    scheduler.add_job(ingest_whales, "interval", seconds=20, id="ingest_whales", replace_existing=True)
    scheduler.add_job(ingest_accounts, "interval", minutes=5, id="ingest_accounts", replace_existing=True)
    scheduler.add_job(ingest_news, "interval", minutes=10, id="ingest_news", replace_existing=True)
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
        cm_eth = await c.fetchval("SELECT COUNT(1) FROM candles_minute WHERE symbol='ETHUSD'")
        cm_btc = await c.fetchval("SELECT COUNT(1) FROM candles_minute WHERE symbol='BTCUSD'")
        ds = await c.fetchval("SELECT COUNT(1) FROM derivatives_snap")
        whales = await c.fetchval("SELECT COUNT(1) FROM whale_events")
        acc = await c.fetchval("SELECT COUNT(1) FROM account_snap")
        nws = await c.fetchval("SELECT COUNT(1) FROM news")
    return {
        "counts":{
            "market_rel": mr, "candles_eth": cm_eth, "candles_btc": cm_btc,
            "derivatives": ds, "whale_events": whales, "account_snap": acc, "news": nws
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

# Webhook: aceita /webhook e /webhook/{token}
async def _handle_update(update: Dict[str, Any]):
    msg = update.get("message") or update.get("edited_message") or update.get("channel_post")
    if not msg: return
    chat_id = str(msg["chat"]["id"])
    text = (msg.get("text") or "").strip()
    low = text.lower()

    if low in ("/start","start"):
        await send_tg("✅ Bot online.\nComandos: /pulse, /eth, /btc, /alpha, /note <texto>, /strat new <nome> | <versão> | <nota>, /strat last, /notes", chat_id); return
    if low == "/pulse":
        await send_tg(await latest_pulse_text(), chat_id); return
    if low == "/eth" or low == "eth":
        m = await pool.fetchrow("SELECT eth_usd, eth_btc_ratio FROM market_rel ORDER BY ts DESC LIMIT 1")
        if m:
            await send_tg(f"ETH: ${float(m['eth_usd']):,.2f} | ETH/BTC: {(float(m['eth_btc_ratio'])):.5f}\n" + action_line(float(m['eth_usd'])), chat_id)
        return
    if low == "/btc" or low == "btc":
        m = await pool.fetchrow("SELECT btc_usd, eth_btc_ratio FROM market_rel ORDER BY ts DESC LIMIT 1")
        if m:
            await send_tg(f"BTC: ${float(m['btc_usd']):,.2f} | ETH/BTC: {(float(m['eth_btc_ratio'])):.5f}\n" + action_line(float(m['btc_usd']) * float(m['eth_btc_ratio'] or 0) if m['eth_btc_ratio'] else float('nan')), chat_id)
        return
    if low == "/notes":
        rows = await pool.fetch("SELECT created_at,text FROM notes ORDER BY id DESC LIMIT 5")
        if not rows: await send_tg("Sem notas ainda.", chat_id); return
        out=["Notas recentes:"]+[f"- {r['created_at']:%m-%d %H:%M} • {r['text']}" for r in rows]
        await send_tg("\n".join(out), chat_id); return
    if low == "/strat last":
        row = await pool.fetchrow("SELECT created_at,name,version,note FROM strategy_versions ORDER BY id DESC LIMIT 1")
        if not row: await send_tg("Nenhuma estratégia salva.", chat_id); return
        await send_tg(f"Última estratégia:\n{row['created_at']:%Y-%m-%d %H:%M}\n{row['name']} v{row['version']}\n{row['note'] or ''}", chat_id); return
    if low.startswith("/note"):
        note = text[len("/note"):].strip()
        if not note: await send_tg("Uso: /note seu texto aqui", chat_id); return
        await pool.execute("INSERT INTO notes(tag,text) VALUES($1,$2)", None, note)
        await send_tg("📝 Nota salva.", chat_id); return
    if low.startswith("/strat new"):
        try:
            payload = text[len("/strat new"):].strip()
            name, version, note = [p.strip() for p in payload.split("|", 2)]
            await pool.execute("INSERT INTO strategy_versions(name,version,note) VALUES($1,$2,$3)", name, version, note)
            await send_tg(f"📌 Estratégia salva: {name} v{version}", chat_id)
        except Exception:
            await send_tg("Uso: /strat new <nome> | <versão> | <nota>", chat_id)
        return
    if low == "/alpha":
        rows = await pool.fetch("SELECT title FROM news ORDER BY id DESC LIMIT 10")
        if not rows:
            await send_tg("Sem notícias recentes ainda. Tente novamente em alguns minutos.", chat_id); return
        titles = [r["title"] for r in rows]
        out = ["📰 Alpha (últimas)"] + summarize_titles(titles, k=6)
        await send_tg("\n".join(out), chat_id); return

    await send_tg("Comando não reconhecido. Use /pulse, /eth, /btc, /alpha, /note, /strat new, /strat last, /notes.", chat_id)

@app.post("/webhook")
async def telegram_webhook_root(request: Request):
    try: update = await request.json()
    except Exception: return {"ok": True}
    await _handle_update(update)
    return {"ok": True}

@app.post("/webhook/{token}")
async def telegram_webhook_token(token: str, request: Request):
    # Aceita chamadas com token no path (evita 404 em setWebhook antigo)
    try: update = await request.json()
    except Exception: return {"ok": True}
    await _handle_update(update)
    return {"ok": True}

# -------- Exports --------
@app.get("/export/notes.csv")
async def export_notes():
    rows = await pool.fetch("SELECT created_at, tag, text FROM notes ORDER BY id DESC")
    buf = io.StringIO(); w = csv.writer(buf)
    w.writerow(["created_at","tag","text"])
    for r in rows: w.writerow([r["created_at"].isoformat(), r["tag"] or "", r["text"]])
    return PlainTextResponse(buf.getvalue(), media_type="text/csv")

@app.get("/export/strats.csv")
async def export_strats():
    rows = await pool.fetch("SELECT created_at, name, version, note FROM strategy_versions ORDER BY id DESC")
    buf = io.StringIO(); w = csv.writer(buf)
    w.writerow(["created_at","name","version","note"])
    for r in rows: w.writerow([r["created_at"].isoformat(), r["name"], r["version"], r["note"] or ""])
    return PlainTextResponse(buf.getvalue(), media_type="text/csv")

@app.get("/accounts/last")
async def accounts_last():
    rows = await pool.fetch("""
        SELECT * FROM account_snap
        WHERE ts > now() - interval '1 hour'
        ORDER BY ts DESC, venue, metric
    """)
    return JSONResponse({"rows": [dict(r) for r in rows]})

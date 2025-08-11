# app.py — Stark DeFi Agent v6.0.6
# Changelog:
# - /version para confirmar runtime (APP_VERSION)
# - Webhook robusto: aceita /webhook e /webhook/{qualquer_coisa}, inclusive token URL-encoded
# - Removida qualquer referência à Binance
# - /pulse, /alpha, /eth, /btc com sparkline + S/R + síntese 8h
# - /run/news e /run/ingest para disparo manual
# - Logs de boot com versão e toggles

import os, hmac, hashlib, time, math, csv, io, asyncio, traceback, re
import datetime as dt
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, List, Tuple

import httpx
import asyncpg
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse

APP_VERSION = "6.0.6"

# ===================== Config =====================
TZ = ZoneInfo(os.getenv("TZ", "America/Sao_Paulo"))
DB_URL = os.getenv("DATABASE_URL")

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "")
SEND_ENABLED = bool(TG_TOKEN)

# Bybit (para funding/OI e private snapshots)
BYBIT_KEY = os.getenv("BYBIT_RO_KEY", "")
BYBIT_SEC = os.getenv("BYBIT_RO_SECRET", "")
BYBIT_ACCOUNT_TYPE = os.getenv("BYBIT_ACCOUNT_TYPE", "UNIFIED")

# Coinbase whales opcional (desligado por padrão)
COINBASE_WHALES = os.getenv("COINBASE_WHALES", "0") == "1"
WHALE_USD_MIN = float(os.getenv("WHALE_USD_MIN", "750000"))
WHALE_COOLDOWN_SEC = int(os.getenv("WHALE_COOLDOWN_SEC", "60"))

# On-chain opcional
AAVE_ADDR = os.getenv("AAVE_ADDR", "")
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")

# Gatilhos hedge
ETH_HEDGE_1 = float(os.getenv("ETH_HEDGE_1", "3900"))
ETH_HEDGE_2 = float(os.getenv("ETH_HEDGE_2", "3800"))
ETH_CLOSE   = float(os.getenv("ETH_CLOSE_HEDGE", "3950"))

# News (RSS)
DISABLE_NEWS = os.getenv("DISABLE_NEWS", "0") == "1"
NEWSSOURCES = os.getenv("NEWSSOURCES", "https://www.coindesk.com/arc/outboundfeeds/rss/?outputType=xml,https://cointelegraph.com/rss").split(",")
NEWSTAGS = [t.strip().lower() for t in os.getenv("NEWSTAGS", "bitcoin,btc,ethereum,eth,crypto,etf,spot,defi,sec,etf inflows,derivatives,volatility,open interest,funding").split(",")]

# Endpoints fixos
TG_SEND = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
COINBASE_TICKER = "https://api.exchange.coinbase.com/products/{pair}/ticker"
COINBASE_TRADES = "https://api.exchange.coinbase.com/products/{pair}/trades?limit=100"
BYBIT_SPOT = "https://api.bybit.com/v5/market/tickers?category=spot&symbol={sym}"
BYBIT_FUND = "https://api.bybit.com/v5/market/funding/history?category=linear&symbol=ETHUSDT&limit=1"
BYBIT_OI   = "https://api.bybit.com/v5/market/open-interest?category=linear&symbol=ETHUSDT&interval=5min"
ETHERSCAN_TX = "https://api.etherscan.io/api?module=account&action=txlist&address={addr}&startblock=0&endblock=99999999&sort=desc&apikey={key}"

# App/DB
app = FastAPI(title="stark-defi-agent")
pool: Optional[asyncpg.Pool] = None

# Scheduler
from apscheduler.schedulers.asyncio import AsyncIOScheduler
scheduler = AsyncIOScheduler(timezone=str(TZ))

# ===================== State =====================
last_run = {
    "ingest_1m": None,
    "ingest_whales": None,
    "ingest_accounts": None,
    "ingest_onchain": None,
    "news": None
}
_last_whale_emit = 0
_last_trade_id = None

# ===================== SQL =====================
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
CREATE TABLE IF NOT EXISTS news_items(
  id serial PRIMARY KEY,
  ts timestamptz NOT NULL,
  source text NOT NULL,
  title text NOT NULL,
  url text
);
"""

# ===================== Utils =====================
def now_tz() -> str:
    return dt.datetime.now(TZ).isoformat(timespec="seconds")

async def db_init():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL não definido.")
    global pool
    pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=5)
    async with pool.acquire() as c:
        await c.execute(CREATE_SQL)

async def fetch_json(url: str, headers: Dict[str,str]|None=None, params: Dict[str,str]|None=None, timeout=12.0):
    async with httpx.AsyncClient(timeout=timeout) as s:
        r = await s.get(url, headers=headers, params=params)
        r.raise_for_status()
        return r.json()

async def fetch_text(url: str, headers: Dict[str,str]|None=None, timeout=10.0) -> str:
    async with httpx.AsyncClient(timeout=timeout) as s:
        r = await s.get(url, headers=headers)
        r.raise_for_status()
        return r.text

async def fetch_json_retry(url: str, attempts=2, timeout=10.0, **kw):
    last_exc=None
    for _ in range(attempts):
        try:
            async with httpx.AsyncClient(timeout=timeout) as s:
                r = await s.get(url, **kw)
                r.raise_for_status()
                return r.json()
        except Exception as e:
            last_exc=e
            await asyncio.sleep(0.8)
    if last_exc: raise last_exc

async def send_tg(text: str, chat_id: Optional[str] = None):
    if not SEND_ENABLED: return
    cid = chat_id or TG_CHAT
    if not cid: return
    async with httpx.AsyncClient(timeout=12) as s:
        await s.post(TG_SEND, json={"chat_id": cid, "text": text, "parse_mode": "HTML"})

def pct(a: float, b: float) -> float:
    if a in (None, 0) or b is None: return 0.0
    return (b-a)/a*100.0

def sparkline(vals: List[float]) -> str:
    if not vals or all(v is None for v in vals): return ""
    clean=[float(v) for v in vals if v is not None]
    if not clean: return ""
    lo, hi = min(clean), max(clean)
    if hi==lo: return "▁"*len(vals)
    chars = "▁▂▃▄▅▆▇█"
    out=[]
    for v in vals:
        if v is None:
            out.append(" ")
        else:
            idx = int((v-lo)/(hi-lo) * (len(chars)-1))
            out.append(chars[idx])
    return "".join(out)

def balloon(title: str, lines: List[str]) -> str:
    body = "\n".join(lines)
    top = f"┌─ {title} ────────────────────"
    bot = "└────────────────────────────"
    return f"<pre>{top}\n{body}\n{bot}</pre>"

# ===================== Market snapshots =====================
async def get_coinbase_pair(pair: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    try:
        j = await fetch_json(COINBASE_TICKER.format(pair=pair))
        price = float(j.get("price") or j.get("last") or j.get("ask") or 0)
        return price, None, None
    except Exception:
        return None, None, None

async def get_spot_snapshot() -> dict:
    ep, eh, el = await get_coinbase_pair("ETH-USD")
    bp, bh, bl = await get_coinbase_pair("BTC-USD")
    try:
        if ep is None or eh is None or el is None:
            eth = (await fetch_json(BYBIT_SPOT.format(sym="ETHUSDT")))["result"]["list"][0]
            ep = ep if ep is not None else float(eth["lastPrice"])
            eh = float(eth["highPrice"]); el = float(eth["lowPrice"])
    except Exception:
        pass
    try:
        if bp is None or bh is None or bl is None:
            btc = (await fetch_json(BYBIT_SPOT.format(sym="BTCUSDT")))["result"]["list"][0]
            bp = bp if bp is not None else float(btc["lastPrice"])
            bh = float(btc["highPrice"]); bl = float(btc["lowPrice"])
    except Exception:
        pass
    if ep is None or bp is None:
        try:
            cg = await fetch_json("https://api.coingecko.com/api/v3/simple/price?ids=ethereum,bitcoin&vs_currencies=usd")
            if ep is None: ep = float(cg["ethereum"]["usd"])
            if bp is None: bp = float(cg["bitcoin"]["usd"])
        except Exception:
            pass
    return {
        "eth": {"price": ep, "high": eh, "low": el},
        "btc": {"price": bp, "high": bh, "low": bl},
    }

async def get_derivatives_snapshot() -> dict:
    funding = None; open_interest = None
    try:
        f = (await fetch_json(BYBIT_FUND, timeout=8.0))["result"]["list"][0]
        funding = float(f["fundingRate"])
    except Exception:
        pass
    try:
        oi = (await fetch_json(BYBIT_OI, timeout=8.0))["result"]["list"][-1]
        open_interest = float(oi["openInterest"])
    except Exception:
        pass
    return {"funding": funding, "oi": open_interest}

# ===================== Ingest routines =====================
async def ingest_1m():
    try:
        now = dt.datetime.now(dt.UTC).replace(second=0, microsecond=0)
        spot = await get_spot_snapshot()
        der  = await get_derivatives_snapshot()
        eth_p = spot["eth"]["price"]; btc_p = spot["btc"]["price"]
        if eth_p is None or btc_p is None: return
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
        last_run["ingest_1m"] = now_tz()

async def ingest_whales_coinbase():
    global _last_trade_id, _last_whale_emit
    if not COINBASE_WHALES:
        last_run["ingest_whales"] = now_tz(); return
    try:
        j = await fetch_json_retry(COINBASE_TRADES.format(pair="ETH-USD"), attempts=2, timeout=8.0)
        trades = j if isinstance(j, list) else []
        new=[]
        top_seen = _last_trade_id
        for t in trades:
            tid = t.get("trade_id")
            if _last_trade_id is None or (isinstance(tid,int) and tid>_last_trade_id):
                new.append(t)
                if top_seen is None or tid>top_seen: top_seen=tid
        if not new:
            last_run["ingest_whales"] = now_tz(); return
        buy_usd=sell_usd=0.0; buy_qty=sell_qty=0.0
        for t in new:
            price=float(t.get("price",0)); qty=float(t.get("size",0))
            side=(t.get("side","")).upper()
            usd=price*qty
            if side=="BUY":  buy_usd+=usd; buy_qty+=qty
            else:            sell_usd+=usd; sell_qty+=qty
        _last_trade_id = top_seen
        nowsec = time.time()
        if nowsec - _last_whale_emit < WHALE_COOLDOWN_SEC:
            last_run["ingest_whales"] = now_tz(); return
        events=[]
        if buy_usd>=WHALE_USD_MIN: events.append(("BUY", buy_qty,  buy_usd,  f"agg {len(new)} trades CB"))
        if sell_usd>=WHALE_USD_MIN: events.append(("SELL", sell_qty, sell_usd, f"agg {len(new)} trades CB"))
        if events:
            ts = dt.datetime.now(dt.UTC)
            async with pool.acquire() as c:
                for side, qty, usd, note in events:
                    await c.execute(
                        "INSERT INTO whale_events(ts,venue,side,qty,usd_value,note) VALUES($1,$2,$3,$4,$5,$6)",
                        ts, "coinbase", side, qty, usd, note
                    )
            for side, qty, usd, note in events:
                await send_tg(f"🐋 {side} ~ ${usd:,.0f} | qty ~ {qty:,.1f} • {note}")
            _last_whale_emit = nowsec
    finally:
        last_run["ingest_whales"] = now_tz()

# Accounts snapshots (Bybit + Aave)
def bybit_sign_qs(secret: str, params: Dict[str, Any]) -> str:
    qs = "&".join([f"{k}={params[k]}" for k in sorted(params)])
    return hmac.new(secret.encode(), qs.encode(), hashlib.sha256).hexdigest()

async def bybit_private_get(path: str, extra: Dict[str, Any]) -> Dict[str, Any]:
    ts = str(int(time.time()*1000))
    base = {"api_key": BYBIT_KEY, "timestamp": ts, "recv_window": "5000"}
    payload = {**base, **extra}
    sign = bybit_sign_qs(BYBIT_SEC, payload)
    payload["sign"] = sign
    url = f"https://api.bybit.com{path}"
    async with httpx.AsyncClient(timeout=20) as s:
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
        pass
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
    if rows:
        async with pool.acquire() as c:
            for r in rows:
                await c.execute("INSERT INTO account_snap(venue,metric,value) VALUES($1,$2,$3)", r["venue"], r["metric"], r["value"])
    last_run["ingest_accounts"] = now_tz()

# On-chain opcional (amostra de grandes txs)
async def ingest_onchain_eth(addr: str):
    if not ETHERSCAN_API_KEY:
        last_run["ingest_onchain"] = now_tz(); return
    try:
        data = await fetch_json(ETHERSCAN_TX.format(addr=addr, key=ETHERSCAN_API_KEY), timeout=12.0)
        txs = data.get("result", [])[:12]
        ts = dt.datetime.now(dt.UTC)
        big=[]
        for t in txs:
            try:
                val_eth = float(t.get("value","0"))/1e18
                if val_eth >= 3000:
                    big.append((val_eth, t.get("from"), t.get("to")))
            except: pass
        if big:
            async with pool.acquire() as c:
                for val_eth, _from, _to in big:
                    await c.execute(
                        "INSERT INTO whale_events(ts,venue,side,qty,usd_value,note) VALUES($1,$2,$3,$4,$5,$6)",
                        ts, "onchain", "ONCHAIN", val_eth, None, f"{_from} -> {_to}"
                    )
    except Exception:
        pass
    last_run["ingest_onchain"] = now_tz()

# ===================== News ingest =====================
NEWS_RE = re.compile(r"<item>.*?<title>(.*?)</title>.*?<link>(.*?)</link>.*?<pubDate>(.*?)</pubDate>", re.S|re.I)
def _clean_xml(txt: str) -> str:
    return re.sub(r"<.*?>","",txt).strip()

async def ingest_news():
    if DISABLE_NEWS:
        last_run["news"] = now_tz(); return
    items=[]
    for src in NEWSSOURCES:
        try:
            txt = await fetch_text(src.strip(), timeout=8.0)
            for m in NEWS_RE.findall(txt):
                title=_clean_xml(m[0]); url=_clean_xml(m[1])
                ttitle=title.lower()
                if any(tag in ttitle for tag in NEWSTAGS):
                    items.append(("rss", title[:300], url[:400]))
        except Exception:
            pass
    if items:
        ts = dt.datetime.now(dt.UTC)
        async with pool.acquire() as c:
            for _, title, url in items[:30]:
                await c.execute("INSERT INTO news_items(ts,source,title,url) VALUES($1,$2,$3,$4)", ts, "rss", title, url)
    last_run["news"] = now_tz()

# ===================== Commentary =====================
async def load_series(symbol: str, minutes: int=120) -> List[float]:
    start = dt.datetime.now(dt.UTC) - dt.timedelta(minutes=minutes)
    async with pool.acquire() as c:
        rows = await c.fetch(
            "SELECT ts, close FROM candles_minute WHERE symbol=$1 AND ts>= $2 ORDER BY ts",
            symbol, start
        )
    return [float(r["close"]) if r["close"] is not None else None for r in rows]

def zones_for_eth(p: float) -> Tuple[List[str], List[str]]:
    supports=[]; res=[]
    for lvl in [4200, 4000, 3850, 3700]:
        if p is not None and p>lvl: supports.append(f"{lvl:.0f}")
    for lvl in [4300, 4400, 4550, 4700]:
        if p is not None and p<lvl: res.append(f"{lvl:.0f}")
    return supports[:3], res[:3]

def zones_for_btc(p: float) -> Tuple[List[str], List[str]]:
    supports=[]; res=[]
    for lvl in [116000, 112000, 108000, 104000]:
        if p is not None and p>lvl: supports.append(f"{lvl:,}".replace(",",".")) 
    for lvl in [120000, 124000, 128000]:
        if p is not None and p<lvl: res.append(f"{lvl:,}".replace(",","."))
    return supports[:3], res[:3]

async def build_commentary_8h() -> str:
    end = dt.datetime.now(dt.UTC); start = end - dt.timedelta(hours=8)
    async with pool.acquire() as c:
        rows = await c.fetch("SELECT ts, eth_usd, btc_usd, eth_btc_ratio FROM market_rel WHERE ts BETWEEN $1 AND $2 ORDER BY ts", start, end)
        deriv = await c.fetch("SELECT ts, funding, open_interest FROM derivatives_snap WHERE ts BETWEEN $1 AND $2 ORDER BY ts", start, end)
        whales = await c.fetch("SELECT ts, side, usd_value FROM whale_events WHERE ts BETWEEN $1 AND $2 ORDER BY ts", start, end)
        news  = await c.fetch("SELECT ts, title FROM news_items WHERE ts BETWEEN $1 AND $2 ORDER BY ts DESC LIMIT 6", start, end)
    if not rows: return "⏳ Sem histórico suficiente (tente em alguns minutos)."
    eth = [float(r["eth_usd"]) for r in rows]; btc = [float(r["btc_usd"]) for r in rows]; ratio = [float(r["eth_btc_ratio"]) for r in rows]
    eth_chg = pct(eth[0], eth[-1]); btc_chg = pct(btc[0], btc[-1]); ratio_chg = pct(ratio[0], ratio[-1])
    funding = float(deriv[-1]["funding"]) if (deriv and deriv[-1]["funding"] is not None) else None
    oi = float(deriv[-1]["open_interest"]) if (deriv and deriv[-1]["open_interest"] is not None) else None
    buy = sum(float(w["usd_value"] or 0) for w in whales if w["side"]=="BUY")
    sell= sum(float(w["usd_value"] or 0) for w in whales if w["side"]=="SELL")
    flow = "neutra"
    if buy>sell*1.3: flow="compradora"
    elif sell>buy*1.3: flow="vendedora"
    news_hint = ""
    if news:
        headlines = [n["title"] for n in news][:3]
        news_hint = " • ".join(h[:80] for h in headlines)
    synth = []
    synth.append(f"ETH: {eth_chg:+.2f}%/8h; " + (f"funding {funding*100:.3f}%/8h, " if funding is not None else "") + (f"OI ~ {oi:,.0f}. " if oi is not None else "") + f"Pressão {flow}.")
    synth.append(f"BTC: {btc_chg:+.2f}%/8h; {'beta menor que ETH' if ratio_chg>0 else 'dominância em alta'}.")
    synth.append(f"ETH/BTC: {ratio_chg:+.2f}%/8h; " + ("ETH com força relativa." if ratio_chg>0 else "BTC comandando o ritmo."))
    if news_hint: synth.append(f"News: {news_hint}")
    if ratio_chg>0 and (funding is None or funding<0.0006):
        synth.append("Leitura: pró-ETH (força relativa + funding contido).")
    elif funding is not None and funding>0.0012:
        synth.append("Leitura: risco de euforia em perp (funding elevado).")
    else:
        synth.append("Leitura: equilíbrio tático; respeitar zonas S/R.")
    return "\n".join(synth)

def action_line(eth_price: Optional[float]) -> str:
    if eth_price is None: return "…"
    if eth_price < ETH_HEDGE_2:
        return f"🚨 ETH < {ETH_HEDGE_2:.0f} → ampliar hedge p/ 20% (29 ETH)."
    if eth_price < ETH_HEDGE_1:
        return f"⚠️ ETH < {ETH_HEDGE_1:.0f} → ativar hedge 15% (22 ETH)."
    if eth_price > ETH_CLOSE:
        return f"↩️ ETH > {ETH_CLOSE:.0f} → avaliar fechar hedge."
    return "✅ Sem gatilho imediato."

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
    eth_series = await load_series("ETHUSDT", 120)
    btc_series = await load_series("BTCUSDT", 120)
    s_eth = sparkline(eth_series[-60:])
    s_btc = sparkline(btc_series[-60:])
    parts=[
        f"🕒 {now}",
        f"ETH: ${eth:,.2f}" + (f" (H:{eh:,.2f}/L:{el:,.2f})" if isinstance(eh,(int,float)) and isinstance(el,(int,float)) else ""),
        f"BTC: ${btc:,.2f}" + (f" (H:{bh:,.2f}/L:{bl:,.2f})" if isinstance(bh,(int,float)) and isinstance(bl,(int,float)) else ""),
        f"ETH/BTC: {ratio:.5f}",
    ]
    if d and d["funding"] is not None: parts.append(f"Funding (ETH): {float(d['funding'])*100:.3f}%/8h")
    if d and d["open_interest"] is not None: parts.append(f"Open Interest (ETH): {float(d['open_interest']):,.0f}")
    box = balloon("PULSE", [f"📈 ETH  {s_eth}", f"₿  BTC  {s_btc}", "", action_line(eth)])
    comment = await build_commentary_8h()
    return "\n".join(parts) + "\n\n" + box + "\n" + comment

def mini_pulse_asset(name: str, price: Optional[float], ch8h: Optional[float], extra: List[str], supports: List[str], res: List[str], trig: Optional[str]=None) -> str:
    sup = " / ".join(supports) if supports else "—"
    rr  = " / ".join(res) if res else "—"
    lines = [
        f"{name}: ${price:,.2f} | Δ8h {ch8h:+.2f}%" if (price is not None and ch8h is not None) else f"{name}: ${price:,.2f}" if price is not None else f"{name}: n/d",
        *extra,
        f"Zonas • S: {sup} | R: {rr}",
        trig or ""
    ]
    return "\n".join([l for l in lines if l])

async def asset_card(symbol: str) -> str:
    end = dt.datetime.now(dt.UTC); start = end - dt.timedelta(hours=8)
    async with pool.acquire() as c:
        rows = await c.fetch("SELECT ts, close, high, low FROM candles_minute WHERE symbol=$1 AND ts BETWEEN $2 AND $3 ORDER BY ts", symbol, start, end)
        d = await c.fetchrow("SELECT * FROM derivatives_snap WHERE symbol='ETHUSDT' AND exchange='bybit' ORDER BY ts DESC LIMIT 1")
    if not rows:
        return "⏳ Aguardando histórico…"
    prices = [float(r["close"]) for r in rows if r["close"] is not None]
    if not prices: return "⏳ Aguardando histórico…"
    ch8h = pct(prices[0], prices[-1])
    price = prices[-1]
    series = await load_series(symbol, 120)
    sl = sparkline(series[-60:])
    if symbol=="ETHUSDT":
        funding = d["funding"] if d and d["funding"] is not None else None
        oi = d["open_interest"] if d and d["open_interest"] is not None else None
        supp, res = zones_for_eth(price)
        extra=[]
        if funding is not None: extra.append(f"Funding: {float(funding)*100:.3f}%/8h")
        if oi is not None: extra.append(f"OI: {float(oi):,.0f}")
        trig = action_line(price)
        core = mini_pulse_asset("ETH", price, ch8h, extra, supp, res, trig)
        box = balloon("ETH", [f"📈 {sl}"])
        return core + "\n\n" + box
    else:
        supp, res = zones_for_btc(price)
        core = mini_pulse_asset("BTC", price, ch8h, [], supp, res, None)
        box = balloon("BTC", [f"📈 {sl}"])
        return core + "\n\n" + box

# ===================== FastAPI =====================
@app.on_event("startup")
async def _startup():
    print(f"[BOOT] Stark DeFi Agent {APP_VERSION}  | TZ={TZ}  | COINBASE_WHALES={COINBASE_WHALES}  | NEWS={'off' if DISABLE_NEWS else 'on'}")
    await db_init()
    scheduler.add_job(ingest_1m, "interval", minutes=1, id="ingest_1m", replace_existing=True, max_instances=1)
    if COINBASE_WHALES:
        scheduler.add_job(ingest_whales_coinbase, "interval", seconds=20, id="ingest_whales", replace_existing=True, max_instances=1)
    scheduler.add_job(ingest_accounts, "interval", minutes=5, id="ingest_accounts", replace_existing=True, max_instances=1)
    if AAVE_ADDR and ETHERSCAN_API_KEY:
        scheduler.add_job(ingest_onchain_eth, "interval", minutes=5, args=[AAVE_ADDR], id="ingest_onchain", replace_existing=True, max_instances=1)
    if not DISABLE_NEWS:
        scheduler.add_job(ingest_news, "interval", minutes=15, id="ingest_news", replace_existing=True, max_instances=1)
    # Boletins (08,14,20)
    scheduler.add_job(send_pulse_to_chat, "cron", hour=8,  minute=0, id="bulletin_08", replace_existing=True)
    scheduler.add_job(send_pulse_to_chat, "cron", hour=14, minute=0, id="bulletin_14", replace_existing=True)
    scheduler.add_job(send_pulse_to_chat, "cron", hour=20, minute=0, id="bulletin_20", replace_existing=True)
    scheduler.start()

async def send_pulse_to_chat():
    await send_tg(await latest_pulse_text())

@app.get("/healthz")
async def healthz():
    return {"ok": True, "time": now_tz(), "version": APP_VERSION}

@app.get("/version")
async def version():
    return {
        "version": APP_VERSION,
        "toggles": {
            "COINBASE_WHALES": COINBASE_WHALES,
            "DISABLE_NEWS": DISABLE_NEWS
        }
    }

@app.get("/status")
async def status():
    async with pool.acquire() as c:
        mr = await c.fetchval("SELECT COUNT(1) FROM market_rel")
        cm_eth = await c.fetchval("SELECT COUNT(1) FROM candles_minute WHERE symbol='ETHUSDT'")
        cm_btc = await c.fetchval("SELECT COUNT(1) FROM candles_minute WHERE symbol='BTCUSDT'")
        ds = await c.fetchval("SELECT COUNT(1) FROM derivatives_snap")
        whales = await c.fetchval("SELECT COUNT(1) FROM whale_events")
        acc = await c.fetchval("SELECT COUNT(1) FROM account_snap")
        news = await c.fetchval("SELECT COUNT(1) FROM news_items")
    return {
        "counts":{
            "market_rel": mr, "candles_eth": cm_eth, "candles_btc": cm_btc,
            "derivatives": ds, "whale_events": whales, "account_snap": acc,
            "news": news,
        },
        "last_run": last_run,
        "version": APP_VERSION
    }

@app.post("/run/accounts")
async def run_accounts():
    await ingest_accounts()
    return {"ok": True, "ran_at": last_run["ingest_accounts"]}

@app.post("/run/news")
async def run_news():
    await ingest_news()
    return {"ok": True, "ran_at": last_run["news"]}

@app.post("/run/ingest")
async def run_ingest():
    await ingest_1m()
    return {"ok": True, "ran_at": last_run["ingest_1m"]}

@app.get("/pulse")
async def pulse():
    text = await latest_pulse_text()
    await send_tg(text)
    return {"ok": True, "message": text}

@app.get("/alpha")
async def alpha():
    txt = await build_commentary_8h()
    await send_tg("🧠 <b>Alpha</b>\n" + txt)
    return {"ok": True, "message": txt}

@app.get("/accounts/last")
async def accounts_last():
    async with pool.acquire() as c:
        rows = await c.fetch("""
            SELECT * FROM account_snap
            WHERE ts > now() - interval '1 hour'
            ORDER BY ts DESC, venue, metric
        """)
    return JSONResponse({"rows": [dict(r) for r in rows]})

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

# ===================== Telegram Webhook =====================
async def _process_update(request: Request) -> Dict[str, Any]:
    try: update = await request.json()
    except Exception: return {"ok": True}
    msg = update.get("message") or update.get("edited_message") or update.get("channel_post")
    if not msg: return {"ok": True}
    chat_id = str(msg["chat"]["id"])
    text = (msg.get("text") or "").strip()
    low = text.lower()

    if low in ("/start","start"):
        await send_tg(
            "✅ Bot online.\n"
            "Comandos:\n"
            "• /pulse — panorama + gráfico\n"
            "• /alpha — comentário 8h c/ notícias\n"
            "• /eth — boletim ETH (preço/H-L, funding/OI, S/R)\n"
            "• /btc — boletim BTC (preço/H-L, S/R)\n"
            "• /note <texto> — salvar nota\n"
            "• /strat new <nome> | <versão> | <nota>\n"
            "• /strat last, /notes",
            chat_id
        )
        return {"ok": True}

    if low == "/pulse":
        await send_tg(await latest_pulse_text(), chat_id); return {"ok": True}
    if low == "/alpha":
        await send_tg("🧠 <b>Alpha</b>\n" + await build_commentary_8h(), chat_id); return {"ok": True}
    if low == "/eth":
        await send_tg(await asset_card("ETHUSDT"), chat_id); return {"ok": True}
    if low == "/btc":
        await send_tg(await asset_card("BTCUSDT"), chat_id); return {"ok": True}

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

    await send_tg("Comando não reconhecido. Use /pulse, /alpha, /eth, /btc, /note, /strat new, /strat last, /notes.", chat_id)
    return {"ok": True}

@app.post("/webhook")
async def webhook_root(request: Request):
    return await _process_update(request)

@app.post("/webhook/{rest:path}")
async def webhook_any(request: Request, rest: str):
    # aceita /webhook/<token> e variações com %3A etc.
    return await _process_update(request)

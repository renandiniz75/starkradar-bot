# app.py — Stark DeFi Agent v5.6.2 (2025-08-10)
# Mudanças:
# - /pulse reescrito (formatação por seções, comentário explicativo, níveis e ação)
# - Manchetes: não lista títulos crus; filtra e sintetiza 2–3 itens relevantes
# - Estatística de derivativos em janela (funding atual; OI nível e delta 8h)
# - Pequenos hardens em fallback de dados
# Observação: continua usando Coinbase p/ preço/24h; Bybit p/ funding/OI (com retry); CG como 2º fallback.

import os, hmac, hashlib, time, math, csv, io, asyncio, traceback, json, logging, re
import datetime as dt
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, List, Tuple

import httpx
import asyncpg
from fastapi import FastAPI, Request, Path, Query
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware

APP_VERSION = "5.6.2"

# ===== Config (pode sobrescrever por ENV) =====
DEFAULT_TG_TOKEN = "8349135220:AAFHKrmSexocLxEhtP0XouE0EBmTxllh9lU"  # fornecido por você
DEFAULT_TG_CHAT  = "47045110"

TZ = ZoneInfo(os.getenv("TZ", "America/Sao_Paulo"))
DB_URL = os.getenv("DATABASE_URL")

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", DEFAULT_TG_TOKEN)
TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID",  DEFAULT_TG_CHAT)
SEND_ENABLED = bool(TG_TOKEN)

BYBIT_KEY = os.getenv("BYBIT_RO_KEY", "")
BYBIT_SEC = os.getenv("BYBIT_RO_SECRET", "")
BYBIT_ACCOUNT_TYPE = os.getenv("BYBIT_ACCOUNT_TYPE", "UNIFIED")

AAVE_ADDR = os.getenv("AAVE_ADDR", "")
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")

ETH_HEDGE_1 = float(os.getenv("ETH_HEDGE_1", "3900"))
ETH_HEDGE_2 = float(os.getenv("ETH_HEDGE_2", "3800"))
ETH_CLOSE   = float(os.getenv("ETH_CLOSE_HEDGE", "3950"))

WHALE_USD_MIN = float(os.getenv("WHALE_USD_MIN", "500000"))
WHALES_ENABLED = os.getenv("WHALES_ENABLED", "true").lower() not in ("0","false","no")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
NEWS_MAX_H = int(os.getenv("NEWS_MAX_H", "12"))
NEWS_MAX_ITEMS = int(os.getenv("NEWS_MAX_ITEMS", "5"))

# ===== Endpoints =====
BYBIT_API = "https://api.bybit.com"
BYBIT_PUBLIC_DOMS = ("https://api.bybit.com", "https://api.bybitglobal.com")
BYBIT_SPOT_QS = "/v5/market/tickers"
BYBIT_FUND = "https://api.bybit.com/v5/market/funding/history?category=linear&symbol=ETHUSDT&limit=1"
BYBIT_OI   = "https://api.bybit.com/v5/market/open-interest?category=linear&symbol=ETHUSDT&interval=5min"

COINBASE_TICK   = "https://api.exchange.coinbase.com/products/{pair}/ticker"
COINBASE_24H    = "https://api.exchange.coinbase.com/products/{pair}/stats"
COINBASE_TRADES = "https://api.exchange.coinbase.com/products/{pair}/trades"

COINGECKO_SIMPLE = "https://api.coingecko.com/api/v3/simple/price"

NEWS_FEEDS = [
  "https://www.coindesk.com/arc/outboundfeeds/rss/",
  "https://cointelegraph.com/rss"
]

TG_SEND   = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
TG_PHOTO  = f"https://api.telegram.org/bot{TG_TOKEN}/sendPhoto"

# ===== Aave Subgraph (fix) =====
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

# ===== App / DB / Sched =====
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
log = logging.getLogger("stark-defi-agent")

app = FastAPI(title="stark-defi-agent", version=APP_VERSION)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

from apscheduler.schedulers.asyncio import AsyncIOScheduler
scheduler = AsyncIOScheduler(timezone=str(TZ))

pool: Optional[asyncpg.Pool] = None
last_run = {"ingest_1m": None, "ingest_whales": None, "ingest_accounts": None, "ingest_onchain": None, "news": None}

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
CREATE TABLE IF NOT EXISTS news_headlines(
  id serial PRIMARY KEY,
  ts timestamptz NOT NULL,
  source text NOT NULL,
  title text NOT NULL
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

# ===== HTTP helpers =====
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
                if r.status_code in (403,429,451,502,503): raise httpx.HTTPStatusError("retryable", request=r.request, response=r)
                r.raise_for_status(); return r.json()
        except Exception as e:
            last_exc = e; await asyncio.sleep(base_delay*(2**i)+0.1*i)
    raise last_exc

async def fetch_text_retry(url: str, *, tries: int = 4, base_delay: float = 0.6):
    hdrs = DEFAULT_HEADERS
    last_exc = None
    for i in range(tries):
        try:
            async with httpx.AsyncClient(timeout=25, headers=hdrs) as s:
                r = await s.get(url)
                if r.status_code in (403,429,451,502,503): raise httpx.HTTPStatusError("retryable", request=r.request, response=r)
                r.raise_for_status(); return r.text
        except Exception as e:
            last_exc = e; await asyncio.sleep(base_delay*(2**i)+0.1*i)
    raise last_exc

async def send_tg(text: str, chat_id: Optional[str] = None):
    if not SEND_ENABLED: return
    cid = str(chat_id or TG_CHAT)
    async with httpx.AsyncClient(timeout=12) as s:
        r = await s.post(TG_SEND, json={"chat_id": cid, "text": text})
        try: r.raise_for_status()
        except Exception: log.exception("Falha Telegram: %s", r.text)

async def send_photo(url: str, caption: str = "", chat_id: Optional[str] = None):
    if not SEND_ENABLED: return
    cid = str(chat_id or TG_CHAT)
    async with httpx.AsyncClient(timeout=20) as s:
        r = await s.post(TG_PHOTO, data={"chat_id": cid, "caption": caption, "photo": url})
        try: r.raise_for_status()
        except Exception: log.exception("Falha sendPhoto: %s", r.text)

def action_line(eth_price: float) -> str:
    if eth_price < ETH_HEDGE_2: return f"🚨 ETH < {ETH_HEDGE_2:.0f} → ampliar hedge p/ 20% (29 ETH)."
    if eth_price < ETH_HEDGE_1: return f"⚠️ ETH < {ETH_HEDGE_1:.0f} → ativar hedge 15% (22 ETH)."
    if eth_price > ETH_CLOSE:   return f"↩️ ETH > {ETH_CLOSE:.0f} → avaliar fechar hedge."
    return "✅ Sem gatilho. Suportes: 4.200/4.000 | Resist: 4.300/4.400."

# ===== Market snapshots =====
async def get_spot_snapshot() -> dict:
    try:
        ce = await fetch_json_retry(COINBASE_TICK.format(pair="ETH-USD"))
        cb = await fetch_json_retry(COINBASE_TICK.format(pair="BTC-USD"))
        se = await fetch_json_retry(COINBASE_24H.format(pair="ETH-USD"))
        sb = await fetch_json_retry(COINBASE_24H.format(pair="BTC-USD"))
        return {
            "eth": {"price": float(ce["price"]), "high": float(se["high"]), "low": float(se["low"]), "vol24": float(se.get("volume", "0") or 0)},
            "btc": {"price": float(cb["price"]), "high": float(sb["high"]), "low": float(sb["low"]), "vol24": float(sb.get("volume", "0") or 0)},
        }
    except Exception:
        pass
    for dom in BYBIT_PUBLIC_DOMS:
        try:
            eth = (await fetch_json_retry(f"{dom}{BYBIT_SPOT_QS}", params={"category":"spot","symbol":"ETHUSDT"}))["result"]["list"][0]
            btc = (await fetch_json_retry(f"{dom}{BYBIT_SPOT_QS}", params={"category":"spot","symbol":"BTCUSDT"}))["result"]["list"][0]
            return {
                "eth": {"price": float(eth["lastPrice"]), "high": float(eth["highPrice"]), "low": float(eth["lowPrice"]), "vol24": math.nan},
                "btc": {"price": float(btc["lastPrice"]), "high": float(btc["highPrice"]), "low": float(btc["lowPrice"]), "vol24": math.nan},
            }
        except Exception:
            continue
    cg = await fetch_json_retry(COINGECKO_SIMPLE, params={"ids":"ethereum,bitcoin","vs_currencies":"usd"})
    return {"eth":{"price":float(cg["ethereum"]["usd"]),"high":math.nan,"low":math.nan,"vol24":math.nan},
            "btc":{"price":float(cg["bitcoin"]["usd"]), "high":math.nan,"low":math.nan,"vol24":math.nan}}

async def get_derivatives_snapshot() -> dict:
    funding = None; open_interest = None
    try:
        f = (await fetch_json_retry(BYBIT_FUND))["result"]["list"][0]; funding = float(f["fundingRate"])
    except Exception:
        pass
    try:
        oi = (await fetch_json_retry(BYBIT_OI))["result"]["list"][-1]; open_interest = float(oi["openInterest"])
    except Exception:
        pass
    return {"funding": funding, "oi": open_interest}

async def ingest_1m():
    try:
        now = dt.datetime.now(dt.UTC).replace(second=0, microsecond=0)
        spot = await get_spot_snapshot(); der = await get_derivatives_snapshot()
        eth_p = spot["eth"]["price"]; btc_p = spot["btc"]["price"]
        eth_h = spot["eth"]["high"];  eth_l = spot["eth"]["low"]
        btc_h = spot["btc"]["high"];  btc_l = spot["btc"]["low"]
        ethbtc = eth_p / btc_p if btc_p else math.nan
        async with pool.acquire() as c:
            await c.execute("INSERT INTO candles_minute(ts,symbol,open,high,low,close,volume) VALUES($1,$2,NULL,$3,$4,$5,NULL) "
                            "ON CONFLICT (ts,symbol) DO UPDATE SET high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close",
                            now, "ETHUSDT", eth_h, eth_l, eth_p)
            await c.execute("INSERT INTO candles_minute(ts,symbol,open,high,low,close,volume) VALUES($1,$2,NULL,$3,$4,$5,NULL) "
                            "ON CONFLICT (ts,symbol) DO UPDATE SET high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close",
                            now, "BTCUSDT", btc_h, btc_l, btc_p)
            await c.execute("INSERT INTO market_rel(ts,eth_usd,btc_usd,eth_btc_ratio) VALUES($1,$2,$3,$4) "
                            "ON CONFLICT (ts) DO UPDATE SET eth_usd=EXCLUDED.eth_usd, btc_usd=EXCLUDED.btc_usd, eth_btc_ratio=EXCLUDED.eth_btc_ratio",
                            now, eth_p, btc_p, ethbtc)
            await c.execute("INSERT INTO derivatives_snap(ts,symbol,exchange,funding,open_interest) VALUES($1,$2,$3,$4,$5) "
                            "ON CONFLICT (ts,symbol,exchange) DO UPDATE SET funding=EXCLUDED.funding, open_interest=EXCLUDED.open_interest",
                            now, "ETHUSDT", "bybit", der["funding"], der["oi"])
    finally:
        last_run["ingest_1m"] = dt.datetime.now(TZ).isoformat(timespec="seconds")

# ===== Whales (proxy via Coinbase trades) =====
_last_trade_id_cb = None
_ws_lock = asyncio.Lock()

async def ingest_whales():
    if not WHALES_ENABLED:
        last_run["ingest_whales"] = dt.datetime.now(TZ).isoformat(timespec="seconds"); return
    global _last_trade_id_cb
    try:
        async with _ws_lock:
            try:
                trades = await fetch_json_retry(COINBASE_TRADES.format(pair="ETH-USD"), params={"limit":"100"})
            except Exception:
                trades = []
            if not trades:
                last_run["ingest_whales"] = dt.datetime.now(TZ).isoformat(timespec="seconds"); return
            new=[]
            for t in trades:
                tid=t.get("trade_id")
                if _last_trade_id_cb is None or (isinstance(tid,int) and tid>_last_trade_id_cb): new.append(t)
            if not new:
                last_run["ingest_whales"] = dt.datetime.now(TZ).isoformat(timespec="seconds"); return
            _last_trade_id_cb = max(int(t["trade_id"]) for t in new if "trade_id" in t)
            buy_usd=sell_usd=buy_qty=sell_qty=0.0
            for t in new:
                price=float(t["price"]); qty=float(t["size"]); usd=price*qty; side=(t.get("side") or "").upper()
                if side=="BUY": buy_usd+=usd; buy_qty+=qty
                else: sell_usd+=usd; sell_qty+=qty
            ts=dt.datetime.now(dt.UTC); events=[]
            if buy_usd>=WHALE_USD_MIN: events.append(("BUY",buy_qty,buy_usd,f"coinbase {len(new)} trades"))
            if sell_usd>=WHALE_USD_MIN: events.append(("SELL",sell_qty,sell_usd,f"coinbase {len(new)} trades"))
            if events:
                async with pool.acquire() as c:
                    for side,qty,usd,note in events:
                        await c.execute("INSERT INTO whale_events(ts,venue,side,qty,usd_value,note) VALUES($1,$2,$3,$4,$5,$6)",
                                        ts,"coinbase",side,qty,usd,note)
                for side,qty,usd,note in events:
                    await send_tg(f"🐋 {side} ~ ${usd:,.0f} | qty ~ {qty:,.1f} | {note}")
    finally:
        last_run["ingest_whales"] = dt.datetime.now(TZ).isoformat(timespec="seconds")

# ===== Accounts / Aave =====
def bybit_sign_qs(secret: str, params: Dict[str, Any]) -> str:
    qs = "&".join([f"{k}={params[k]}" for k in sorted(params)])
    return hmac.new(secret.encode(), qs.encode(), hashlib.sha256).hexdigest()

async def bybit_private_get(path: str, extra: Dict[str, Any]) -> Dict[str, Any]:
    ts = str(int(time.time()*1000))
    base = {"api_key": BYBIT_KEY, "timestamp": ts, "recv_window": "5000"}
    payload = {**base, **extra}; payload["sign"] = bybit_sign_qs(BYBIT_SEC, payload)
    url = f"{BYBIT_API}{path}"
    async with httpx.AsyncClient(timeout=20, headers=DEFAULT_HEADERS) as s:
        r = await s.get(url, params=payload); r.raise_for_status(); return r.json()

async def snapshot_bybit() -> List[Dict[str, Any]]:
    if not (BYBIT_KEY and BYBIT_SEC): return []
    out=[]
    try:
        r = await bybit_private_get("/v5/account/wallet-balance", {"accountType": BYBIT_ACCOUNT_TYPE})
        lst = r.get("result",{}).get("list",[])
        if lst: out.append({"venue":"bybit","metric":"total_equity","value": float(lst[0].get("totalEquity",0))})
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
            r.raise_for_status(); data=r.json()
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
    rows=[]; rows += await snapshot_bybit(); rows += await snapshot_aave()
    if rows:
        async with pool.acquire() as c:
            for r in rows:
                await c.execute("INSERT INTO account_snap(venue,metric,value) VALUES($1,$2,$3)", r["venue"], r["metric"], r["value"])
    last_run["ingest_accounts"] = dt.datetime.now(TZ).isoformat(timespec="seconds")

# ===== On-chain opcional =====
ETHERSCAN_TX = "https://api.etherscan.io/api?module=account&action=txlist&address={addr}&startblock=0&endblock=99999999&sort=desc&apikey={key}"
async def ingest_onchain_eth(addr: str):
    if not ETHERSCAN_API_KEY:
        last_run["ingest_onchain"] = dt.datetime.now(TZ).isoformat(timespec="seconds"); return
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

# ===== News (RSS) =====
def _parse_rss_items(xml: str) -> List[Dict[str,str]]:
    items=[]
    for m in re.finditer(r"<item>(.*?)</item>", xml, re.DOTALL|re.IGNORECASE):
        block = m.group(1)
        t_match = re.search(r"<title>\s*<!\[CDATA\[(.*?)\]\]>\s*</title>|<title>(.*?)</title>", block, re.DOTALL|re.IGNORECASE)
        d_match = re.search(r"<pubDate>(.*?)</pubDate>", block, re.DOTALL|re.IGNORECASE)
        title = (t_match.group(1) or t_match.group(2) or "").strip() if t_match else ""
        pub = (d_match.group(1) or "").strip() if d_match else ""
        items.append({"title": re.sub(r"\s+", " ", title), "pub": pub})
    return items

def _rss_date_recent(pub: str, max_hours: int) -> bool:
    try:
        dt_pub = dt.datetime.strptime(pub[:25], "%a, %d %b %Y %H:%M:%S").replace(tzinfo=dt.timezone.utc)
        return (dt.datetime.now(dt.timezone.utc)-dt_pub).total_seconds() <= max_hours*3600
    except Exception:
        return True

async def fetch_news():
    headlines=[]
    for url in NEWS_FEEDS:
        try:
            xml = await fetch_text_retry(url)
            items = _parse_rss_items(xml)
            for it in items:
                if _rss_date_recent(it["pub"], NEWS_MAX_H):
                    headlines.append((url.split("/")[2], it["title"]))
        except Exception:
            continue
    seen=set(); out=[]
    for src, t in headlines:
        key=(src, t.lower())
        if key in seen: continue
        seen.add(key); out.append((src, t))
        if len(out)>=NEWS_MAX_ITEMS: break
    if out:
        async with pool.acquire() as c:
            for src, t in out:
                await c.execute("INSERT INTO news_headlines(ts,source,title) VALUES(now(),$1,$2)", src, t)
    last_run["news"] = dt.datetime.now(TZ).isoformat(timespec="seconds")
    return out

def _synthesize_news(items: List[Tuple[str,str]]) -> List[str]:
    """Converte manchetes em frases de impacto (heurística simples)."""
    out=[]
    for src, t in items[:3]:
        tl=t.lower()
        if any(k in tl for k in ("etf", "sec", "approval", "aprov", "spot")):
            out.append("Possível efeito de **ETF/fluxo regulatório** elevando apetite por risco.")
        elif any(k in tl for k in ("upgrade", "hard fork", "dencun", "proto-dank", "eip")):
            out.append("Narrativa **técnica/upgrade** reforçando expectativa de uso e throughput.")
        elif any(k in tl for k in ("hack", "exploit", "bridge", "rug")):
            out.append("Risco **segurança/hack** pesando temporariamente no sentimento.")
        elif any(k in tl for k in ("liquidation", "short squeeze", "long squeeze")):
            out.append("Movimento **impulsionado por liquidações** em derivativos.")
        elif any(k in tl for k in ("whale", "flows", "inflow", "outflow", "funds")):
            out.append("Indícios de **fluxos institucionais/baleias** influenciando direção.")
        else:
            out.append("Evento noticioso relevante no período, com impacto moderado.")
    return list(dict.fromkeys(out))  # dedup

# ===== Estatística janela =====
def _pct(a,b): 
    try:
        return 0.0 if a==0 else (b-a)/a*100.0
    except Exception:
        return 0.0

def _stdev(xs):
    n=len(xs); 
    if n<2: return 0.0
    m=sum(xs)/n
    var=sum((x-m)**2 for x in xs)/(n-1)
    return var**0.5

async def _deriv_stats_window(start: dt.datetime, end: dt.datetime) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Retorna (funding_último, oi_último, oi_delta_8h)."""
    async with pool.acquire() as c:
        rows = await c.fetch("SELECT ts,funding,open_interest FROM derivatives_snap WHERE ts BETWEEN $1 AND $2 ORDER BY ts", start, end)
    if not rows: return (None, None, None)
    first = rows[0]; last = rows[-1]
    f_last = float(last["funding"]) if last["funding"] is not None else None
    oi_last = float(last["open_interest"]) if last["open_interest"] is not None else None
    oi_first = float(first["open_interest"]) if first["open_interest"] is not None else None
    oi_delta = (oi_last - oi_first) if (oi_last is not None and oi_first is not None) else None
    return (f_last, oi_last, oi_delta)

# ===== Comentário / Pulse =====
async def build_commentary_text() -> str:
    end = dt.datetime.now(dt.UTC); start = end - dt.timedelta(hours=8)
    async with pool.acquire() as c:
        rows = await c.fetch("SELECT ts, eth_usd, btc_usd, eth_btc_ratio FROM market_rel WHERE ts BETWEEN $1 AND $2 ORDER BY ts", start, end)
        whales = await c.fetch("SELECT ts, side, usd_value FROM whale_events WHERE ts BETWEEN $1 AND $2 ORDER BY ts")
        last120 = await c.fetch("SELECT ts, close FROM candles_minute WHERE symbol='ETHUSDT' ORDER BY ts DESC LIMIT 120")
        news = await c.fetch("SELECT ts, source, title FROM news_headlines WHERE ts BETWEEN $1 AND $2 ORDER BY ts DESC", start, end)
    if not rows:
        return "⏳ Aguardando histórico para comentário (volte em alguns minutos)."

    eth = [float(r["eth_usd"]) for r in rows]; btc = [float(r["btc_usd"]) for r in rows]; ratio = [float(r["eth_btc_ratio"]) for r in rows]
    eth_chg=_pct(eth[0],eth[-1]); btc_chg=_pct(btc[0],btc[-1]); ratio_chg=_pct(ratio[0],ratio[-1])
    f_last, oi_last, oi_delta = await _deriv_stats_window(start, end)

    buy = sum(float(w["usd_value"] or 0) for w in whales if w["side"]=="BUY")
    sell= sum(float(w["usd_value"] or 0) for w in whales if w["side"]=="SELL")
    flow="neutra"
    if buy>sell*1.3: flow="compradora"
    elif sell>buy*1.3: flow="vendedora"

    vol1h=0.0
    if last120:
        prices=[float(r["close"]) for r in reversed(last120)]
        if len(prices)>=60:
            vol1h = _stdev(prices[-60:])/(sum(prices[-60:])/60)*100.0

    # Síntese de impacto de notícias (no máx 3)
    news_items=[(n["source"], n["title"]) for n in news][:6]
    news_synth=_synthesize_news(news_items)

    explain=[]
    # Regras explicativas
    if eth_chg>0.6 and (oi_delta is not None and oi_delta>0):
        explain.append("Alta com **OI em alta** sugere entrada de alavancados acompanhando o movimento.")
    if eth_chg< -0.6 and (oi_delta is not None and oi_delta<0):
        explain.append("Queda com **redução de OI** indica desenrolamento de posições (longs fechando).")
    if f_last is not None and f_last>0.001 and eth_chg>0:
        explain.append("**Funding positivo elevado**: atenção a euforia dos comprados.")
    if f_last is not None and f_last< -0.0005 and eth_chg<0:
        explain.append("**Funding negativo** com queda: pressão de shorts consistente.")
    if flow=="compradora":
        explain.append("Fluxo de **compras grandes** predominou no período (proxy de baleias).")
    if flow=="vendedora":
        explain.append("Fluxo de **vendas grandes** predominou (realização/hedge).")
    if not explain:
        explain.append("Movimento consistente com **beta de mercado** e microestrutura (sem anomalias marcantes).")

    # Montagem diagramada
    lines=[]
    now_local = dt.datetime.now(TZ).strftime("%Y-%m-%d %H:%M")
    lines.append(f"🕒 {now_local} • Stark DeFi Agent v{APP_VERSION}")
    lines.append("")
    lines.append("# 📈 Preço & Faixa (8h)")
    lines.append(f"• ETH: {eth[-1]:,.2f} USD ({eth_chg:+.2f}%)")
    lines.append(f"• BTC: {btc[-1]:,.2f} USD ({btc_chg:+.2f}%)")
    lines.append(f"• ETH/BTC: {ratio[-1]:.5f} ({ratio_chg:+.2f}%)")
    lines.append("")
    lines.append("# 🧮 Derivativos")
    if f_last is not None: lines.append(f"• Funding (ETH): {f_last*100:.3f}%/8h")
    if oi_last is not None: 
        if oi_delta is not None:
            lines.append(f"• Open Interest (ETH): {oi_last:,.0f} (Δ8h {oi_delta:+,.0f})")
        else:
            lines.append(f"• Open Interest (ETH): {oi_last:,.0f}")
    lines.append("")
    lines.append("# 💧 Fluxo & Volatilidade")
    lines.append(f"• Fluxo ‘baleias’: {flow}")
    lines.append(f"• Vol(1h) ≈ {vol1h:.2f}%")
    lines.append("")
    lines.append("# 📝 Comentário")
    for e in explain: lines.append(f"• {e}")
    if news_synth:
        lines.append("")
        lines.append("# 🗞️ Contexto de notícias (filtrado)")
        for s in news_synth: lines.append(f"• {s}")
    lines.append("")
    lines.append("# 🧭 Cenários & Níveis")
    lines.append("• Suportes: 4.200 / 4.000")
    lines.append("• Resistências: 4.300 / 4.400")
    # Ação será adicionada no caller com preço atual
    return "\n".join(lines)

async def latest_pulse_text() -> str:
    # pega instantâneo mais recente
    async with pool.acquire() as c:
        m = await c.fetchrow("SELECT * FROM market_rel ORDER BY ts DESC LIMIT 1")
    if not m: 
        return "⏳ Aguardando primeiros dados…"
    eth = float(m["eth_usd"])
    base = await build_commentary_text()
    return base + "\n" + action_line(eth)

# ===== Chart helper (QuickChart) =====
def chart_url(series: List[float], label: str) -> str:
    xs=list(range(len(series)))
    cfg={
      "type":"line",
      "data":{"labels": xs, "datasets":[{"label":label, "data": series, "fill": False, "pointRadius": 0, "borderWidth":2}]},
      "options":{"responsive": True, "plugins":{"legend":{"display": False}}}
    }
    return "https://quickchart.io/chart?w=800&h=400&devicePixelRatio=2&c=" + httpx.URL("").copy_with(query={"": json.dumps(cfg)}).query[1:]

# ===== Telegram tasks =====
async def send_pulse_to_chat(): await send_tg(await latest_pulse_text())

# ===== Startup =====
@app.on_event("startup")
async def _startup():
    await db_init()
    scheduler.add_job(ingest_1m, "interval", minutes=1, id="ingest_1m", replace_existing=True)
    scheduler.add_job(ingest_whales, "interval", seconds=45, id="ingest_whales", replace_existing=True)
    scheduler.add_job(ingest_accounts, "interval", minutes=5, id="ingest_accounts", replace_existing=True)
    scheduler.add_job(fetch_news, "interval", minutes=15, id="news", replace_existing=True)
    if AAVE_ADDR and ETHERSCAN_API_KEY:
        scheduler.add_job(ingest_onchain_eth, "interval", minutes=5, args=[AAVE_ADDR], id="ingest_onchain", replace_existing=True)
    scheduler.add_job(send_pulse_to_chat, "cron", hour=8,  minute=0, id="bulletin_08", replace_existing=True)
    scheduler.add_job(send_pulse_to_chat, "cron", hour=14, minute=0, id="bulletin_14", replace_existing=True)
    scheduler.add_job(send_pulse_to_chat, "cron", hour=20, minute=0, id="bulletin_20", replace_existing=True)
    scheduler.start()
    log.info("Startup OK. Versão %s", APP_VERSION)

# ===== Endpoints =====
@app.get("/")
async def root(): return {"ok": True, "service": "stark-defi-agent", "version": APP_VERSION}

@app.get("/healthz")
async def healthz(): return {"ok": True, "time": dt.datetime.now(TZ).isoformat(), "version": APP_VERSION}

@app.get("/status")
async def status():
    async with pool.acquire() as c:
        mr = await c.fetchval("SELECT COUNT(1) FROM market_rel")
        cm_eth = await c.fetchval("SELECT COUNT(1) FROM candles_minute WHERE symbol='ETHUSDT'")
        cm_btc = await c.fetchval("SELECT COUNT(1) FROM candles_minute WHERE symbol='BTCUSDT'")
        ds = await c.fetchval("SELECT COUNT(1) FROM derivatives_snap")
        whales = await c.fetchval("SELECT COUNT(1) FROM whale_events")
        acc = await c.fetchval("SELECT COUNT(1) FROM account_snap")
        news = await c.fetchval("SELECT COUNT(1) FROM news_headlines")
    return {"counts":{"market_rel": mr, "candles_eth": cm_eth, "candles_btc": cm_btc,
                      "derivatives": ds, "whale_events": whales, "account_snap": acc, "news": news},
            "last_run": last_run}

@app.post("/run/accounts")
async def run_accounts():
    await ingest_accounts(); return {"ok": True, "ran_at": last_run["ingest_accounts"]}

@app.post("/run/news")
async def run_news():
    out = await fetch_news(); return {"ok": True, "added": len(out)}

@app.post("/run/pulse")
async def run_pulse():
    text = await latest_pulse_text(); await send_tg(text); return {"ok": True, "message": text}

@app.get("/pulse")
async def pulse():
    text = await latest_pulse_text(); await send_tg(text); return {"ok": True, "message": text}

# ===== /chart =====
@app.get("/chart")
async def chart(sym: str = Query("eth", pattern="^(eth|btc)$"), send: bool = True):
    symbol = "ETHUSDT" if sym=="eth" else "BTCUSDT"
    async with pool.acquire() as c:
        rows = await c.fetch("SELECT ts, close FROM candles_minute WHERE symbol=$1 ORDER BY ts DESC LIMIT 120", symbol)
    if not rows: return {"ok": False, "error": "sem dados ainda"}
    series=[float(r["close"]) for r in reversed(rows)]
    url=chart_url(series, symbol)
    if send: await send_photo(url, caption=f"{symbol} – últimas 2h")
    return {"ok": True, "url": url}

# ===== Telegram Webhook =====
async def handle_update(update: Dict[str, Any]):
    msg = update.get("message") or update.get("edited_message") or update.get("channel_post")
    if not msg: return
    chat_id = msg["chat"]["id"]; text = (msg.get("text") or "").strip(); low = text.lower()

    if low in ("/start","start"):
        await send_tg("✅ Bot online.\nComandos: /pulse, /chart eth|btc, /eth, /btc, /alfa, /diag, /note <texto>, /strat new <nome> | <versão> | <nota>, /strat last, /notes", chat_id); return
    if low == "/pulse": await send_tg(await latest_pulse_text(), chat_id); return
    if low.startswith("/chart"):
        parts=low.split(); sym="eth"
        if len(parts)>1 and parts[1] in ("eth","btc"): sym=parts[1]
        await chart(sym=sym, send=True); return
    if low in ("/eth","/btc"):
        snap = await get_spot_snapshot()
        eth = snap["eth"]["price"]; btc = snap["btc"]["price"]; ratio = eth/btc if btc else math.nan
        await send_tg(f"ETH: ${eth:,.2f} | BTC: ${btc:,.2f} | ETH/BTC: {ratio:.5f}\n{action_line(eth)}", chat_id); return
    if low == "/alfa": 
        await send_tg(await build_commentary_text(), chat_id); return
    if low == "/notes":
        async with pool.acquire() as c:
            rows = await c.fetch("SELECT created_at,text FROM notes ORDER BY id DESC LIMIT 5")
        if not rows: await send_tg("Sem notas ainda.", chat_id); return
        out=["Notas recentes:"]+[f"- {r['created_at']:%m-%d %H:%M} • {r['text']}" for r in rows]
        await send_tg("\n".join(out), chat_id); return
    if low == "/strat last":
        async with pool.acquire() as c:
            row = await c.fetchrow("SELECT created_at,name,version,note FROM strategy_versions ORDER BY id DESC LIMIT 1")
        if not row: await send_tg("Nenhuma estratégia salva.", chat_id); return
        await send_tg(f"Última estratégia:\n{row['created_at']:%Y-%m-%d %H:%M}\n{row['name']} v{row['version']}\n{row['note'] or ''}", chat_id); return
    if low.startswith("/note"):
        note = text[len("/note"):].strip()
        if not note: await send_tg("Uso: /note seu texto aqui", chat_id); return
        async with pool.acquire() as c: await c.execute("INSERT INTO notes(tag,text) VALUES($1,$2)", None, note)
        await send_tg("📝 Nota salva.", chat_id); return
    if low.startswith("/strat new"):
        try:
            payload = text[len("/strat new"):].strip()
            name, version, note = [p.strip() for p in payload.split("|", 2)]
        except Exception:
            await send_tg("Uso: /strat new <nome> | <versão> | <nota>", chat_id); return
        async with pool.acquire() as c:
            await c.execute("INSERT INTO strategy_versions(name,version,note) VALUES($1,$2,$3)", name, version, note)
        await send_tg(f"📌 Estratégia salva: {name} v{version}", chat_id); return
    if low == "/diag":
        rep = await diag_text()
        await send_tg(rep, chat_id); return
    await send_tg("Comando não reconhecido. Use /pulse, /chart, /eth, /btc, /alfa, /diag, /note, /strat new, /strat last, /notes.", chat_id)

@app.post("/webhook")
async def telegram_webhook_plain(request: Request):
    try:
        update = await request.json(); logging.info("Webhook (plain) update_id=%s", update.get("update_id"))
        await handle_update(update)
    except Exception: logging.exception("Erro no webhook (plain).")
    return {"ok": True}

@app.post("/webhook/{token}")
async def telegram_webhook_token(request: Request, token: str = Path(...)):
    if token != TG_TOKEN: return JSONResponse({"ok": False, "error": "token mismatch"}, status_code=403)
    try:
        update = await request.json(); logging.info("Webhook (token) update_id=%s", update.get("update_id"))
        await handle_update(update)
    except Exception: logging.exception("Erro no webhook (token).")
    return {"ok": True}

# ===== Diag =====
async def diag_text() -> str:
    lines=["🔧 Diag:"]
    # Coinbase tick
    t0=time.time()
    ok_cb=True
    try:
        ce = await fetch_json_retry(COINBASE_TICK.format(pair="ETH-USD"))
        cb = await fetch_json_retry(COINBASE_TICK.format(pair="BTC-USD"))
        dtick=(time.time()-t0)*1000
        lines.append(f"- Coinbase tick OK ~{dtick:.0f}ms (ETH ${float(ce['price']):,.2f} / BTC ${float(cb['price']):,.2f})")
    except Exception as e:
        ok_cb=False; lines.append(f"- Coinbase tick FAIL: {e}")
    # Bybit fund/OI
    try:
        _ = await fetch_json_retry(BYBIT_FUND); _ = await fetch_json_retry(BYBIT_OI)
        lines.append("- Bybit fund/OI: tentativa OK")
    except Exception:
        lines.append("- Bybit fund/OI: bloqueado/indisponível (Render IP)")
    # DB write/read
    try:
        async with pool.acquire() as c:
            await c.execute("INSERT INTO actions_log(action,details) VALUES($1,$2)", "diag", json.dumps({"t": time.time()}))
            n = await c.fetchval("SELECT COUNT(1) FROM market_rel")
        lines.append(f"- DB OK (market_rel rows: {n})")
    except Exception as e:
        lines.append(f"- DB FAIL: {e}")
    # pulse preview
    if ok_cb:
        _ = await latest_pulse_text()
        lines.append("- Pulse preview pronto.")
    return "\n".join(lines)

# ===== Exports =====
@app.get("/export/notes.csv")
async def export_notes():
    async with pool.acquire() as c:
        rows = await c.fetch("SELECT created_at, tag, text FROM notes ORDER BY id DESC")
    buf = io.StringIO(); w = csv.writer(buf); w.writerow(["created_at","tag","text"])
    for r in rows: w.writerow([r["created_at"].isoformat(), r["tag"] or "", r["text"]])
    return PlainTextResponse(buf.getvalue(), media_type="text/csv")

@app.get("/export/strats.csv")
async def export_strats():
    async with pool.acquire() as c:
        rows = await c.fetch("SELECT created_at, name, version, note FROM strategy_versions ORDER BY id DESC")
    buf = io.StringIO(); w = csv.writer(buf); w.writerow(["created_at","name","version","note"])
    for r in rows: w.writerow([r["created_at"].isoformat(), r["name"], r["version"], r["note"] or ""])
    return PlainTextResponse(buf.getvalue(), media_type="text/csv")

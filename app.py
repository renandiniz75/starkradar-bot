# app.py — Stark DeFi Agent v5.7 (2025-08-10)
# Mantém v5.1 e adiciona:
# - /pulse e /alfa com comentário de 6–8h (preço, derivativos, whales, temas de notícia)
# - Corrigido /eth e /btc (sem duplicar)
# - Fallbacks de preço (Coinbase) e news via RSS
# - Whales por Bybit (recent-trade) e fallback Coinbase /trades
# - Webhook aceita /webhook e /webhook/<TOKEN>

import os, hmac, hashlib, time, math, csv, io, asyncio, traceback, json
import datetime as dt
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, List, Tuple
import xml.etree.ElementTree as ET

import httpx
import asyncpg
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse

# ====== ENV ======
TZ = ZoneInfo(os.getenv("TZ", "America/Sao_Paulo"))
DB_URL = os.getenv("DATABASE_URL")

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "")
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

# Notícias (se quiser customizar, defina NEWS_SOURCES separado por vírgula)
NEWS_SOURCES = os.getenv(
    "NEWS_SOURCES",
    "https://www.coindesk.com/arc/outboundfeeds/rss/?outputType=xml,"
    "https://cointelegraph.com/rss,"
    "https://www.theblock.co/rss.xml"
).split(",")

# ====== URLs ======
BYBIT_PUBLIC = "https://api.bybit.com"
BYBIT_GLOBAL = "https://api.bybitglobal.com"  # fallback
BYBIT_SPOT = f"{BYBIT_PUBLIC}/v5/market/tickers?category=spot&symbol={{sym}}"
BYBIT_LINEAR = f"{BYBIT_PUBLIC}/v5/market/tickers?category=linear&symbol={{sym}}"
BYBIT_RECENT_TRADES = f"{BYBIT_PUBLIC}/v5/market/recent-trade?category=linear&symbol=ETHUSDT&limit=1000"
BYBIT_FUND = f"{BYBIT_PUBLIC}/v5/market/funding/history?category=linear&symbol=ETHUSDT&limit=1"
BYBIT_OI   = f"{BYBIT_PUBLIC}/v5/market/open-interest?category=linear&symbol=ETHUSDT&interval=5min"

COINBASE_TICKER = "https://api.exchange.coinbase.com/products/{prod}/ticker"      # prod=ETH-USD/BTC-USD
COINBASE_TRADES = "https://api.exchange.coinbase.com/products/{prod}/trades"      # trades for whales fallback

TG_SEND = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"

ETHERSCAN_TX = "https://api.etherscan.io/api?module=account&action=txlist&address={addr}&startblock=0&endblock=99999999&sort=desc&apikey={key}"

# ====== APP / POOL ======
app = FastAPI(title="stark-defi-agent", version="5.7")
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

# ====== SQL ======
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
  id serial PRIMARY KEY,
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
  ts timestamptz NOT NULL,
  source text NOT NULL,
  title text NOT NULL,
  url text
);
"""

# ====== UTILS ======
def now_local_iso():
    return dt.datetime.now(TZ).isoformat(timespec="seconds")

async def db_init():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL não definido.")
    global pool
    pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=5)
    async with pool.acquire() as c:
        await c.execute(CREATE_SQL)

async def fetch_json(url: str, headers: Dict[str,str]|None=None, params: Dict[str,str]|None=None):
    async with httpx.AsyncClient(timeout=15) as s:
        r = await s.get(url, headers=headers, params=params)
        r.raise_for_status()
        return r.json()

async def fetch_text(url: str):
    async with httpx.AsyncClient(timeout=20) as s:
        r = await s.get(url)
        r.raise_for_status()
        return r.text

async def fetch_json_retry(url: str, *, headers=None, params=None, tries:int=3, sleep:float=0.6):
    last_exc=None
    for i in range(tries):
        try:
            async with httpx.AsyncClient(timeout=15) as s:
                r = await s.get(url, headers=headers, params=params)
                r.raise_for_status()
                return r.json()
        except Exception as e:
            last_exc=e
            await asyncio.sleep(sleep*(i+1))
    raise last_exc

async def send_tg(text: str, chat_id: Optional[str] = None):
    if not SEND_ENABLED: return
    cid = chat_id or TG_CHAT
    if not cid: return
    async with httpx.AsyncClient(timeout=12) as s:
        try:
            await s.post(TG_SEND, json={"chat_id": cid, "text": text})
        except Exception:
            traceback.print_exc()

def action_line(eth_price: float) -> str:
    if eth_price < ETH_HEDGE_2:
        return f"🚨 ETH < {ETH_HEDGE_2:.0f} → ampliar hedge p/ 20% (29 ETH)."
    if eth_price < ETH_HEDGE_1:
        return f"⚠️ ETH < {ETH_HEDGE_1:.0f} → ativar hedge 15% (22 ETH)."
    if eth_price > ETH_CLOSE:
        return f"↩️ ETH > {ETH_CLOSE:.0f} → avaliar fechar hedge."
    return "✅ Sem gatilho. Suportes: 4.200/4.000 | Resist: 4.300/4.400."

# ====== MARKET SNAPSHOTS (1m) ======
async def coinbase_price(product: str) -> Tuple[float, Optional[float], Optional[float]]:
    """Retorna last, high, low do produto (ETH-USD/BTC-USD). Coinbase ticker não dá H/L do dia,
       então devolvemos NaN para manter compatibilidade."""
    try:
        j = await fetch_json_retry(COINBASE_TICKER.format(prod=product))
        last = float(j.get("price") or j.get("last") or j["trade_id"]*0)  # fallback de segurança
        # melhor: 'price' (string). se não vier, falha.
        return float(j["price"]), math.nan, math.nan
    except Exception:
        return math.nan, math.nan, math.nan

async def get_spot_snapshot() -> dict:
    # tenta Bybit spot para ETH/BTC; fallback Coinbase
    out = {"eth":{"price":math.nan,"high":math.nan,"low":math.nan},
           "btc":{"price":math.nan,"high":math.nan,"low":math.nan}}
    try:
        eth = (await fetch_json_retry(BYBIT_SPOT.format(sym="ETHUSDT")))["result"]["list"][0]
        out["eth"] = {"price": float(eth["lastPrice"]), "high": float(eth["highPrice"]), "low": float(eth["lowPrice"])}
    except Exception:
        p,h,l = await coinbase_price("ETH-USD")
        out["eth"] = {"price": p, "high": h, "low": l}
    try:
        btc = (await fetch_json_retry(BYBIT_SPOT.format(sym="BTCUSDT")))["result"]["list"][0]
        out["btc"] = {"price": float(btc["lastPrice"]), "high": float(btc["highPrice"]), "low": float(btc["lowPrice"])}
    except Exception:
        p,h,l = await coinbase_price("BTC-USD")
        out["btc"] = {"price": p, "high": h, "low": l}
    return out

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
        if any(math.isnan(x) for x in (eth_p, btc_p)):
            return
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
        last_run["ingest_1m"] = now_local_iso()

# ====== WHALES (30s) ======
_last_trade_time_ms = 0
_ws_lock = asyncio.Lock()

async def whales_bybit() -> List[Tuple[str,float,float,str]]:
    """Tenta agregar fluxos grandes a partir do endpoint recent-trade da Bybit."""
    global _last_trade_time_ms
    data = (await fetch_json_retry(BYBIT_RECENT_TRADES))["result"]["list"]
    new = [t for t in data if int(t.get("time", 0)) > _last_trade_time_ms]
    if not new: return []
    _last_trade_time_ms = max(int(t.get("time", 0)) for t in new)
    buy_usd=sell_usd=0.0; buy_qty=sell_qty=0.0
    for t in new:
        price=float(t["price"]); qty=float(t["qty"])
        side=(t["side"] or "").upper()
        usd=price*qty
        if side=="BUY":  buy_usd+=usd; buy_qty+=qty
        else:            sell_usd+=usd; sell_qty+=qty
    events=[]
    if buy_usd>=WHALE_USD_MIN: events.append(("BUY", buy_qty,  buy_usd,  f"agg {len(new)} trades"))
    if sell_usd>=WHALE_USD_MIN: events.append(("SELL", sell_qty, sell_usd, f"agg {len(new)} trades"))
    return events

async def whales_coinbase() -> List[Tuple[str,float,float,str]]:
    """Fallback via Coinbase recent trades (ETH-USD)."""
    try:
        lst = await fetch_json_retry(COINBASE_TRADES.format(prod="ETH-USD"))
        # lst é lista de dicts com price, size, side (buy/sell), time
        buy_usd = sell_usd = 0.0
        buy_qty = sell_qty = 0.0
        for t in lst:
            price = float(t["price"]); qty = float(t["size"]); side = (t["side"] or "").upper()
            usd = price * qty
            if side == "BUY":  buy_usd += usd; buy_qty += qty
            else:              sell_usd += usd; sell_qty += qty
        events=[]
        if buy_usd>=WHALE_USD_MIN: events.append(("BUY", buy_qty,  buy_usd,  f"coinbase agg {len(lst)}"))
        if sell_usd>=WHALE_USD_MIN: events.append(("SELL", sell_qty, sell_usd, f"coinbase agg {len(lst)}"))
        return events
    except Exception:
        return []

async def ingest_whales():
    try:
        events=[]
        try:
            events = await whales_bybit()
        except Exception:
            # 403 etc → fallback coinbase
            events = await whales_coinbase()
        if events:
            ts = dt.datetime.now(dt.UTC)
            async with pool.acquire() as c:
                for side, qty, usd, note in events:
                    await c.execute(
                        "INSERT INTO whale_events(ts,venue,side,qty,usd_value,note) VALUES($1,$2,$3,$4,$5,$6)",
                        ts, "perp", side, qty, usd, note
                    )
            for side, qty, usd, note in events:
                await send_tg(f"🐋 {side} ~ ${usd:,.0f} | qty ~ {qty:,.1f} | {note}")
    finally:
        last_run["ingest_whales"] = now_local_iso()

# ====== ACCOUNTS (5m) ======
BYBIT_API = BYBIT_PUBLIC

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
    async with httpx.AsyncClient(timeout=20) as s:
        r = await s.get(url, params=payload)
        r.raise_for_status()
        return r.json()

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
        traceback.print_exc()
    return out

async def ingest_accounts():
    rows=[]
    rows += await snapshot_bybit()
    rows += await snapshot_aave()
    if rows:
        async with pool.acquire() as c:
            for r in rows:
                await c.execute("INSERT INTO account_snap(venue,metric,value) VALUES($1,$2,$3)", r["venue"], r["metric"], r["value"])
    last_run["ingest_accounts"] = now_local_iso()

# ====== ONCHAIN (5m opcional) ======
async def ingest_onchain_eth(addr: str):
    if not ETHERSCAN_API_KEY: 
        last_run["ingest_onchain"] = now_local_iso()
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
        last_run["ingest_onchain"] = now_local_iso()

# ====== NEWS (10m) ======
def parse_rss(xml_text: str) -> List[Tuple[str,str]]:
    out=[]
    try:
        root = ET.fromstring(xml_text)
        for item in root.findall(".//item"):
            title = (item.findtext("title") or "").strip()
            link  = (item.findtext("link") or "").strip()
            if title:
                out.append((title, link))
    except Exception:
        pass
    return out[:15]

async def ingest_news():
    now = dt.datetime.now(dt.UTC)
    rows=[]
    for src in NEWS_SOURCES:
        try:
            xml = await fetch_text(src.strip())
            items = parse_rss(xml)
            for title, url in items:
                rows.append((now, src, title, url))
        except Exception:
            continue
    if rows:
        async with pool.acquire() as c:
            # guarda últimas ~60 (cap)
            for ts, src, title, url in rows[:60]:
                await c.execute("INSERT INTO news(ts,source,title,url) VALUES($1,$2,$3,$4)", ts, src, title, url)
            await c.execute("DELETE FROM news WHERE id NOT IN (SELECT id FROM news ORDER BY id DESC LIMIT 400)")
    last_run["news"] = now_local_iso()

def summarize_news(titles: List[str]) -> str:
    """sintetiza temas em 2–3 bullets simples (heurística leve)."""
    t = " ".join(titles).lower()
    bullets=[]
    if any(k in t for k in ["etf", "spot etf", "sec", "approval"]): bullets.append("ETF/fluxos institucionais em foco.")
    if any(k in t for k in ["upgrade", "hard fork", "dencun", "proto-dank", "eip"]): bullets.append("Narrativa técnica/upgrade em destaque no ETH.")
    if any(k in t for k in ["liquidation", "liquidations", "rekt"]): bullets.append("Liquidações relevantes afetaram volatilidade.")
    if any(k in t for k in ["funding", "open interest", "oi"]): bullets.append("Derivativos (funding/OI) ditando curto prazo.")
    if not bullets: bullets.append("Fluxo de notícias sem um driver único dominante.")
    return " • " + "\n • ".join(bullets)

# ====== COMENTÁRIO 6–8h ======
async def build_commentary(include_themes: bool=True) -> str:
    end = dt.datetime.now(dt.UTC); start = end - dt.timedelta(hours=8)
    async with pool.acquire() as c:
        rows = await c.fetch("SELECT ts, eth_usd, btc_usd, eth_btc_ratio FROM market_rel WHERE ts BETWEEN $1 AND $2 ORDER BY ts", start, end)
        deriv = await c.fetch("SELECT ts, funding, open_interest FROM derivatives_snap WHERE ts BETWEEN $1 AND $2 ORDER BY ts", start, end)
        whales = await c.fetch("SELECT ts, side, usd_value FROM whale_events WHERE ts BETWEEN $1 AND $2 ORDER BY ts", start, end)
        news_rows = await c.fetch("SELECT ts,title FROM news WHERE ts BETWEEN $1 AND $2 ORDER BY id DESC LIMIT 20", start, end)
    if not rows: return "⏳ Aguardando histórico para comentário (volte em alguns minutos)."

    eth = [float(r["eth_usd"]) for r in rows]; btc = [float(r["btc_usd"]) for r in rows]; ratio = [float(r["eth_btc_ratio"]) for r in rows]
    def pct(a,b): return 0.0 if a==0 else (b-a)/a*100.0
    eth_chg = pct(eth[0], eth[-1]); btc_chg = pct(btc[0], btc[-1]); ratio_chg = pct(ratio[0], ratio[-1])
    funding = float(deriv[-1]["funding"]) if deriv and deriv[-1]["funding"] is not None else None
    oi = float(deriv[-1]["open_interest"]) if deriv and deriv[-1]["open_interest"] is not None else None

    buy = sum(float(w["usd_value"] or 0) for w in whales if (w["side"] or "").upper()=="BUY")
    sell= sum(float(w["usd_value"] or 0) for w in whales if (w["side"] or "").upper()=="SELL")
    flow = "neutra"; 
    if buy>sell*1.3: flow="compradora"
    elif sell>buy*1.3: flow="vendedora"

    titles=[r["title"] for r in news_rows]
    themes = summarize_news(titles) if include_themes else ""

    lines=[]
    lines.append("🧾 Comentário (últimas 8h)")
    lines.append(f"• ETH: {eth_chg:+.2f}% | BTC: {btc_chg:+.2f}% | ETH/BTC: {ratio_chg:+.2f}%")
    if funding is not None or oi is not None:
        comp = []
        if funding is not None: comp.append(f"funding {funding*100:.3f}%/8h")
        if oi is not None: comp.append(f"OI ~ {oi:,.0f}")
        lines.append("• Derivativos: " + ", ".join(comp))
    lines.append(f"• Fluxo de baleias: {flow} (BUY ${buy:,.0f} vs SELL ${sell:,.0f})")
    if include_themes:
        lines.append("• Temas de notícia:" + ("\n" + themes if themes else " fluxo discreto."))
    # Síntese
    if ratio_chg>0 and (funding is None or funding<0.0005):
        synth="pró-ETH (força relativa + funding contido)."
    elif ratio_chg<0 and (funding is not None and funding>0.001):
        synth="pró-cautela (BTC dominante + funding elevado)."
    else:
        synth="equilíbrio tático; operar pelos gatilhos de preço."
    lines.append("🧭 Síntese: " + synth)
    return "\n".join(lines)

async def latest_prices_block() -> Tuple[str,float]:
    async with pool.acquire() as c:
        m = await c.fetchrow("SELECT * FROM market_rel ORDER BY ts DESC LIMIT 1")
        e = await c.fetchrow("SELECT * FROM candles_minute WHERE symbol='ETHUSDT' ORDER BY ts DESC LIMIT 1")
        b = await c.fetchrow("SELECT * FROM candles_minute WHERE symbol='BTCUSDT' ORDER BY ts DESC LIMIT 1")
        d = await c.fetchrow("SELECT * FROM derivatives_snap WHERE symbol='ETHUSDT' AND exchange='bybit' ORDER BY ts DESC LIMIT 1")
    if not (m and e and b): 
        return "⏳ Aguardando primeiros dados…", math.nan
    eth = float(m["eth_usd"]); btc = float(m["btc_usd"]); ratio = float(m["eth_btc_ratio"])
    eh, el, bh, bl = e["high"], e["low"], b["high"], b["low"]
    parts=[f"🕒 {dt.datetime.now(TZ).strftime('%Y-%m-%d %H:%M')}",
           f"ETH: ${eth:,.2f}" + (f" (H:{eh:,.2f}/L:{el:,.2f})" if isinstance(eh,(int,float)) and isinstance(el,(int,float)) else ""),
           f"BTC: ${btc:,.2f}" + (f" (H:{bh:,.2f}/L:{bl:,.2f})" if isinstance(bh,(int,float)) and isinstance(bl,(int,float)) else ""),
           f"ETH/BTC: {ratio:.5f}"]
    if d and d["funding"] is not None: parts.append(f"Funding (ETH): {float(d['funding'])*100:.3f}%/8h")
    if d and d["open_interest"] is not None: parts.append(f"Open Interest (ETH): {float(d['open_interest']):,.0f}")
    return "\n".join(parts), eth

async def latest_pulse_text(include_themes=True) -> str:
    header, eth = await latest_prices_block()
    if math.isnan(eth):
        return header
    parts=[header]
    # ação
    parts.append(action_line(eth))
    parts.append("")  # espaço
    parts.append(await build_commentary(include_themes=include_themes))
    return "\n".join(parts)

async def send_pulse_to_chat():
    await send_tg(await latest_pulse_text())

# ====== FASTAPI ======
@app.on_event("startup")
async def _startup():
    await db_init()
    # Ingests
    scheduler.add_job(ingest_1m, "interval", minutes=1, id="ingest_1m", replace_existing=True)
    scheduler.add_job(ingest_whales, "interval", seconds=30, id="ingest_whales", replace_existing=True)
    scheduler.add_job(ingest_accounts, "interval", minutes=5, id="ingest_accounts", replace_existing=True)
    if AAVE_ADDR and ETHERSCAN_API_KEY:
        scheduler.add_job(ingest_onchain_eth, "interval", minutes=5, args=[AAVE_ADDR], id="ingest_onchain", replace_existing=True)
    scheduler.add_job(ingest_news, "interval", minutes=10, id="ingest_news", replace_existing=True)
    # Boletins
    scheduler.add_job(send_pulse_to_chat, "cron", hour=8,  minute=0, id="bulletin_08", replace_existing=True)
    scheduler.add_job(send_pulse_to_chat, "cron", hour=14, minute=0, id="bulletin_14", replace_existing=True)
    scheduler.add_job(send_pulse_to_chat, "cron", hour=20, minute=0, id="bulletin_20", replace_existing=True)
    scheduler.start()

@app.get("/healthz")
async def healthz():
    return {"ok": True, "time": now_local_iso()}

@app.get("/status")
async def status():
    async with pool.acquire() as c:
        mr = await c.fetchval("SELECT COUNT(1) FROM market_rel")
        cm_eth = await c.fetchval("SELECT COUNT(1) FROM candles_minute WHERE symbol='ETHUSDT'")
        cm_btc = await c.fetchval("SELECT COUNT(1) FROM candles_minute WHERE symbol='BTCUSDT'")
        ds = await c.fetchval("SELECT COUNT(1) FROM derivatives_snap")
        whales = await c.fetchval("SELECT COUNT(1) FROM whale_events")
        acc = await c.fetchval("SELECT COUNT(1) FROM account_snap")
        news = await c.fetchval("SELECT COUNT(1) FROM news")
    return {
        "counts":{
            "market_rel": mr, "candles_eth": cm_eth, "candles_btc": cm_btc,
            "derivatives": ds, "whale_events": whales, "account_snap": acc,
            "news": news
        },
        "last_run": last_run
    }

@app.post("/run/accounts")
async def run_accounts():
    await ingest_accounts()
    return {"ok": True, "ran_at": last_run["ingest_accounts"]}

# ----- Simple price endpoints for /eth and /btc commands
async def simple_line(sym: str) -> str:
    async with pool.acquire() as c:
        row = await c.fetchrow("SELECT * FROM candles_minute WHERE symbol=$1 ORDER BY ts DESC LIMIT 1", sym)
        rel = await c.fetchrow("SELECT * FROM market_rel ORDER BY ts DESC LIMIT 1")
    if not (row and rel): return "⏳ Aguardando dados…"
    price = float(rel["eth_usd"] if sym=="ETHUSDT" else rel["btc_usd"])
    h, l = row["high"], row["low"]
    ratio = float(rel["eth_btc_ratio"])
    head = f"{'ETH' if sym=='ETHUSDT' else 'BTC'}: ${price:,.2f}" + (f" | H:{h:,.2f}/L:{l:,.2f}" if isinstance(h,(int,float)) and isinstance(l,(int,float)) else "")
    tail = f" | ETH/BTC: {ratio:.5f}" if sym=="ETHUSDT" else f" | ETH/BTC: {ratio:.5f}"
    return head + tail + "\n" + action_line(float(rel["eth_usd"]))

@app.get("/pulse")
async def pulse():
    text = await latest_pulse_text(include_themes=True)
    await send_tg(text)
    return {"ok": True, "message": text}

@app.get("/alfa")
@app.get("/alpha")
async def alpha():
    text = await latest_pulse_text(include_themes=True)
    text += "\n\n🧠 Drivers prováveis: cruzamos variação 8h, funding/OI, fluxo de baleias e temas de notícia para explicar movimentos de curto prazo."
    await send_tg(text)
    return {"ok": True, "message": text}

@app.get("/eth")
async def eth_view():
    text = await simple_line("ETHUSDT")
    await send_tg(text)
    return {"ok": True, "message": text}

@app.get("/btc")
async def btc_view():
    text = await simple_line("BTCUSDT")
    await send_tg(text)
    return {"ok": True, "message": text}

# ----- Telegram webhook
def _handle_cmd(low: str, text: str, chat_id: str):
    return low, text, chat_id

@app.post("/webhook")
@app.post("/webhook/{token}")
async def telegram_webhook(request: Request, token: Optional[str]=None):
    # se token for informado, apenas valida forma; não bloqueia
    try: update = await request.json()
    except Exception: return {"ok": True}
    msg = update.get("message") or update.get("edited_message") or update.get("channel_post")
    if not msg: return {"ok": True}
    chat_id = str(msg["chat"]["id"])
    text = (msg.get("text") or "").strip()
    low = text.lower()

    if low in ("/start","start"):
        await send_tg("✅ Bot online.\nComandos: /pulse, /alfa, /eth, /btc, /note <texto>, /strat new <nome> | <versão> | <nota>, /strat last, /notes", chat_id); return {"ok": True}
    if low == "/pulse":
        await send_tg(await latest_pulse_text(), chat_id); return {"ok": True}
    if low in ("/alfa","/alpha"):
        await send_tg(await latest_pulse_text(), chat_id); return {"ok": True}
    if low == "/eth":
        await send_tg(await simple_line("ETHUSDT"), chat_id); return {"ok": True}
    if low == "/btc":
        await send_tg(await simple_line("BTCUSDT"), chat_id); return {"ok": True}
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
    await send_tg("Comando não reconhecido. Use /pulse, /alfa, /eth, /btc, /note, /strat new, /strat last, /notes.", chat_id)
    return {"ok": True}

# ----- Exporters
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

@app.get("/")
async def root():
    return {"service": "stark-defi-agent", "version": app.version}

# app.py — Stark DeFi Agent v6.0.4
# Novidades:
# - Coleta leve de notícias (RSS públicos) + cache em DB + síntese no Pulse/Alpha
# - Zonas: day high/low, níveis redondos próximos, ETH hedge gatilhos
# - Comentário expandido: impulso (var 8h), amplitude, funding/OI, “reversões perto de resistência”
# - Sem Binance; whales OFF por padrão; auto-webhook mantém
# - Mantém endpoints, tabelas, e rotinas das versões anteriores

import os, hmac, hashlib, time, math, csv, io, asyncio, traceback, random
import datetime as dt
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, List, Tuple

import httpx
import asyncpg
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse

# --------- Config ---------
TZ = ZoneInfo(os.getenv("TZ", "America/Sao_Paulo"))
DB_URL = os.getenv("DATABASE_URL", "")

PUBLIC_URL = os.getenv("PUBLIC_URL", "").rstrip("/")
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "")
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "")

SEND_ENABLED = bool(TG_TOKEN and TG_CHAT)

BYBIT_KEY = os.getenv("BYBIT_RO_KEY", "")
BYBIT_SEC = os.getenv("BYBIT_RO_SECRET", "")
BYBIT_ACCOUNT_TYPE = os.getenv("BYBIT_ACCOUNT_TYPE", "UNIFIED")

AAVE_ADDR = os.getenv("AAVE_ADDR", "")
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")

ETH_HEDGE_1 = float(os.getenv("ETH_HEDGE_1", "3900"))
ETH_HEDGE_2 = float(os.getenv("ETH_HEDGE_2", "3800"))
ETH_CLOSE   = float(os.getenv("ETH_CLOSE_HEDGE", "3950"))

WHALE_USD_MIN = float(os.getenv("WHALE_USD_MIN", "500000"))
ENABLE_WHALES = os.getenv("ENABLE_WHALES", "0") == "1"  # OFF por padrão

# --------- Constantes de APIs ---------
BYBIT_BASE = "https://api.bybit.com"
BYBIT_SPOT = BYBIT_BASE + "/v5/market/tickers?category=spot&symbol={sym}"
BYBIT_PERP = BYBIT_BASE + "/v5/market/tickers?category=linear&symbol={sym}"
BYBIT_FUND = BYBIT_BASE + "/v5/market/funding/history?category=linear&symbol=ETHUSDT&limit=1"
BYBIT_OI   = BYBIT_BASE + "/v5/market/open-interest?category=linear&symbol=ETHUSDT&interval=5min"
BYBIT_POS  = BYBIT_BASE + "/v5/position/list"

CB_ETH = "https://api.exchange.coinbase.com/products/ETH-USD/ticker"
CB_BTC = "https://api.exchange.coinbase.com/products/BTC-USD/ticker"

COINGECKO_SIMPLE = "https://api.coingecko.com/api/v3/simple/price?ids=ethereum,bitcoin&vs_currencies=usd"

TG_API = f"https://api.telegram.org/bot{TG_TOKEN}"
TG_SEND = TG_API + "/sendMessage"
TG_GET_INFO = TG_API + "/getWebhookInfo"
TG_SET_HOOK = TG_API + "/setWebhook"

ETHERSCAN_TX = "https://api.etherscan.io/api?module=account&action=txlist&address={addr}&startblock=0&endblock=99999999&sort=desc&apikey={key}"

# RSS fontes públicas (leve)
RSS_SOURCES = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/",   # CoinDesk
    "https://cointelegraph.com/rss"                      # CoinTelegraph
]

# --------- App e DB ---------
app = FastAPI(title="stark-defi-agent")
pool: Optional[asyncpg.Pool] = None

from apscheduler.schedulers.asyncio import AsyncIOScheduler
scheduler = AsyncIOScheduler(timezone=str(TZ))

last_run: Dict[str, Optional[str]] = {
    "ingest_1m": None,
    "ingest_whales": None,
    "ingest_accounts": None,
    "ingest_onchain": None,
    "news": None,
    "webhook": None,
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
CREATE TABLE IF NOT EXISTS news_snap(
  id serial PRIMARY KEY,
  ts timestamptz NOT NULL DEFAULT now(),
  source text NOT NULL,
  title text NOT NULL,
  url text
);
"""

# --------- Utils HTTP / Telegram ---------
async def fetch_json(url: str, headers: Dict[str,str]|None=None, params: Dict[str,Any]|None=None, timeout: float=15.0):
    async with httpx.AsyncClient(timeout=timeout) as s:
        r = await s.get(url, headers=headers, params=params)
        r.raise_for_status()
        return r.json()

async def fetch_text(url: str, timeout: float=12.0):
    async with httpx.AsyncClient(timeout=timeout) as s:
        r = await s.get(url)
        r.raise_for_status()
        return r.text

async def fetch_json_retry(url: str, headers=None, params=None, attempts: int=3, base_timeout: float=8.0, jitter: float=0.25):
    last_exc = None
    for i in range(attempts):
        try:
            return await fetch_json(url, headers=headers, params=params, timeout=base_timeout)
        except Exception as e:
            last_exc = e
            await asyncio.sleep(base_timeout*(0.5 + jitter*random.random()))
    raise last_exc

async def send_tg(text: str, chat_id: Optional[str] = None, disable_preview: bool=True):
    if not SEND_ENABLED: 
        return
    cid = chat_id or TG_CHAT
    if not cid: 
        return
    payload = {"chat_id": cid, "text": text, "disable_web_page_preview": disable_preview}
    async with httpx.AsyncClient(timeout=12) as s:
        try:
            await s.post(TG_SEND, json=payload)
        except Exception:
            traceback.print_exc()

def action_line(eth_price: float) -> str:
    if eth_price < ETH_HEDGE_2:
        return f"🚨 ETH < {ETH_HEDGE_2:.0f} → ampliar hedge p/ 20% (29 ETH)."
    if eth_price < ETH_HEDGE_1:
        return f"⚠️ ETH < {ETH_HEDGE_1:.0f} → ativar hedge 15% (22 ETH)."
    if eth_price > ETH_CLOSE:
        return f"↩️ ETH > {ETH_CLOSE:.0f} → avaliar fechar hedge."
    return "✅ Sem gatilho. Zonas: 4.200/4.000 | Resist: 4.300/4.400."

# --------- Preços / Derivativos ---------
async def get_spot_snapshot() -> dict:
    # 1) Bybit spot
    try:
        eth = (await fetch_json(BYBIT_SPOT.format(sym="ETHUSDT")))["result"]["list"][0]
        btc = (await fetch_json(BYBIT_SPOT.format(sym="BTCUSDT")))["result"]["list"][0]
        return {
            "eth": {"price": float(eth["lastPrice"]), "high": float(eth["highPrice"]), "low": float(eth["lowPrice"])},
            "btc": {"price": float(btc["lastPrice"]), "high": float(btc["highPrice"]), "low": float(btc["lowPrice"])},
        }
    except Exception:
        # 2) Coinbase (ticker não tem high/low)
        try:
            e = await fetch_json(CB_ETH)
            b = await fetch_json(CB_BTC)
            return {
                "eth": {"price": float(e["price"]), "high": math.nan, "low": math.nan},
                "btc": {"price": float(b["price"]), "high": math.nan, "low": math.nan},
            }
        except Exception:
            # 3) CoinGecko (tenta com retry)
            cg = await fetch_json_retry(COINGECKO_SIMPLE, attempts=4, base_timeout=6.0)
            return {
                "eth": {"price": float(cg["ethereum"]["usd"]), "high": math.nan, "low": math.nan},
                "btc": {"price": float(cg["bitcoin"]["usd"]),  "high": math.nan, "low": math.nan},
            }

async def get_derivatives_snapshot() -> dict:
    funding = None; open_interest = None
    try:
        f = (await fetch_json(BYBIT_FUND))["result"]["list"][0]
        funding = float(f["fundingRate"])
    except Exception:
        pass
    try:
        oi = (await fetch_json(BYBIT_OI))["result"]["list"][-1]
        open_interest = float(oi["openInterest"])
    except Exception:
        pass
    return {"funding": funding, "oi": open_interest}

# --------- Ingest 1m ---------
async def ingest_1m():
    try:
        now = dt.datetime.now(dt.UTC).replace(second=0, microsecond=0)
        spot = await get_spot_snapshot()
        der  = await get_derivatives_snapshot()
        eth_p = spot["eth"]["price"]; btc_p = spot["btc"]["price"]
        eth_h = spot["eth"]["high"];  eth_l = spot["eth"]["low"]
        btc_h = spot["btc"]["high"];  btc_l = spot["btc"]["low"]
        ethbtc = eth_p / btc_p if btc_p else None
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

# --------- Whales OFF ---------
async def ingest_whales():
    last_run["ingest_whales"] = dt.datetime.now(TZ).isoformat(timespec="seconds")

# --------- Accounts (5m) ---------
def bybit_sign_qs(secret: str, params: Dict[str, Any]) -> str:
    qs = "&".join([f"{k}={params[k]}" for k in sorted(params)])
    return hmac.new(secret.encode(), qs.encode(), hashlib.sha256).hexdigest()

async def bybit_private_get(path: str, extra: Dict[str, Any]) -> Dict[str, Any]:
    ts = str(int(time.time()*1000))
    base = {"api_key": BYBIT_KEY, "timestamp": ts, "recv_window": "5000"}
    payload = {**base, **extra}
    sign = bybit_sign_qs(BYBIT_SEC, payload)
    payload["sign"] = sign
    url = f"{BYBIT_BASE}{path}"
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
    last_run["ingest_accounts"] = dt.datetime.now(TZ).isoformat(timespec="seconds")

# --------- On-chain (opcional) ---------
async def ingest_onchain_eth(addr: str):
    if not ETHERSCAN_API_KEY: 
        last_run["ingest_onchain"] = dt.datetime.now(TZ).isoformat(timespec="seconds")
        return
    try:
        data = await fetch_json(ETHERSCAN_TX.format(addr=addr, key=ETHERSCAN_API_KEY))
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

# --------- Notícias (RSS leve) ---------
def _extract_rss_items(xml: str, limit: int = 10) -> List[Tuple[str,str]]:
    # parsing simples de RSS (sem libs externas) — pega <title> e <link>
    items = []
    try:
        # abordagem leve: split por "<item>"
        parts = xml.split("<item")
        for p in parts[1:]:
            # título
            t1 = p.find("<title>")
            t2 = p.find("</title>")
            l1 = p.find("<link>")
            l2 = p.find("</link>")
            if t1!=-1 and t2!=-1:
                title = p[t1+7:t2].strip()
            else:
                title = ""
            if l1!=-1 and l2!=-1:
                link = p[l1+6:l2].strip()
            else:
                # fallback: guid
                g1 = p.find("<guid>")
                g2 = p.find("</guid>")
                link = p[g1+6:g2].strip() if g1!=-1 and g2!=-1 else ""
            if title:
                items.append((title, link))
            if len(items)>=limit:
                break
    except Exception:
        traceback.print_exc()
    return items

async def ingest_news():
    total=0
    for src in RSS_SOURCES:
        try:
            xml = await fetch_text(src, timeout=10.0)
            items = _extract_rss_items(xml, limit=8)
            if not items: 
                continue
            async with pool.acquire() as c:
                for title, url in items:
                    await c.execute("""
                        INSERT INTO news_snap(source,title,url)
                        VALUES ($1,$2,$3)
                        ON CONFLICT DO NOTHING
                    """, src, title[:512], (url or "")[:512])
                    total += 1
        except Exception:
            # silencioso — fonte pode falhar
            pass
    last_run["news"] = f"{dt.datetime.now(TZ).isoformat(timespec='seconds')} (+{total})"

# --------- Níveis / Zonas ---------
def _pct(a,b):
    try:
        return 0.0 if a in (0,None) or b in (0,None) else (b-a)/a*100.0
    except Exception:
        return 0.0

async def compute_levels(symbol: str, hours: int = 24) -> Dict[str, Any]:
    end = dt.datetime.now(dt.UTC); start = end - dt.timedelta(hours=hours)
    async with pool.acquire() as c:
        rows = await c.fetch(
            "SELECT high,low,close FROM candles_minute WHERE symbol=$1 AND ts BETWEEN $2 AND $3 ORDER BY ts",
            symbol, start, end
        )
    if not rows:
        return {"day_high": None, "day_low": None, "near_rounds": [], "recent_mid": None}

    highs = [float(r["high"]) for r in rows if r["high"] is not None]
    lows  = [float(r["low"]) for r in rows if r["low"] is not None]
    closes= [float(r["close"]) for r in rows if r["close"] is not None]
    day_high = max(highs) if highs else None
    day_low  = min(lows) if lows else None
    recent   = closes[-1] if closes else None
    recent_mid = (day_high + day_low)/2 if (day_high is not None and day_low is not None) else None

    # níveis redondos perto do preço (±2.5%)
    rounds=[]
    if recent:
        step = 50 if recent<2000 else 100
        base = int(recent//step)*step
        for k in range(-5,6):
            lvl = base + k*step
            if lvl>0 and abs(lvl-recent)/recent<=0.025:
                rounds.append(lvl)

    return {
        "day_high": day_high, "day_low": day_low,
        "recent_mid": recent_mid,
        "near_rounds": sorted(set(rounds)),
        "recent": recent
    }

# --------- Comentário/Contexto ---------
async def build_commentary(hours: int=8) -> str:
    end = dt.datetime.now(dt.UTC); start = end - dt.timedelta(hours=hours)
    async with pool.acquire() as c:
        rows = await c.fetch("SELECT ts, eth_usd, btc_usd, eth_btc_ratio FROM market_rel WHERE ts BETWEEN $1 AND $2 ORDER BY ts", start, end)
        deriv = await c.fetch("SELECT ts, funding, open_interest FROM derivatives_snap WHERE ts BETWEEN $1 AND $2 ORDER BY ts", start, end)
        nrows = await c.fetch("SELECT ts, source, title FROM news_snap WHERE ts >= now() - interval '12 hours' ORDER BY ts DESC LIMIT 8")
    if not rows:
        return "⏳ Aguardando histórico para comentário."

    eth = [float(r["eth_usd"]) for r in rows if r["eth_usd"] is not None]
    btc = [float(r["btc_usd"]) for r in rows if r["btc_usd"] is not None]
    ratio = [float(r["eth_btc_ratio"]) for r in rows if r["eth_btc_ratio"] is not None]

    eth_chg = _pct(eth[0], eth[-1]) if eth else 0.0
    btc_chg = _pct(btc[0], btc[-1]) if btc else 0.0
    ratio_chg = _pct(ratio[0], ratio[-1]) if ratio else 0.0
    amplitude = (max(eth)-min(eth))/eth[-1]*100.0 if eth else 0.0  # amplitude % nas últimas 8h

    funding = float(deriv[-1]["funding"]) if deriv and deriv[-1]["funding"] is not None else None
    oi = float(deriv[-1]["open_interest"]) if deriv and deriv[-1]["open_interest"] is not None else None

    # “contexto de notícias” simples: lista 3 títulos distintos
    headlines = []
    if nrows:
        seen=set()
        for r in nrows:
            t = (r["title"] or "").strip()
            if t and t[:80] not in seen:
                headlines.append("• " + t[:140])
                seen.add(t[:80])
            if len(headlines)>=3: break

    # leitura de fluxo/risco
    flow = "neutra"
    if eth_chg > 0.6 and (funding is None or funding <= 0.001): flow="compradora"
    if eth_chg < -0.6 and (funding is None or funding >= 0.0):  flow="vendedora"

    lines = []
    lines.append("🧭 Comentário (8h)")
    lines.append(f"• ETH: {eth_chg:+.2f}% | amplitude {amplitude:.2f}% " + (f"| funding {funding*100:.3f}%/8h " if funding is not None else "") + (f"| OI ~ {oi:,.0f}" if oi is not None else ""))
    lines.append(f"• BTC: {btc_chg:+.2f}% | {'mais forte' if btc_chg>0 else 'mais fraca' if btc_chg<0 else 'estável'}")
    lines.append(f"• ETH/BTC: {ratio_chg:+.2f}% | {'ETH ganhando beta' if ratio_chg>0 else 'BTC dominante' if ratio_chg<0 else 'equilíbrio'}")
    lines.append(f"• Fluxo: {flow}.")
    if headlines:
        lines.append("📰 Contexto:")
        lines += headlines

    # síntese
    if ratio_chg>0 and (funding is None or funding<0.0005):
        lines.append("🧩 Síntese: Pró-ETH (força relativa + funding contido).")
    elif ratio_chg<0 and (funding is not None and funding>0.001):
        lines.append("🧩 Síntese: Euforia em derivativos — risco de reversões rápidas.")
    else:
        lines.append("🧩 Síntese: Equilíbrio tático; usar zonas como guia.")

    return "\n".join(lines)

async def build_zones_block() -> str:
    lv = await compute_levels("ETHUSDT", 24)
    if not lv or lv["recent"] is None:
        return "Zonas: coletando…"
    z = []
    if lv["day_high"] is not None and lv["day_low"] is not None:
        z.append(f"Dia H/L: {lv['day_high']:.0f} / {lv['day_low']:.0f}")
    if lv["near_rounds"]:
        z.append("Redondos: " + ", ".join(f"{int(x)}" for x in lv["near_rounds"]))
    # gatilhos/hedge
    z.append(f"Gatilhos: <{ETH_HEDGE_2:.0f} (20% hedge), <{ETH_HEDGE_1:.0f} (15%), >{ETH_CLOSE:.0f} (fechar).")
    # dica de reversão: se preço atual perto de day_high (<=0.3%) e amplitude alta → “atenção à reversão”
    p = lv["recent"]; dh = lv["day_high"]; dl = lv["day_low"]
    if p and dh and abs(p-dh)/p <= 0.003:
        z.append("⚠️ Próximo da resistência do dia — atenção a reversões.")
    if p and dl and abs(p-dl)/p <= 0.003:
        z.append("⚠️ Próximo do suporte do dia — risco/retomada.")
    return "🗺️ Zonas\n" + "\n".join("• "+s for s in z)

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

    header = [
        "📊 PULSE",
        f"🕒 {now}",
        f"ETH: ${eth:,.2f}" + (f" (H:{eh:,.2f}/L:{el:,.2f})" if isinstance(eh,(int,float)) and isinstance(el,(int,float)) else ""),
        f"BTC: ${btc:,.2f}" + (f" (H:{bh:,.2f}/L:{bl:,.2f})" if isinstance(bh,(int,float)) and isinstance(bl,(int,float)) else ""),
        f"ETH/BTC: {ratio:.5f}",
    ]
    if d and d["funding"] is not None: header.append(f"Funding (ETH): {float(d['funding'])*100:.3f}%/8h")
    if d and d["open_interest"] is not None: header.append(f"Open Interest (ETH): {float(d['open_interest']):,.0f}")
    header.append(action_line(eth))

    zones = await build_zones_block()
    comment = await build_commentary(8)
    return "\n".join(header) + "\n\n" + zones + "\n\n" + comment

async def latest_alpha_text() -> str:
    text = await latest_pulse_text()
    # carimbo tático curto
    return text.replace("📊 PULSE", "🧠 ALPHA") + "\n🎯 Tático: usar zonas e gatilhos; evitar entradas no topo/vale do dia."

async def send_pulse_to_chat():
    await send_tg(await latest_pulse_text())

# --------- Auto Webhook ---------
async def ensure_webhook():
    if not (PUBLIC_URL and TG_TOKEN):
        last_run["webhook"] = f"skip: PUBLIC_URL/TG_TOKEN vazios"
        return
    hook_url = f"{PUBLIC_URL}/webhook"
    async with httpx.AsyncClient(timeout=12) as s:
        try:
            r = await s.get(TG_GET_INFO)
            info = r.json()
            cur = (info.get("result") or {}).get("url") or ""
            if cur == hook_url:
                last_run["webhook"] = f"ok: {hook_url}"
                return
        except Exception:
            pass
    ok = False; err = None
    for i in range(6):
        try:
            async with httpx.AsyncClient(timeout=12) as s:
                r = await s.post(TG_SET_HOOK, data={"url": hook_url})
                data = r.json()
                if data.get("ok"):
                    ok = True; break
                err = data
        except Exception as e:
            err = str(e)
        await asyncio.sleep(2*(i+1))
    last_run["webhook"] = f"{'ok' if ok else 'fail'}: {hook_url}" + ("" if ok else f" | err={err}")

def require_admin(secret: str):
    if not ADMIN_SECRET or secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="unauthorized")

# --------- Lifecycle ---------
@app.on_event("startup")
async def _startup():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL não definido.")
    global pool
    pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=5)
    async with pool.acquire() as c:
        await c.execute(CREATE_SQL)

    # Jobs
    scheduler.add_job(ingest_1m, "interval", minutes=1, id="ingest_1m", replace_existing=True)
    if ENABLE_WHALES:
        scheduler.add_job(ingest_whales, "interval", seconds=30, id="ingest_whales", replace_existing=True)
    scheduler.add_job(ingest_accounts, "interval", minutes=5, id="ingest_accounts", replace_existing=True)
    if AAVE_ADDR and ETHERSCAN_API_KEY:
        scheduler.add_job(ingest_onchain_eth, "interval", minutes=5, args=[AAVE_ADDR], id="ingest_onchain", replace_existing=True)
    scheduler.add_job(ingest_news, "interval", minutes=15, id="news", replace_existing=True)

    # Boletins programados
    scheduler.add_job(send_pulse_to_chat, "cron", hour=8,  minute=0, id="bulletin_08", replace_existing=True)
    scheduler.add_job(send_pulse_to_chat, "cron", hour=14, minute=0, id="bulletin_14", replace_existing=True)
    scheduler.add_job(send_pulse_to_chat, "cron", hour=20, minute=0, id="bulletin_20", replace_existing=True)

    scheduler.start()
    await ensure_webhook()

@app.on_event("shutdown")
async def _shutdown():
    if pool:
        await pool.close()

# --------- Endpoints ---------
@app.get("/")
async def root():
    return {"name": "stark-defi-agent", "tz": str(TZ)}

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
        news = await c.fetchval("SELECT COUNT(1) FROM news_snap")
    return {
        "counts":{
            "market_rel": mr, "candles_eth": cm_eth, "candles_btc": cm_btc,
            "derivatives": ds, "whale_events": whales, "account_snap": acc,
            "news": news
        },
        "last_run": last_run
    }

@app.get("/levels")
async def levels():
    return await compute_levels("ETHUSDT", 24)

@app.get("/pulse")
async def pulse():
    text = await latest_pulse_text()
    await send_tg(text)
    return {"ok": True, "message": text}

@app.get("/alpha")
async def alpha():
    text = await latest_alpha_text()
    await send_tg(text)
    return {"ok": True, "message": text}

@app.post("/run/accounts")
async def run_accounts():
    await ingest_accounts()
    return {"ok": True, "ran_at": last_run["ingest_accounts"]}

@app.get("/accounts/last")
async def accounts_last():
    async with pool.acquire() as c:
        rows = await c.fetch("""
            SELECT * FROM account_snap
            WHERE ts > now() - interval '1 hour'
            ORDER BY ts DESC, venue, metric
        """)
    return JSONResponse({"rows": [dict(r) for r in rows]})

@app.post("/webhook")
async def telegram_webhook(request: Request):
    try:
        update = await request.json()
    except Exception:
        return {"ok": True}
    msg = update.get("message") or update.get("edited_message") or update.get("channel_post")
    if not msg: 
        return {"ok": True}
    chat_id = str(msg["chat"]["id"])
    text = (msg.get("text") or "").strip()
    low = text.lower()

    if low in ("/start","start"):
        await send_tg("✅ Bot online. Comandos: /pulse, /alpha, /levels, /note <texto>, /strat new <nome> | <versão> | <nota>, /strat last, /notes", chat_id)
        return {"ok": True}
    if low == "/pulse":
        await send_tg(await latest_pulse_text(), chat_id); return {"ok": True}
    if low == "/alpha":
        await send_tg(await latest_alpha_text(), chat_id); return {"ok": True}
    if low == "/levels":
        lv = await compute_levels("ETHUSDT", 24)
        txt = ["🗺️ Zonas (24h)"]
        if lv.get("day_high") and lv.get("day_low"): txt.append(f"Dia H/L: {lv['day_high']:.0f} / {lv['day_low']:.0f}")
        if lv.get("near_rounds"): txt.append("Redondos: " + ", ".join(f"{int(x)}" for x in lv["near_rounds"]))
        txt.append(f"Gatilhos: <{ETH_HEDGE_2:.0f}, <{ETH_HEDGE_1:.0f}, >{ETH_CLOSE:.0f}")
        await send_tg("\n".join(txt), chat_id); return {"ok": True}
    if low == "/notes":
        async with pool.acquire() as c:
            rows = await c.fetch("SELECT created_at,text FROM notes ORDER BY id DESC LIMIT 5")
        if not rows: await send_tg("Sem notas ainda.", chat_id); return {"ok": True}
        out=["🗒️ Notas recentes:"]+[f"- {r['created_at']:%m-%d %H:%M} • {r['text']}" for r in rows]
        await send_tg("\n".join(out), chat_id); return {"ok": True}
    if low == "/strat last":
        async with pool.acquire() as c:
            row = await c.fetchrow("SELECT created_at,name,version,note FROM strategy_versions ORDER BY id DESC LIMIT 1")
        if not row: await send_tg("Nenhuma estratégia salva.", chat_id); return {"ok": True}
        await send_tg(f"📌 Última estratégia:\n{row['created_at']:%Y-%m-%d %H:%M}\n{row['name']} v{row['version']}\n{row['note'] or ''}", chat_id); return {"ok": True}
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
        await send_tg(f"📎 Estratégia salva: {name} v{version}", chat_id); return {"ok": True}

    await send_tg("Comando não reconhecido. Use /pulse, /alpha, /levels, /note, /strat new, /strat last, /notes.", chat_id)
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

# --------- Admin ---------
@app.post("/admin/refresh_webhook")
async def admin_refresh_webhook(secret: str):
    if not ADMIN_SECRET or secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="unauthorized")
    await ensure_webhook()
    return {"ok": True, "webhook": last_run["webhook"]}

@app.post("/admin/news_demo")
async def admin_news_demo(secret: str):
    if not ADMIN_SECRET or secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="unauthorized")
    async with pool.acquire() as c:
        await c.execute(
            "INSERT INTO news_snap(source,title,url) VALUES($1,$2,$3)",
            "demo", "Movimento técnico relevante nas últimas horas (demo)", "https://example.com/x"
        )
    last_run["news"] = dt.datetime.now(TZ).isoformat(timespec="seconds")
    return {"ok": True}

# app.py — Stark DeFi Agent v6.0.6-full
# ---------------------------------------------------------------------------
# Objetivo: manter tudo que já estava funcionando e adicionar comentários
# inteligentes em /pulse, /eth e /btc, com fallback robusto de dados,
# níveis/táticas claras e sem dependências que quebram (Binance removido).
# Compatível com Render + Railway Postgres. Não requer rodar SQL manual.
# ---------------------------------------------------------------------------
# Principais mudanças desta versão:
# - Robustez de DB: criação/alteração de colunas que faltarem (news_items.ts)
# - Comentário rico (8h/12h) em /pulse, /eth, /btc, com síntese causal + ação
# - Sem Binance. Preço/SPOT via Coinbase, fallback CoinGecko. Funding/OI Bybit
#   com tolerância a 403 (ignora e segue). Sem SPAM de TG; cooldown básico.
# - Dois webhooks aceitos: /webhook e /webhook/{token}
# - Endpoints de diagnóstico: /diag/summary e /ping/telegram
# - Mantém endpoints anteriores (/healthz, /status, /pulse, /run/accounts, etc.)
# ---------------------------------------------------------------------------

import os, hmac, hashlib, time, math, csv, io, asyncio, traceback, json
import datetime as dt
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, List, Tuple

import httpx
import asyncpg
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ====================== Config / ENV ======================
VERSION = "v6.0.6-full"
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
NEWS_VERBOSE = os.getenv("NEWS_VERBOSE", "true").lower() == "true"

# Urgência do Pulse (percentual de variação em 8h)
PULSE_URGENCY_ETH = float(os.getenv("PULSE_URGENCY_ETH", "2.0"))
PULSE_URGENCY_BTC = float(os.getenv("PULSE_URGENCY_BTC", "1.5"))

# ====================== URLs externas ======================
TG_SEND = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"

# Coinbase spot (ticker simples)
CB_ETH = "https://api.exchange.coinbase.com/products/ETH-USD/ticker"
CB_BTC = "https://api.exchange.coinbase.com/products/BTC-USD/ticker"

# CoinGecko fallback
CG_SIMPLE = "https://api.coingecko.com/api/v3/simple/price?ids=ethereum,bitcoin&vs_currencies=usd"

# Bybit (funding / OI) — tolerar 403
BYBIT_FUND = "https://api.bybit.com/v5/market/funding/history?category=linear&symbol=ETHUSDT&limit=1"
BYBIT_OI   = "https://api.bybit.com/v5/market/open-interest?category=linear&symbol=ETHUSDT&interval=5min"

# Simplificação whales (desligado por padrão)
WHALES_ENABLED = os.getenv("WHALES_ENABLED", "false").lower() == "true"

# ====================== App / Pool ======================
app = FastAPI(title="stark-defi-agent", version=VERSION)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

pool: Optional[asyncpg.Pool] = None
scheduler = AsyncIOScheduler(timezone=str(TZ))

last_run = {
    "ingest_1m": None,
    "ingest_whales": None,
    "ingest_accounts": None,
    "ingest_onchain": None,
    "news": None,
}

# ====================== SQL ======================
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
-- Tabela de notícias mínima para comentários
CREATE TABLE IF NOT EXISTS news_items(
  id serial PRIMARY KEY,
  ts timestamptz NOT NULL DEFAULT now(),
  title text,
  url text
);
"""

async def _ensure_news_columns(conn: asyncpg.Connection):
    cols = {r[0] for r in await conn.fetch(
        "SELECT column_name FROM information_schema.columns WHERE table_name='news_items'"
    )}
    statements = []
    if "ts" not in cols:
        statements.append("ALTER TABLE news_items ADD COLUMN IF NOT EXISTS ts timestamptz NOT NULL DEFAULT now()")
    if "title" not in cols:
        statements.append("ALTER TABLE news_items ADD COLUMN IF NOT EXISTS title text")
    if "url" not in cols:
        statements.append("ALTER TABLE news_items ADD COLUMN IF NOT EXISTS url text")
    for st in statements:
        await conn.execute(st)

async def db_init():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL não definido.")
    global pool
    pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=5)
    async with pool.acquire() as c:
        await c.execute(CREATE_SQL)
        await _ensure_news_columns(c)

# ====================== HTTP helpers ======================
async def fetch_json(url: str, headers: Dict[str,str]|None=None, params: Dict[str,str]|None=None, timeout: float=15.0):
    async with httpx.AsyncClient(timeout=timeout) as s:
        r = await s.get(url, headers=headers, params=params)
        r.raise_for_status()
        return r.json()

async def fetch_json_retry(url: str, *, attempts: int=2, timeout: float=10.0, **kw):
    last = None
    for i in range(attempts):
        try:
            return await fetch_json(url, timeout=timeout, **kw)
        except Exception as e:
            last = e
            await asyncio.sleep(0.8*(i+1))
    if last: raise last

# ====================== Telegram ======================
_last_tg_msg_at: Dict[str,float] = {}

async def send_tg(text: str, chat_id: Optional[str] = None, cooldown_sec: int=3):
    if not SEND_ENABLED: return
    cid = chat_id or TG_CHAT
    if not cid: return
    now = time.time()
    last = _last_tg_msg_at.get(cid, 0)
    if now - last < cooldown_sec:
        await asyncio.sleep(cooldown_sec - (now-last))
    async with httpx.AsyncClient(timeout=12) as s:
        try:
            await s.post(TG_SEND, json={"chat_id": cid, "text": text})
            _last_tg_msg_at[cid] = time.time()
        except Exception:
            traceback.print_exc()

# ====================== Dados de Mercado ======================
async def get_spot_snapshot() -> dict:
    # Tenta Coinbase; fallback CoinGecko
    try:
        ce = await fetch_json_retry(CB_ETH, attempts=2)
        cb = await fetch_json_retry(CB_BTC, attempts=2)
        eth_p = float(ce.get("price") or ce.get("ask") or ce.get("bid"))
        btc_p = float(cb.get("price") or cb.get("ask") or cb.get("bid"))
        # Coinbase não traz H/L intraday simples; manter NaN
        return {
            "eth": {"price": eth_p, "high": math.nan, "low": math.nan},
            "btc": {"price": btc_p, "high": math.nan, "low": math.nan},
        }
    except Exception:
        traceback.print_exc()
        cg = await fetch_json_retry(CG_SIMPLE, attempts=2)
        return {
            "eth": {"price": float(cg["ethereum"]["usd"]), "high": math.nan, "low": math.nan},
            "btc": {"price": float(cg["bitcoin"]["usd"]),  "high": math.nan, "low": math.nan},
        }

async def get_derivatives_snapshot() -> dict:
    funding = None; open_interest = None
    try:
        f = (await fetch_json(BYBIT_FUND))
        lst = f.get("result",{}).get("list") or []
        if lst:
            funding = float(lst[0].get("fundingRate"))
    except Exception:
        pass
    try:
        oi = (await fetch_json(BYBIT_OI))
        lst = oi.get("result",{}).get("list") or []
        if lst:
            open_interest = float(lst[-1].get("openInterest"))
    except Exception:
        pass
    return {"funding": funding, "oi": open_interest}

# ====================== Ingest (1m) ======================
async def ingest_1m():
    try:
        now = dt.datetime.now(dt.UTC).replace(second=0, microsecond=0)
        spot = await get_spot_snapshot()
        der  = await get_derivatives_snapshot()
        eth_p = spot["eth"]["price"]; btc_p = spot["btc"]["price"]
        ethbtc = eth_p / btc_p if btc_p else math.nan
        async with pool.acquire() as c:
            await c.execute(
                "INSERT INTO candles_minute(ts,symbol,open,high,low,close,volume) VALUES($1,$2,NULL,$3,$4,$5,NULL) "
                "ON CONFLICT (ts,symbol) DO UPDATE SET high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close",
                now, "ETHUSDT", spot["eth"]["high"], spot["eth"]["low"], eth_p
            )
            await c.execute(
                "INSERT INTO candles_minute(ts,symbol,open,high,low,close,volume) VALUES($1,$2,NULL,$3,$4,$5,NULL) "
                "ON CONFLICT (ts,symbol) DO UPDATE SET high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close",
                now, "BTCUSDT", spot["btc"]["high"], spot["btc"]["low"], btc_p
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

# ====================== Whales (opcional) ======================
_ws_lock = asyncio.Lock()
_last_trade_time_ms = 0

async def ingest_whales():
    # Desligado por padrão para evitar 403/451. Se habilitar, deve apontar fonte confiável.
    if not WHALES_ENABLED:
        last_run["ingest_whales"] = dt.datetime.now(TZ).isoformat(timespec="seconds")
        return
    async with _ws_lock:
        # Placeholder (não gera eventos até habilitar uma fonte estável)
        last_run["ingest_whales"] = dt.datetime.now(TZ).isoformat(timespec="seconds")

# ====================== Accounts (Bybit + Aave) ======================
BYBIT_API = "https://api.bybit.com"

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
            dec=int(it["reserve"]["decimals"]) if it.get("reserve") else 18
            a=float(it.get("scaledATokenBalance") or 0)/(10**dec)
            d=float(it.get("scaledVariableDebt") or 0)/(10**dec)
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

# ====================== News (placeholder leve) ======================
NEWS_FEEDS = [
    ("CoinDesk", "https://www.coindesk.com/arc/outboundfeeds/rss/?outputType=xml"),
    ("Decrypt",  "https://decrypt.co/feed"),
    ("The Block", "https://www.theblock.co/rss"),
]

async def _fetch_feed_title_only(url: str) -> List[Tuple[str,str]]:
    # Não parseia RSS pesado: pega 2-3 títulos do HTML/XML bruto (heurística leve)
    try:
        txt = await fetch_text(url)
        titles = []
        for line in txt.splitlines():
            line=line.strip()
            if line.lower().startswith("<title>") and len(titles)<3:
                t = line.replace("<title>","").replace("</title>","").strip()
                if t and "CoinDesk" not in t and "Decrypt" not in t and "The Block" not in t:
                    titles.append((t, url))
        return titles
    except Exception:
        return []

async def fetch_text(url: str, timeout: float=10.0) -> str:
    async with httpx.AsyncClient(timeout=timeout) as s:
        r = await s.get(url)
        r.raise_for_status()
        return r.text

async def ingest_news_light():
    try:
        items: List[Tuple[str,str]] = []
        for _, url in NEWS_FEEDS:
            items += await _fetch_feed_title_only(url)
        if not items:
            return
        now = dt.datetime.now(dt.UTC)
        async with pool.acquire() as c:
            for title, url in items[:6]:
                await c.execute(
                    "INSERT INTO news_items(ts,title,url) VALUES($1,$2,$3)", now, title[:280], url
                )
        last_run["news"] = dt.datetime.now(TZ).isoformat(timespec="seconds")
    except Exception:
        traceback.print_exc()
        last_run["news"] = dt.datetime.now(TZ).isoformat(timespec="seconds")

# ====================== Comentários / Pulse ======================
LEVELS_ETH = {
    "supports": [4200, 4000],
    "resists":  [4300, 4400],
}
LEVELS_BTC = {
    "supports": [62000, 60000],
    "resists":  [65000, 68000],
}

def _fmt_levels(d: Dict[str,List[float]]) -> str:
    s = ", ".join([f"{int(x):,}".replace(",",".") for x in d.get("supports",[])])
    r = ", ".join([f"{int(x):,}".replace(",",".") for x in d.get("resists",[])])
    return f"Suportes: {s} | Resist: {r}"

async def _market_windows():
    end = dt.datetime.now(dt.UTC)
    start8 = end - dt.timedelta(hours=8)
    start12 = end - dt.timedelta(hours=12)
    async with pool.acquire() as c:
        rows8 = await c.fetch("SELECT ts, eth_usd, btc_usd, eth_btc_ratio FROM market_rel WHERE ts BETWEEN $1 AND $2 ORDER BY ts", start8, end)
        rows12= await c.fetch("SELECT ts, eth_usd, btc_usd, eth_btc_ratio FROM market_rel WHERE ts BETWEEN $1 AND $2 ORDER BY ts", start12, end)
        deriv = await c.fetch("SELECT ts, funding, open_interest FROM derivatives_snap WHERE ts BETWEEN $1 AND $2 ORDER BY ts", start8, end)
        try:
            news  = await c.fetch("SELECT ts, title FROM news_items WHERE ts BETWEEN $1 AND $2 ORDER BY ts DESC LIMIT 6", start12, end)
        except Exception:
            news = []
    return rows8, rows12, deriv, news

def _pct(a: float, b: float) -> float:
    if a in (None, 0) or b is None: return 0.0
    try: return (b-a)/a*100.0
    except Exception: return 0.0

async def build_commentary(asset: str|None=None) -> str:
    rows8, rows12, deriv, news = await _market_windows()
    if not rows8:
        return "⏳ Aguardando histórico para comentário (volte em alguns minutos)."
    eth8 = [float(r["eth_usd"]) for r in rows8]; btc8 = [float(r["btc_usd"]) for r in rows8]
    rb8  = [float(r["eth_btc_ratio"]) for r in rows8]
    eth12= [float(r["eth_usd"]) for r in rows12] if rows12 else eth8
    btc12= [float(r["btc_usd"]) for r in rows12] if rows12 else btc8

    eth_chg8 = _pct(eth8[0], eth8[-1]); btc_chg8 = _pct(btc8[0], btc8[-1]); rb_chg8 = _pct(rb8[0], rb8[-1])
    eth_chg12= _pct(eth12[0], eth12[-1]) if len(eth12)>1 else eth_chg8
    btc_chg12= _pct(btc12[0], btc12[-1]) if len(btc12)>1 else btc_chg8

    funding = float(deriv[-1]["funding"]) if deriv and deriv[-1]["funding"] is not None else None
    oi      = float(deriv[-1]["open_interest"]) if deriv and deriv[-1]["open_interest"] is not None else None

    # Diagnóstico causal simples
    lines = []
    if asset is None:
        lines.append(f"ETH: {eth_chg8:+.2f}% (8h), {eth_chg12:+.2f}% (12h).")
        lines.append(f"BTC: {btc_chg8:+.2f}% (8h), {btc_chg12:+.2f}% (12h).  ETH/BTC: {rb_chg8:+.2f}% (8h).")
    elif asset == "ETH":
        lines.append(f"ETH: {eth_chg8:+.2f}% (8h), {eth_chg12:+.2f}% (12h).  vs BTC (8h): {rb_chg8:+.2f}%.")
    else:
        lines.append(f"BTC: {btc_chg8:+.2f}% (8h), {btc_chg12:+.2f}% (12h).  ETH/BTC (8h): {rb_chg8:+.2f}%.")

    # Funding/OI
    fd = f"Funding {funding*100:.3f}%/8h, " if funding is not None else ""
    oi_s= f"OI ~ {oi:,.0f}.".replace(",", ".") if oi is not None else ""

    # Narrativa baseada em sinais relativos
    bias = []
    if rb_chg8 < -1.0: bias.append("BTC dominante")
    elif rb_chg8 > 1.0: bias.append("ETH ganhando beta")
    if funding is not None:
        if funding > 0.0012: bias.append("euforia perp (funding alto)")
        elif funding < -0.0005: bias.append("perp inclinado à venda")

    why = "; ".join(bias) or "equilíbrio tático"

    # Níveis + Ação
    levels_eth = _fmt_levels(LEVELS_ETH)
    levels_btc = _fmt_levels(LEVELS_BTC)

    # Urgência (baseada em 8h)
    def urg(chg: float, thr: float) -> str:
        if abs(chg) >= thr*1.8: return "🔴"
        if abs(chg) >= thr:     return "🟡"
        return "🟢"

    if asset == "ETH":
        lines.append(f"{fd}{oi_s} Por quê: {why}.")
        lines.append(f"Níveis ETH: {levels_eth}")
        lines.append("Ação: usar gatilhos em quebras/fechamentos dos níveis; ajustar hedge ETHT 2x se gatilhos próximos.")
        lines.append(urg(eth_chg8, PULSE_URGENCY_ETH))
    elif asset == "BTC":
        lines.append(f"{fd}{oi_s} Por quê: {why}.")
        lines.append(f"Níveis BTC: {levels_btc}")
        lines.append("Ação: observar defesas em suportes; oportunidade em pullbacks após rompimentos válidos.")
        lines.append(urg(btc_chg8, PULSE_URGENCY_BTC))
    else:
        lines.append(f"{fd}{oi_s} Por quê: {why}.")
        lines.append(f"NÍVEIS ETH: {levels_eth}")
        lines.append(f"NÍVEIS BTC: {levels_btc}")

    # Notícias (somente títulos curtos)
    if news:
        tops = [n["title"] for n in news[:3]] if not NEWS_VERBOSE else [n["title"] for n in news[:6]]
        lines.append("Notícias: " + "; ".join(tops))

    return "\n".join(lines)

async def latest_pulse_text(include_header: bool=True) -> str:
    async with pool.acquire() as c:
        m = await c.fetchrow("SELECT * FROM market_rel ORDER BY ts DESC LIMIT 1")
        e = await c.fetchrow("SELECT * FROM candles_minute WHERE symbol='ETHUSDT' ORDER BY ts DESC LIMIT 1")
        b = await c.fetchrow("SELECT * FROM candles_minute WHERE symbol='BTCUSDT' ORDER BY ts DESC LIMIT 1")
        d = await c.fetchrow("SELECT * FROM derivatives_snap WHERE symbol='ETHUSDT' AND exchange='bybit' ORDER BY ts DESC LIMIT 1")
    if not (m and e and b): return "⏳ Aguardando primeiros dados…"
    now = dt.datetime.now(TZ).strftime("%Y-%m-%d %H:%M")
    eth = float(m["eth_usd"]); btc = float(m["btc_usd"]); ratio = float(m["eth_btc_ratio"])
    header = f"🕒 {now} • {VERSION}\nETH ${eth:,.2f}\nBTC ${btc:,.2f}\nETH/BTC {ratio:.5f}".replace(",", ".")

    # Ação de hedge
    act = None
    if eth < ETH_HEDGE_2: act = f"🚨 ETH<{ETH_HEDGE_2:.0f}: ampliar hedge p/ 20% (29 ETH)."
    elif eth < ETH_HEDGE_1: act = f"⚠️ ETH<{ETH_HEDGE_1:.0f}: ativar hedge 15% (22 ETH)."
    elif eth > ETH_CLOSE: act = f"↩️ ETH>{ETH_CLOSE:.0f}: avaliar fechar hedge."

    body = await build_commentary()
    if include_header:
        parts = [header]
        if act: parts.append(act)
        parts.append(body)
        return "\n".join(parts)
    else:
        return body

# ====================== FastAPI ======================
@app.on_event("startup")
async def _startup():
    await db_init()
    # Schedulers
    scheduler.add_job(ingest_1m, "interval", minutes=1, id="ingest_1m", replace_existing=True)
    scheduler.add_job(ingest_whales, "interval", seconds=30, id="ingest_whales", replace_existing=True)
    scheduler.add_job(ingest_accounts, "interval", minutes=5, id="ingest_accounts", replace_existing=True)
    scheduler.add_job(ingest_news_light, "interval", minutes=15, id="news", replace_existing=True)
    if AAVE_ADDR and ETHERSCAN_API_KEY:
        # Onchain opcional — não crítico
        pass
    # Boletins horários configuráveis se quiser (mantidos diários fixos):
    # Enviar 08:00 / 14:00 / 20:00 locais
    scheduler.add_job(lambda: send_tg_sync_wrapper(), "cron", hour=8, minute=0, id="bulletin_08", replace_existing=True)
    scheduler.add_job(lambda: send_tg_sync_wrapper(), "cron", hour=14, minute=0, id="bulletin_14", replace_existing=True)
    scheduler.add_job(lambda: send_tg_sync_wrapper(), "cron", hour=20, minute=0, id="bulletin_20", replace_existing=True)
    scheduler.start()

async def send_tg_sync_wrapper():
    try:
        await send_tg(await latest_pulse_text())
    except Exception:
        traceback.print_exc()

@app.get("/")
async def root():
    return {"ok": True, "service": "stark-defi-agent", "version": VERSION}

@app.get("/healthz")
async def healthz():
    return {"ok": True, "time": dt.datetime.now(TZ).isoformat(), "version": VERSION}

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
            "news": news
        },
        "last_run": last_run,
        "version": VERSION
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

@app.get("/diag/summary")
async def diag_summary():
    return {"ok": True, "message": await latest_pulse_text(include_header=False)}

@app.get("/eth")
async def eth_endpoint():
    text = await build_commentary("ETH")
    await send_tg(text)
    return {"ok": True, "message": text}

@app.get("/btc")
async def btc_endpoint():
    text = await build_commentary("BTC")
    await send_tg(text)
    return {"ok": True, "message": text}

# ====================== Telegram Webhook ======================
async def _process_update(request: Request):
    try:
        update = await request.json()
    except Exception:
        return {"ok": True}
    msg = update.get("message") or update.get("edited_message") or update.get("channel_post")
    if not msg: return {"ok": True}
    chat_id = str(msg["chat"]["id"])
    text = (msg.get("text") or "").strip()
    low = text.lower()

    if low in ("/start", "start"):
        await send_tg(
            "✅ Bot online ("+VERSION+"). Comandos: /pulse, /eth, /btc, /note <texto>, /strat new <nome> | <versão> | <nota>, /strat last, /notes",
            chat_id
        ); return {"ok": True}
    if low == "/pulse":
        await send_tg(await latest_pulse_text(), chat_id); return {"ok": True}
    if low == "/eth":
        await send_tg(await build_commentary("ETH"), chat_id); return {"ok": True}
    if low == "/btc":
        await send_tg(await build_commentary("BTC"), chat_id); return {"ok": True}
    if low == "/notes":
        async with pool.acquire() as c:
            rows = await c.fetch("SELECT created_at,text FROM notes ORDER BY id DESC LIMIT 5")
        if not rows:
            await send_tg("Sem notas ainda.", chat_id); return {"ok": True}
        out=["Notas recentes:"]+[f"- {r['created_at']:%m-%d %H:%M} • {r['text']}" for r in rows]
        await send_tg("\n".join(out), chat_id); return {"ok": True}
    if low == "/strat last":
        async with pool.acquire() as c:
            row = await c.fetchrow("SELECT created_at,name,version,note FROM strategy_versions ORDER BY id DESC LIMIT 1")
        if not row:
            await send_tg("Nenhuma estratégia salva.", chat_id); return {"ok": True}
        await send_tg(f"Última estratégia:\n{row['created_at']:%Y-%m-%d %H:%M}\n{row['name']} v{row['version']}\n{row['note'] or ''}", chat_id); return {"ok": True}
    if low.startswith("/note"):
        note = text[len("/note"):].strip()
        if not note:
            await send_tg("Uso: /note seu texto aqui", chat_id); return {"ok": True}
        async with pool.acquire() as c:
            await c.execute("INSERT INTO notes(tag,text) VALUES($1,$2)", None, note)
        await send_tg("📝 Nota salva.", chat_id); return {"ok": True}
    if low.startswith("/strat new"):
        try:
            payload = text[len("/strat new"):].strip()
            name, version, note = [p.strip() for p in payload.split("|", 3)][:3]
        except Exception:
            await send_tg("Uso: /strat new <nome> | <versão> | <nota>", chat_id); return {"ok": True}
        async with pool.acquire() as c:
            await c.execute("INSERT INTO strategy_versions(name,version,note) VALUES($1,$2,$3)", name, version, note)
        await send_tg(f"📌 Estratégia salva: {name} v{version}", chat_id); return {"ok": True}

    await send_tg("Comando não reconhecido. Use /pulse, /eth, /btc, /note, /strat new, /strat last, /notes.", chat_id)
    return {"ok": True}

@app.post("/webhook")
async def webhook_root(request: Request):
    return await _process_update(request)

@app.post("/webhook/{token}")
async def webhook_token(token: str, request: Request):
    # Aceita /webhook/<token> para compatibilidade, mas ignora o valor
    return await _process_update(request)

# ====================== Export/consulta ======================
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
        rows = await c.fetch(
            """
            SELECT * FROM account_snap
            WHERE ts > now() - interval '1 hour'
            ORDER BY ts DESC, venue, metric
            """
        )
    return JSONResponse({"rows": [dict(r) for r in rows]})

@app.get("/ping/telegram")
async def ping_telegram():
    try:
        await send_tg("Ping Telegram OK")
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# ====================== Rodapé ======================
# Linhas aproximadas deste arquivo: ~760
# Versão: v6.0.6-full (repete) — pronto para Render + Railway.

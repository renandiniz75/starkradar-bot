# app.py — Stark DeFi Agent v6.0.5-full
# ------------------------------------------------------------
# Principais mudanças desta versão:
# - Migração automática do schema (inclui news_items, colunas ts/published_at)
# - /start, /pulse, /eth, /btc, /alpha (alias /alfa) reativados
# - Comentário 8h mais inteligente (sem gráficos) com síntese tática e níveis
# - Ingest redundante: Coinbase/Bybit (+ fallback CoinGecko), funding/OI Bybit (silencioso se 403)
# - News (opcional): leitura de RSS básicos (CoinDesk/Cointelegraph/Decrypt) sem API-key
# - Webhook: aceita /webhook e /webhook/{BOT_TOKEN}; também permite /healthz e /status
# - Env toggles pra ligar/desligar módulos sem redeploy
# ------------------------------------------------------------

import os, io, csv, math, hmac, json, hashlib, time, asyncio, traceback
import datetime as dt
from typing import Optional, Dict, Any, List, Tuple
from zoneinfo import ZoneInfo

import httpx
import asyncpg
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse

# -------------------- Versão / Ambiente --------------------
BOOT_VERSION = os.getenv("BOOT_VERSION", "6.0.5-full")
TZ = ZoneInfo(os.getenv("TZ", "America/Sao_Paulo"))

DB_URL = os.getenv("DATABASE_URL")  # REQUIRED

# Telegram
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "")
SEND_ENABLED = bool(TG_TOKEN)

# Bybit (privado opcional p/ accounts)
BYBIT_KEY = os.getenv("BYBIT_RO_KEY", "")
BYBIT_SEC = os.getenv("BYBIT_RO_SECRET", "")
BYBIT_ACCOUNT_TYPE = os.getenv("BYBIT_ACCOUNT_TYPE", "UNIFIED")  # UNIFIED/CONTRACT/SPOT

# On-chain (opcional)
AAVE_ADDR = os.getenv("AAVE_ADDR", "")
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")

# Boletim / Hedge thresholds
ETH_HEDGE_1 = float(os.getenv("ETH_HEDGE_1", "3900"))
ETH_HEDGE_2 = float(os.getenv("ETH_HEDGE_2", "3800"))
ETH_CLOSE   = float(os.getenv("ETH_CLOSE_HEDGE", "3950"))

# Whales (opcional)
WHALE_USD_MIN = float(os.getenv("WHALE_USD_MIN", "500000"))
ENABLE_WHALES = os.getenv("ENABLE_WHALES", "0") == "1"  # desativado por padrão aqui

# News (opcional)
ENABLE_NEWS = os.getenv("ENABLE_NEWS", "1") == "1"

# Scheduler toggles
ENABLE_ACCOUNTS = os.getenv("ENABLE_ACCOUNTS", "1") == "1"
ENABLE_ONCHAIN  = os.getenv("ENABLE_ONCHAIN", "0") == "1" and bool(AAVE_ADDR and ETHERSCAN_API_KEY)

# -----------------------------------------------------------
# Constantes de API públicas (usar sem key)
TG_SEND = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage" if TG_TOKEN else None

COINBASE_TICK = "https://api.exchange.coinbase.com/products/{sym}/ticker"   # ETH-USD / BTC-USD
BYBIT_SPOT    = "https://api.bybit.com/v5/market/tickers?category=spot&symbol={sym}"
BYBIT_FUND    = "https://api.bybit.com/v5/market/funding/history?category=linear&symbol=ETHUSDT&limit=1"
BYBIT_OI      = "https://api.bybit.com/v5/market/open-interest?category=linear&symbol=ETHUSDT&interval=5min"
COINGECKO_SP  = "https://api.coingecko.com/api/v3/simple/price?ids=ethereum,bitcoin&vs_currencies=usd"

# Notícias (RSS simples)
NEWS_FEEDS = [
    ("CoinDesk", "https://www.coindesk.com/arc/outboundfeeds/rss/"),
    ("Cointelegraph", "https://cointelegraph.com/rss"),
    ("Decrypt", "https://decrypt.co/feed")
]

ETHERSCAN_TX = "https://api.etherscan.io/api?module=account&action=txlist&address={addr}&startblock=0&endblock=99999999&sort=desc&apikey={key}"

# -----------------------------------------------------------
app = FastAPI(title="stark-defi-agent", version=BOOT_VERSION)
pool: Optional[asyncpg.Pool] = None

from apscheduler.schedulers.asyncio import AsyncIOScheduler
scheduler = AsyncIOScheduler(timezone=str(TZ))

last_run: Dict[str, Optional[str]] = {
    "ingest_1m": None,
    "ingest_whales": None,
    "ingest_accounts": None,
    "ingest_onchain": None,
    "news": None
}

# -------------------- DB schema & migração -----------------
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
  ts timestamptz,
  source text,
  title text,
  url text,
  impact int
);
"""

ALTERS = [
    "ALTER TABLE news_items ADD COLUMN IF NOT EXISTS ts timestamptz",
    "ALTER TABLE news_items ADD COLUMN IF NOT EXISTS source text",
    "ALTER TABLE news_items ADD COLUMN IF NOT EXISTS title text",
    "ALTER TABLE news_items ADD COLUMN IF NOT EXISTS url text",
    "ALTER TABLE news_items ADD COLUMN IF NOT EXISTS impact int",
]

async def db_init():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL não definido.")
    global pool
    pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=5)
    async with pool.acquire() as c:
        await c.execute(CREATE_SQL)
        for q in ALTERS:
            try:
                await c.execute(q)
            except Exception:
                pass
        # normaliza nulos
        try:
            await c.execute("UPDATE news_items SET ts = COALESCE(ts, now()) WHERE ts IS NULL")
        except Exception:
            pass

# -------------------- Utils HTTP/Telegram ------------------
async def fetch_json(url: str, *, timeout: float = 15.0, headers: Dict[str,str]|None=None, params: Dict[str,str]|None=None) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=timeout) as s:
        r = await s.get(url, headers=headers, params=params)
        r.raise_for_status()
        return r.json()

async def fetch_text(url: str, *, timeout: float = 15.0) -> str:
    async with httpx.AsyncClient(timeout=timeout) as s:
        r = await s.get(url)
        r.raise_for_status()
        return r.text

async def fetch_json_retry(url: str, *, attempts: int = 2, timeout: float = 12.0, **kwargs) -> Optional[Dict[str, Any]]:
    last = None
    for i in range(attempts):
        try:
            return await fetch_json(url, timeout=timeout, **kwargs)
        except Exception as e:
            last = e
            await asyncio.sleep(0.6*(i+1))
    if last:
        print(f"[WARN] fetch_json_retry failed {url}: {last}")
    return None

async def send_tg(text: str, chat_id: Optional[str] = None):
    if not (SEND_ENABLED and TG_SEND): 
        return
    cid = chat_id or TG_CHAT
    if not cid: 
        return
    async with httpx.AsyncClient(timeout=12) as s:
        try:
            await s.post(TG_SEND, json={"chat_id": cid, "text": text})
        except Exception as e:
            print(f"[WARN] send_tg error: {e}")

def pct(a: float, b: float) -> float:
    try:
        if a == 0: return 0.0
        return (b - a) / a * 100.0
    except Exception:
        return 0.0

# -------------------- Market Snapshots (1m) ----------------
async def get_prices_spot() -> Dict[str, Dict[str, float]]:
    # 1) Coinbase
    try:
        e = await fetch_json_retry(COINBASE_TICK.format(sym="ETH-USD"))
        b = await fetch_json_retry(COINBASE_TICK.format(sym="BTC-USD"))
        if e and b:
            eth = float(e.get("price") or e.get("ask") or e.get("bid"))
            btc = float(b.get("price") or b.get("ask") or b.get("bid"))
            return {
                "eth": {"price": eth, "high": math.nan, "low": math.nan},
                "btc": {"price": btc, "high": math.nan, "low": math.nan},
            }
    except Exception:
        pass
    # 2) Bybit spot (às vezes 403; tentar)
    try:
        ee = await fetch_json_retry(BYBIT_SPOT.format(sym="ETHUSDT"))
        bb = await fetch_json_retry(BYBIT_SPOT.format(sym="BTCUSDT"))
        if ee and bb:
            eth0 = ee["result"]["list"][0]; btc0 = bb["result"]["list"][0]
            return {
                "eth": {"price": float(eth0["lastPrice"]), "high": float(eth0["highPrice"]), "low": float(eth0["lowPrice"])},
                "btc": {"price": float(btc0["lastPrice"]), "high": float(btc0["highPrice"]), "low": float(btc0["lowPrice"])},
            }
    except Exception:
        pass
    # 3) CoinGecko fallback
    cg = await fetch_json_retry(COINGECKO_SP)
    if cg:
        return {
            "eth": {"price": float(cg["ethereum"]["usd"]), "high": math.nan, "low": math.nan},
            "btc": {"price": float(cg["bitcoin"]["usd"]),  "high": math.nan, "low": math.nan},
        }
    raise RuntimeError("Sem fonte de preço disponível agora.")

async def get_derivatives_snapshot() -> Dict[str, Optional[float]]:
    funding = None; open_interest = None
    # Bybit (silencioso se 403)
    f = await fetch_json_retry(BYBIT_FUND, attempts=2, timeout=8.0)
    if f and f.get("result",{}).get("list"):
        try:
            funding = float(f["result"]["list"][0]["fundingRate"])
        except Exception: pass
    oi = await fetch_json_retry(BYBIT_OI, attempts=2, timeout=8.0)
    if oi and oi.get("result",{}).get("list"):
        try:
            open_interest = float(oi["result"]["list"][-1]["openInterest"])
        except Exception: pass
    return {"funding": funding, "oi": open_interest}

async def ingest_1m():
    try:
        now = dt.datetime.now(dt.UTC).replace(second=0, microsecond=0)
        spot = await get_prices_spot()
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
    except Exception as e:
        print(f"[ERR] ingest_1m: {e}")
    finally:
        last_run["ingest_1m"] = dt.datetime.now(TZ).isoformat(timespec="seconds")

# -------------------- Whales (desligado por padrão) --------
async def ingest_whales():
    # placeholder — desativado por padrão nessa versão
    last_run["ingest_whales"] = dt.datetime.now(TZ).isoformat(timespec="seconds")

# -------------------- Accounts (5m) ------------------------
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
            out.append({"venue":"bybit","metric":"total_equity","value": float(lst[0].get("totalEquity",0) or 0)})
        r2 = await bybit_private_get("/v5/position/list", {"category":"linear","symbol":"ETHUSDT"})
        pos = r2.get("result",{}).get("list",[])
        if pos:
            p0 = pos[0]
            out += [
                {"venue":"bybit","metric":"ethusdt_perp_size","value": float(p0.get("size",0) or 0)},
                {"venue":"bybit","metric":"ethusdt_perp_leverage","value": float(p0.get("leverage",0) or 0)},
            ]
    except Exception as e:
        print(f"[WARN] snapshot_bybit: {e}")
    return out

# Aave on-chain via Etherscan simplificado (somente detectar grandes tx do address)
async def ingest_onchain_eth(addr: str):
    if not ETHERSCAN_API_KEY:
        last_run["ingest_onchain"] = dt.datetime.now(TZ).isoformat(timespec="seconds")
        return
    try:
        data = await fetch_json_retry(ETHERSCAN_TX.format(addr=addr, key=ETHERSCAN_API_KEY), attempts=2, timeout=18.0)
        if not data: 
            return
        txs = data.get("result", [])[:12]
        big=[]
        for t in txs:
            val_eth = float(t.get("value","0"))/1e18
            if val_eth >= 3000:  # alerta de tamanho
                big.append((val_eth, t.get("from"), t.get("to")))
        if big:
            ts = dt.datetime.now(dt.UTC)
            async with pool.acquire() as c:
                for val_eth, _from, _to in big:
                    await c.execute(
                        "INSERT INTO whale_events(ts,venue,side,qty,usd_value,note) VALUES($1,$2,$3,$4,$5,$6)",
                        ts, "onchain", "ONCHAIN", val_eth, None, f"{_from} -> {_to}"
                    )
    except Exception as e:
        print(f"[WARN] ingest_onchain_eth: {e}")
    finally:
        last_run["ingest_onchain"] = dt.datetime.now(TZ).isoformat(timespec="seconds")

async def ingest_accounts():
    rows=[]
    rows += await snapshot_bybit()
    if not rows:
        last_run["ingest_accounts"] = dt.datetime.now(TZ).isoformat(timespec="seconds")
        return
    async with pool.acquire() as c:
        for r in rows:
            await c.execute("INSERT INTO account_snap(venue,metric,value) VALUES($1,$2,$3)", r["venue"], r["metric"], r["value"])
    last_run["ingest_accounts"] = dt.datetime.now(TZ).isoformat(timespec="seconds")

# -------------------- News ingest (15m) --------------------
def _parse_rss_titles(xml: str, limit: int = 10) -> List[Tuple[str,str]]:
    # Retorna [(title, link), ...]
    out=[]
    try:
        import re
        items = re.split(r"<item>|<entry>", xml)[1:]
        for it in items:
            t = re.search(r"<title>(.*?)</title>", it, flags=re.I|re.S)
            l = re.search(r"<link>(.*?)</link>", it, flags=re.I|re.S)
            if not l:
                l = re.search(r'href="([^"]+)"', it, flags=re.I|re.S)
            title = t.group(1).strip() if t else None
            link  = l.group(1).strip() if l else None
            if title and link:
                # limpar html entities simples
                title = re.sub(r"&[^;]+;", " ", title)
                out.append((title, link))
            if len(out) >= limit:
                break
    except Exception:
        pass
    return out

async def ingest_news():
    if not ENABLE_NEWS:
        last_run["news"] = dt.datetime.now(TZ).isoformat(timespec="seconds")
        return
    rows=[]
    for src, url in NEWS_FEEDS:
        try:
            xml = await fetch_text(url, timeout=12.0)
            items = _parse_rss_titles(xml, limit=6)
            now = dt.datetime.now(dt.UTC)
            for title, link in items:
                rows.append((now, src, title, link, None))
        except Exception as e:
            print(f"[WARN] news fetch {src}: {e}")
    if rows:
        async with pool.acquire() as c:
            for ts, src, title, link, imp in rows:
                try:
                    await c.execute(
                        "INSERT INTO news_items(ts,source,title,url,impact) VALUES($1,$2,$3,$4,$5)",
                        ts, src, title[:500], link[:600], imp
                    )
                except Exception:
                    pass
    last_run["news"] = dt.datetime.now(TZ).isoformat(timespec="seconds")

# -------------------- Comentários / Pulse ------------------
def _levels_eth(price: float) -> str:
    # Níveis heurísticos (ajuste fino conforme estratégia)
    levels = []
    if price <= ETH_HEDGE_2: levels.append(f"🚨 < {ETH_HEDGE_2:.0f} (hedge 20%)")
    elif price <= ETH_HEDGE_1: levels.append(f"⚠️ < {ETH_HEDGE_1:.0f} (hedge 15%)")
    else: levels.append(f"Suportes: 4.200/4.000  | Resist: 4.300/4.400")
    return " | ".join(levels)

def _levels_btc(price: float) -> str:
    # Níveis simples de referência
    bands = []
    if price < 60000: bands.append("Suportes: 60k / 58k")
    else: bands.append("Suportes: 62k / 60k")
    bands.append("Resist: 65k / 68k")
    return " | ".join(bands)

async def build_commentary_8h() -> str:
    end = dt.datetime.now(dt.UTC); start = end - dt.timedelta(hours=8)
    async with pool.acquire() as c:
        rows = await c.fetch("SELECT ts, eth_usd, btc_usd, eth_btc_ratio FROM market_rel WHERE ts BETWEEN $1 AND $2 ORDER BY ts", start, end)
        deriv = await c.fetch("SELECT ts, funding, open_interest FROM derivatives_snap WHERE ts BETWEEN $1 AND $2 ORDER BY ts", start, end)
        news  = await c.fetch("SELECT ts, title FROM news_items WHERE ts BETWEEN $1 AND $2 ORDER BY ts DESC LIMIT 6", start, end)
    if not rows:
        return "⏳ Aguardando histórico suficiente (tente novamente em alguns minutos)."

    eth = [float(r["eth_usd"]) for r in rows]; btc = [float(r["btc_usd"]) for r in rows]; ratio = [float(r["eth_btc_ratio"]) for r in rows if r["eth_btc_ratio"] is not None]
    eth_chg = pct(eth[0], eth[-1]); btc_chg = pct(btc[0], btc[-1]); ratio_chg = pct(ratio[0], ratio[-1]) if ratio else 0.0
    funding = float(deriv[-1]["funding"]) if (deriv and deriv[-1]["funding"] is not None) else None
    oi      = float(deriv[-1]["open_interest"]) if (deriv and deriv[-1]["open_interest"] is not None) else None

    # Síntese baseada em força relativa e funding
    if ratio_chg > 0.3 and (funding is None or funding < 0.001):
        stance = "viés pró-ETH (ganho de beta com funding contido)."
    elif ratio_chg < -0.3 and (funding is not None and funding > 0.001):
        stance = "viés pró-BTC (ETH perde beta com funding mais alto)."
    else:
        stance = "equilíbrio tático; operar nos níveis."

    lines=[]
    lines.append(f"ETH: {eth_chg:+.2f}% em 8h" + (f" • funding {funding*100:.3f}%/8h" if funding is not None else "") + (f" • OI ~ {oi:,.0f}" if oi is not None else ""))
    lines.append(f"BTC: {btc_chg:+.2f}% em 8h • relação ETH/BTC: {ratio_chg:+.2f}%")
    if news:
        snippet = "; ".join([n["title"] for n in news[:3]])
        lines.append(f"Notícias: {snippet}")
    lines.append(f"SÍNTESE: {stance}")
    return "\n".join(lines)

async def latest_pulse_text() -> str:
    async with pool.acquire() as c:
        m = await c.fetchrow("SELECT * FROM market_rel ORDER BY ts DESC LIMIT 1")
        e = await c.fetchrow("SELECT * FROM candles_minute WHERE symbol='ETHUSDT' ORDER BY ts DESC LIMIT 1")
        b = await c.fetchrow("SELECT * FROM candles_minute WHERE symbol='BTCUSDT' ORDER BY ts DESC LIMIT 1")
        d = await c.fetchrow("SELECT * FROM derivatives_snap WHERE symbol='ETHUSDT' AND exchange='bybit' ORDER BY ts DESC LIMIT 1")
    if not (m and e and b):
        return "⏳ Sem dados suficientes para pulse ainda."

    now = dt.datetime.now(TZ).strftime("%Y-%m-%d %H:%M")
    eth = float(m["eth_usd"]); btc = float(m["btc_usd"]); ratio = float(m["eth_btc_ratio"] or 0)

    parts = [
        f"🕒 {now} • v{BOOT_VERSION}",
        f"ETH ${eth:,.2f}" + (f"  (H:{e['high']:,.2f}/L:{e['low']:,.2f})" if isinstance(e["high"], (int,float)) and isinstance(e["low"], (int,float)) else ""),
        f"BTC ${btc:,.2f}" + (f"  (H:{b['high']:,.2f}/L:{b['low']:,.2f})" if isinstance(b["high"], (int,float)) and isinstance(b["low"], (int,float)) else ""),
        f"ETH/BTC {ratio:.5f}",
    ]
    if d and d["funding"] is not None: parts.append(f"Funding(ETH) {float(d['funding'])*100:.3f}%/8h")
    if d and d["open_interest"] is not None: parts.append(f"Open Interest(ETH) {float(d['open_interest']):,.0f}")

    # Gatilhos
    if eth < ETH_HEDGE_2: parts.append(f"🚨 ETH<{ETH_HEDGE_2:.0f}: ampliar hedge p/ 20% (29 ETH).")
    elif eth < ETH_HEDGE_1: parts.append(f"⚠️ ETH<{ETH_HEDGE_1:.0f}: ativar hedge 15% (22 ETH).")
    elif eth > ETH_CLOSE: parts.append(f"↩️ ETH>{ETH_CLOSE:.0f}: avaliar fechar hedge.")
    else: parts.append("✅ Sem gatilho imediato.")

    # Níveis sucintos
    parts.append("NÍVEIS ETH: " + _levels_eth(eth))
    parts.append("NÍVEIS BTC: " + _levels_btc(btc))

    comment = await build_commentary_8h()
    return "\n".join(parts) + "\n\n" + comment

async def coin_comment(side: str) -> str:
    # side in {"ETH", "BTC"}
    end = dt.datetime.now(dt.UTC); start = end - dt.timedelta(hours=8)
    async with pool.acquire() as c:
        rows = await c.fetch("SELECT ts, eth_usd, btc_usd, eth_btc_ratio FROM market_rel WHERE ts BETWEEN $1 AND $2 ORDER BY ts", start, end)
        d = await c.fetch("SELECT ts, funding, open_interest FROM derivatives_snap WHERE ts BETWEEN $1 AND $2 ORDER BY ts", start, end)
        news  = await c.fetch("SELECT ts, title FROM news_items WHERE ts BETWEEN $1 AND $2 ORDER BY ts DESC LIMIT 6", start, end)
    if not rows: return "⏳ Aguardando histórico…"

    eth = [float(r["eth_usd"]) for r in rows]; btc = [float(r["btc_usd"]) for r in rows]; ratio = [float(r["eth_btc_ratio"]) for r in rows if r["eth_btc_ratio"] is not None]
    funding = float(d[-1]["funding"]) if (d and d[-1]["funding"] is not None) else None
    oi      = float(d[-1]["open_interest"]) if (d and d[-1]["open_interest"] is not None) else None

    if side == "ETH":
        ch = pct(eth[0], eth[-1]); lv = _levels_eth(eth[-1])
        bias = "tendência de alta" if ch > 0.5 else ("pressão vendedora" if ch < -0.5 else "lateral")
        txt = [
            f"ETH • {ch:+.2f}% em 8h • " + (f"funding {funding*100:.3f}%/8h • " if funding is not None else "") + (f"OI ~ {oi:,.0f}" if oi is not None else ""),
            f"Leitura: {bias}; relação ETH/BTC: {(pct(ratio[0], ratio[-1]) if ratio else 0):+.2f}%.",
            f"Níveis: {lv}",
            "Ação: usar níveis/hedge conforme gatilhos; revisar margem se violar suporte."
        ]
        return "\n".join(txt)
    else:
        ch = pct(btc[0], btc[-1]); lv = _levels_btc(btc[-1])
        bias = "fluxo comprador" if ch > 0.5 else ("fluxo vendedor" if ch < -0.5 else "neutro")
        tx2 = [
            f"BTC • {ch:+.2f}% em 8h.",
            f"Leitura: {bias}; impacto via ETH/BTC {(pct(ratio[0], ratio[-1]) if ratio else 0):+.2f}% (ETH {'forte' if (ratio and ratio[-1]>ratio[0]) else 'fraco'}).",
            f"Níveis: {lv}",
            "Ação: observar defesas nos suportes; gatilhos em quebras/fechamentos acima das resistências."
        ]
        return "\n".join(tx2)

# -------------------- FastAPI endpoints --------------------
@app.on_event("startup")
async def _startup():
    print(f"[BOOT] v{BOOT_VERSION} starting…")
    await db_init()
    # Schedulers
    scheduler.add_job(ingest_1m, "interval", minutes=1, id="ingest_1m", replace_existing=True)
    if ENABLE_WHALES:
        scheduler.add_job(ingest_whales, "interval", seconds=30, id="ingest_whales", replace_existing=True)
    if ENABLE_ACCOUNTS:
        scheduler.add_job(ingest_accounts, "interval", minutes=5, id="ingest_accounts", replace_existing=True)
    if ENABLE_ONCHAIN:
        scheduler.add_job(ingest_onchain_eth, "interval", minutes=5, args=[AAVE_ADDR], id="ingest_onchain", replace_existing=True)
    if ENABLE_NEWS:
        scheduler.add_job(ingest_news, "interval", minutes=15, id="news", replace_existing=True)
    # Boletins automáticos (08h/14h/20h)
    scheduler.add_job(lambda: send_tg_sync(latest_pulse_text), "cron", hour=8,  minute=0, id="bulletin_08", replace_existing=True)
    scheduler.add_job(lambda: send_tg_sync(latest_pulse_text), "cron", hour=14, minute=0, id="bulletin_14", replace_existing=True)
    scheduler.add_job(lambda: send_tg_sync(latest_pulse_text), "cron", hour=20, minute=0, id="bulletin_20", replace_existing=True)
    scheduler.start()
    print("[BOOT] schedulers started")

def send_tg_sync(coro_factory):
    # wrapper para scheduler aceitar função síncrona
    asyncio.create_task(_send_pulse_task(coro_factory))

async def _send_pulse_task(coro_factory):
    try:
        text = await coro_factory()
        await send_tg(text)
    except Exception as e:
        print(f"[WARN] auto pulse send: {e}")

@app.get("/healthz")
async def healthz():
    return {"ok": True, "time": dt.datetime.now(TZ).isoformat(), "version": BOOT_VERSION}

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
        "version": BOOT_VERSION,
        "counts":{
            "market_rel": mr, "candles_eth": cm_eth, "candles_btc": cm_btc,
            "derivatives": ds, "whale_events": whales, "account_snap": acc, "news": news
        },
        "last_run": last_run
    }

@app.get("/pulse")
async def pulse():
    text = await latest_pulse_text()
    await send_tg(text)
    return {"ok": True, "message": text}

@app.post("/run/accounts")
async def run_accounts():
    await ingest_accounts()
    return {"ok": True, "ran_at": last_run["ingest_accounts"]}

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

# -------------------- Telegram Webhook ---------------------
async def _process_update(request: Request):
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
        await send_tg(f"✅ Bot online (v{BOOT_VERSION}). Comandos: /pulse, /eth, /btc, /alpha, /note <texto>, /strat new <nome>|<versão>|<nota>, /strat last, /notes", chat_id); 
        return {"ok": True}

    if low == "/pulse":
        await send_tg(await latest_pulse_text(), chat_id); return {"ok": True}

    if low in ("/eth","/ethereum","/ethusd"):
        await send_tg(await coin_comment("ETH"), chat_id); return {"ok": True}

    if low in ("/btc","/bitcoin","/btcusd","/vtc"):  # inclui seu alias pedido
        await send_tg(await coin_comment("BTC"), chat_id); return {"ok": True}

    if low in ("/alpha","/alfa"):
        txt = await build_commentary_8h()
        await send_tg(txt, chat_id); 
        return {"ok": True}

    if low == "/notes":
        async with pool.acquire() as c:
            rows = await c.fetch("SELECT created_at,text FROM notes ORDER BY id DESC LIMIT 5")
        if not rows: 
            await send_tg("Sem notas ainda.", chat_id); 
            return {"ok": True}
        out=["Notas recentes:"]+[f"- {r['created_at']:%m-%d %H:%M} • {r['text']}" for r in rows]
        await send_tg("\n".join(out), chat_id); return {"ok": True}

    if low == "/strat last":
        async with pool.acquire() as c:
            row = await c.fetchrow("SELECT created_at,name,version,note FROM strategy_versions ORDER BY id DESC LIMIT 1")
        if not row: 
            await send_tg("Nenhuma estratégia salva.", chat_id); 
            return {"ok": True}
        await send_tg(f"Última estratégia:\n{row['created_at']:%Y-%m-%d %H:%M}\n{row['name']} v{row['version']}\n{row['note'] or ''}", chat_id); 
        return {"ok": True}

    if low.startswith("/note"):
        note = text[len("/note"):].strip()
        if not note:
            await send_tg("Uso: /note seu texto aqui", chat_id); 
            return {"ok": True}
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

    await send_tg("Comando não reconhecido. Use /pulse, /eth, /btc, /alpha, /note, /strat new, /strat last, /notes.", chat_id)
    return {"ok": True}

@app.post("/webhook")
async def webhook_root(request: Request):
    return await _process_update(request)

@app.post("/webhook/{token}")
async def webhook_token(token: str, request: Request):
    # aceita webhook com token na URL (mais robusto pra Telegram)
    return await _process_update(request)

# -----------------------------------------------------------
# Fim
# -----------------------------------------------------------

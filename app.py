# app.py — Stark DeFi Agent v5.7.1
# - Anti-spam de baleias: cooldown + Coinbase só alerta se muito grande
# - Coleta robusta de preços (Coinbase fallback) e derivativos (Bybit)
# - Ingest de notícias (RSS), classificação de temas e comentário inteligente no /pulse
# - Mesma API da v5.x com melhorias
# ---------------------------------------------------------------

import os, hmac, hashlib, time, math, csv, io, asyncio, traceback, re
import datetime as dt
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, List, Tuple

import httpx
import asyncpg
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse

# --------- Config base
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

# Anti-ruído e controle de whales
WHALE_COOLDOWN_SEC = int(os.getenv("WHALE_COOLDOWN_SEC", "600"))          # 10 min
COINBASE_MIN_ALERT_USD = float(os.getenv("COINBASE_MIN_ALERT_USD", "2000000"))
WHALE_SOURCE = os.getenv("WHALE_SOURCE", "auto").lower()                  # auto|bybit|coinbase
WHALE_ALERTS_ENABLED = os.getenv("WHALE_ALERTS_ENABLED", "on").lower() in ("on","1","true")

# Notícias
NEWS_SOURCES = [s.strip() for s in os.getenv("NEWS_SOURCES", "coindesk,cointelegraph").split(",") if s.strip()]
NEWS_WINDOW_H = int(os.getenv("NEWS_WINDOW_H", "12"))
NEWS_MAX_ITEMS = int(os.getenv("NEWS_MAX_ITEMS", "20"))

# --------- Constantes de API
BYBIT_PUBLIC = "https://api.bybit.com"
BYBIT_PUBLIC_ALT = "https://api.bybitglobal.com"
BYBIT_SPOT = BYBIT_PUBLIC + "/v5/market/tickers?category=spot&symbol={sym}"
BYBIT_FUND = BYBIT_PUBLIC + "/v5/market/funding/history?category=linear&symbol=ETHUSDT&limit=1"
BYBIT_OI   = BYBIT_PUBLIC + "/v5/market/open-interest?category=linear&symbol=ETHUSDT&interval=5min"
BYBIT_RECENT_TRADES = BYBIT_PUBLIC + "/v5/market/recent-trade?category=linear&symbol=ETHUSDT&limit=1000"

COINBASE_TICKER = "https://api.exchange.coinbase.com/products/{prod}/ticker"
COINBASE_TRADES = "https://api.exchange.coinbase.com/products/{prod}/trades"
BINANCE_24H     = "https://api.binance.com/api/v3/ticker/24hr?symbol={sym}"
BINANCE_AGG     = "https://api.binance.com/api/v3/aggTrades?symbol={sym}&limit=1000"

TG_SEND = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"

ETHERSCAN_TX = "https://api.etherscan.io/api?module=account&action=txlist&address={addr}&startblock=0&endblock=99999999&sort=desc&apikey={key}"

# RSS
RSS = {
    "coindesk": "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "cointelegraph": "https://cointelegraph.com/rss",
}

# --------- App & DB
app = FastAPI(title="stark-defi-agent")
pool: Optional[asyncpg.Pool] = None

from apscheduler.schedulers.asyncio import AsyncIOScheduler
scheduler = AsyncIOScheduler(timezone=str(TZ))

def now_local_iso():
    return dt.datetime.now(TZ).isoformat(timespec="seconds")

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
CREATE TABLE IF NOT EXISTS news_items(
  id bigserial PRIMARY KEY,
  ts_ingested timestamptz NOT NULL DEFAULT now(),
  ts_pub timestamptz,
  source text NOT NULL,
  title text NOT NULL,
  url text NOT NULL,
  themes text
);
"""

async def db_init():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL não definido.")
    global pool
    pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=5)
    async with pool.acquire() as c:
        await c.execute(CREATE_SQL)

# --------- HTTP helpers
_DEFAULT_HEADERS = {
    "User-Agent": "stark-defi-agent/5.7.1 (+https://starkradar-bot.onrender.com)"
}

async def fetch_json(url: str, headers: Dict[str,str]|None=None, params: Dict[str,str]|None=None):
    async with httpx.AsyncClient(timeout=20, headers={**_DEFAULT_HEADERS, **(headers or {})}) as s:
        r = await s.get(url, headers=headers, params=params)
        r.raise_for_status()
        return r.json()

async def fetch_json_retry(url: str, params: Dict[str,str]|None=None, tries: int = 3, backoff: float = 0.8):
    last_exc = None
    for i in range(tries):
        try:
            async with httpx.AsyncClient(timeout=20, headers=_DEFAULT_HEADERS) as s:
                r = await s.get(url, params=params)
                r.raise_for_status()
                return r.json()
        except Exception as e:
            last_exc = e
            await asyncio.sleep(backoff * (i+1))
    raise last_exc

async def fetch_text(url: str):
    async with httpx.AsyncClient(timeout=20, headers=_DEFAULT_HEADERS) as s:
        r = await s.get(url)
        r.raise_for_status()
        return r.text

# --------- Telegram
async def send_tg(text: str, chat_id: Optional[str] = None):
    if not SEND_ENABLED: return
    cid = chat_id or TG_CHAT
    if not cid: return
    async with httpx.AsyncClient(timeout=12) as s:
        await s.post(TG_SEND, json={"chat_id": cid, "text": text})

def action_line(eth_price: float) -> str:
    if eth_price < ETH_HEDGE_2:
        return f"🚨 ETH < {ETH_HEDGE_2:.0f} → ampliar hedge p/ 20% (29 ETH)."
    if eth_price < ETH_HEDGE_1:
        return f"⚠️ ETH < {ETH_HEDGE_1:.0f} → ativar hedge 15% (22 ETH)."
    if eth_price > ETH_CLOSE:
        return f"↩️ ETH > {ETH_CLOSE:.0f} → avaliar fechar hedge."
    return "✅ Sem gatilho. Suportes: 4.200/4.000 | Resist: 4.300/4.400."

# --------- Market snapshots (1m)
async def get_spot_prices() -> dict:
    # Prioriza Coinbase (confiável e sem 403 regionais). Tenta Bybit/Binance como extra.
    try:
        e = await fetch_json_retry(COINBASE_TICKER.format(prod="ETH-USD"))
        b = await fetch_json_retry(COINBASE_TICKER.format(prod="BTC-USD"))
        eth_p = float(e["price"]); btc_p = float(b["price"])
        # Try highs/lows via Binance 24h (pode dar 451; ignora se falhar)
        try:
            e24 = await fetch_json_retry(BINANCE_24H.format(sym="ETHUSDT"))
            b24 = await fetch_json_retry(BINANCE_24H.format(sym="BTCUSDT"))
            eth_h = float(e24.get("highPrice", "nan")); eth_l = float(e24.get("lowPrice", "nan"))
            btc_h = float(b24.get("highPrice", "nan")); btc_l = float(b24.get("lowPrice", "nan"))
        except Exception:
            eth_h = eth_l = btc_h = btc_l = math.nan
        return {
            "eth": {"price": eth_p, "high": eth_h, "low": eth_l},
            "btc": {"price": btc_p, "high": btc_h, "low": btc_l},
        }
    except Exception:
        # Fallback final: Coingecko (pode 429)
        cg = await fetch_json_retry("https://api.coingecko.com/api/v3/simple/price?ids=ethereum,bitcoin&vs_currencies=usd", tries=2)
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
        spot = await get_spot_prices()
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
        last_run["ingest_1m"] = now_local_iso()

# --------- Whales (30s) com anti-spam
_last_trade_time_ms = 0
_ws_lock = asyncio.Lock()
_last_whale_tg_ts: float = 0.0
_last_whale_sig: str = ""

async def whales_bybit() -> List[Tuple[str,float,float,str]]:
    global _last_trade_time_ms
    try:
        data = (await fetch_json_retry(BYBIT_RECENT_TRADES))["result"]["list"]
    except Exception:
        # tenta domínio alternativo
        alt = BYBIT_PUBLIC_ALT + "/v5/market/recent-trade?category=linear&symbol=ETHUSDT&limit=1000"
        data = (await fetch_json_retry(alt))["result"]["list"]
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
    if buy_usd>=WHALE_USD_MIN: events.append(("BUY", buy_qty,  buy_usd,  f"bybit agg {len(new)}"))
    if sell_usd>=WHALE_USD_MIN: events.append(("SELL", sell_qty, sell_usd, f"bybit agg {len(new)}"))
    return events

async def whales_coinbase() -> Tuple[List[Tuple[str,float,float,str]], Optional[Tuple[float,float,float,float,int]]]:
    try:
        lst = await fetch_json_retry(COINBASE_TRADES.format(prod="ETH-USD"))
        buy_usd = sell_usd = 0.0
        buy_qty = sell_qty = 0.0
        for t in lst:
            price = float(t["price"]); qty = float(t["size"]); side = (t["side"] or "").upper()
            usd = price * qty
            if side == "BUY":  buy_usd += usd; buy_qty += qty
            else:              sell_usd += usd; sell_qty += qty
        events=[]
        if buy_usd>=COINBASE_MIN_ALERT_USD: events.append(("BUY", buy_qty,  buy_usd,  f"coinbase agg {len(lst)}"))
        if sell_usd>=COINBASE_MIN_ALERT_USD: events.append(("SELL", sell_qty, sell_usd, f"coinbase agg {len(lst)}"))
        return events, (buy_usd, sell_usd, buy_qty, sell_qty, len(lst))
    except Exception:
        return [], None

async def ingest_whales():
    global _last_whale_tg_ts, _last_whale_sig
    try:
        events: List[Tuple[str,float,float,str]] = []
        raw_cb = None

        src = WHALE_SOURCE
        if src == "bybit":
            events = await whales_bybit()
        elif src == "coinbase":
            events, raw_cb = await whales_coinbase()
        else:  # auto
            try:
                events = await whales_bybit()
            except Exception:
                events, raw_cb = await whales_coinbase()

        ts = dt.datetime.now(dt.UTC)
        async with pool.acquire() as c:
            if raw_cb:
                buy_usd, sell_usd, buy_qty, sell_qty, n = raw_cb
                if buy_usd >= WHALE_USD_MIN:
                    await c.execute(
                        "INSERT INTO whale_events(ts,venue,side,qty,usd_value,note) VALUES($1,$2,$3,$4,$5,$6)",
                        ts, "coinbase", "BUY", buy_qty, buy_usd, f"agg {n}"
                    )
                if sell_usd >= WHALE_USD_MIN:
                    await c.execute(
                        "INSERT INTO whale_events(ts,venue,side,qty,usd_value,note) VALUES($1,$2,$3,$4,$5,$6)",
                        ts, "coinbase", "SELL", sell_qty, sell_usd, f"agg {n}"
                    )
            for side, qty, usd, note in events:
                venue = "bybit" if "bybit" in (note or "") else "coinbase"
                await c.execute(
                    "INSERT INTO whale_events(ts,venue,side,qty,usd_value,note) VALUES($1,$2,$3,$4,$5,$6)",
                    ts, venue, side, qty, usd, note
                )

        # Anti-spam: cooldown + dedupe
        if WHALE_ALERTS_ENABLED and events:
            now = time.time()
            if now - _last_whale_tg_ts >= WHALE_COOLDOWN_SEC:
                top = max(events, key=lambda x: x[2])
                sig = f"{top[0]}:{int(top[2]//100000)}:{top[3].split()[0]}"
                if sig != _last_whale_sig:
                    for side, qty, usd, note in events:
                        await send_tg(f"🐋 {side} ~ ${usd:,.0f} | qty ~ {qty:,.1f} | {note}")
                    _last_whale_sig = sig
                    _last_whale_tg_ts = now
    finally:
        last_run["ingest_whales"] = now_local_iso()

# --------- Accounts (5m)
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
    async with httpx.AsyncClient(timeout=20, headers=_DEFAULT_HEADERS) as s:
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

AAVE_SUBGRAPH_URL = "https://api.thegraph.com/subgraphs/name/aave/protocol-v3"
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
        async with httpx.AsyncClient(timeout=25, headers=_DEFAULT_HEADERS) as s:
            r=await s.post(AAVE_SUBGRAPH_URL,json={"query":AAVE_QUERY,"variables":{"user":AAVE_ADDR.lower()}})
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
    if not rows:
        last_run["ingest_accounts"] = now_local_iso()
        return
    async with pool.acquire() as c:
        for r in rows:
            await c.execute("INSERT INTO account_snap(venue,metric,value) VALUES($1,$2,$3)", r["venue"], r["metric"], r["value"])
    last_run["ingest_accounts"] = now_local_iso()

# --------- On-chain (opcional)
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

# --------- Notícias (RSS) + análise
_THEMES = [
    (r"\betf\b|\bspot etf\b", "ETF"),
    (r"liquidation|short squeeze|longs|funding", "Derivativos"),
    (r"hack|exploit|vulnerability|rug", "Segurança"),
    (r"sec\b|regulat|policy|ban|approval", "Regulatório"),
    (r"halving|issuance|supply|burn", "Oferta"),
    (r"layer-2|L2|rollup|optimism|arbitrum|base\b", "L2"),
    (r"whale|flow|inflow|outflow", "Fluxo"),
    (r"defi|aave|compound|uniswap|lending|staking", "DeFi"),
    (r"bitcoin|btc", "BTC"),
    (r"ethereum|eth", "ETH"),
]

def extract_rss_items(xml: str, source: str) -> List[Dict[str,Any]]:
    # parse simples via regex (suficiente para títulos/links/datas)
    items=[]
    for m in re.finditer(r"<item>(.*?)</item>", xml, flags=re.S|re.I):
        block = m.group(1)
        title = re.search(r"<title>(<!\[CDATA\[)?(.*?)(\]\]>)?</title>", block, flags=re.S|re.I)
        link  = re.search(r"<link>(.*?)</link>", block, flags=re.S|re.I)
        pub   = re.search(r"<pubDate>(.*?)</pubDate>", block, flags=re.S|re.I)
        t = (title.group(2) if title and title.group(2) else (title.group(1) if title else "")).strip()
        u = (link.group(1) if link else "").strip()
        p = (pub.group(1) if pub else "").strip()
        items.append({"title": t, "url": u, "pub": p, "source": source})
    return items

def classify_themes(title: str) -> List[str]:
    t = title.lower()
    themes = []
    for pat, lab in _THEMES:
        if re.search(pat, t):
            themes.append(lab)
    return list(dict.fromkeys(themes))[:4]  # únicas, até 4

def parse_rfc2822(s: str) -> Optional[dt.datetime]:
    try:
        from email.utils import parsedate_to_datetime
        d = parsedate_to_datetime(s)
        if d.tzinfo is None: d = d.replace(tzinfo=dt.timezone.utc)
        return d.astimezone(dt.timezone.utc)
    except Exception:
        return None

async def ingest_news():
    window = dt.datetime.now(dt.UTC) - dt.timedelta(hours=NEWS_WINDOW_H)
    all_items=[]
    for src in NEWS_SOURCES:
        url = RSS.get(src)
        if not url: continue
        try:
            xml = await fetch_text(url)
            items = extract_rss_items(xml, src)
            for it in items:
                ts_pub = parse_rfc2822(it["pub"])
                if ts_pub and ts_pub < window: continue
                it["ts_pub"] = ts_pub
                it["themes"] = ", ".join(classify_themes(it["title"]))
                all_items.append(it)
        except Exception:
            continue
    # Ordena por ts_pub desc, limita
    all_items.sort(key=lambda x: x.get("ts_pub") or dt.datetime.now(dt.UTC), reverse=True)
    all_items = all_items[:NEWS_MAX_ITEMS]
    if not all_items:
        last_run["news"] = now_local_iso(); return
    async with pool.acquire() as c:
        for it in all_items:
            await c.execute(
                "INSERT INTO news_items(ts_pub, source, title, url, themes) VALUES($1,$2,$3,$4,$5)",
                it.get("ts_pub"), it["source"], it["title"], it["url"], it["themes"]
            )
    last_run["news"] = now_local_iso()

async def news_summary(hours: int = 8) -> Tuple[str, List[str]]:
    start = dt.datetime.now(dt.UTC) - dt.timedelta(hours=hours)
    async with pool.acquire() as c:
        rows = await c.fetch("""
            SELECT ts_pub, source, title, url, themes
            FROM news_items
            WHERE (ts_pub IS NULL OR ts_pub >= $1)
            ORDER BY ts_pub DESC NULLS LAST
            LIMIT 40
        """, start)
    if not rows:
        return "Fluxo de notícias sem um driver único dominante.", []
    # score por tema
    score={}
    links=[]
    for r in rows:
        th = (r["themes"] or "").split(",")
        for t in th:
            t=t.strip()
            if not t: continue
            score[t]=score.get(t,0)+1
        if len(links)<6:
            links.append(f"• [{r['source']}] {r['title']}")
    # escolhe 2–3 temas principais
    ranked = sorted(score.items(), key=lambda x:x[1], reverse=True)
    tops = [t for t,_ in ranked[:3]]
    if not tops:
        text = "Fluxo de notícias misto; sem tema dominante."
    else:
        text = "Temas de notícia: " + ", ".join(tops) + "."
    return text, links[:6]

# --------- Comentário 6–8h
def pct(a,b): 
    try:
        return 0.0 if a==0 else (b-a)/a*100.0
    except Exception:
        return 0.0

async def build_commentary() -> str:
    end = dt.datetime.now(dt.UTC); start = end - dt.timedelta(hours=8)
    async with pool.acquire() as c:
        rows = await c.fetch("SELECT ts, eth_usd, btc_usd, eth_btc_ratio FROM market_rel WHERE ts BETWEEN $1 AND $2 ORDER BY ts", start, end)
        deriv = await c.fetch("SELECT ts, funding, open_interest FROM derivatives_snap WHERE ts BETWEEN $1 AND $2 ORDER BY ts", start, end)
        whales = await c.fetch("SELECT ts, side, usd_value, venue FROM whale_events WHERE ts BETWEEN $1 AND $2 ORDER BY ts", start, end)
    if not rows:
        return "⏳ Aguardando histórico para comentário (volte em alguns minutos)."

    eth = [float(r["eth_usd"]) for r in rows]; btc = [float(r["btc_usd"]) for r in rows]; ratio = [float(r["eth_btc_ratio"]) for r in rows]
    eth_chg = pct(eth[0], eth[-1]); btc_chg = pct(btc[0], btc[-1]); ratio_chg = pct(ratio[0], ratio[-1])
    funding = float(deriv[-1]["funding"]) if deriv and deriv[-1]["funding"] is not None else None
    oi = float(deriv[-1]["open_interest"]) if deriv and deriv[-1]["open_interest"] is not None else None
    buy = sum(float(w["usd_value"] or 0) for w in whales if w["side"]=="BUY")
    sell= sum(float(w["usd_value"] or 0) for w in whales if w["side"]=="SELL")
    flow = "neutra"
    if buy>sell*1.3: flow="compradora"
    elif sell>buy*1.3: flow="vendedora"

    news_text, news_bullets = await news_summary(8)

    lines=[]
    lines.append("🧾 Comentário (últimas 8h)")
    lines.append(f"• ETH: {eth_chg:+.2f}% | BTC: {btc_chg:+.2f}% | ETH/BTC: {ratio_chg:+.2f}%")
    if funding is not None or oi is not None:
        fr = (f" | funding {funding*100:.3f}%/8h" if funding is not None else "")
        orr = (f" | OI ~ {oi:,.0f}" if oi is not None else "")
        lines.append(f"• Derivativos:{fr}{orr}")
    lines.append(f"• Fluxo de baleias: {flow} (BUY ${buy:,.0f} vs SELL ${sell:,.0f})")
    lines.append("• " + news_text)
    if news_bullets:
        lines.append("• Pautas:")
        for b in news_bullets[:4]:
            lines.append("  " + b)
    # Síntese
    if ratio_chg>0 and (funding is None or funding<0.0005):
        synth = "pró-ETH (força relativa + funding contido)."
    elif ratio_chg<0 and (funding is not None and funding>0.001):
        synth = "BTC dominante / atenção à euforia (funding alto)."
    else:
        synth = "equilíbrio tático; usar gatilhos de preço."
    lines.append(f"🧭 Síntese: {synth}")
    return "\n".join(lines)

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
    parts=[
        f"🕒 {now}",
        f"ETH: ${eth:,.2f}" + (f" | H:{eh:,.2f} / L:{el:,.2f}" if isinstance(eh,(int,float)) and isinstance(el,(int,float)) else ""),
        f"BTC: ${btc:,.2f}" + (f" | H:{bh:,.2f} / L:{bl:,.2f}" if isinstance(bh,(int,float)) and isinstance(bl,(int,float)) else ""),
        f"ETH/BTC: {ratio:.5f}",
        f"↪ {action_line(eth)}",
        "",
        await build_commentary()
    ]
    return "\n".join(parts)

async def send_pulse_to_chat():
    await send_tg(await latest_pulse_text())

# --------- FastAPI
@app.on_event("startup")
async def _startup():
    await db_init()
    # jobs
    scheduler.add_job(ingest_1m, "interval", minutes=1, id="ingest_1m", replace_existing=True)
    scheduler.add_job(ingest_whales, "interval", seconds=30, id="ingest_whales", replace_existing=True)
    scheduler.add_job(ingest_accounts, "interval", minutes=5, id="ingest_accounts", replace_existing=True)
    scheduler.add_job(ingest_news, "interval", minutes=15, id="ingest_news", replace_existing=True)
    if AAVE_ADDR and ETHERSCAN_API_KEY:
        scheduler.add_job(ingest_onchain_eth, "interval", minutes=5, args=[AAVE_ADDR], id="ingest_onchain", replace_existing=True)
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
        news = await c.fetchval("SELECT COUNT(1) FROM news_items")
    return {
        "counts":{
            "market_rel": mr, "candles_eth": cm_eth, "candles_btc": cm_btc,
            "derivatives": ds, "whale_events": whales, "account_snap": acc, "news": news
        },
        "last_run": last_run
    }

@app.post("/run/accounts")
async def run_accounts():
    await ingest_accounts()
    return {"ok": True, "ran_at": last_run["ingest_accounts"]}

@app.post("/run/news")
async def run_news():
    await ingest_news()
    return {"ok": True, "ran_at": last_run["news"]}

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
        await send_tg("✅ Bot online. Comandos: /pulse, /chart eth|btc, /eth, /btc, /alfa, /diag, /note <texto>, /strat new <nome> | <versão> | <nota>, /strat last, /notes", chat_id); 
        return {"ok": True}

    if low == "/pulse":
        await send_tg(await latest_pulse_text(), chat_id); return {"ok": True}

    if low in ("/eth","eth"):
        async with pool.acquire() as c:
            m = await c.fetchrow("SELECT eth_usd, eth_btc_ratio FROM market_rel ORDER BY ts DESC LIMIT 1")
        if m:
            await send_tg(f"ETH: ${float(m['eth_usd']):,.2f} | ETH/BTC: {float(m['eth_btc_ratio']):.5f}\n↪ {action_line(float(m['eth_usd']))}", chat_id)
        return {"ok": True}

    if low in ("/btc","btc"):
        async with pool.acquire() as c:
            m = await c.fetchrow("SELECT btc_usd, eth_btc_ratio FROM market_rel ORDER BY ts DESC LIMIT 1")
        if m:
            await send_tg(f"BTC: ${float(m['btc_usd']):,.2f} | ETH/BTC: {float(m['eth_btc_ratio']):.5f}\n↪ {action_line(float(m['btc_usd']) * float(m['eth_btc_ratio']))}", chat_id)
        return {"ok": True}

    if low in ("/alfa","/alpha"):
        await send_tg(await build_commentary(), chat_id); return {"ok": True}

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

    await send_tg("Comando não reconhecido. Use /pulse, /eth, /btc, /alfa, /note, /strat new, /strat last, /notes.", chat_id)
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

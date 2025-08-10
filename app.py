# app.py — Stark DeFi Agent v6.0 (2025-08-10)
# Changelog v6.0
# - Pulse/Alpha com leitura de cenário (8h): preço, high/low, funding, OI, fluxo de baleias, notícias resumidas.
# - Retentativas com backoff exponencial (HTTP 429/5xx) e timeouts afinados.
# - Anti-spam de baleias (agregação + cooldown) e envio só acima de limiar USD.
# - Alertas reativos (ETH/BTC): dispara se variação de 1h passar limites.
# - /status mostra contagens + last_run; /run/accounts dispara coleta manual.
# - Webhook robusto: /webhook/{token}; respostas rápidas para /start, /pulse, /eth, /btc, /alpha, /note, /strat.
# - Código à prova de falhas: uma fonte fora do ar não para o bot.

import os, io, csv, math, hmac, time, asyncio, hashlib, traceback
import datetime as dt
from typing import Optional, Dict, Any, List, Tuple
from zoneinfo import ZoneInfo

import httpx
import asyncpg
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse

# ========= ENV =========
TZ = ZoneInfo(os.getenv("TZ", "America/Sao_Paulo"))
DB_URL = os.getenv("DATABASE_URL")

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "")  # string (pode ser negativo se canal)
SEND_ENABLED = bool(TG_TOKEN)

# BYBIT (read-only keys p/ contas/posições)
BYBIT_KEY = os.getenv("BYBIT_RO_KEY", "")
BYBIT_SEC = os.getenv("BYBIT_RO_SECRET", "")
BYBIT_ACCOUNT_TYPE = os.getenv("BYBIT_ACCOUNT_TYPE", "UNIFIED")  # UNIFIED ou CONTRACT

# On-chain opcional (Etherscan) e Aave subgraph
AAVE_ADDR = os.getenv("AAVE_ADDR", "")
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")

# Gatilhos de preço (ETH)
ETH_HEDGE_1 = float(os.getenv("ETH_HEDGE_1", "3900"))
ETH_HEDGE_2 = float(os.getenv("ETH_HEDGE_2", "3800"))
ETH_CLOSE   = float(os.getenv("ETH_CLOSE_HEDGE", "3950"))

# “Baleias”
WHALE_USD_MIN = float(os.getenv("WHALE_USD_MIN", "700000"))  # ↑ eleva p/ reduzir ruído
WHALE_COOLDOWN_SEC = int(os.getenv("WHALE_COOLDOWN_SEC", "30"))  # antispam

# Alertas reativos (1h)
ALERT_1H_ETH_PCT = float(os.getenv("ALERT_1H_ETH_PCT", "2.0"))
ALERT_1H_BTC_PCT = float(os.getenv("ALERT_1H_BTC_PCT", "2.0"))

# Notícias (opcional – usa RSS públicas; sem chave)
NEWS_FEEDS = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cointelegraph.com/rss",
]

# ========= CONSTS/APIs =========
app = FastAPI(title="stark-defi-agent")
pool: Optional[asyncpg.Pool] = None

TG_SEND = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
TG_SET  = f"https://api.telegram.org/bot{TG_TOKEN}/setWebhook"

# Spot & derivativos — múltiplas fontes com fallback
COINBASE_TICKER = {
    "ETHUSD": "https://api.exchange.coinbase.com/products/ETH-USD/ticker",
    "BTCUSD": "https://api.exchange.coinbase.com/products/BTC-USD/ticker",
}
BINANCE_24H = "https://api.binance.com/api/v3/ticker/24hr"  # ?symbol=ETHUSDT
BYBIT_FUND = "https://api.bybit.com/v5/market/funding/history?category=linear&symbol=ETHUSDT&limit=1"
BYBIT_OI   = "https://api.bybit.com/v5/market/open-interest?category=linear&symbol=ETHUSDT&interval=5min"

# “Whales” agregadas (usaremos Binance aggTrades por abrangência — 451 em algumas regiões;
# o código faz fallback e nunca derruba o bot)
BINANCE_AGG = "https://api.binance.com/api/v3/aggTrades"  # ?symbol=ETHUSDT&limit=1000

# Aave subgraph (proxy simples)
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

# Etherscan txs (opcional)
ETHERSCAN_TX = "https://api.etherscan.io/api?module=account&action=txlist&address={addr}&startblock=0&endblock=99999999&sort=desc&apikey={key}"

# ========= STATE =========
from apscheduler.schedulers.asyncio import AsyncIOScheduler
scheduler = AsyncIOScheduler(timezone=str(TZ))

last_run = {
    "ingest_1m": None,
    "ingest_whales": None,
    "ingest_accounts": None,
    "ingest_onchain": None,
    "news": None,
    "alerts": None,
}

_last_trade_time_ms = 0
_ws_lock = asyncio.Lock()
_last_whale_send = 0  # cooldown anti-spam

# ========= DB =========
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
  ts timestamptz NOT NULL DEFAULT now(),
  source text NOT NULL,
  title text NOT NULL,
  url text NOT NULL
);
"""

async def db_init():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL não definido.")
    global pool
    pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=5)
    async with pool.acquire() as c:
        await c.execute(CREATE_SQL)

# ========= HTTP Helpers =========
def _sleep_for(attempt: int) -> float:
    # 0.5s, 1s, 2s, 4s, max 6s
    return min(0.5 * (2 ** attempt), 6.0)

async def fetch_json_retry(
    url: str,
    params: Dict[str, Any] | None = None,
    headers: Dict[str, str] | None = None,
    attempts: int = 4,
    timeout: float = 12.0,
) -> Dict[str, Any]:
    last_exc: Optional[Exception] = None
    async with httpx.AsyncClient(timeout=timeout) as s:
        for i in range(attempts):
            try:
                r = await s.get(url, params=params, headers=headers)
                if r.status_code in (429, 500, 502, 503, 504):
                    raise httpx.HTTPStatusError("retryable", request=r.request, response=r)
                r.raise_for_status()
                return r.json()
            except Exception as e:
                last_exc = e
                await asyncio.sleep(_sleep_for(i))
        if last_exc:
            raise last_exc
        return {}

async def send_tg(text: str, chat_id: Optional[str] = None, disable_web_page_preview: bool = True):
    if not SEND_ENABLED: 
        return
    cid = chat_id or TG_CHAT
    if not cid:
        return
    payload = {"chat_id": cid, "text": text, "disable_web_page_preview": disable_web_page_preview}
    try:
        async with httpx.AsyncClient(timeout=10) as s:
            await s.post(TG_SEND, json=payload)
    except Exception:
        pass

# ========= Market snapshots =========
async def _coinbase_ticker(symbol: str) -> Optional[Tuple[float,float,float]]:
    try:
        url = COINBASE_TICKER[symbol]
        data = await fetch_json_retry(url, attempts=3, timeout=8.0)
        price = float(data.get("price"))
        # Sem high/low intraday; deixamos NaN
        return price, math.nan, math.nan
    except Exception:
        return None

async def _binance_24h(symbol: str) -> Optional[Tuple[float,float,float]]:
    try:
        data = await fetch_json_retry(BINANCE_24H, params={"symbol": symbol}, attempts=3, timeout=8.0)
        lastp = float(data["lastPrice"])
        high = float(data["highPrice"]); low = float(data["lowPrice"])
        return lastp, high, low
    except Exception:
        return None

async def get_spot_pair() -> dict:
    # ETHUSD / BTCUSD com múltiplos fallbacks
    eth = await _binance_24h("ETHUSDT") or await _coinbase_ticker("ETHUSD")
    btc = await _binance_24h("BTCUSDT") or await _coinbase_ticker("BTCUSD")
    out = {"eth": {"price": None, "high": math.nan, "low": math.nan},
           "btc": {"price": None, "high": math.nan, "low": math.nan}}
    if eth: out["eth"] = {"price": eth[0], "high": eth[1], "low": eth[2]}
    if btc: out["btc"] = {"price": btc[0], "high": btc[1], "low": btc[2]}
    return out

async def get_derivatives_snapshot() -> dict:
    funding = None; open_interest = None
    # funding / OI pela Bybit (403 em algumas regiões; tentar e falhar silencioso)
    try:
        f = (await fetch_json_retry(BYBIT_FUND, attempts=2, timeout=6.0))["result"]["list"][0]
        funding = float(f["fundingRate"])
    except Exception:
        pass
    try:
        oi = (await fetch_json_retry(BYBIT_OI, attempts=2, timeout=6.0))["result"]["list"][-1]
        open_interest = float(oi["openInterest"])
    except Exception:
        pass
    return {"funding": funding, "oi": open_interest}

async def ingest_1m():
    try:
        now = dt.datetime.now(dt.UTC).replace(second=0, microsecond=0)
        spot = await get_spot_pair()
        if not spot["eth"]["price"] or not spot["btc"]["price"]:
            return
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

# ========= Whales (agregado + cooldown) =========
def _now_ts() -> float:
    return time.time()

async def ingest_whales():
    global _last_trade_time_ms, _last_whale_send
    try:
        # Binance aggTrades (pode retornar 451 em algumas regiões)
        data = await fetch_json_retry(BINANCE_AGG, params={"symbol": "ETHUSDT", "limit": "1000"}, attempts=2, timeout=8.0)
        lst = data if isinstance(data, list) else data.get("list") or []

        # Normaliza estrutura: [{p:price, q:qty, T:time, m:isSell}]
        trades = []
        for t in lst:
            try:
                price = float(t.get("p") or t.get("price"))
                qty   = float(t.get("q") or t.get("qty"))
                tms   = int(t.get("T") or t.get("time") or 0)  # ms
                isSell = bool(t.get("m"))  # Binance: m=true se maker é vendedor
                trades.append((tms, price, qty, isSell))
            except Exception:
                continue
        if not trades:
            return

        # filtra novos pela marca temporal
        new = [x for x in trades if x[0] > _last_trade_time_ms]
        if not new:
            return
        _last_trade_time_ms = max(x[0] for x in new)

        # agrega USD por lado
        buy_usd=sell_usd=0.0; buy_qty=sell_qty=0.0
        for tms, price, qty, isSell in new:
            usd = price * qty
            if isSell:
                sell_usd += usd; sell_qty += qty
            else:
                buy_usd  += usd; buy_qty  += qty

        events=[]
        if buy_usd >= WHALE_USD_MIN:
            events.append(("BUY", buy_qty, buy_usd, f"agg {len(new)} trades"))
        if sell_usd >= WHALE_USD_MIN:
            events.append(("SELL", sell_qty, sell_usd, f"agg {len(new)} trades"))

        if not events:
            return

        # cooldown p/ não poluir o chat
        if _now_ts() - _last_whale_send < WHALE_COOLDOWN_SEC:
            return
        _last_whale_send = _now_ts()

        ts = dt.datetime.now(dt.UTC)
        async with pool.acquire() as c:
            for side, qty, usd, note in events:
                await c.execute(
                    "INSERT INTO whale_events(ts,venue,side,qty,usd_value,note) VALUES($1,$2,$3,$4,$5,$6)",
                    ts, "binance", side, qty, usd, note
                )
        # envio resumido
        for side, qty, usd, note in events:
            await send_tg(f"🐋 {side} ~ ${usd:,.0f} | qty ~ {qty:,.1f} | {note}")
    finally:
        last_run["ingest_whales"] = dt.datetime.now(TZ).isoformat(timespec="seconds")

# ========= Accounts (Bybit + Aave) =========
def bybit_sign_qs(secret: str, params: Dict[str, Any]) -> str:
    qs = "&".join([f"{k}={params[k]}" for k in sorted(params)])
    return hmac.new(secret.encode(), qs.encode(), hashlib.sha256).hexdigest()

async def bybit_private_get(path: str, extra: Dict[str, Any]) -> Dict[str, Any]:
    if not (BYBIT_KEY and BYBIT_SEC):
        return {}
    ts = str(int(time.time()*1000))
    base = {"api_key": BYBIT_KEY, "timestamp": ts, "recv_window": "5000"}
    payload = {**base, **extra}
    payload["sign"] = bybit_sign_qs(BYBIT_SEC, payload)
    url = f"https://api.bybit.com{path}"
    return await fetch_json_retry(url, params=payload, attempts=2, timeout=12.0)

async def snapshot_bybit() -> List[Dict[str, Any]]:
    if not (BYBIT_KEY and BYBIT_SEC): 
        return []
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

async def snapshot_aave() -> List[Dict[str, Any]]:
    if not AAVE_ADDR: 
        return []
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
    last_run["ingest_accounts"] = dt.datetime.now(TZ).isoformat(timespec="seconds")

# ========= On-chain (opcional) =========
async def ingest_onchain_eth(addr: str):
    if not ETHERSCAN_API_KEY: 
        last_run["ingest_onchain"] = dt.datetime.now(TZ).isoformat(timespec="seconds")
        return
    try:
        data = await fetch_json_retry(ETHERSCAN_TX.format(addr=addr, key=ETHERSCAN_API_KEY), attempts=2, timeout=15.0)
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
        pass
    finally:
        last_run["ingest_onchain"] = dt.datetime.now(TZ).isoformat(timespec="seconds")

# ========= News (RSS simples) =========
def _extract_rss_items(xml_text: str, source: str, max_n: int = 10) -> List[Tuple[str,str]]:
    # parsing ultra simples (sem dependências)
    items = []
    try:
        parts = xml_text.split("<item>")
        for p in parts[1:]:
            try:
                title = p.split("<title>")[1].split("</title>")[0]
                link  = p.split("<link>")[1].split("</link>")[0]
                title = title.strip()
                link  = link.strip()
                if title and link:
                    items.append((title, link))
                if len(items) >= max_n:
                    break
            except Exception:
                continue
    except Exception:
        pass
    return items

async def ingest_news():
    got = 0
    try:
        async with httpx.AsyncClient(timeout=10) as s:
            for feed in NEWS_FEEDS:
                try:
                    r = await s.get(feed)
                    r.raise_for_status()
                    items = _extract_rss_items(r.text, feed, max_n=10)
                    if not items:
                        continue
                    async with pool.acquire() as c:
                        for title, url in items[:5]:
                            await c.execute(
                                "INSERT INTO news_items(source,title,url) VALUES($1,$2,$3)",
                                feed, title, url
                            )
                            got += 1
                except Exception:
                    continue
    finally:
        last_run["news"] = dt.datetime.now(TZ).isoformat(timespec="seconds")
    return got

async def latest_news_titles(n: int = 6) -> List[Tuple[str,str]]:
    async with pool.acquire() as c:
        rows = await c.fetch("SELECT source,title,url FROM news_items ORDER BY id DESC LIMIT $1", n)
    return [(r["title"], r["url"]) for r in rows]

# ========= Commentary & Pulse =========
def _pct(a: float, b: float) -> float:
    try:
        return 0.0 if a==0 else (b-a)/a*100.0
    except Exception:
        return 0.0

async def build_commentary(hours: int = 8) -> str:
    end = dt.datetime.now(dt.UTC); start = end - dt.timedelta(hours=hours)
    async with pool.acquire() as c:
        rows = await c.fetch("SELECT ts, eth_usd, btc_usd, eth_btc_ratio FROM market_rel WHERE ts BETWEEN $1 AND $2 ORDER BY ts", start, end)
        deriv = await c.fetch("SELECT ts, funding, open_interest FROM derivatives_snap WHERE ts BETWEEN $1 AND $2 ORDER BY ts", start, end)
        whales = await c.fetch("SELECT ts, side, usd_value FROM whale_events WHERE ts BETWEEN $1 AND $2 ORDER BY ts", start, end)
    if not rows:
        return "⏳ Aguardando histórico para comentário…"

    eth = [float(r["eth_usd"]) for r in rows]
    btc = [float(r["btc_usd"]) for r in rows]
    ratio = [float(r["eth_btc_ratio"]) for r in rows]

    eth_chg = _pct(eth[0], eth[-1]); btc_chg = _pct(btc[0], btc[-1]); ratio_chg = _pct(ratio[0], ratio[-1])
    funding = float(deriv[-1]["funding"]) if deriv and deriv[-1]["funding"] is not None else None
    oi = float(deriv[-1]["open_interest"]) if deriv and deriv[-1]["open_interest"] is not None else None

    buy = sum(float(w["usd_value"] or 0) for w in whales if w["side"]=="BUY")
    sell= sum(float(w["usd_value"] or 0) for w in whales if w["side"]=="SELL")
    flow = "neutra"; 
    if buy>sell*1.3: flow="compradora"
    elif sell>buy*1.3: flow="vendedora"

    # Notícias (tópicos curtos)
    news = await latest_news_titles(6)
    if news:
        topics = "• " + "\n• ".join([t[0] for t in news[:3]])
    else:
        topics = "• Fluxo de notícias sem um driver único dominante."

    lines = []
    lines.append("🧾 *Comentário (últimas 8h)*")
    lines.append(f"• ETH: {eth_chg:+.2f}% | BTC: {btc_chg:+.2f}% | ETH/BTC: {ratio_chg:+.2f}%")
    lines.append(f"• Fluxo de baleias: {flow} (BUY ${buy:,.0f} vs SELL ${sell:,.0f})")
    if funding is not None or oi is not None:
        frag=[]
        if funding is not None: frag.append(f"funding {funding*100:.3f}%/8h")
        if oi is not None: frag.append(f"OI ~ {oi:,.0f}")
        lines.append("• Derivativos: " + " | ".join(frag))
    lines.append("• Temas de notícia:")
    lines.append(topics)
    # Síntese
    if ratio_chg>0 and (funding is None or funding<0.0005):
        synth = "pró-ETH (força relativa + funding contido)."
    elif ratio_chg<0 and (funding is not None and funding>0.001):
        synth = "BTC dominante / euforia no funding (cautela)."
    else:
        synth = "equilíbrio tático — usar gatilhos de preço e níveis."
    lines.append(f"🧭 *Síntese:* {synth}")
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
        f"🕒 *{now}*",
        f"ETH: ${eth:,.2f}" + (f" _(H:{eh:,.2f}/L:{el:,.2f})_" if isinstance(eh,(int,float)) and isinstance(el,(int,float)) else ""),
        f"BTC: ${btc:,.2f}" + (f" _(H:{bh:,.2f}/L:{bl:,.2f})_" if isinstance(bh,(int,float)) and isinstance(bl,(int,float)) else ""),
        f"ETH/BTC: {ratio:.5f}",
    ]
    if d and d["funding"] is not None: parts.append(f"Funding (ETH): {float(d['funding'])*100:.3f}%/8h")
    if d and d["open_interest"] is not None: parts.append(f"Open Interest (ETH): {float(d['open_interest']):,.0f}")

    # Gatilhos hedge
    if eth < ETH_HEDGE_2: parts.append(f"🚨 ETH < {ETH_HEDGE_2:.0f} → *ampliar hedge* p/ 20% (≈29 ETH).")
    elif eth < ETH_HEDGE_1: parts.append(f"⚠️ ETH < {ETH_HEDGE_1:.0f} → *ativar hedge* 15% (≈22 ETH).")
    elif eth > ETH_CLOSE: parts.append(f"↩️ ETH > {ETH_CLOSE:.0f} → *avaliar fechar hedge*.")
    else: parts.append("✅ Sem gatilho imediato.")

    comment = await build_commentary(8)
    return "\n".join(parts) + "\n\n" + comment

# ========= Alertas reativos (1h) =========
async def reactive_alerts():
    try:
        end = dt.datetime.now(dt.UTC)
        start = end - dt.timedelta(hours=1, minutes=5)
        async with pool.acquire() as c:
            e = await c.fetch("SELECT ts, close FROM candles_minute WHERE symbol='ETHUSDT' AND ts BETWEEN $1 AND $2 ORDER BY ts", start, end)
            b = await c.fetch("SELECT ts, close FROM candles_minute WHERE symbol='BTCUSDT' AND ts BETWEEN $1 AND $2 ORDER BY ts", start, end)
        if e and len(e) > 5:
            eth_chg = _pct(float(e[0]["close"]), float(e[-1]["close"]))
            if abs(eth_chg) >= ALERT_1H_ETH_PCT:
                direction = "↑" if eth_chg>0 else "↓"
                await send_tg(f"⏱️ Alerta 1h (ETH): {direction} {eth_chg:+.2f}% — verificar posições e hedge.")
        if b and len(b) > 5:
            btc_chg = _pct(float(b[0]["close"]), float(b[-1]["close"]))
            if abs(btc_chg) >= ALERT_1H_BTC_PCT:
                direction = "↑" if btc_chg>0 else "↓"
                await send_tg(f"⏱️ Alerta 1h (BTC): {direction} {btc_chg:+.2f}% — revisar risco.")
    finally:
        last_run["alerts"] = dt.datetime.now(TZ).isoformat(timespec="seconds")

# ========= FastAPI =========
@app.on_event("startup")
async def _startup():
    await db_init()
    # jobs
    scheduler.add_job(ingest_1m,        "interval", minutes=1,  id="ingest_1m",       replace_existing=True)
    scheduler.add_job(ingest_whales,    "interval", seconds=20, id="ingest_whales",   replace_existing=True)
    scheduler.add_job(ingest_accounts,  "interval", minutes=5,  id="ingest_accounts", replace_existing=True)
    scheduler.add_job(ingest_news,      "interval", minutes=15, id="ingest_news",     replace_existing=True)
    scheduler.add_job(reactive_alerts,  "interval", minutes=5,  id="alerts_1h",       replace_existing=True)
    if AAVE_ADDR and ETHERSCAN_API_KEY:
        scheduler.add_job(ingest_onchain_eth, "interval", minutes=5, args=[AAVE_ADDR], id="ingest_onchain", replace_existing=True)
    # boletins horários (8h, 14h, 20h)
    scheduler.add_job(lambda: send_tg(asyncio.run(latest_pulse_text())), "cron", hour=8,  minute=0, id="bulletin_08", replace_existing=True)
    scheduler.add_job(lambda: send_tg(asyncio.run(latest_pulse_text())), "cron", hour=14, minute=0, id="bulletin_14", replace_existing=True)
    scheduler.add_job(lambda: send_tg(asyncio.run(latest_pulse_text())), "cron", hour=20, minute=0, id="bulletin_20", replace_existing=True)
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

@app.get("/pulse")
async def pulse():
    text = await latest_pulse_text()
    await send_tg(text, disable_web_page_preview=True)
    return {"ok": True, "message": text}

@app.get("/alpha")
async def alpha():
    text = await build_commentary(8)
    await send_tg(text, disable_web_page_preview=True)
    return {"ok": True, "message": text}

# Webhook com token na rota (compatível com setWebhook url=.../webhook/<TOKEN>)
@app.post("/webhook/{token}")
async def telegram_webhook(token: str, request: Request):
    if not TG_TOKEN or token != TG_TOKEN:
        return {"ok": False}
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

    # Comandos
    if low in ("/start","start"):
        await send_tg("✅ Bot online.\nComandos: /pulse, /alpha, /eth, /btc,\n/note <texto>, /strat new <nome> | <versão> | <nota>, /strat last, /notes", chat_id)
        return {"ok": True}

    if low == "/pulse":
        await send_tg(await latest_pulse_text(), chat_id); return {"ok": True}

    if low == "/alpha":
        await send_tg(await build_commentary(8), chat_id); return {"ok": True}

    if low == "/eth" or low == "eth":
        async with pool.acquire() as c:
            r = await c.fetchrow("SELECT * FROM market_rel ORDER BY ts DESC LIMIT 1")
        if r:
            eth=float(r["eth_usd"]); btc=float(r["btc_usd"]); ratio=float(r["eth_btc_ratio"])
            msg_txt = f"ETH: ${eth:,.2f} | BTC: ${btc:,.2f} | ETH/BTC: {ratio:.5f}\n↩️ ETH > {ETH_CLOSE:.0f} → avaliar fechar hedge."
            await send_tg(msg_txt, chat_id)
        return {"ok": True}

    if low == "/btc" or low == "btc":
        async with pool.acquire() as c:
            r = await c.fetchrow("SELECT * FROM market_rel ORDER BY ts DESC LIMIT 1")
        if r:
            eth=float(r["eth_usd"]); btc=float(r["btc_usd"]); ratio=float(r["eth_btc_ratio"])
            msg_txt = f"BTC: ${btc:,.2f} | ETH/BTC: {ratio:.5f}\n↩️ ETH > {ETH_CLOSE:.0f} → avaliar fechar hedge."
            await send_tg(msg_txt, chat_id)
        return {"ok": True}

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
        await send_tg(f"Última estratégia:\n{row['created_at']:%Y-%m-%d %H:%M}\n{row['name']} v{row['version']}\n{row['note'] or ''}", chat_id); 
        return {"ok": True}

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
            name, version, note = [p.strip() for p in payload.split("|", 3)[:3]]
        except Exception:
            await send_tg("Uso: /strat new <nome> | <versão> | <nota>", chat_id); return {"ok": True}
        async with pool.acquire() as c:
            await c.execute("INSERT INTO strategy_versions(name,version,note) VALUES($1,$2,$3)", name, version, note)
        await send_tg(f"📌 Estratégia salva: {name} v{version}", chat_id); return {"ok": True}

    # fallback
    await send_tg("Comando não reconhecido. Use /pulse, /alpha, /eth, /btc, /note, /strat.", chat_id)
    return {"ok": True}

# Exports
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

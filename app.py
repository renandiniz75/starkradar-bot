# app.py — Stark DeFi Agent v6.0.3
# Changelog principal
# - Auto-migração no startup (news_items.ts; índice; app_versions)
# - Suporte a /webhook e /webhook/{token} (evita 404 do Telegram)
# - /start, /pulse, /eth, /btc e /alpha reativados e diagramados
# - Removido Binance (sem HTTP 451). Whales desativado por padrão.
# - Autochecagem de webhook opcional (TELEGRAM_SELF_CHECK=1)
# - Log de versão em app_versions e /status com BOT_VERSION

import os, hmac, hashlib, time, math, csv, io, asyncio, traceback, json
import datetime as dt
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, List, Tuple

import httpx
import asyncpg
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse

# ---------- ENV ----------
TZ = ZoneInfo(os.getenv("TZ", "America/Sao_Paulo"))
DB_URL = os.getenv("DATABASE_URL")

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "")
SEND_ENABLED = bool(TG_TOKEN)

BOT_VERSION = os.getenv("BOT_VERSION", "6.0.3")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")
SELF_CHECK  = os.getenv("TELEGRAM_SELF_CHECK", "0") == "1"
WHALES_ENABLED = os.getenv("WHALES_ENABLED", "0") == "1"

# Derivativos (Bybit público; pode falhar — tratamos)
BYBIT_FUND = "https://api.bybit.com/v5/market/funding/history?category=linear&symbol=ETHUSDT&limit=1"
BYBIT_OI   = "https://api.bybit.com/v5/market/open-interest?category=linear&symbol=ETHUSDT&interval=5min"

# Spot (Coinbase, estável p/ preço e H/L intradiário)
CB_TICKER = "https://api.exchange.coinbase.com/products/{pair}/ticker"
CB_STATS  = "https://api.exchange.coinbase.com/products/{pair}/stats"

# Telegram
TG_SEND = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
TG_GET_WEBHOOK = f"https://api.telegram.org/bot{TG_TOKEN}/getWebhookInfo"
TG_SET_WEBHOOK = f"https://api.telegram.org/bot{TG_TOKEN}/setWebhook"

# Hedge zones (ajustáveis por ENV)
ETH_HEDGE_1 = float(os.getenv("ETH_HEDGE_1", "3900"))
ETH_HEDGE_2 = float(os.getenv("ETH_HEDGE_2", "3800"))
ETH_CLOSE   = float(os.getenv("ETH_CLOSE_HEDGE", "3950"))

# On-chain/accounts (opcional)
BYBIT_KEY = os.getenv("BYBIT_RO_KEY", "")
BYBIT_SEC = os.getenv("BYBIT_RO_SECRET", "")
BYBIT_ACCOUNT_TYPE = os.getenv("BYBIT_ACCOUNT_TYPE", "UNIFIED")
AAVE_ADDR = os.getenv("AAVE_ADDR", "")
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")

app = FastAPI(title="stark-defi-agent")
pool: Optional[asyncpg.Pool] = None

from apscheduler.schedulers.asyncio import AsyncIOScheduler
scheduler = AsyncIOScheduler(timezone=str(TZ))

last_run: Dict[str, Optional[str]] = {
    "migrations": None,
    "ingest_1m": None,
    "ingest_whales": None,
    "ingest_accounts": None,
    "ingest_onchain": None,
    "news": None,
}

# ---------- DB bootstrap ----------
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
-- Registro de versões do app
CREATE TABLE IF NOT EXISTS app_versions(
  id serial PRIMARY KEY,
  created_at timestamptz NOT NULL DEFAULT now(),
  version text NOT NULL UNIQUE,
  git_commit text,
  note text
);
-- Notícias (genérica); garantiremos coluna ts adiante
CREATE TABLE IF NOT EXISTS news_items(
  id serial PRIMARY KEY,
  source text,
  title text,
  url text,
  published_at timestamptz
);
"""

async def db_init():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL não definido.")
    global pool
    pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=5)
    async with pool.acquire() as c:
        await c.execute(CREATE_SQL)
        # Migração: garantir coluna ts em news_items
        col = await c.fetchval("""
            SELECT 1 FROM information_schema.columns
            WHERE table_name='news_items' AND column_name='ts'
            """)
        if not col:
            await c.execute("ALTER TABLE news_items ADD COLUMN ts timestamptz")
            # backfill a partir de published_at
            await c.execute("UPDATE news_items SET ts = COALESCE(published_at, now()) WHERE ts IS NULL")
            # índice
            await c.execute("CREATE INDEX IF NOT EXISTS idx_news_items_ts ON news_items(ts DESC)")
        # Registrar versão se ainda não registrada
        await c.execute(
            "INSERT INTO app_versions(version, git_commit, note) VALUES($1,$2,$3) ON CONFLICT (version) DO NOTHING",
            BOT_VERSION, os.getenv("RENDER_GIT_COMMIT") or os.getenv("GIT_COMMIT") or None,
            "auto-migration startup"
        )
    last_run["migrations"] = dt.datetime.now(TZ).isoformat(timespec="seconds")

# ---------- HTTP helpers ----------
async def fetch_json_retry(url: str, params: Dict[str, Any]|None=None, headers: Dict[str,str]|None=None,
                           attempts: int=2, timeout: float=12.0) -> dict:
    exc = None
    for _ in range(attempts):
        try:
            async with httpx.AsyncClient(timeout=timeout) as s:
                r = await s.get(url, params=params, headers=headers)
                r.raise_for_status()
                return r.json()
        except Exception as e:
            exc = e
            await asyncio.sleep(0.6)
    if exc: raise exc
    return {}

async def send_tg(text: str, chat_id: Optional[str] = None):
    if not SEND_ENABLED: return
    cid = chat_id or TG_CHAT
    if not cid: return
    try:
        async with httpx.AsyncClient(timeout=12) as s:
            await s.post(TG_SEND, json={"chat_id": cid, "text": text})
    except Exception:
        traceback.print_exc()

# ---------- Market snapshots ----------
async def coinbase_pair(pair: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    try:
        t = await fetch_json_retry(CB_TICKER.format(pair=pair))
        p = float(t.get("price"))
    except Exception:
        p = None
    hi = lo = None
    try:
        st = await fetch_json_retry(CB_STATS.format(pair=pair))
        hi = float(st.get("high", "nan"))
        lo = float(st.get("low", "nan"))
    except Exception:
        pass
    return p, hi, lo

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

def pct(a: float, b: float) -> float:
    try:
        if a == 0 or a is None or b is None: return 0.0
        return (b - a) / a * 100.0
    except Exception:
        return 0.0

def sparkline(vals: List[float], width: int = 38) -> str:
    # reduz/normaliza e monta "barrinhas" simples
    if not vals: return ""
    n = min(len(vals), width)
    if len(vals) > n:
        step = len(vals)/n
        seq = [vals[int(i*step)] for i in range(n)]
    else:
        seq = vals
    lo, hi = min(seq), max(seq)
    rng = (hi - lo) or 1e-9
    chars = "▁▂▃▄▅▆▇█"
    out=[]
    for v in seq:
        lvl = int((v - lo) / rng * (len(chars)-1))
        out.append(chars[lvl])
    return "".join(out)

async def ingest_1m():
    try:
        now = dt.datetime.now(dt.UTC).replace(second=0, microsecond=0)
        eth_p, eth_h, eth_l = await coinbase_pair("ETH-USD")
        btc_p, btc_h, btc_l = await coinbase_pair("BTC-USD")
        if eth_p is None or btc_p is None: return
        ethbtc = eth_p / btc_p
        der  = await get_derivatives_snapshot()
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

# ---------- Whales (opcional) ----------
_last_trade_time_ms = 0
_ws_lock = asyncio.Lock()

async def ingest_whales():
    if not WHALES_ENABLED:
        last_run["ingest_whales"] = dt.datetime.now(TZ).isoformat(timespec="seconds")
        return
    # Placeholder seguro (desativado). Pode ligar no futuro com fonte estável.
    last_run["ingest_whales"] = dt.datetime.now(TZ).isoformat(timespec="seconds")

# ---------- Accounts (opcional) ----------
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
    last_run["ingest_accounts"] = dt.datetime.now(TZ).isoformat(timespec="seconds")

# ---------- On-chain (opcional) ----------
async def ingest_onchain_eth(addr: str):
    if not ETHERSCAN_API_KEY: 
        last_run["ingest_onchain"] = dt.datetime.now(TZ).isoformat(timespec="seconds")
        return
    try:
        url = f"https://api.etherscan.io/api?module=account&action=txlist&address={addr}&startblock=0&endblock=99999999&sort=desc&apikey={ETHERSCAN_API_KEY}"
        data = await fetch_json_retry(url, attempts=2)
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

# ---------- Commentary / Pulse ----------
def fmt_price(p: float) -> str:
    return f"${p:,.2f}"

def action_line(eth_price: float) -> str:
    if eth_price < ETH_HEDGE_2:
        return f"🚨 ETH < {ETH_HEDGE_2:.0f} → ampliar hedge p/ 20% (29 ETH)."
    if eth_price < ETH_HEDGE_1:
        return f"⚠️ ETH < {ETH_HEDGE_1:.0f} → ativar hedge 15% (22 ETH)."
    if eth_price > ETH_CLOSE:
        return f"↩️ ETH > {ETH_CLOSE:.0f} → avaliar fechar hedge."
    return "✅ Sem gatilho imediato."

async def build_commentary_8h() -> str:
    end = dt.datetime.now(dt.UTC); start = end - dt.timedelta(hours=8)
    async with pool.acquire() as c:
        rows = await c.fetch("SELECT ts, eth_usd, btc_usd, eth_btc_ratio FROM market_rel WHERE ts BETWEEN $1 AND $2 ORDER BY ts", start, end)
        deriv = await c.fetch("SELECT ts, funding, open_interest FROM derivatives_snap WHERE ts BETWEEN $1 AND $2 ORDER BY ts", start, end)
        whales = await c.fetch("SELECT ts, side, usd_value FROM whale_events WHERE ts BETWEEN $1 AND $2 ORDER BY ts", start, end)
        news  = await c.fetch("SELECT ts, title FROM news_items WHERE ts BETWEEN $1 AND $2 ORDER BY ts DESC LIMIT 6", start, end)
    if not rows:
        return "⏳ Aguardando histórico para comentário (volte em alguns minutos)."
    eth = [float(r["eth_usd"]) for r in rows]; btc = [float(r["btc_usd"]) for r in rows]; ratio = [float(r["eth_btc_ratio"]) for r in rows]
    eth_chg = pct(eth[0], eth[-1]); btc_chg = pct(btc[0], btc[-1]); ratio_chg = pct(ratio[0], ratio[-1])
    funding = (float(deriv[-1]["funding"]) if deriv and deriv[-1]["funding"] is not None else None)
    oi      = (float(deriv[-1]["open_interest"]) if deriv and deriv[-1]["open_interest"] is not None else None)
    buy = sum(float(w["usd_value"] or 0) for w in whales if w["side"]=="BUY")
    sell= sum(float(w["usd_value"] or 0) for w in whales if w["side"]=="SELL")

    lines=[]
    lines.append("🧾  Comentário (últimas 8h)")
    lines.append(f"• ETH: {eth_chg:+.2f}% | BTC: {btc_chg:+.2f}% | ETH/BTC: {ratio_chg:+.2f}%")
    if funding is not None or oi is not None:
        add=[]
        if funding is not None: add.append(f"funding {funding*100:.3f}%/8h")
        if oi is not None: add.append(f"OI ~ {oi:,.0f}")
        lines.append("• Derivativos: " + " | ".join(add))
    if buy or sell:
        side = "compradora" if buy>sell*1.3 else ("vendedora" if sell>buy*1.3 else "neutra")
        lines.append(f"• Fluxo de baleias: {side} (BUY ${buy:,.0f} vs SELL ${sell:,.0f})")
    if news:
        lines.append("• Temas de notícia:")
        for n in news[:4]:
            t = n["title"]
            if len(t)>110: t=t[:107]+"…"
            lines.append(f"  · {t}")
    # Síntese
    if ratio_chg>0 and (funding is None or funding<0.0005): synth="pró-ETH (força relativa + funding contido)."
    elif ratio_chg<0 and (funding is not None and funding>0.001): synth="atenção à euforia (funding alto)."
    else: synth="equilíbrio tático; usar gatilhos de preço."
    lines.append(f"🧭 Síntese: {synth}")
    return "\n".join(lines)

async def latest_pulse_text() -> str:
    async with pool.acquire() as c:
        m = await c.fetchrow("SELECT * FROM market_rel ORDER BY ts DESC LIMIT 1")
        e = await c.fetchrow("SELECT * FROM candles_minute WHERE symbol='ETHUSDT' ORDER BY ts DESC LIMIT 1")
        b = await c.fetchrow("SELECT * FROM candles_minute WHERE symbol='BTCUSDT' ORDER BY ts DESC LIMIT 1")
        d = await c.fetchrow("SELECT * FROM derivatives_snap WHERE symbol='ETHUSDT' AND exchange='bybit' ORDER BY ts DESC LIMIT 1")
        series_eth = [float(r["close"]) for r in await c.fetch("SELECT close FROM candles_minute WHERE symbol='ETHUSDT' ORDER BY ts DESC LIMIT 180")]
        series_btc = [float(r["close"]) for r in await c.fetch("SELECT close FROM candles_minute WHERE symbol='BTCUSDT' ORDER BY ts DESC LIMIT 180")]
    if not (m and e and b):
        return "⏳ Aguardando primeiros dados…"
    now = dt.datetime.now(TZ).strftime("%Y-%m-%d %H:%M")
    eth = float(m["eth_usd"]); btc = float(m["btc_usd"]); ratio = float(m["eth_btc_ratio"])
    # Zonas simples (pivôs vizinhos)
    zones_eth_s = "4200 / 4000 / 3850"; zones_eth_r="4300 / 4400 / 4550"
    zones_btc_s = "116.000 / 112.000 / 108.000"; zones_btc_r="120.000 / 124.000 / 128.000"
    def small_card(title:str, seq: List[float]) -> str:
        return f"┌ {title}\n│ {sparkline(list(reversed(seq)))}\n└"
    parts=[f"🕒 {now}",
           f"ETH: {fmt_price(eth)} | BTC: {fmt_price(btc)} | ETH/BTC: {ratio:.5f}",
           action_line(eth),
           "",
           f"📈 Zonas ETH • S: {zones_eth_s} | R: {zones_eth_r}",
           small_card("ETH", series_eth),
           "",
           f"📈 Zonas BTC • S: {zones_btc_s} | R: {zones_btc_r}",
           small_card("BTC", series_btc),
           "",
           await build_commentary_8h()]
    return "\n".join(parts)

async def coin_card(sym: str, zones_s: str, zones_r: str) -> str:
    async with pool.acquire() as c:
        m = await c.fetchrow("SELECT * FROM market_rel ORDER BY ts DESC LIMIT 1")
        series = [float(r["close"]) for r in await c.fetch(
            "SELECT close FROM candles_minute WHERE symbol=$1 ORDER BY ts DESC LIMIT 200", sym)]
    if not m: return "⏳ Aguardando dados…"
    price = float(m["eth_usd"] if sym=="ETHUSDT" else m["btc_usd"])
    chg8 = 0.0
    try:
        async with pool.acquire() as c:
            rows = await c.fetch("SELECT close FROM candles_minute WHERE symbol=$1 ORDER BY ts DESC LIMIT 480", sym)
        if len(rows)>1:
            chg8 = pct(float(rows[-1]["close"]), float(rows[0]["close"]))
    except Exception:
        pass
    head = f"{'ETH' if sym=='ETHUSDT' else 'BTC'}: {fmt_price(price)} | Δ8h {chg8:+.2f}%"
    return "\n".join([
        head,
        f"Zonas • S: {zones_s} | R: {zones_r}",
        action_line(float(m["eth_usd"])) if sym=="ETHUSDT" else "",
        f"```{sparkline(list(reversed(series)))}```"
    ])

# ---------- Telegram Processing ----------
HELP_TEXT = (
    "✅ Bot online.\n"
    "Comandos: /pulse, /eth, /btc, /alpha, /note <texto>, /strat new <nome> | <versão> | <nota>, /strat last, /notes"
)

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

    if low in ("/start","start"):
        await send_tg(HELP_TEXT, chat_id); return {"ok": True}
    if low == "/pulse":
        await send_tg(await latest_pulse_text(), chat_id); return {"ok": True}
    if low == "/eth":
        card = await coin_card("ETHUSDT","4200 / 4000 / 3850","4300 / 4400 / 4550")
        await send_tg(card, chat_id); return {"ok": True}
    if low == "/btc":
        card = await coin_card("BTCUSDT","116.000 / 112.000 / 108.000","120.000 / 124.000 / 128.000")
        await send_tg(card, chat_id); return {"ok": True}
    if low == "/alpha":
        # digest de notícias (se houver)
        end = dt.datetime.now(dt.UTC); start = end - dt.timedelta(hours=12)
        async with pool.acquire() as c:
            rows = await c.fetch("SELECT ts,title FROM news_items WHERE ts BETWEEN $1 AND $2 ORDER BY ts DESC LIMIT 10", start, end)
        if not rows:
            await send_tg("Sem notícias recentes salvas.", chat_id); return {"ok": True}
        out = ["🗞️ Alpha (12h):"] + [f"• {r['title']}" for r in rows]
        await send_tg("\n".join(out), chat_id); return {"ok": True}
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

    await send_tg("Comando não reconhecido. Use /pulse, /eth, /btc, /alpha, /note, /strat new, /strat last, /notes.", chat_id)
    return {"ok": True}

# ---------- FastAPI ----------
@app.on_event("startup")
async def _startup():
    await db_init()
    # Autochecagem de webhook (log orientativo — não força mudança)
    if SELF_CHECK and TG_TOKEN:
        try:
            async with httpx.AsyncClient(timeout=10) as s:
                r = await s.get(TG_GET_WEBHOOK); info = r.json().get("result", {})
            want = os.getenv("RENDER_EXTERNAL_URL") or os.getenv("EXTERNAL_URL") or ""
            if not want:
                # tentativa a partir da env padrão do Render
                want = f"https://{os.getenv('RENDER_SERVICE_NAME','starkradar-bot')}.onrender.com"
            want_url = want.rstrip("/") + "/webhook"
            cur = (info or {}).get("url") or ""
            if cur != want_url:
                fix = f"curl -s -X POST \"https://api.telegram.org/bot{TG_TOKEN}/setWebhook\" -d \"url={want_url}\""
                print(f"[SELF_CHECK] Webhook diferente.\nAtual: {cur}\nDesejado: {want_url}\nPara ajustar: {fix}")
        except Exception:
            pass

    # Schedulers
    scheduler.add_job(ingest_1m, "interval", minutes=1, id="ingest_1m", replace_existing=True)
    scheduler.add_job(ingest_whales, "interval", seconds=20, id="ingest_whales", replace_existing=True)
    scheduler.add_job(ingest_accounts, "interval", minutes=5, id="ingest_accounts", replace_existing=True)
    if AAVE_ADDR and ETHERSCAN_API_KEY:
        scheduler.add_job(ingest_onchain_eth, "interval", minutes=5, args=[AAVE_ADDR], id="ingest_onchain", replace_existing=True)
    # Boletins horários padrão
    scheduler.add_job(lambda: send_tg("⏱️ ping-telegram-ok"), "cron", minute=32, id="ping_tele", replace_existing=True)
    scheduler.start()

@app.get("/")
async def root():
    return {"ok": True, "service": "stark-defi-agent", "version": BOT_VERSION, "tz": str(TZ)}

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
        av  = await c.fetch("SELECT created_at,version FROM app_versions ORDER BY id DESC LIMIT 5")
    return {
        "counts":{
            "market_rel": mr, "candles_eth": cm_eth, "candles_btc": cm_btc,
            "derivatives": ds, "whale_events": whales, "account_snap": acc,
            "news": news
        },
        "last_run": last_run,
        "version": BOT_VERSION,
        "app_versions": [{"created_at": f"{r['created_at']:%Y-%m-%d %H:%M}", "version": r["version"]} for r in av]
    }

@app.post("/run/accounts")
async def run_accounts():
    await ingest_accounts()
    return {"ok": True, "ran_at": last_run["ingest_accounts"]}

@app.get("/pulse")
async def pulse_http():
    text = await latest_pulse_text()
    await send_tg(text)
    return {"ok": True, "message": text}

@app.get("/versions")
async def versions():
    async with pool.acquire() as c:
        rows = await c.fetch("SELECT created_at, version, git_commit, note FROM app_versions ORDER BY id DESC LIMIT 20")
    return JSONResponse({"rows": [dict(r) for r in rows]})

# Admin patch opcional (apenas se ADMIN_TOKEN)
@app.get("/admin/patch-news-ts")
async def patch_news_ts(token: str):
    if not ADMIN_TOKEN or token != ADMIN_TOKEN:
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
    async with pool.acquire() as c:
        await c.execute("UPDATE news_items SET ts = COALESCE(ts, published_at, now()) WHERE ts IS NULL")
    return {"ok": True}

# Telegram webhook (aceita /webhook e /webhook/{token})
@app.post("/webhook")
async def webhook_root(request: Request):
    return await _process_update(request)

@app.post("/webhook/{token}")
async def webhook_token(token: str, request: Request):
    # Aceita somente se corresponder ao token real para evitar abusos
    if TG_TOKEN and token != TG_TOKEN:
        return JSONResponse({"ok": False}, status_code=403)
    return await _process_update(request)

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

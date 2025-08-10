# app.py — Stark DeFi Agent (Railway) v3
# FastAPI + APScheduler + asyncpg + httpx
# Features:
# - Boletins automáticos 08:00, 14:00, 20:00 (America/Sao_Paulo)
# - Ingestão 1-min (ETH/BTC spot, funding, OI) com fallbacks (Bybit -> Binance -> CoinGecko)
# - Memória (notes/strategy_versions) + CSV export
# - Telegram webhook (/webhook) + /pulse
# - Snapshots de contas: Bybit (RO) e Aave (se variáveis existirem)

import os, math, json, hmac, hashlib, time, csv, io, traceback
import datetime as dt
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, List

import httpx
import asyncpg
from fastapi import FastAPI, Request, Response
from fastapi.responses import PlainTextResponse, JSONResponse

# ---------- Config ----------
TZ = ZoneInfo(os.getenv("TZ", "America/Sao_Paulo"))
DB_URL = os.getenv("DATABASE_URL")
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "")
SEND_ENABLED = bool(TG_TOKEN)

ETH_HEDGE_1 = float(os.getenv("ETH_HEDGE_1", "3900"))
ETH_HEDGE_2 = float(os.getenv("ETH_HEDGE_2", "3800"))
ETH_CLOSE   = float(os.getenv("ETH_CLOSE_HEDGE", "3950"))

# Bybit RO (opcional)
BYBIT_KEY = os.getenv("BYBIT_RO_KEY", "")
BYBIT_SEC = os.getenv("BYBIT_RO_SECRET", "")
# Aave (opcional)
AAVE_ADDR = os.getenv("AAVE_ADDR", "")  # seu endereço EVM

# Market providers
BYBIT_SPOT = "https://api.bybit.com/v5/market/tickers?category=spot&symbol={sym}"
BYBIT_FUND = "https://api.bybit.com/v5/market/funding/history?category=linear&symbol=ETHUSDT&limit=1"
BYBIT_OI   = "https://api.bybit.com/v5/market/open-interest?category=linear&symbol=ETHUSDT&interval=5min"
BINANCE_SPOT = "https://api.binance.com/api/v3/ticker/24hr?symbol={sym}"  # ETHUSDT/BTCUSDT
COINGECKO_SIMPLE = "https://api.coingecko.com/api/v3/simple/price?ids=ethereum,bitcoin&vs_currencies=usd"
TG_SEND    = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"

app = FastAPI(title="stark-defi-agent")
pool: Optional[asyncpg.Pool] = None

# ---------- Scheduler ----------
from apscheduler.schedulers.asyncio import AsyncIOScheduler
scheduler = AsyncIOScheduler(timezone=str(TZ))

# ---------- DB Schema ----------
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
-- memória “da casa”
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
  tag text,
  text text NOT NULL
);
CREATE TABLE IF NOT EXISTS actions_log(
  id serial PRIMARY KEY,
  ts timestamptz NOT NULL DEFAULT now(),
  action text NOT NULL,
  details jsonb
);
-- snapshots de conta (venue=bybit/aave)
CREATE TABLE IF NOT EXISTS account_snap(
  ts timestamptz NOT NULL DEFAULT now(),
  venue text NOT NULL,
  metric text NOT NULL,
  value numeric,
  PRIMARY KEY (ts, venue, metric)
);
"""

# ---------- Boot ----------
async def db_init():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL não definido.")
    global pool
    pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=5)
    async with pool.acquire() as c:
        await c.execute(CREATE_SQL)

# ---------- Helpers ----------
async def fetch_json(url: str, headers: Dict[str, str] | None = None, params: Dict[str, str] | None = None):
    async with httpx.AsyncClient(timeout=15) as s:
        r = await s.get(url, headers=headers, params=params)
        r.raise_for_status()
        return r.json()

async def send_tg(text: str, chat_id: Optional[str] = None):
    if not SEND_ENABLED: return
    cid = chat_id or TG_CHAT
    if not cid: return
    async with httpx.AsyncClient(timeout=12) as s:
        await s.post(TG_SEND, json={"chat_id": cid, "text": text})

async def log_action(action: str, details: dict):
    async with pool.acquire() as c:
        await c.execute("INSERT INTO actions_log(action, details) VALUES($1,$2)", action, json.dumps(details))

def action_line(eth_price: float) -> str:
    if eth_price < ETH_HEDGE_2:
        return f"🚨 ETH < {ETH_HEDGE_2:.0f} → ampliar hedge p/ 20% (29 ETH)."
    if eth_price < ETH_HEDGE_1:
        return f"⚠️ ETH < {ETH_HEDGE_1:.0f} → ativar hedge 15% (22 ETH)."
    if eth_price > ETH_CLOSE:
        return f"↩️ ETH > {ETH_CLOSE:.0f} → avaliar fechar hedge."
    return "✅ Sem gatilho. Suportes: 4.200/4.000 | Resist: 4.300/4.400."

# ---------- Market fetch with fallbacks ----------
async def get_spot_snapshot() -> dict:
    # 1) Bybit
    try:
        eth = (await fetch_json(BYBIT_SPOT.format(sym="ETHUSDT")))["result"]["list"][0]
        btc = (await fetch_json(BYBIT_SPOT.format(sym="BTCUSDT")))["result"]["list"][0]
        return {
            "eth": {"price": float(eth["lastPrice"]), "high": float(eth["highPrice"]), "low": float(eth["lowPrice"])},
            "btc": {"price": float(btc["lastPrice"]), "high": float(btc["highPrice"]), "low": float(btc["lowPrice"])},
            "source": "bybit"
        }
    except Exception as e:
        print("[WARN] Bybit spot falhou:", repr(e))
        traceback.print_exc()

    # 2) Binance
    try:
        eth = await fetch_json(BINANCE_SPOT.format(sym="ETHUSDT"))
        btc = await fetch_json(BINANCE_SPOT.format(sym="BTCUSDT"))
        return {
            "eth": {"price": float(eth["lastPrice"]), "high": float(eth["highPrice"]), "low": float(eth["lowPrice"])},
            "btc": {"price": float(btc["lastPrice"]), "high": float(btc["highPrice"]), "low": float(btc["lowPrice"])},
            "source": "binance"
        }
    except Exception as e:
        print("[WARN] Binance spot falhou:", repr(e))
        traceback.print_exc()

    # 3) CoinGecko (só preço)
    try:
        cg = await fetch_json(COINGECKO_SIMPLE)
        return {
            "eth": {"price": float(cg["ethereum"]["usd"]), "high": math.nan, "low": math.nan},
            "btc": {"price": float(cg["bitcoin"]["usd"]),  "high": math.nan, "low": math.nan},
            "source": "coingecko"
        }
    except Exception as e:
        print("[ERROR] CoinGecko também falhou:", repr(e))
        traceback.print_exc()
        raise

async def get_derivatives_snapshot() -> dict:
    funding = None; open_interest = None
    try:
        f = (await fetch_json(BYBIT_FUND))["result"]["list"][0]
        funding = float(f["fundingRate"])
    except Exception as e:
        print("[WARN] Funding Bybit falhou:", repr(e))
    try:
        oi = (await fetch_json(BYBIT_OI))["result"]["list"][-1]
        open_interest = float(oi["openInterest"])
    except Exception as e:
        print("[WARN] OI Bybit falhou:", repr(e))
    return {"funding": funding, "oi": open_interest}

# ---------- Bybit private (read-only) ----------
BYBIT_API = "https://api.bybit.com"
def bybit_signature(secret: str, payload: Dict[str, Any]) -> str:
    # assinatura v5: concatena query em ordem de chave
    qs = "&".join([f"{k}={payload[k]}" for k in sorted(payload)])
    return hmac.new(secret.encode(), qs.encode(), hashlib.sha256).hexdigest()

async def bybit_private_get(path: str, query: Dict[str, Any]) -> Dict[str, Any]:
    if not (BYBIT_KEY and BYBIT_SEC):
        raise RuntimeError("Bybit RO vars não definidas")
    ts = str(int(time.time() * 1000))
    payload = {
        "api_key": BYBIT_KEY,
        "timestamp": ts,
        "recv_window": "5000",
        **query
    }
    sign = bybit_signature(BYBIT_SEC, payload)
    headers = {"X-BAPI-API-KEY": BYBIT_KEY, "X-BAPI-SIGN": sign, "X-BAPI-TIMESTAMP": ts, "X-BAPI-RECV-WINDOW": "5000"}
    url = f"{BYBIT_API}{path}?"+ "&".join([f"{k}={payload[k]}" for k in sorted(payload)])
    async with httpx.AsyncClient(timeout=15) as s:
        r = await s.get(url, headers=headers)
        r.raise_for_status()
        return r.json()

async def snapshot_bybit() -> List[Dict[str, Any]]:
    """Equity/margem básicas (unified). Salva métricas simples."""
    metrics = []
    try:
        # wallet balance (unified)
        r = await bybit_private_get("/v5/account/wallet-balance", {"accountType": "UNIFIED"})
        lst = r.get("result", {}).get("list", [])
        if lst:
            total_equity = float(lst[0].get("totalEquity", 0.0))
            metrics.append(("bybit", "total_equity", total_equity))
        # positions (ETHUSDT perp)
        r2 = await bybit_private_get("/v5/position/list", {"category": "linear", "symbol": "ETHUSDT"})
        pos = r2.get("result", {}).get("list", [])
        if pos:
            size = float(pos[0].get("size", 0.0))
            leverage = float(pos[0].get("leverage", 0.0) or 0.0)
            metrics.append(("bybit", "ethusdt_perp_size", size))
            metrics.append(("bybit", "ethusdt_perp_leverage", leverage))
    except Exception as e:
        print("[WARN] snapshot_bybit falhou:", repr(e))
    return [{"venue": v, "metric": m, "value": val} for (v, m, val) in metrics]

# ---------- Aave (via subgraph simples) ----------
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
    if not AAVE_ADDR:
        return []
    out = []
    try:
        async with httpx.AsyncClient(timeout=20) as s:
            r = await s.post(AAVE_SUBGRAPH, json={"query": AAVE_QUERY, "variables": {"user": AAVE_ADDR.lower()}})
            r.raise_for_status()
            data = r.json()
        # cálculo simplificado (sem índices de liquidez) — usamos como tendência, não valor absoluto
        reserves = data.get("data", {}).get("userReserves", [])
        total_coll = 0.0; total_debt = 0.0
        for it in reserves:
            sym = it["reserve"]["symbol"]; dec = int(it["reserve"]["decimals"])
            a = float(it["scaledATokenBalance"] or 0) / (10**dec)
            d = float(it["scaledVariableDebt"] or 0) / (10**dec)
            if a > 0: total_coll += a  # proxy
            if d > 0: total_debt += d
        out.append({"venue":"aave","metric":"collateral_proxy","value": total_coll})
        out.append({"venue":"aave","metric":"debt_proxy","value": total_debt})
    except Exception as e:
        print("[WARN] snapshot_aave falhou:", repr(e))
    return out

async def save_account_metrics(rows: List[Dict[str, Any]]):
    if not rows: return
    async with pool.acquire() as c:
        for r in rows:
            await c.execute(
                "INSERT INTO account_snap(venue,metric,value) VALUES($1,$2,$3)",
                r["venue"], r["metric"], r["value"]
            )

# ---------- Ingestões ----------
async def ingest_1m():
    now = dt.datetime.now(dt.UTC).replace(second=0, microsecond=0)
    spot = await get_spot_snapshot()
    der  = await get_derivatives_snapshot()

    eth_p = spot["eth"]["price"]; btc_p = spot["btc"]["price"]
    eth_h = spot["eth"]["high"];  eth_l = spot["eth"]["low"]
    btc_h = spot["btc"]["high"];  btc_l = spot["btc"]["low"]
    ethbtc = eth_p / btc_p

    async with pool.acquire() as c:
        # ETH
        await c.execute(
            "INSERT INTO candles_minute(ts,symbol,open,high,low,close,volume) "
            "VALUES($1,$2,NULL,$3,$4,$5,NULL) ON CONFLICT (ts,symbol) DO UPDATE "
            "SET high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close",
            now, "ETHUSDT", eth_h, eth_l, eth_p
        )
        # BTC
        await c.execute(
            "INSERT INTO candles_minute(ts,symbol,open,high,low,close,volume) "
            "VALUES($1,$2,NULL,$3,$4,$5,NULL) ON CONFLICT (ts,symbol) DO UPDATE "
            "SET high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close",
            now, "BTCUSDT", btc_h, btc_l, btc_p
        )
        # Relação
        await c.execute(
            "INSERT INTO market_rel(ts,eth_usd,btc_usd,eth_btc_ratio) "
            "VALUES($1,$2,$3,$4) ON CONFLICT (ts) DO UPDATE "
            "SET eth_usd=EXCLUDED.eth_usd, btc_usd=EXCLUDED.btc_usd, eth_btc_ratio=EXCLUDED.eth_btc_ratio",
            now, eth_p, btc_p, ethbtc
        )
        # Derivativos
        await c.execute(
            "INSERT INTO derivatives_snap(ts,symbol,exchange,funding,open_interest) "
            "VALUES($1,$2,$3,$4,$5) ON CONFLICT (ts,symbol,exchange) DO UPDATE "
            "SET funding=EXCLUDED.funding, open_interest=EXCLUDED.open_interest",
            now, "ETHUSDT", "bybit", der["funding"], der["oi"]
        )

async def ingest_accounts():
    rows: List[Dict[str, Any]] = []
    # Bybit RO
    if BYBIT_KEY and BYBIT_SEC:
        rows += await snapshot_bybit()
    # Aave
    if AAVE_ADDR:
        rows += await snapshot_aave()
    await save_account_metrics(rows)

# ---------- Texto do panorama ----------
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
    funding = d["funding"] if d else None
    oi = d["open_interest"] if d else None
    lines = [
        f"🕒 {now}",
        f"ETH: ${eth:,.2f}" + (f" (H:{eh:,.2f}/L:{el:,.2f})" if (eh is not None and not (isinstance(eh,float) and math.isnan(eh))) else ""),
        f"BTC: ${btc:,.2f}" + (f" (H:{bh:,.2f}/L:{bl:,.2f})" if (bh is not None and not (isinstance(bh,float) and math.isnan(bh))) else ""),
        f"ETH/BTC: {ratio:.5f}",
    ]
    if funding is not None: lines.append(f"Funding (ETH): {float(funding)*100:.3f}%/8h")
    if oi is not None:      lines.append(f"Open Interest (ETH): {float(oi):,.0f}")
    lines.append(action_line(eth))
    return "\n".join(lines)

async def send_pulse_to_chat():
    text = await latest_pulse_text()
    await send_tg(text)

# ---------- Rotas ----------
@app.on_event("startup")
async def _startup():
    await db_init()
    # Ingestões
    scheduler.add_job(ingest_1m, "interval", minutes=1, id="ingest_1m", replace_existing=True)
    scheduler.add_job(ingest_accounts, "interval", minutes=5, id="ingest_accounts", replace_existing=True)
    # Boletins automáticos: 08:00, 14:00 e 20:00 (America/Sao_Paulo)
    scheduler.add_job(send_pulse_to_chat, "cron", hour=8,  minute=0, id="bulletin_08", replace_existing=True)
    scheduler.add_job(send_pulse_to_chat, "cron", hour=14, minute=0, id="bulletin_14", replace_existing=True)
    scheduler.add_job(send_pulse_to_chat, "cron", hour=20, minute=0, id="bulletin_20", replace_existing=True)
    scheduler.start()

@app.get("/healthz")
async def healthz():
    return {"ok": True, "time": dt.datetime.now(TZ).isoformat()}

@app.get("/pulse")
async def pulse():
    text = await latest_pulse_text()
    await send_tg(text)
    return {"ok": True, "message": text}

@app.post("/webhook")
async def telegram_webhook(request: Request):
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
        await send_tg("✅ Bot online. Comandos: /pulse, /note <texto>, /strat new <nome> | <versão> | <nota>, /strat last, /notes", chat_id)
        return {"ok": True}

    if low == "/pulse":
        await send_tg(await latest_pulse_text(), chat_id); return {"ok": True}

    if low.startswith("/note"):
        note = text[len("/note"):].strip()
        if not note:
            await send_tg("Uso: /note seu texto aqui", chat_id); return {"ok": True}
        async with pool.acquire() as c:
            await c.execute("INSERT INTO notes(tag,text) VALUES($1,$2)", None, note)
        await log_action("note_add", {"text": note})
        await send_tg("📝 Nota salva.", chat_id); return {"ok": True}

    if low.startswith("/strat new"):
        try:
            payload = text[len("/strat new"):].strip()
            name, version, note = [p.strip() for p in payload.split("|", 2)]
        except Exception:
            await send_tg("Uso: /strat new <nome> | <versão> | <nota>", chat_id); return {"ok": True}
        async with pool.acquire() as c:
            await c.execute("INSERT INTO strategy_versions(name,version,note) VALUES($1,$2,$3)", name, version, note)
        await log_action("strat_new", {"name": name, "version": version})
        await send_tg(f"📌 Estratégia salva: {name} v{version}", chat_id); return {"ok": True}

    if low == "/strat last":
        async with pool.acquire() as c:
            row = await c.fetchrow("SELECT created_at,name,version,note FROM strategy_versions ORDER BY id DESC LIMIT 1")
        if not row:
            await send_tg("Nenhuma estratégia salva.", chat_id); return {"ok": True}
        txt = f"Última estratégia:\n{row['created_at']:%Y-%m-%d %H:%M}\n{row['name']} v{row['version']}\n{row['note'] or ''}"
        await send_tg(txt, chat_id); return {"ok": True}

    if low == "/notes":
        async with pool.acquire() as c:
            rows = await c.fetch("SELECT created_at,text FROM notes ORDER BY id DESC LIMIT 5")
        if not rows:
            await send_tg("Sem notas ainda.", chat_id); return {"ok": True}
        out = ["Notas recentes:"]
        for r in rows:
            out.append(f"- {r['created_at']:%m-%d %H:%M} • {r['text']}")
        await send_tg("\n".join(out), chat_id); return {"ok": True}

    await send_tg("Comando não reconhecido. Use /pulse, /note, /strat new, /strat last, /notes.", chat_id)
    return {"ok": True}

# ---------- Exports ----------
@app.get("/export/notes.csv")
async def export_notes():
    async with pool.acquire() as c:
        rows = await c.fetch("SELECT created_at, tag, text FROM notes ORDER BY id DESC")
    buf = io.StringIO(); w = csv.writer(buf)
    w.writerow(["created_at","tag","text"])
    for r in rows:
        w.writerow([r["created_at"].isoformat(), r["tag"] or "", r["text"]])
    return PlainTextResponse(buf.getvalue(), media_type="text/csv")

@app.get("/export/strats.csv")
async def export_strats():
    async with pool.acquire() as c:
        rows = await c.fetch("SELECT created_at, name, version, note FROM strategy_versions ORDER BY id DESC")
    buf = io.StringIO(); w = csv.writer(buf)
    w.writerow(["created_at","name","version","note"])
    for r in rows:
        w.writerow([r["created_at"].isoformat(), r["name"], r["version"], r["note"] or ""])
    return PlainTextResponse(buf.getvalue(), media_type="text/csv")

@app.get("/accounts/last")
async def accounts_last():
    async with pool.acquire() as c:
        rows = await c.fetch("""
            SELECT * FROM account_snap
            WHERE ts > now() - interval '1 hour'
            ORDER BY ts DESC, venue, metric
        """)
    data = [dict(r) for r in rows]
    return JSONResponse({"rows": data})

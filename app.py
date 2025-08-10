# app.py — Stark DeFi Agent (Railway) v2
# FastAPI + APScheduler + asyncpg + httpx
# Rotas: /healthz, /pulse (GET), /webhook (POST Telegram)
# Features: Bybit -> Binance -> CoinGecko fallback; DB histórico; memória de estratégias/notes.

import os, math, json, traceback
import datetime as dt
from zoneinfo import ZoneInfo
from typing import Optional

import httpx
import asyncpg
from fastapi import FastAPI, Request
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ---------- Config ----------
TZ = ZoneInfo(os.getenv("TZ", "America/Sao_Paulo"))
DB_URL = os.getenv("DATABASE_URL")
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "")
SEND_ENABLED = bool(TG_TOKEN)

ETH_HEDGE_1 = float(os.getenv("ETH_HEDGE_1", "3900"))
ETH_HEDGE_2 = float(os.getenv("ETH_HEDGE_2", "3800"))
ETH_CLOSE   = float(os.getenv("ETH_CLOSE_HEDGE", "3950"))

# Market providers
BYBIT_SPOT = "https://api.bybit.com/v5/market/tickers?category=spot&symbol={sym}"
BYBIT_FUND = "https://api.bybit.com/v5/market/funding/history?category=linear&symbol=ETHUSDT&limit=1"
BYBIT_OI   = "https://api.bybit.com/v5/market/open-interest?category=linear&symbol=ETHUSDT&interval=5min"
BINANCE_SPOT = "https://api.binance.com/api/v3/ticker/24hr?symbol={sym}"  # e.g., ETHUSDT
COINGECKO_SIMPLE = "https://api.coingecko.com/api/v3/simple/price?ids=ethereum,bitcoin&vs_currencies=usd"
TG_SEND    = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"

# ---------- App ----------
app = FastAPI(title="stark-defi-agent")
pool: Optional[asyncpg.Pool] = None
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
async def fetch_json(url: str):
    async with httpx.AsyncClient(timeout=12) as s:
        r = await s.get(url)
        r.raise_for_status()
        return r.json()

def action_line(eth_price: float) -> str:
    if eth_price < ETH_HEDGE_2:
        return f"🚨 ETH < {ETH_HEDGE_2:.0f} → ampliar hedge p/ 20% (29 ETH)."
    if eth_price < ETH_HEDGE_1:
        return f"⚠️ ETH < {ETH_HEDGE_1:.0f} → ativar hedge 15% (22 ETH)."
    if eth_price > ETH_CLOSE:
        return f"↩️ ETH > {ETH_CLOSE:.0f} → avaliar fechar hedge."
    return "✅ Sem gatilho. Suportes: 4.200/4.000 | Resist: 4.300/4.400."

async def send_tg(text: str, chat_id: Optional[str] = None):
    if not SEND_ENABLED: return
    cid = chat_id or TG_CHAT
    if not cid: return
    async with httpx.AsyncClient(timeout=12) as s:
        await s.post(TG_SEND, json={"chat_id": cid, "text": text})

async def log_action(action: str, details: dict):
    async with pool.acquire() as c:
        await c.execute("INSERT INTO actions_log(action, details) VALUES($1,$2)", action, json.dumps(details))

# ---------- Market fetch with fallbacks ----------
async def get_spot_snapshot() -> dict:
    """
    Retorna dict:
    {
      'eth': {'price':float,'high':float|None,'low':float|None},
      'btc': {...}
    }
    Ordem: Bybit -> Binance -> CoinGecko
    """
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

    # 3) CoinGecko (somente preço)
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

# ---------- Ingestão 1m ----------
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
        f"ETH: ${eth:,.2f}" + (f" (H:{eh:,.2f}/L:{el:,.2f})" if (eh is not None and not math.isnan(float(eh))) else ""),
        f"BTC: ${btc:,.2f}" + (f" (H:{bh:,.2f}/L:{bl:,.2f})" if (bh is not None and not math.isnan(float(bh))) else ""),
        f"ETH/BTC: {ratio:.5f}",
    ]
    if funding is not None: lines.append(f"Funding (ETH): {float(funding)*100:.3f}%/8h")
    if oi is not None:      lines.append(f"Open Interest (ETH): {float(oi):,.0f}")
    lines.append(action_line(eth))
    return "\n".join(lines)

# ---------- Rotas ----------
@app.on_event("startup")
async def _startup():
    await db_init()
    scheduler.add_job(ingest_1m, "interval", minutes=1, id="ingest_1m", replace_existing=True)
    scheduler.start()

@app.get("/healthz")
async def healthz():
    return {"ok": True, "time": dt.datetime.now(TZ).isoformat()}

@app.get("/pulse")
async def pulse():
    text = await latest_pulse_text()
    await send_tg(text)
    return {"ok": True, "message": text}

# --- Telegram webhook + comandos de memória ---
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
        await send_tg("✅ Bot online. Use /pulse para panorama.\nComandos: /note <texto>, /strat new <nome> | <versão> | <nota>, /strat last, /notes", chat_id)
        return {"ok": True}

    if low == "/pulse":
        panorama = await latest_pulse_text()
        await send_tg(panorama, chat_id)
        return {"ok": True}

    # /note <texto>
    if low.startswith("/note"):
        note = text[len("/note"):].strip()
        if not note:
            await send_tg("Uso: /note seu texto aqui", chat_id); return {"ok": True}
        async with pool.acquire() as c:
            await c.execute("INSERT INTO notes(tag,text) VALUES($1,$2)", None, note)
        await log_action("note_add", {"text": note})
        await send_tg("📝 Nota salva.", chat_id)
        return {"ok": True}

    # /strat new <nome> | <versão> | <nota>
    if low.startswith("/strat new"):
        try:
            payload = text[len("/strat new"):].strip()
            name, version, note = [p.strip() for p in payload.split("|", 2)]
        except Exception:
            await send_tg("Uso: /strat new <nome> | <versão> | <nota>", chat_id); return {"ok": True}
        async with pool.acquire() as c:
            await c.execute("INSERT INTO strategy_versions(name,version,note) VALUES($1,$2,$3)", name, version, note)
        await log_action("strat_new", {"name": name, "version": version})
        await send_tg(f"📌 Estratégia salva: {name} v{version}", chat_id)
        return {"ok": True}

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

    # fallback
    await send_tg("Comando não reconhecido. Use /pulse, /note, /strat new, /strat last, /notes.", chat_id)
    return {"ok": True}

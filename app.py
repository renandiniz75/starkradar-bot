
import os, io, math, json, asyncio, textwrap, datetime as dt
from typing import Tuple, List, Dict, Any, Optional
from loguru import logger

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
import asyncpg

import ccxt.async_support as ccxt_async  # async exchanges
from dotenv import load_dotenv

# ---------- Config & Globals ----------
VERSION = "6.0.15-full"
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN","")
HOST_URL  = os.getenv("HOST_URL","").rstrip("/")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET","")
DB_URL = os.getenv("DATABASE_URL","")
WEBHOOK_AUTO = os.getenv("WEBHOOK_AUTO","0") == "1"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY","")
NEWS_FEEDS = [u.strip() for u in os.getenv("NEWS_FEEDS","").split(",") if u.strip()]

TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}" if BOT_TOKEN else None

app = FastAPI(title="StarkRadar Bot", version=VERSION)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

pool: Optional[asyncpg.Pool] = None

# ---------- Utilities ----------
def now_utc() -> dt.datetime:
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)

async def run_sql(conn: asyncpg.Connection, sql: str, *args):
    try:
        return await conn.execute(sql, *args)
    except Exception as e:
        logger.error(f"SQL error: {e}")
        raise

async def fetch_sql(conn: asyncpg.Connection, sql: str, *args):
    try:
        return await conn.fetch(sql, *args)
    except Exception as e:
        logger.error(f"SQL fetch error: {e}")
        raise

async def send_tg(chat_id: int, text: str, parse_mode: Optional[str]="HTML"):
    if not TELEGRAM_API: return
    async with httpx.AsyncClient(timeout=20) as cli:
        await cli.post(f"{TELEGRAM_API}/sendMessage",
                       json={"chat_id":chat_id,"text":text,"parse_mode":parse_mode, "disable_web_page_preview":True})

async def send_tg_photo(chat_id: int, png_bytes: bytes, caption: str=""):
    if not TELEGRAM_API: return
    files = {"photo": ("spark.png", png_bytes, "image/png")}
    data = {"chat_id": str(chat_id), "caption": caption, "parse_mode":"HTML"}
    async with httpx.AsyncClient(timeout=30) as cli:
        await cli.post(f"{TELEGRAM_API}/sendPhoto", files=files, data=data)

# ---------- DB Migrations ----------
async def migrate(conn: asyncpg.Connection):
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS news_items (
        id SERIAL PRIMARY KEY,
        ts TIMESTAMPTZ NOT NULL DEFAULT now(),
        title TEXT,
        source TEXT NOT NULL DEFAULT '',
        url TEXT,
        author TEXT,
        published_at TIMESTAMPTZ,
        inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        tickers TEXT[]
    );
    """)
    await conn.execute("""
    CREATE INDEX IF NOT EXISTS idx_news_ts ON news_items(ts DESC);
    """\)

# ---------- Market Data via CCXT ----------
async def fetch_ticker(symbol: str, exchange: str="bybit") -> Dict[str, Any]:
    ex = None
    try:
        ex = getattr(ccxt_async, exchange)()
        t = await ex.fetch_ticker(symbol)
        return t or {}
    except Exception as e:
        logger.warning(f"{exchange}.fetch_ticker({symbol}) failed: {e}")
        return {}
    finally:
        if ex:
            try: await ex.close()
            except: pass

async def fetch_ohlcv(symbol: str, timeframe: str="15m", limit: int=100, exchange: str="bybit") -> List[List[float]]:
    ex=None
    try:
        ex = getattr(ccxt_async, exchange)()
        o = await ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        return o or []
    except Exception as e:
        logger.warning(f"{exchange}.fetch_ohlcv({symbol}) failed: {e}")
        return []
    finally:
        if ex:
            try: await ex.close()
            except: pass

async def prices_snapshot() -> Dict[str, Any]:
    eth = await fetch_ticker("ETH/USDT","bybit") or await fetch_ticker("ETH/USDT","binance")
    btc = await fetch_ticker("BTC/USDT","bybit") or await fetch_ticker("BTC/USDT","binance")

    out = {}
    if eth: out["ETH"] = {"price": eth.get("last") or eth.get("close"), "high": eth.get("high"), "low": eth.get("low")}
    if btc: out["BTC"] = {"price": btc.get("last") or btc.get("close"), "high": btc.get("high"), "low": btc.get("low")}

    if out.get("ETH") and out.get("BTC") and out["BTC"]["price"]:
        out["ETHBTC"] = out["ETH"]["price"] / out["BTC"]["price"]
    else:
        out["ETHBTC"] = None

    eth_ohlcv = await fetch_ohlcv("ETH/USDT","15m", 96)
    btc_ohlcv = await fetch_ohlcv("BTC/USDT","15m", 96)
    out["ETH_SERIES_24H"] = [c[4] for c in eth_ohlcv] if eth_ohlcv else []
    out["BTC_SERIES_24H"] = [c[4] for c in btc_ohlcv] if btc_ohlcv else []
    return out

# ---------- Levels (dynamic) ----------
def round_step(symbol: str) -> float:
    return 50.0 if symbol.startswith("BTC") else 10.0

def dynamic_levels_from_series(symbol: str, closes: List[float]) -> Tuple[List[float], List[float]]:
    if not closes or all([(c is None) or (isinstance(c,float) and np.isnan(c)) for c in closes]):
        if symbol.startswith("BTC"):
            return [60000, 62000], [65000, 68000]
        return [4000, 4200], [4300, 4400]
    arr = np.array([c for c in closes if c is not None], dtype=float)
    lo, hi = float(np.nanmin(arr)), float(np.nanmax(arr))
    mid = (lo+hi)/2.0
    step = round_step(symbol)
    def r(x): return round(x/step)*step
    sups = sorted(set([r(lo), r(mid-step)]))
    ress = sorted(set([r(mid+step), r(hi)]))
    return sups, ress

# ---------- Plot ----------
def make_sparkline_png(series: List[float]) -> bytes:
    if not series:
        series = [1,1,1,1,1]
    fig = plt.figure(figsize=(4,1.2), dpi=200)
    ax = fig.add_subplot(111)
    ax.plot(series)
    ax.set_xticks([]); ax.set_yticks([])
    for spine in ["top","right","left","bottom"]: ax.spines[spine].set_visible(False)
    buf = io.BytesIO()
    plt.tight_layout()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    return buf.getvalue()

# ---------- News (light) ----------
async def get_recent_news(hours: int=12, limit: int=6) -> List[Dict[str,Any]]:
    if not pool: return []
    async with pool.acquire() as c:
        rows = await c.fetch("""
            SELECT ts, source, title, url
            FROM news_items
            WHERE ts >= now() - $1::interval
            ORDER BY ts DESC
            LIMIT $2
        """, dt.timedelta(hours=hours), limit)
    return [{"ts": r["ts"], "source": r["source"], "title": r["title"], "url": r["url"]} for r in rows]

# ---------- OpenAI (text + voice) ----------
async def transcribe_voice_ogg(file_url: str) -> str:
    if not OPENAI_API_KEY:
        return "(Transcrição indisponível: falta OPENAI_API_KEY)"
    async with httpx.AsyncClient(timeout=60) as cli:
        ogg = await cli.get(file_url)
        ogg.raise_for_status()
        data = ogg.content
    from openai import AsyncOpenAI
    client = AsyncOpenAI(api_key=OPENAI_API_KEY)
    fname = "audio.oga"
    with open(fname,"wb") as f: f.write(data)
    try:
        tr = await client.audio.transcriptions.create(
            model="whisper-1",
            file=open(fname,"rb")
        )
        return tr.text or ""
    finally:
        try: os.remove(fname)
        except: pass

async def ask_openai(prompt: str) -> str:
    if not OPENAI_API_KEY:
        return "(Resposta indisponível: falta OPENAI_API_KEY)"
    from openai import AsyncOpenAI
    client = AsyncOpenAI(api_key=OPENAI_API_KEY)
    resp = await client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role":"system","content":"Você é um analista cripto, sucinto e claro."},
                  {"role":"user","content":prompt}],
        temperature=0.3,
        max_tokens=500,
    )
    return resp.choices[0].message.content.strip()

# ---------- Bot logic ----------
async def build_pulse_text() -> str:
    snap = await prices_snapshot()
    eth = snap.get("ETH",{}); btc = snap.get("BTC",{})
    eth_p = eth.get("price"); btc_p = btc.get("price")
    ratio = snap.get("ETHBTC")

    eth_sups, eth_ress = dynamic_levels_from_series("ETH", snap.get("ETH_SERIES_24H",[]))
    btc_sups, btc_ress = dynamic_levels_from_series("BTC", snap.get("BTC_SERIES_24H",[]))

    def change(series, hours):
        if not series or len(series)<2: return None
        steps = int(hours*60/15)
        if len(series) <= steps: return None
        now, past = series[-1], series[-steps-1]
        if past == 0: return None
        return 100*(now-past)/past

    eth_ch8 = change(snap.get("ETH_SERIES_24H",[]), 8)
    btc_ch8 = change(snap.get("BTC_SERIES_24H",[]), 8)
    ch_ratio8 = None
    if eth_ch8 is not None and btc_ch8 is not None:
        ch_ratio8 = eth_ch8 - btc_ch8

    lines = []
    lines.append("ANÁLISE:")
    if eth_p and btc_p:
        bias = "neutro/tático"
        if ch_ratio8 is not None:
            if ch_ratio8 < -1.0: bias = "BTC dominante (ETH fraco)"
            elif ch_ratio8 > 1.0: bias = "ETH dominante"
        lines.append(f"• ETH {eth_p:,.2f} | BTC {btc_p:,.2f} | ETH/BTC {ratio:.5f}" if ratio else f"• ETH {eth_p:,.2f} | BTC {btc_p:,.2f}")
        if ch_ratio8 is not None:
            lines.append(f"• 8h: ETH {eth_ch8:+.2f}% | BTC {btc_ch8:+.2f}% → {bias}")
    lines.append(f"• ETH S:{', '.join(map(lambda x: f'{x:,.0f}', eth_sups))} | R:{', '.join(map(lambda x: f'{x:,.0f}', eth_ress))}")
    lines.append(f"• BTC S:{', '.join(map(lambda x: f'{x:,.0f}', btc_sups))} | R:{', '.join(map(lambda x: f'{x:,.0f}', btc_ress))}")
    lines.append("• Ação: operar gatilhos em rompimentos válidos; defesa nos suportes.")

    news = await get_recent_news()
    if news:
        lines.append("")
        lines.append("FONTES (12h):")
        for n in news[:6]:
            ts = n["ts"].strftime("%H:%M")
            lines.append(f"• {ts} {n['source']} — {n['title']}")

    return "\n".join(lines)

async def handle_start(chat_id: int):
    snap = await prices_snapshot()
    txt = "Bem-vindo! 👋\nUse /pulse, /eth, /btc, /panel."
    await send_tg(chat_id, txt)
    png = make_sparkline_png(snap.get("ETH_SERIES_24H",[]))
    await send_tg_photo(chat_id, png, caption=f"ETH 24h — v{VERSION}")

async def handle_pulse(chat_id: int):
    text = await build_pulse_text()
    await send_tg(chat_id, text)

async def handle_asset(chat_id: int, sym: str):
    snap = await prices_snapshot()
    series = snap.get(f"{sym}_SERIES_24H",[]) if sym in ["ETH","BTC"] else []
    sups, ress = dynamic_levels_from_series(sym, series)
    last = (snap.get(sym,{}).get("price"))
    lines = [f"{sym}: {last:,.2f}" if last else f"{sym}: n/d",
             f"S:{', '.join(map(lambda x: f'{x:,.0f}', sups))} | R:{', '.join(map(lambda x: f'{x:,.0f}', ress))}",
             "Ação: mesma disciplina do /pulse."]
    await send_tg(chat_id, "\n".join(lines))
    if series:
        png = make_sparkline_png(series)
        await send_tg_photo(chat_id, png, caption=f"{sym} 24h")

async def handle_panel(chat_id: int):
    snap = await prices_snapshot()
    cap = f"ETH {snap.get('ETH',{}).get('price','n/d')} | BTC {snap.get('BTC',{}).get('price','n/d')}"
    png = make_sparkline_png(snap.get("BTC_SERIES_24H",[]))
    await send_tg_photo(chat_id, png, caption=cap)

async def handle_voice(chat_id: int, file_id: str):
    async with httpx.AsyncClient(timeout=30) as cli:
        r = await cli.get(f"{TELEGRAM_API}/getFile", params={"file_id": file_id})
        j = r.json()
        file_path = j["result"]["file_path"]
        file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
    transcript = await transcribe_voice_ogg(file_url)
    reply = await ask_openai(f"Transcrição: {transcript}\n\nResponda de forma útil e direta.")
    await send_tg(chat_id, f"🗣️ <b>Você disse</b>:\n{transcript}\n\n🤖 <b>Resposta</b>:\n{reply}")

async def handle_ask(chat_id: int, prompt: str):
    ans = await ask_openai(prompt)
    await send_tg(chat_id, f"🤖 {ans}")

# ---------- Webhook ----------
@app.post("/webhook")
async def webhook_root(request: Request):
    if WEBHOOK_SECRET and request.headers.get("X-Webhook-Secret") != WEBHOOK_SECRET:
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)

    body = await request.json()
    msg = body.get("message") or body.get("edited_message")
    if not msg: return JSONResponse({"ok": True})

    chat_id = msg["chat"]["id"]
    text = (msg.get("text") or "").strip()

    if "voice" in msg:
        file_id = msg["voice"]["file_id"]
        await handle_voice(chat_id, file_id)
        return {"ok": True}

    if text.startswith("/") or text.startswith("."):
        cmd, *rest = text.split(" ",1)
        arg = rest[0] if rest else ""
        cmd = cmd.lower()
        if cmd in ("/start",".start","/help",".help"):
            await handle_start(chat_id)
        elif cmd in ("/pulse",".pulse"):
            await handle_pulse(chat_id)
        elif cmd in ("/eth",".eth"):
            await handle_asset(chat_id, "ETH")
        elif cmd in ("/btc",".btc"):
            await handle_asset(chat_id, "BTC")
        elif cmd in ("/panel",".panel"):
            await handle_panel(chat_id)
        elif cmd in ("/ask",".ask"):
            await handle_ask(chat_id, arg or "Explique o momento atual de ETH vs BTC e riscos.")
        else:
            await send_tg(chat_id, "Comandos: /pulse /eth /btc /panel /ask <pergunta>")
    return {"ok": True}

# ---------- Admin & Status ----------
@app.get("/status")
async def status():
    return JSONResponse({"ok": True, "version": VERSION, "linecount": 0, "last_error": None})

@app.get("/admin/setwebhook")
async def setwebhook():
    if not TELEGRAM_API or not HOST_URL:
        return JSONResponse({"ok": False, "error": "BOT_TOKEN/HOST_URL ausente"})
    async with httpx.AsyncClient(timeout=20) as cli:
        url = f"{HOST_URL}/webhook"
        headers = {"X-Webhook-Secret": WEBHOOK_SECRET} if WEBHOOK_SECRET else {}
        r = await cli.get(f"{TELEGRAM_API}/setWebhook", params={"url": url}, headers=headers)
        return PlainTextResponse(r.text)

# ---------- Lifespan ----------
@app.on_event("startup")
async def _startup():
    global pool
    if DB_URL:
        pool = await asyncpg.create_pool(dsn=DB_URL, min_size=1, max_size=2)
        async with pool.acquire() as c:
            await migrate(c)
    if WEBHOOK_AUTO and TELEGRAM_API and HOST_URL:
        try:
            async with httpx.AsyncClient(timeout=20) as cli:
                headers = {"X-Webhook-Secret": WEBHOOK_SECRET} if WEBHOOK_SECRET else {}
                await cli.get(f"{TELEGRAM_API}/setWebhook", params={"url": f"{HOST_URL}/webhook"}, headers=headers)
        except Exception as e:
            logger.warning(f"setWebhook auto falhou: {e}")

@app.on_event("shutdown")
async def _shutdown():
    global pool
    if pool:
        await pool.close()

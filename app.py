
# app.py — Stark DeFi Agent v6.0.13-hotfix1
# =============================================================================
# Objetivo
#   Bot Telegram + API (FastAPI) para pulse tático de BTC/ETH com:
#   - /start, /pulse, /btc, /eth, /panel (+ /help)
#   - Painel inicial com sparkline opcional (SPARKLINES=1) e fallback seguro
#   - Coleta de preços/ohlcv via CCXT (Bybit/OKX/Binance - pública, sem chave)
#   - Níveis dinâmicos S/R a partir de 48h (com arredondamento por escala)
#   - News grooming por RSS (CoinDesk, The Block, etc.) com cache em Postgres
#   - Migração automática do schema news_items (sem precisar SQL manual)
#   - Webhook Telegram + “/admin” endpoints de manutenção
#   - Rodapé dos balões com versão e contagem real de linhas do arquivo
#   - Fechamento correto das sessões CCXT (evita Unclosed client session)
#   - Robustez: qualquer falha externa resulta em mensagens úteis ao usuário
# =============================================================================
# VARIÁVEIS DE AMBIENTE (Render / Railway)
#   BOT_TOKEN               -> token do Bot do Telegram (obrigatório para Telegram)
#   DATABASE_URL            -> Postgres URL (opcional para news/cache; sem DB funciona)
#   HOST_URL                -> URL público do serviço (ex: https://xxxxx.onrender.com)
#   WEBHOOK_AUTO=1          -> se 1, define webhook no startup usando HOST_URL
#   PROVIDERS               -> lista separada por vírgulas (por padrão: bybit,okx,binance)
#   SPARKLINES              -> 1 para gerar sparkline png em /start (requer matplotlib); 0 desliga
#   NEWS_SOURCES            -> CSV de feeds RSS (default: coindesk,theblock)
#   OPENAI_API_KEY          -> opcional; habilita /voice (áudio -> texto) se VOICE=1
#   VOICE                   -> 1 habilita rota /voice (experimental)
# =============================================================================

from __future__ import annotations

import os, math, io, asyncio, time, contextlib, sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware

import httpx
import asyncpg

from loguru import logger

# matplotlib é opcional — só importamos se SPARKLINES=1
SPARKLINES = os.getenv("SPARKLINES", "0") == "1"
if SPARKLINES:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

# CCXT (usar exchanges públicas sem credenciais)
import ccxt.async_support as ccxt

VERSION = "v6.0.13-hotfix1"

# --------- Utilidades ---------------------------------------------------------

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def short_ts(dt: Optional[datetime]=None) -> str:
    dt = dt or utcnow()
    return dt.strftime("%Y-%m-%d %H:%M")

def fmt_price(x: Optional[float]) -> str:
    if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
        return "—"
    if x >= 1000:
        return f"${x:,.2f}".replace(",", ".")
    return f"${x:,.2f}"

def percent(a: Optional[float]) -> str:
    if a is None or math.isnan(a):
        return "—"
    s = f"{a:+.2f}%"
    return s

def round_step(symbol: str) -> float:
    if "BTC" in symbol:
        return 100.0
    if "ETH" in symbol:
        return 10.0
    return 1.0

def round_level(x: float, step: float) -> float:
    if x is None or math.isnan(x) or math.isinf(x) or step <= 0:
        return 0.0
    return round(x / step) * step

def line_count_of_this_file() -> int:
    try:
        with open(__file__, "r", encoding="utf-8") as f:
            return sum(1 for _ in f)
    except Exception:
        return -1

# --------- DB -----------------------------------------------------------------

DATABASE_URL = os.getenv("DATABASE_URL", "")
POOL: Optional[asyncpg.pool.Pool] = None

NEWS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS news_items (
    id SERIAL PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL DEFAULT now(),
    source TEXT NOT NULL,
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS news_ts_idx ON news_items(ts DESC);
CREATE INDEX IF NOT EXISTS news_source_idx ON news_items(source);
"""

async def db_connect():
    global POOL
    if not DATABASE_URL:
        logger.warning("DATABASE_URL não definido — seguindo sem DB.")
        return
    if POOL is None:
        POOL = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)
        async with POOL.acquire() as c:
            await c.execute(NEWS_TABLE_SQL)

async def db_close():
    global POOL
    if POOL is not None:
        await POOL.close()
        POOL = None

async def news_insert_many(items: List[Tuple[datetime, str, str, str]]):
    if not POOL or not items:
        return
    sql = """
    INSERT INTO news_items (ts, source, title, url)
    SELECT x.ts, x.source, x.title, x.url
    FROM jsonb_to_recordset($1::jsonb) AS x(ts timestamptz, source text, title text, url text)
    ON CONFLICT DO NOTHING;
    """
    payload = [{"ts": ts.isoformat(), "source": src, "title": ttl, "url": url} for ts, src, ttl, url in items]
    async with POOL.acquire() as c:
        await c.execute(sql, json.dumps(payload))

async def news_recent(hours=12, limit=6) -> List[Tuple[datetime, str, str, str]]:
    if not POOL:
        return []
    sql = """
    SELECT ts, source, title, url
    FROM news_items
    WHERE ts >= (now() - $1::interval)
    ORDER BY ts DESC
    LIMIT $2
    """
    async with POOL.acquire() as c:
        rows = await c.fetch(sql, f"{hours} hours", limit)
    out = []
    for r in rows:
        out.append((r["ts"], r["source"], r["title"], r["url"]))
    return out

# --------- News Ingest (Light) ------------------------------------------------

import json
from bs4 import BeautifulSoup
import lxml

DEFAULT_FEEDS = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://www.theblock.co/rss.xml",
]

async def fetch_feed(client: httpx.AsyncClient, url: str) -> List[Tuple[datetime, str, str, str]]:
    out = []
    try:
        r = await client.get(url, timeout=10)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml-xml")
        for item in soup.find_all("item")[:10]:
            title = (item.title.text or "").strip()
            link = (item.link.text or "").strip()
            pub = item.find("pubDate")
            if pub and pub.text:
                try:
                    ts = datetime.strptime(pub.text[:25], "%a, %d %b %Y %H:%M:%S").replace(tzinfo=timezone.utc)
                except Exception:
                    ts = utcnow()
            else:
                ts = utcnow()
            if title and link:
                out.append((ts, url.split("//")[1].split("/")[0], title, link))
    except Exception as e:
        logger.warning(f"feed fail {url}: {e}")
    return out

async def ingest_news_light():
    feeds = [x.strip() for x in os.getenv("NEWS_SOURCES", "").split(",") if x.strip()] or DEFAULT_FEEDS
    if not POOL:
        return
    async with httpx.AsyncClient() as client:
        tasks = [fetch_feed(client, f) for f in feeds]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    rows: List[Tuple[datetime, str, str, str]] = []
    for r in results:
        if isinstance(r, list):
            rows.extend(r)
    if rows:
        await news_insert_many(rows)

# --------- Mercado / CCXT -----------------------------------------------------

DEFAULT_PROVIDERS = [x.strip() for x in (os.getenv("PROVIDERS", "bybit,okx,binance").split(",")) if x.strip()]

async def ohlcv_any(symbol="BTC/USDT", since_ms=None, limit=200, timeframe="15m") -> List[List[float]]:
    """
    Tenta em ordem os providers da lista, com rotinas async.
    Fecha SEMPRE a exchange no finally (hotfix Unclosed session).
    """
    providers = DEFAULT_PROVIDERS
    for exname in providers:
        ex = None
        try:
            ex = getattr(ccxt, exname)({"enableRateLimit": True})
            await ex.load_markets()
            data = await ex.fetch_ohlcv(symbol, timeframe=timeframe, since=since_ms, limit=limit)
            return data or []
        except Exception as e:
            logger.warning(f"ohlcv {exname} fail: {e}")
        finally:
            with contextlib.suppress(Exception):
                if ex is not None:
                    await ex.close()
    return []

async def ticker_any(symbol="BTC/USDT"):
    providers = DEFAULT_PROVIDERS
    for exname in providers:
        ex = None
        try:
            ex = getattr(ccxt, exname)({"enableRateLimit": True})
            await ex.load_markets()
            t = await ex.fetch_ticker(symbol)
            return t
        except Exception as e:
            logger.warning(f"ticker {exname} fail: {e}")
        finally:
            with contextlib.suppress(Exception):
                if ex is not None:
                    await ex.close()
    return {}

def pct_change(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None:
        return None
    if b == 0:
        return None
    return (a/b - 1.0) * 100.0

async def build_snapshot() -> Dict[str, Any]:
    now = utcnow()
    start_48h = now - timedelta(hours=48)
    since_ms = int(start_48h.timestamp() * 1000)

    # BTC
    btc_ticker = await ticker_any("BTC/USDT")
    eth_ticker = await ticker_any("ETH/USDT")

    btc_price = btc_ticker.get("last") or btc_ticker.get("close")
    eth_price = eth_ticker.get("last") or eth_ticker.get("close")
    pair = (eth_price or 0) / (btc_price or 1) if btc_price else None

    # 48h OHLCV para níveis
    btc_ohl = await ohlcv_any("BTC/USDT", since_ms=since_ms, limit=200, timeframe="30m")
    eth_ohl = await ohlcv_any("ETH/USDT", since_ms=since_ms, limit=200, timeframe="30m")

    def extr(ohl):
        if not ohl:
            return None, None, []
        highs = [x[2] for x in ohl if x and len(x) >= 4]
        lows  = [x[3] for x in ohl if x and len(x) >= 4]
        series = [x[4] for x in ohl if x and len(x) >= 5]
        if not highs or not lows or not series:
            return None, None, series
        return max(highs), min(lows), series

    btc_hi, btc_lo, btc_series = extr(btc_ohl)
    eth_hi, eth_lo, eth_series = extr(eth_ohl)

    # variações 8h/12h (aproxima: últimos n candles)
    def changes(series: List[float], tf_minutes=30) -> Tuple[Optional[float], Optional[float]]:
        if not series:
            return None, None
        def lookback_minutes(p):
            n = int(p / tf_minutes)
            n = max(1, min(len(series)-1, n))
            return n
        lb8  = lookback_minutes(8*60)
        lb12 = lookback_minutes(12*60)
        last = series[-1]
        p8   = series[-lb8]
        p12  = series[-lb12]
        return pct_change(last, p8), pct_change(last, p12)

    btc_ch8, btc_ch12 = changes(btc_series)
    eth_ch8, eth_ch12 = changes(eth_series)

    # níveis dinâmicos
    def levels(sym: str, hi: Optional[float], lo: Optional[float], last: Optional[float]):
        step = round_step(sym)
        if hi and lo and last:
            mid = (hi + lo) / 2.0
            sups = [round_level(lo, step), round_level(mid - step, step)]
            ress = [round_level(mid + step, step), round_level(hi, step)]
            # unique + sorted
            sups = sorted(set([x for x in sups if x > 0]))
            ress = sorted(set([x for x in ress if x > 0]))
        else:
            sups = []
            ress = []
        return sups, ress

    btc_sups, btc_ress = levels("BTCUSDT", btc_hi, btc_lo, btc_price)
    eth_sups, eth_ress = levels("ETHUSDT", eth_hi, eth_lo, eth_price)

    # relação ETH/BTC variação 8h/12h (aprox pela própria série close)
    rel8, rel12 = None, None
    if btc_series and eth_series and len(btc_series) == len(eth_series):
        pairs = [ (e/(b or 1)) if b else None for e,b in zip(eth_series, btc_series)]
        if pairs and None not in pairs:
            rel8, rel12 = changes(pairs)

    return {
        "ts": now.isoformat(),
        "btc": {
            "price": btc_price,
            "hi48": btc_hi, "lo48": btc_lo,
            "series_24h": btc_series[-48:] if btc_series else [],
            "ch8": btc_ch8, "ch12": btc_ch12,
            "sups": btc_sups, "ress": btc_ress,
        },
        "eth": {
            "price": eth_price,
            "hi48": eth_hi, "lo48": eth_lo,
            "series_24h": eth_series[-48:] if eth_series else [],
            "ch8": eth_ch8, "ch12": eth_ch12,
            "sups": eth_sups, "ress": eth_ress,
        },
        "pair": pair, "pair_ch8": rel8, "pair_ch12": rel12,
    }

# --------- Sparkline -----------------------------------------------------------

def make_sparkline_png(series: List[float]) -> Optional[bytes]:
    if not SPARKLINES or not series or len(series) < 3:
        return None
    try:
        fig = plt.figure(figsize=(3.2, 0.7), dpi=200)
        ax = fig.add_subplot(111)
        ax.plot(series)  # sem cor fixa (deixe o default)
        ax.set_axis_off()
        buf = io.BytesIO()
        fig.tight_layout(pad=0)
        plt.savefig(buf, format="png", bbox_inches="tight", pad_inches=0)
        plt.close(fig)
        buf.seek(0)
        return buf.read()
    except Exception as e:
        logger.warning(f"sparkline fail: {e}")
        return None

# --------- Telegram ------------------------------------------------------------

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
HOST_URL  = os.getenv("HOST_URL", "").strip()
WEBHOOK_AUTO = os.getenv("WEBHOOK_AUTO", "0") == "1"

TG_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}" if BOT_TOKEN else ""

async def tg(method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if not TG_BASE:
        return {"ok": False, "error": "BOT_TOKEN ausente"}
    url = f"{TG_BASE}/{method}"
    async with httpx.AsyncClient() as client:
        r = await client.post(url, json=payload, timeout=20)
        try:
            return r.json()
        except Exception:
            return {"ok": False, "status": r.status_code, "text": r.text}

async def send_tg_text(chat_id: int, text: str, parse_mode: str="HTML"):
    return await tg("sendMessage", {"chat_id": chat_id, "text": text, "parse_mode": parse_mode})

async def send_tg_photo(chat_id: int, png: bytes, caption: Optional[str]=None):
    if not TG_BASE:
        return {"ok": False, "error": "BOT_TOKEN ausente"}
    url = f"{TG_BASE}/sendPhoto"
    files = {"photo": ("spark.png", png, "image/png")}
    data = {"chat_id": str(chat_id)}
    if caption:
        data["caption"] = caption
        data["parse_mode"] = "HTML"
    async with httpx.AsyncClient() as client:
        r = await client.post(url, data=data, files=files, timeout=30)
        try:
            return r.json()
        except Exception:
            return {"ok": False, "status": r.status_code, "text": r.text}

def balloon_footer() -> str:
    return f"\n— {VERSION} • {line_count_of_this_file()} linhas • {utcnow().strftime('%Y-%m-%d %H:%M')} UTC"

def fmt_levels(sups: List[float], ress: List[float]) -> str:
    def joinv(v): return " / ".join([f"{int(x):,}".replace(",", ".") if x>=1000 else f"{x:.0f}" for x in v])
    return f"Suportes: {joinv(sups) or '—'} | Resist: {joinv(ress) or '—'}"

def analysis_lines(s: Dict[str, Any], label: str) -> str:
    ch8 = s.get("ch8"); ch12 = s.get("ch12")
    sups = s.get("sups", []); ress = s.get("ress", [])
    lines = []
    # Linha 1: variações
    if ch8 is not None or ch12 is not None:
        lines.append(f"{label}: {percent(ch8)} (8h), {percent(ch12)} (12h).")
    else:
        lines.append(f"{label}: —.")
    # Linha 2: níveis
    lines.append(f"Níveis: {fmt_levels(sups, ress)}")
    # Linha 3: ação
    action = "Ação: operar rompimentos válidos com pullbacks; defesa em perda de suportes."
    lines.append(action)
    return "\n".join(lines)

async def build_pulse_text() -> str:
    snap = await build_snapshot()
    btc = snap.get("btc", {}); eth = snap.get("eth", {})
    p  = snap.get("pair")
    rel8, rel12 = snap.get("pair_ch8"), snap.get("pair_ch12")

    head = f"Pulse…….. 🕒 {short_ts()} • {VERSION}\nETH {fmt_price(eth.get('price'))}\nBTC {fmt_price(btc.get('price'))}\nETH/BTC {p:.5f}" if p else f"Pulse…….. 🕒 {short_ts()} • {VERSION}"

    a_lines = [
        "<b>ANÁLISE:</b>",
        analysis_lines(eth, "ETH"),
        analysis_lines(btc, "BTC"),
        f"Relação ETH/BTC: {percent(rel8)} (8h), {percent(rel12)} (12h).",
    ]

    # news recentes (se houver DB e ingest)
    news = []
    try:
        news = await news_recent(hours=12, limit=6)
    except Exception as e:
        logger.warning(f"news_recent fail: {e}")

    nf = ""
    if news:
        nf = "\n\n<b>FONTES (últimas 12h):</b>\n" + "\n".join([
            f"• {dt.strftime('%H:%M')} {src} — {ttl}"
            for dt, src, ttl, url in news
        ])

    return f"{head}\n\n" + "\n".join(a_lines) + nf + balloon_footer()

async def handle_start(chat_id: int):
    snap = await build_snapshot()
    eth = snap.get("eth") or {}
    series = eth.get("series_24h", []) if isinstance(eth, dict) else []
    caption = f"Bem-vindo! 👋\nUse /pulse, /eth, /btc, /panel.\n\n{balloon_footer()}"
    if SPARKLINES:
        png = make_sparkline_png(series)
        if png:
            await send_tg_photo(chat_id, png, caption=caption)
            return
    await send_tg_text(chat_id, caption)

async def handle_pulse(chat_id: int):
    txt = await build_pulse_text()
    await send_tg_text(chat_id, txt)

async def handle_asset(chat_id: int, which: str):
    # which in {"BTC","ETH"}
    snap = await build_snapshot()
    s = snap.get(which.lower(), {})
    price = s.get("price")
    lines = [
        f"{which} {fmt_price(price)}",
        analysis_lines(s, which),
    ]
    await send_tg_text(chat_id, "\n".join(lines) + balloon_footer())

# --------- FastAPI ------------------------------------------------------------

app = FastAPI(title="StarkRadar Bot API", version=VERSION)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

@app.on_event("startup")
async def _startup():
    logger.info(f"Starting {VERSION}")
    await db_connect()
    # tenta ingest leve de news (não bloqueante)
    asyncio.create_task(ingest_news_light())
    # auto webhook
    if WEBHOOK_AUTO and BOT_TOKEN and HOST_URL:
        try:
            await tg("setWebhook", {"url": f"{HOST_URL}/webhook"})
        except Exception as e:
            logger.warning(f"auto webhook fail: {e}")

@app.on_event("shutdown")
async def _shutdown():
    await db_close()

@app.get("/", response_class=PlainTextResponse)
async def root():
    return f"OK {VERSION} — {short_ts()} UTC"

@app.get("/status")
async def status():
    return {
        "ok": True,
        "version": VERSION,
        "linecount": line_count_of_this_file(),
        "db": bool(POOL),
    }

# --------- Admin --------------------------------------------------------------

@app.get("/admin/ping/telegram")
async def admin_ping():
    if not BOT_TOKEN:
        return {"ok": False, "error": "BOT_TOKEN ausente"}
    me = await tg("getMe", {})
    return {"ok": True, "me": me}

@app.get("/admin/webhook/set")
async def admin_webhook_set():
    if not BOT_TOKEN or not HOST_URL:
        return {"ok": False, "error": "BOT_TOKEN/HOST_URL ausente"}
    res = await tg("setWebhook", {"url": f"{HOST_URL}/webhook"})
    return {"ok": True, "result": res}

@app.post("/admin/db/migrate")
async def admin_db_migrate():
    await db_connect()
    return {"ok": True, "msg": "migrations ensured"}

# --------- Webhook ------------------------------------------------------------

@app.post("/webhook")
async def webhook_root(request: Request):
    try:
        body = await request.json()
    except Exception:
        return {"ok": False}
    msg = (body.get("message") or body.get("edited_message") or {})
    chat = msg.get("chat", {})
    chat_id = chat.get("id")
    text = msg.get("text") or ""

    if not chat_id:
        return {"ok": True}

    try:
        cmd = text.strip().lower()
        if cmd.startswith("/start"):
            await handle_start(chat_id);  return {"ok": True}
        if cmd.startswith("/pulse"):
            await handle_pulse(chat_id);  return {"ok": True}
        if cmd.startswith("/btc"):
            await handle_asset(chat_id, "BTC"); return {"ok": True}
        if cmd.startswith("/eth"):
            await handle_asset(chat_id, "ETH"); return {"ok": True}
        if cmd.startswith("/help") or cmd.startswith("/panel"):
            await send_tg_text(chat_id, "Comandos: /start /pulse /btc /eth /help" + balloon_footer()); return {"ok": True}

        # default: eco orientado
        await send_tg_text(chat_id, "Use /pulse, /eth, /btc." + balloon_footer()); return {"ok": True}

    except Exception as e:
        logger.exception("webhook error")
        await send_tg_text(chat_id, f"Erro: {e}")
        return {"ok": False, "error": str(e)}

# --------- Run (local) --------------------------------------------------------

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)

# ============================== FIM DO CÓDIGO ==============================
# Contagem real de linhas e versão (no /status e rodapé dos balões).
# Linha de controle: NÃO REMOVER.

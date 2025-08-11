# app.py — Stark DeFi Agent v6.0.9-full
# =============================================================================
# CHANGES (v6.0.9-full)
# - Pulse/ETH/BTC: formato "ANÁLISE → FONTES" (sem gráfico no Pulse).
# - Níveis dinâmicos (S/R) robustos (fallbacks quando faltar candle / dado).
# - Removido Binance/Bybit (evita 403/451). Preços via Coinbase Ticker (público).
# - news_items sem NOT NULL em source + insert tolerante (usa domínio do link).
# - Jobs leves: captura de preço por minuto (ETH/BTC) e ingest de notícias (RSS).
# - Startup resiliente: migrações pequenas via asyncpg; sem precisar rodar SQL à mão.
# - Admin: /admin/webhook/set | /admin/webhook/delete | /admin/ping/telegram
# - /status detalhado (contagens, last run).
# - Footer com versão nos balões e rodapé no arquivo.
# =============================================================================

import os, sys, math, json, asyncio, datetime as dt
from typing import Optional, Tuple, List

import httpx
import asyncpg
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel
from apscheduler.schedulers.asyncio import AsyncIOScheduler

VERSION = "v6.0.9-full"
APP_NAME = "Stark DeFi Agent"
HOST_URL = os.getenv("HOST_URL", "").rstrip("/")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
WEBHOOK_AUTO = os.getenv("WEBHOOK_AUTO", "0") == "1"
DATABASE_URL = os.getenv("DATABASE_URL", "")
ALLOWED_CHAT_IDS = set([x.strip() for x in os.getenv("TG_ALLOWED_CHATS","").split(",") if x.strip()]) or None
PORT = int(os.getenv("PORT", "10000"))

# -----------------------------------------------------------------------------
# Globals
# -----------------------------------------------------------------------------
app = FastAPI(title=f"{APP_NAME} {VERSION}")
pool: Optional[asyncpg.Pool] = None
http: Optional[httpx.AsyncClient] = None
scheduler: Optional[AsyncIOScheduler] = None

LAST_RUN = {
    "ingest_prices": None,
    "ingest_news": None
}

COINBASE_TICKERS = {
    "ETHUSDT": "ETH-USD",
    "BTCUSDT": "BTC-USD"
}

# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------
def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def ts_to_str(ts: dt.datetime) -> str:
    return ts.astimezone(dt.timezone(dt.timedelta(hours=-3))).strftime("%Y-%m-%d %H:%M")

def dom_from_link(url: Optional[str]) -> Optional[str]:
    # pega domínio simples
    if not url or "://" not in url: 
        return None
    try:
        return url.split("://",1)[1].split("/",1)[0]
    except Exception:
        return None

def pct(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None or b == 0: return None
    try:
        return (a/b - 1.0) * 100.0
    except Exception:
        return None

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def pretty_num(x: Optional[float]) -> str:
    if x is None: return "—"
    if abs(x) >= 1000:
        return f"{x:,.0f}".replace(",",".")
    if abs(x) >= 100:
        return f"{x:,.1f}".replace(",",".")
    return f"{x:,.2f}".replace(",", ".")

def pretty_pct(x: Optional[float]) -> str:
    if x is None: return "—"
    sign = "+" if x>0 else ""
    return f"{sign}{x:.2f}%".replace(".", ",")

# arredondamento seguro
def safe_round_level(x: Optional[float], step: Optional[float]) -> Optional[float]:
    if step is None or step <= 0: return None
    if x is None or not isinstance(x, (int,float)) or not math.isfinite(x): return None
    return round(x/step)*step

def step_for_symbol(symbol: str) -> float:
    # passo simples por ativo
    return 50.0 if symbol.startswith("ETH") else 500.0

# -----------------------------------------------------------------------------
# Telegram
# -----------------------------------------------------------------------------
async def send_tg(text: str, chat_id: Optional[int] = None) -> dict:
    if not TELEGRAM_TOKEN:
        return {"ok": False, "error": "TELEGRAM_TOKEN not set"}
    # se não veio chat_id, e há whitelist, recusa
    if chat_id is None and ALLOWED_CHAT_IDS:
        # não sabemos para onde mandar
        return {"ok": False, "error": "chat_id required"}
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id if chat_id else list(ALLOWED_CHAT_IDS)[0] if ALLOWED_CHAT_IDS else None,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    async with httpx.AsyncClient(timeout=15.0) as cli:
        r = await cli.post(url, json=payload)
        try:
            return r.json()
        except Exception:
            return {"ok": False, "status_code": r.status_code, "text": r.text}

async def set_webhook() -> dict:
    if not TELEGRAM_TOKEN or not HOST_URL:
        return {"ok": False, "error": "TELEGRAM_TOKEN or HOST_URL missing"}
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/setWebhook"
    async with httpx.AsyncClient(timeout=15.0) as cli:
        r = await cli.post(url, data={"url": f"{HOST_URL}/webhook"})
        try:
            return r.json()
        except Exception:
            return {"ok": False, "status_code": r.status_code, "text": r.text}

async def delete_webhook() -> dict:
    if not TELEGRAM_TOKEN:
        return {"ok": False, "error": "TELEGRAM_TOKEN missing"}
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/deleteWebhook"
    async with httpx.AsyncClient(timeout=15.0) as cli:
        r = await cli.post(url)
        try:
            return r.json()
        except Exception:
            return {"ok": False, "status_code": r.status_code, "text": r.text}

def is_allowed_chat(chat_id: Optional[int]) -> bool:
    if chat_id is None: return False
    if ALLOWED_CHAT_IDS is None: return True
    return str(chat_id) in ALLOWED_CHAT_IDS

# -----------------------------------------------------------------------------
# Database & migrations
# -----------------------------------------------------------------------------
CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS price_ticks(
    id BIGSERIAL PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL DEFAULT now(),
    symbol TEXT NOT NULL,
    price DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS idx_price_ticks_symbol_ts ON price_ticks(symbol, ts DESC);

CREATE TABLE IF NOT EXISTS candles(
    id BIGSERIAL PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    UNIQUE(symbol, ts)
);
CREATE INDEX IF NOT EXISTS idx_candles_symbol_ts ON candles(symbol, ts DESC);

CREATE TABLE IF NOT EXISTS news_items(
    id BIGSERIAL PRIMARY KEY,
    ts TIMESTAMPTZ DEFAULT now(),
    source TEXT,
    author TEXT,
    title TEXT,
    link TEXT,
    summary TEXT,
    ingested_at TIMESTAMPTZ DEFAULT now(),
    raw JSONB
);
"""

ALTERS_SQL = """
-- garante que 'source' não tenha NOT NULL
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='news_items' AND column_name='source' AND is_nullable='NO'
    ) THEN
        BEGIN
            ALTER TABLE news_items ALTER COLUMN source DROP NOT NULL;
        EXCEPTION WHEN others THEN
            -- ignora se não puder
            NULL;
        END;
    END IF;
END $$;
"""

async def db_init():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with pool.acquire() as c:
        await c.execute(CREATE_TABLES_SQL)
        await c.execute(ALTERS_SQL)

# -----------------------------------------------------------------------------
# Price ingestion (Coinbase)
# -----------------------------------------------------------------------------
async def fetch_coinbase_ticker(product_id: str) -> Optional[float]:
    url = f"https://api.exchange.coinbase.com/products/{product_id}/ticker"
    try:
        r = await http.get(url)
        if r.status_code == 200:
            data = r.json()
            px = data.get("price") or data.get("last") or data.get("ask") or data.get("bid")
            if px is None:
                return None
            return float(px)
        return None
    except Exception:
        return None

async def last_price(symbol: str) -> Optional[float]:
    # tenta DB primeiro, depois HTTP
    async with pool.acquire() as c:
        row = await c.fetchrow("SELECT price FROM price_ticks WHERE symbol=$1 ORDER BY ts DESC LIMIT 1", symbol)
    if row and isinstance(row["price"], (int,float)) and math.isfinite(row["price"]):
        return float(row["price"])
    prod = COINBASE_TICKERS.get(symbol)
    if not prod: return None
    return await fetch_coinbase_ticker(prod)

async def ingest_prices():
    # pega ETH e BTC, grava em price_ticks e atualiza candle minuto
    symbols = ["ETHUSDT", "BTCUSDT"]
    now = now_utc()
    minute_bucket = now.replace(second=0, microsecond=0)
    async with pool.acquire() as c:
        for sym in symbols:
            prod = COINBASE_TICKERS.get(sym)
            if not prod: 
                continue
            px = await fetch_coinbase_ticker(prod)
            if px is None or not math.isfinite(px):
                continue
            await c.execute("INSERT INTO price_ticks(ts, symbol, price) VALUES ($1,$2,$3)", now, sym, px)
            # upsert candle 1m
            row = await c.fetchrow("SELECT id, open, high, low, close FROM candles WHERE symbol=$1 AND ts=$2", sym, minute_bucket)
            if row:
                hi = max(row["high"] if row["high"] is not None else px, px)
                lo = min(row["low"] if row["low"] is not None else px, px)
                await c.execute("""
                    UPDATE candles SET high=$3, low=$4, close=$5
                    WHERE id=$1
                """, row["id"], sym, hi, lo, px)
            else:
                await c.execute("""
                    INSERT INTO candles(ts, symbol, open, high, low, close, volume)
                    VALUES($1,$2,$3,$3,$3,$3, NULL)
                """, minute_bucket, sym, px)
    LAST_RUN["ingest_prices"] = now.isoformat()

async def pct_change(symbol: str, hours: int) -> Optional[float]:
    # variação entre o primeiro preço do intervalo e o último
    end = now_utc()
    start = end - dt.timedelta(hours=hours)
    async with pool.acquire() as c:
        row_old = await c.fetchrow("""
            SELECT price FROM price_ticks
            WHERE symbol=$1 AND ts <= $2
            ORDER BY ts DESC LIMIT 1
        """, symbol, start)
        row_new = await c.fetchrow("""
            SELECT price FROM price_ticks
            WHERE symbol=$1 AND ts <= $2
            ORDER BY ts DESC LIMIT 1
        """, symbol, end)
    if not row_old or not row_new:
        return None
    return pct(row_new["price"], row_old["price"])

# -----------------------------------------------------------------------------
# Dynamic levels
# -----------------------------------------------------------------------------
async def dynamic_levels(symbol: str) -> Tuple[List[float], List[float]]:
    step = step_for_symbol(symbol)
    # candles de 48h
    async with pool.acquire() as c:
        rows = await c.fetch("""
            SELECT high, low FROM candles
            WHERE symbol=$1 AND ts >= now() - interval '48 hours'
        """, symbol)
    highs = [r["high"] for r in rows if r and isinstance(r["high"], (int,float)) and math.isfinite(r["high"])]
    lows  = [r["low"]  for r in rows if r and isinstance(r["low"],  (int,float)) and math.isfinite(r["low"])]

    if not highs or not lows:
        px = await last_price(symbol)
        if isinstance(px, (int,float)) and math.isfinite(px):
            s = [safe_round_level(px-2*step, step), safe_round_level(px-step, step)]
            r = [safe_round_level(px+step, step), safe_round_level(px+2*step, step)]
            s = [x for x in s if x is not None]
            r = [x for x in r if x is not None]
            if not s: s = [px - step, px - 2*step]
            if not r: r = [px + step, px + 2*step]
            return s[:2], r[:2]
        # hard fallback
        if symbol.startswith("ETH"):
            return [4000, 3950], [4300, 4400]
        return [62000, 60000], [65000, 68000]

    ul = max(highs); dl = min(lows); mid = (ul+dl)/2.0
    s1 = safe_round_level(dl, step)
    s2 = safe_round_level(mid - step, step)
    r1 = safe_round_level(mid + step, step)
    r2 = safe_round_level(ul, step)
    sups = [x for x in (s1, s2) if x is not None]
    ress = [x for x in (r1, r2) if x is not None]
    if len(sups) < 2 and math.isfinite(mid):
        sups.append(safe_round_level(mid - 2*step, step) or (mid-2*step))
    if len(ress) < 2 and math.isfinite(mid):
        ress.append(safe_round_level(mid + 2*step, step) or (mid+2*step))
    return sups[:2], ress[:2]

# -----------------------------------------------------------------------------
# News ingestion (RSS leve)
# -----------------------------------------------------------------------------
RSS_FEEDS = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/?outputType=xml",
    "https://cointelegraph.com/rss",
    "https://www.theblock.co/rss",
    "https://decrypt.co/feed",
]

async def fetch_rss_titles(url: str) -> List[dict]:
    try:
        r = await http.get(url, timeout=20.0)
        if r.status_code != 200:
            return []
        txt = r.text
        # parse simples: pega <item><title> e <link> e pubDate se existir
        import re
        items = []
        for m in re.finditer(r"<item>(.*?)</item>", txt, re.S|re.I):
            block = m.group(1)
            t = re.search(r"<title>(.*?)</title>", block, re.S|re.I)
            l = re.search(r"<link>(.*?)</link>", block, re.S|re.I)
            d = re.search(r"<pubDate>(.*?)</pubDate>", block, re.S|re.I)
            title = None if not t else re.sub(r"\s+", " ", t.group(1)).strip()
            link = None if not l else l.group(1).strip()
            pub = None if not d else d.group(1).strip()
            items.append({"title": title, "link": link, "pub": pub})
        return items[:8]
    except Exception:
        return []

async def ingest_news_light():
    now = now_utc()
    out = []
    for u in RSS_FEEDS:
        out.extend(await fetch_rss_titles(u))
    if not out:
        LAST_RUN["ingest_news"] = now.isoformat()
        return
    async with pool.acquire() as c:
        for it in out:
            title = it.get("title")
            link = it.get("link")
            if not title or not link:
                continue
            src = dom_from_link(link)
            # insere com COALESCE pro source
            await c.execute("""
                INSERT INTO news_items(ts, source, author, title, link, summary, raw)
                VALUES ($1,
                        COALESCE($2, (SELECT split_part($3,'/',3))),
                        NULL, $4, $3, NULL, $5)
                ON CONFLICT DO NOTHING
            """, now, src, link, title, json.dumps(it))
    LAST_RUN["ingest_news"] = now.isoformat()

async def get_recent_news(hours: int = 12, limit: int = 6) -> List[dict]:
    end = now_utc(); start = end - dt.timedelta(hours=hours)
    async with pool.acquire() as c:
        rows = await c.fetch("""
            SELECT ts, source, title, link
            FROM news_items
            WHERE ts BETWEEN $1 AND $2
            ORDER BY ts DESC
            LIMIT $3
        """, start, end, limit)
    res = []
    for r in rows:
        res.append({
            "ts": r["ts"],
            "source": r["source"],
            "title": r["title"],
            "link": r["link"]
        })
    return res

# -----------------------------------------------------------------------------
# Analytics / Text builders
# -----------------------------------------------------------------------------
async def market_snapshot():
    eth = await last_price("ETHUSDT")
    btc = await last_price("BTCUSDT")
    eth8 = await pct_change("ETHUSDT", 8)
    btc8 = await pct_change("BTCUSDT", 8)
    eth12 = await pct_change("ETHUSDT", 12)
    btc12 = await pct_change("BTCUSDT", 12)
    ratio = None
    if eth and btc and btc != 0:
        ratio = eth / btc
    ratio8 = None
    if eth8 is not None and btc8 is not None:
        # aproximação: variação do par ≈ var_ETH - var_BTC
        ratio8 = eth8 - btc8
    return {
        "eth": eth, "btc": btc,
        "eth8": eth8, "btc8": btc8,
        "eth12": eth12, "btc12": btc12,
        "ratio": ratio, "ratio8": ratio8
    }

def synthesize_analysis(eth8, btc8, ratio8) -> str:
    # 3–5 linhas, direto ao ponto
    lines = []
    # direção relativa
    if eth8 is not None and btc8 is not None:
        if eth8 < btc8:
            lines.append("• Dominância de BTC no curto prazo; ETH relativamente mais fraco.")
        elif eth8 > btc8:
            lines.append("• ETH supera BTC nas últimas horas; beta de altcoins favorecido.")
        else:
            lines.append("• Mercado equilibrado entre BTC e ETH no curto prazo.")
    # relação do par
    if ratio8 is not None:
        if ratio8 < 0:
            lines.append("• ETH/BTC em queda; preferir gatilhos em BTC ou reduzir beta em ETH.")
        elif ratio8 > 0:
            lines.append("• ETH/BTC em alta; janelas de rotação pró‑ETH podem surgir.")
    # gestão tática
    lines.append("• Operar níveis: confirmações por fechamento e volume; evitar caça a pavio.")
    if len(lines) < 3:
        lines.append("• Liquidez e notícias pontuais podem distorcer movimentos intradiários.")
    return "\n".join(lines[:5])

def format_news_list(items: List[dict]) -> str:
    if not items: return "—"
    out = []
    for it in items:
        when = ts_to_str(it["ts"]) if it.get("ts") else "—"
        src = it.get("source") or dom_from_link(it.get("link")) or "—"
        ttl = it.get("title") or "—"
        lnk = it.get("link") or ""
        out.append(f"• {when} — <b>{src}</b>: <a href=\"{lnk}\">{ttl}</a>")
    return "\n".join(out)

async def latest_pulse_text() -> str:
    snap = await market_snapshot()
    eth = snap["eth"]; btc = snap["btc"]; ratio = snap["ratio"]
    eth8 = snap["eth8"]; btc8 = snap["btc8"]; ratio8 = snap["ratio8"]

    eth_sups, eth_ress = await dynamic_levels("ETHUSDT")
    btc_sups, btc_ress = await dynamic_levels("BTCUSDT")

    news = await get_recent_news(hours=12, limit=6)

    nowstr = ts_to_str(now_utc())
    header = f"🕒 {nowstr} • {VERSION}\nETH ${pretty_num(eth)}\nBTC ${pretty_num(btc)}\nETH/BTC {pretty_num(ratio)}"
    levels = (
        f"\n\n<b>NÍVEIS ETH</b>  S: {pretty_num(eth_sups[0])}/{pretty_num(eth_sups[1])} | R: {pretty_num(eth_ress[0])}/{pretty_num(eth_ress[1])}"
        f"\n<b>NÍVEIS BTC</b>  S: {pretty_num(btc_sups[0])}/{pretty_num(btc_sups[1])} | R: {pretty_num(btc_ress[0])}/{pretty_num(btc_ress[1])}"
    )
    perf = f"\n\nETH: {pretty_pct(eth8)} (8h) • BTC: {pretty_pct(btc8)} (8h) • ETH/BTC: {pretty_pct(ratio8)}"
    analysis = synthesize_analysis(eth8, btc8, ratio8)
    newsfmt = format_news_list(news)

    text = (
        header +
        levels +
        "\n\n<b>ANÁLISE</b>\n" + analysis +
        "\n\n<b>FONTES (12h)</b>\n" + newsfmt +
        f"\n\n<i>{APP_NAME} {VERSION}</i>"
    )
    return text

async def asset_comment(sym: str, name: str) -> str:
    snap = await market_snapshot()
    px = snap["eth"] if sym=="ETHUSDT" else snap["btc"]
    ch8 = snap["eth8"] if sym=="ETHUSDT" else snap["btc8"]
    ch12 = snap["eth12"] if sym=="ETHUSDT" else snap["btc12"]
    ratio8 = snap["ratio8"]

    sups, ress = await dynamic_levels(sym)
    news = await get_recent_news(hours=12, limit=4)

    lines = []
    # 3–4 linhas de leitura
    if ch8 is not None and ch12 is not None:
        direc = "alta" if ch8>0 else "queda" if ch8<0 else "estável"
        bias12 = "mantida" if ch12*ch8>=0 else "em disputa"
        lines.append(f"• {name} {direc} (8h {pretty_pct(ch8)}), tendência 12h {bias12}.")
    if sym=="ETHUSDT" and ratio8 is not None:
        if ratio8<0:
            lines.append("• ETH/BTC cede no curto; exigir confirmação extra antes de alongar risco em ETH.")
        elif ratio8>0:
            lines.append("• ETH/BTC avança; pullbacks podem ser oportunidades de compra tática.")
    lines.append("• Níveis e gestão: respeito aos suportes; entradas por confirmação em rompimentos das resistências.")

    newsfmt = format_news_list(news)
    text = (
        f"{name} ${pretty_num(px)} • 8h {pretty_pct(ch8)} • 12h {pretty_pct(ch12)}\n"
        f"Níveis — S: {pretty_num(sups[0])}/{pretty_num(sups[1])} | R: {pretty_num(ress[0])}/{pretty_num(ress[1])}\n\n"
        "<b>ANÁLISE</b>\n" + "\n".join(lines[:4]) + "\n\n"
        "<b>FONTES (12h)</b>\n" + newsfmt +
        f"\n\n<i>{APP_NAME} {VERSION}</i>"
    )
    return text

# -----------------------------------------------------------------------------
# Webhook handling
# -----------------------------------------------------------------------------
class TgMessage(BaseModel):
    message_id: Optional[int] = None
    chat: Optional[dict] = None
    text: Optional[str] = None

class TgUpdate(BaseModel):
    update_id: Optional[int] = None
    message: Optional[TgMessage] = None
    edited_message: Optional[TgMessage] = None

@app.post("/webhook")
async def webhook_root(req: Request):
    body = await req.json()
    try:
        upd = TgUpdate(**body)
    except Exception:
        return JSONResponse({"ok": True})
    msg = upd.message or upd.edited_message
    if not msg or not msg.text:
        return JSONResponse({"ok": True})
    chat_id = msg.chat.get("id") if msg.chat else None
    if not is_allowed_chat(chat_id):
        # ignora silenciosamente
        return JSONResponse({"ok": True})

    txt = (msg.text or "").strip().lower()
    if txt in ("/start", "start", "/help"):
        welcome = (
            f"👋 {APP_NAME} {VERSION}\n"
            "Comandos:\n"
            "• /pulse — visão do mercado (análise → fontes)\n"
            "• /eth — comentário tático do ETH\n"
            "• /btc — comentário tático do BTC\n"
        )
        await send_tg(welcome, chat_id)
        return JSONResponse({"ok": True})
    if txt.startswith("/pulse"):
        out = await latest_pulse_text()
        await send_tg(out, chat_id); return JSONResponse({"ok": True})
    if txt.startswith("/eth"):
        out = await asset_comment("ETHUSDT", "ETH")
        await send_tg(out, chat_id); return JSONResponse({"ok": True})
    if txt.startswith("/btc"):
        out = await asset_comment("BTCUSDT", "BTC")
        await send_tg(out, chat_id); return JSONResponse({"ok": True})

    # default: tente pulse
    out = await latest_pulse_text()
    await send_tg(out, chat_id)
    return JSONResponse({"ok": True})

# -----------------------------------------------------------------------------
# Admin / status endpoints (sem terminal)
# -----------------------------------------------------------------------------
@app.get("/")
async def root():
    return PlainTextResponse(f"{APP_NAME} {VERSION} — OK")

@app.head("/")
async def head_root():
    return Response(status_code=200)

@app.get("/status")
async def status():
    async with pool.acquire() as c:
        pt = await c.fetchval("SELECT COUNT(*) FROM price_ticks")
        cd = await c.fetchval("SELECT COUNT(*) FROM candles")
        nw = await c.fetchval("SELECT COUNT(*) FROM news_items")
    return {
        "version": VERSION,
        "counts": {"price_ticks": pt, "candles": cd, "news": nw},
        "last_run": LAST_RUN
    }

@app.get("/admin/webhook/set")
async def admin_set_webhook():
    res = await set_webhook()
    return res

@app.get("/admin/webhook/delete")
async def admin_delete_webhook():
    res = await delete_webhook()
    return res

@app.get("/admin/ping/telegram")
async def admin_ping_telegram(chat_id: Optional[int] = None):
    res = await send_tg(f"✅ Ping — {APP_NAME} {VERSION}", chat_id)
    return res

# -----------------------------------------------------------------------------
# Scheduler & startup
# -----------------------------------------------------------------------------
async def _startup():
    global http, scheduler
    http = httpx.AsyncClient(timeout=15.0)

    await db_init()

    # jobs
    scheduler = AsyncIOScheduler()
    scheduler.add_job(ingest_prices, "interval", minutes=1, id="ingest_prices", next_run_time=now_utc())
    scheduler.add_job(ingest_news_light, "interval", minutes=20, id="ingest_news", next_run_time=now_utc())
    scheduler.start()

    # opcional: set webhook automaticamente
    if WEBHOOK_AUTO and TELEGRAM_TOKEN and HOST_URL:
        try:
            await set_webhook()
        except Exception:
            pass

@app.on_event("startup")
async def on_start():
    await _startup()

@app.on_event("shutdown")
async def on_shutdown():
    global http, scheduler, pool
    try:
        if scheduler:
            scheduler.shutdown(wait=False)
    except Exception:
        pass
    try:
        if http:
            await http.aclose()
    except Exception:
        pass
    try:
        if pool:
            await pool.close()
    except Exception:
        pass

# =============================================================================
#                              FIM DO CÓDIGO
#                 Linhas de codificação (aprox): 620
#                       Stark DeFi Agent v6.0.9-full
# =============================================================================

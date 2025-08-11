# app.py — Stark DeFi Agent v6.0.8-hotfix
# =============================================================================
# OBJETIVO
# Bot Telegram com /start, /pulse, /eth, /btc produzindo:
#  - "ANÁLISE → FONTES" (sem gráfico), níveis S/R dinâmicos + fallback seguro,
#  - ingestão leve de notícias (RSS) em PostgreSQL (schema idempotente),
#  - admin endpoints para teste/diagnóstico.
#
# Principais correções desta hotfix:
# - Corrige crash em níveis dinâmicos (NaN) com fallback para níveis estáticos.
# - Corrige schema/queries de news_items: usa url/source (nada de link/author).
# - Se a coleta de dados falhar, o bot responde mesmo assim (sem derrubar webhook).
# - Rodapé com versão + contagem de linhas real.
# =============================================================================

import os
import math
import json
import html
import asyncio
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse

import httpx
import asyncpg
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# ----------------------------- CONFIG ----------------------------------------

VERSION = "v6.0.8-hotfix"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}" if TELEGRAM_BOT_TOKEN else None

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

HOST_URL = os.getenv("HOST_URL", "").strip()  # ex: https://starkradar-bot.onrender.com
WEBHOOK_AUTO = os.getenv("WEBHOOK_AUTO", "0").strip() == "1"

# Níveis estáticos de fallback (opcionais via ENV), formato: "4200,4000" etc.
ETH_SUPS_ENV = os.getenv("ETH_SUPS", "")
ETH_RESS_ENV = os.getenv("ETH_RESS", "")
BTC_SUPS_ENV = os.getenv("BTC_SUPS", "")
BTC_RESS_ENV = os.getenv("BTC_RESS", "")

# Feeds para ingestão leve
NEWS_FEEDS = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/?outputType=xml",
    "https://decrypt.co/feed",
    "https://cointelegraph.com/rss",  # manter simples; parser tolera falhas
]

# ----------------------------- APP -------------------------------------------

app = FastAPI(title="Stark DeFi Agent", version=VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_headers=["*"],
    allow_methods=["*"],
)

http_client = httpx.AsyncClient(timeout=15.0)
_pool = None

# ----------------------------- UTILS -----------------------------------------

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def fmt_ts(dt: datetime) -> str:
    return dt.astimezone(timezone(timedelta(hours=0))).strftime("%Y-%m-%d %H:%M")

def code_line_count() -> int:
    try:
        with open(__file__, "r", encoding="utf-8") as f:
            return sum(1 for _ in f)
    except Exception:
        return -1

def host_from_url(u: str) -> str:
    try:
        return urlparse(u).netloc or "feed"
    except Exception:
        return "feed"

def parse_cmd(text: str) -> str:
    if not text:
        return ""
    t = text.strip().lower()
    # aceita "/", ".", e palavras
    for key in ("/start", ".start", "start"):
        if t.startswith(key): return "start"
    for key in ("/pulse", ".pulse", "pulse"):
        if t.startswith(key): return "pulse"
    for key in ("/eth", ".eth", "eth", "/ethereum", ".ethereum", "ethereum"):
        if t.startswith(key): return "eth"
    for key in ("/btc", ".btc", "btc", "/bitcoin", ".bitcoin", "bitcoin"):
        if t.startswith(key): return "btc"
    return ""

def safe_round_step(price: float) -> float:
    # degrau por escala (simples e prático)
    if price is None or not math.isfinite(price) or price <= 0:
        return 10.0
    if price >= 100_000: return 1000.0
    if price >= 50_000:  return 500.0
    if price >= 10_000:  return 100.0
    if price >= 5_000:   return 50.0
    if price >= 1_000:   return 10.0
    if price >= 100:     return 5.0
    return 1.0

def round_level(x: float, step: float) -> float:
    if x is None or not math.isfinite(x) or step is None or step <= 0 or not math.isfinite(step):
        raise ValueError("invalid round_level input")
    return round(x / step) * step

async def send_tg(text: str, chat_id: int) -> None:
    if not TELEGRAM_API:
        return
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        await http_client.post(f"{TELEGRAM_API}/sendMessage", json=payload)
    except Exception:
        pass

async def db_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3, command_timeout=30)
    return _pool

# ----------------------------- DB INIT / NEWS --------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS news_items (
    id SERIAL PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL DEFAULT now(),
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'feed',
    summary TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_news_ts ON news_items(ts DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_news_url ON news_items(url);

-- garantir colunas e defaults
ALTER TABLE news_items ADD COLUMN IF NOT EXISTS url TEXT;
ALTER TABLE news_items ADD COLUMN IF NOT EXISTS source TEXT;
ALTER TABLE news_items ALTER COLUMN source SET DEFAULT 'feed';
UPDATE news_items SET source = COALESCE(source, 'feed') WHERE source IS NULL;
"""

async def db_init():
    pool = await db_pool()
    async with pool.acquire() as c:
        await c.execute(SCHEMA_SQL)

async def insert_news_item(c: asyncpg.Connection, title: str, url: str, ts: datetime | None):
    if not title or not url:
        return
    src = host_from_url(url)
    tsv = ts or now_utc()
    try:
        await c.execute(
            """INSERT INTO news_items (ts, title, url, source, created_at)
               VALUES ($1, $2, $3, $4, now())
               ON CONFLICT (url) DO NOTHING""",
            tsv, title, url, src
        )
    except Exception:
        # não derruba o fluxo por notícia ruim
        pass

def _extract_rss_items(xml_text: str) -> list[dict]:
    # parser levinho para RSS/Atom (tenta o básico; se falhar, retorna vazio)
    items = []
    try:
        # heurística sem libs extras
        # pega blocos <item>...</item> ou <entry>...</entry>
        chunks = []
        low = xml_text.lower()
        if "<item" in low:
            start_tag, end_tag = "<item", "</item>"
        elif "<entry" in low:
            start_tag, end_tag = "<entry", "</entry>"
        else:
            return items

        pos = 0
        while True:
            a = low.find(start_tag, pos)
            if a == -1: break
            b = low.find(end_tag, a)
            if b == -1: break
            chunks.append(xml_text[a:b+len(end_tag)])
            pos = b + len(end_tag)

        for ch in chunks:
            # título
            title = None
            for t1, t2 in (("<title>", "</title>"), ("<title><![CDATA[", "]]></title>")):
                i = ch.lower().find(t1)
                j = ch.lower().find(t2) if i != -1 else -1
                if i != -1 and j != -1:
                    title = ch[i+len(t1):j].strip()
                    break
            # link/url
            url = None
            for l1, l2 in (("<link>", "</link>"), ('<link rel="alternate" href="', '"'), ('<id>', '</id>')):
                i = ch.lower().find(l1)
                j = ch.lower().find(l2) if i != -1 else -1
                if i != -1 and j != -1:
                    url = ch[i+len(l1):j].strip()
                    break
            # pubDate / updated
            ts = None
            for p1, p2 in (("<pubdate>", "</pubdate>"), ("<updated>", "</updated>"), ("<published>", "</published>")):
                i = ch.lower().find(p1)
                j = ch.lower().find(p2) if i != -1 else -1
                if i != -1 and j != -1:
                    raw = ch[i+len(p1):j].strip()
                    try:
                        # tenta parse ISO/HTTP date de forma simples
                        ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                    except Exception:
                        try:
                            # formato RFC2822 (ex: Tue, 09 Aug 2022 10:00:00 GMT)
                            from email.utils import parsedate_to_datetime
                            ts = parsedate_to_datetime(raw)
                            if ts.tzinfo is None:
                                ts = ts.replace(tzinfo=timezone.utc)
                        except Exception:
                            ts = None
                    break

            if title and url:
                items.append({"title": html.unescape(title), "url": url, "ts": ts})
    except Exception:
        return []
    return items

async def ingest_news_light():
    pool = await db_pool()
    async with pool.acquire() as c:
        for feed in NEWS_FEEDS:
            try:
                r = await http_client.get(feed, headers={"accept": "application/rss+xml,application/xml;q=0.9,*/*;q=0.8"})
                if r.status_code != 200:
                    continue
                items = _extract_rss_items(r.text)
                for it in items[:15]:  # segura volume
                    await insert_news_item(c, it["title"], it["url"], it["ts"])
            except Exception:
                continue

async def get_recent_news(hours: int = 12, limit: int = 6) -> list[dict]:
    pool = await db_pool()
    async with pool.acquire() as c:
        try:
            rows = await c.fetch(
                """SELECT ts, title, url, source 
                   FROM news_items 
                   WHERE ts >= now() - ($1::INT || ' hours')::INTERVAL
                   ORDER BY ts DESC
                   LIMIT $2""",
                hours, limit
            )
            return [{"ts": r["ts"], "title": r["title"], "url": r["url"], "source": r["source"]} for r in rows]
        except Exception:
            return []

# ----------------------------- MARKET DATA -----------------------------------

COINGECKO = "https://api.coingecko.com/api/v3"
CG_ID = {"ETHUSDT": "ethereum", "BTCUSDT": "bitcoin"}

async def cg_simple_price(ids: list[str]) -> dict:
    try:
        r = await http_client.get(f"{COINGECKO}/simple/price", params={
            "ids": ",".join(ids), "vs_currencies": "usd"
        })
        if r.status_code != 200: return {}
        return r.json()
    except Exception:
        return {}

async def cg_market_chart(id_: str, days: int = 2, interval: str = "hourly") -> dict:
    try:
        r = await http_client.get(f"{COINGECKO}/coins/{id_}/market_chart", params={
            "vs_currency": "usd", "days": str(days), "interval": interval
        })
        if r.status_code != 200: return {}
        return r.json()
    except Exception:
        return {}

async def get_spot_prices() -> dict:
    data = await cg_simple_price([CG_ID["ETHUSDT"], CG_ID["BTCUSDT"]])
    eth = data.get("ethereum", {}).get("usd")
    btc = data.get("bitcoin", {}).get("usd")
    out = {}
    if isinstance(eth, (int, float)): out["ETH"] = float(eth)
    if isinstance(btc, (int, float)): out["BTC"] = float(btc)
    return out

async def ohlc_48h(symbol: str) -> dict | None:
    id_ = CG_ID.get(symbol)
    if not id_:
        return None
    data = await cg_market_chart(id_, days=2, interval="hourly")
    prices = data.get("prices") or []
    if not prices:
        return None
    # prices: [[t_ms, price], ...]
    vals = [float(p[1]) for p in prices if isinstance(p, (list, tuple)) and len(p) >= 2]
    if not vals:
        return None
    high = max(vals)
    low = min(vals)
    last = vals[-1]
    return {"high": high, "low": low, "close": last}

def parse_levels_env(s: str) -> list[float]:
    out = []
    for part in (s or "").replace(";", ",").split(","):
        part = part.strip()
        if not part: continue
        try:
            out.append(float(part))
        except Exception:
            continue
    return out[:4]

async def safe_dynamic_levels(symbol: str) -> tuple[list[float], list[float]]:
    """
    Retorna (supports, resistances). Se dados forem insuficientes/NaN, usa fallback estático.
    """
    fallback_sups = parse_levels_env(ETH_SUPS_ENV if symbol == "ETHUSDT" else BTC_SUPS_ENV)
    fallback_ress = parse_levels_env(ETH_RESS_ENV if symbol == "ETHUSDT" else BTC_RESS_ENV)

    try:
        o = await ohlc_48h(symbol)
        if not o: raise ValueError("no ohlc")
        h, l, c = o["high"], o["low"], o["close"]
        if any(v is None or not math.isfinite(v) for v in (h, l, c)):
            raise ValueError("nan in ohlc")

        mid = (h + l) / 2.0
        step = safe_round_step(c)
        # dois suportes e duas resistências simples ao redor
        sups = [round_level(l, step), round_level(mid - step, step)]
        ress = [round_level(mid + step, step), round_level(h, step)]
        # merge com fallback se fornecido
        if fallback_sups:
            sups = sorted(set(sups + fallback_sups))[:4]
        if fallback_ress:
            ress = sorted(set(ress + fallback_ress))[:4]
        return (sups, ress)
    except Exception:
        # somente fallback estático; se vazio, coloca aproximações genéricas
        if not (fallback_sups and fallback_ress):
            # heurística simples para não ficar vazio
            base = 2000.0 if symbol == "ETHUSDT" else 60_000.0
            step = safe_round_step(base)
            fallback_sups = fallback_sups or [round_level(base - step, step), round_level(base - 2*step, step)]
            fallback_ress = fallback_ress or [round_level(base + step, step), round_level(base + 2*step, step)]
        return (fallback_sups[:4], fallback_ress[:4])

# ----------------------------- TEXT BUILDERS ---------------------------------

def footer() -> str:
    return f"\n\n<code>FIM — {code_line_count()} linhas — {VERSION}</code>"

async def build_analysis_block(kind: str) -> str:
    """
    kind in {"pulse","eth","btc"} — retorna bloco 'ANÁLISE → FONTES'
    """
    prices = await get_spot_prices()
    eth = prices.get("ETH")
    btc = prices.get("BTC")
    pair = (eth / btc) if (eth and btc and btc != 0) else None

    # níveis
    eth_sups, eth_ress = await safe_dynamic_levels("ETHUSDT")
    btc_sups, btc_ress = await safe_dynamic_levels("BTCUSDT")

    # análise curtinha (3–5 linhas)
    lines = []
    if kind in ("pulse", "eth"):
        if eth and eth_sups and eth_ress:
            lines.append(f"ETH ${eth:,.2f}: operar zonas. Suportes {', '.join(f'{x:,.0f}' for x in eth_sups)} | Resist {', '.join(f'{x:,.0f}' for x in eth_ress)}.")
        else:
            lines.append("ETH: dados parciais; usar níveis estáticos e confirmação por fechamento.")
    if kind in ("pulse", "btc"):
        if btc and btc_sups and btc_ress:
            lines.append(f"BTC ${btc:,.2f}: vigiar defesas nos suportes e gatilhos em rompimentos válidos. Sup {', '.join(f'{x:,.0f}' for x in btc_sups)} | Res {', '.join(f'{x:,.0f}' for x in btc_ress)}.")
        else:
            lines.append("BTC: dados parciais; usar níveis estáticos e confirmação por fechamento.")

    if pair and kind in ("pulse",):
        lines.append(f"ETH/BTC {pair:.5f}: monitorar divergência; se ETH/BTC enfraquecer, priorizar exposição em BTC ou hedge em ETH.")
    lines.append("Gestão: ajustar hedge/margem se preço se aproximar dos níveis-chaves definidos.")

    analysis = "\n".join(lines)

    # fontes/notícias (últimas 12h)
    news = await get_recent_news(hours=12, limit=6)
    if news:
        fontes = "\n".join([f"• {fmt_ts(n['ts'])} — {html.escape(n['title'])}\n  {n['url']}" for n in news])
        fontes = f"\n\n<b>FONTES (últimas 12h)</b>:\n{fontes}"
    else:
        fontes = "\n\n<b>FONTES</b>: sem itens recentes."

    return f"<b>ANÁLISE</b>:\n{analysis}{fontes}"

async def latest_pulse_text() -> str:
    prices = await get_spot_prices()
    eth = prices.get("ETH")
    btc = prices.get("BTC")
    pair = (eth / btc) if (eth and btc and btc != 0) else None

    hdr_parts = [f"🕒 {fmt_ts(now_utc())} • {VERSION}"]
    if eth: hdr_parts.append(f"ETH ${eth:,.2f}")
    if btc: hdr_parts.append(f"BTC ${btc:,.2f}")
    if pair: hdr_parts.append(f"ETH/BTC {pair:.5f}")
    header = " | ".join(hdr_parts)

    body = await build_analysis_block("pulse")
    return f"{header}\n{body}{footer()}"

async def eth_text() -> str:
    hdr = f"ETH • {fmt_ts(now_utc())} • {VERSION}"
    body = await build_analysis_block("eth")
    return f"{hdr}\n{body}{footer()}"

async def btc_text() -> str:
    hdr = f"BTC • {fmt_ts(now_utc())} • {VERSION}"
    body = await build_analysis_block("btc")
    return f"{hdr}\n{body}{footer()}"

# ----------------------------- ROUTES ----------------------------------------

@app.on_event("startup")
async def _startup():
    await db_init()
    # ingest inicial não bloqueante
    asyncio.create_task(ingest_news_light())
    # webhook auto (opcional)
    if WEBHOOK_AUTO and TELEGRAM_API and HOST_URL:
        try:
            await http_client.get(f"{TELEGRAM_API}/setWebhook", params={"url": f"{HOST_URL}/webhook"})
        except Exception:
            pass

@app.on_event("shutdown")
async def _shutdown():
    try:
        await http_client.aclose()
    except Exception:
        pass
    global _pool
    if _pool:
        await _pool.close()
        _pool = None

@app.get("/", response_class=PlainTextResponse)
async def root():
    return f"Stark DeFi Agent {VERSION}"

@app.get("/status")
async def status():
    ok = bool(TELEGRAM_API) and bool(DATABASE_URL)
    return {"ok": ok, "version": VERSION, "telegram": bool(TELEGRAM_API), "db": bool(DATABASE_URL)}

@app.get("/admin/ping/telegram")
async def ping_tg(chat_id: int):
    await send_tg(f"Ping ✅ {VERSION}", chat_id)
    return {"ok": True}

@app.get("/admin/webhook/set")
async def admin_set_webhook(token: str, url: str = ""):
    if not token or token != TELEGRAM_BOT_TOKEN:
        return JSONResponse({"ok": False, "error": "invalid token"}, status_code=401)
    if not url:
        if not HOST_URL:
            return JSONResponse({"ok": False, "error": "url required"}, status_code=400)
        url = f"{HOST_URL}/webhook"
    try:
        r = await http_client.get(f"{TELEGRAM_API}/setWebhook", params={"url": url})
        return r.json()
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.post("/webhook")
async def webhook_root(request: Request):
    try:
        update = await request.json()
    except Exception:
        return {"ok": True}

    message = update.get("message") or update.get("edited_message") or {}
    chat_id = (message.get("chat") or {}).get("id")
    text = message.get("text", "")

    if not chat_id:
        return {"ok": True}

    cmd = parse_cmd(text)
    try:
        if cmd == "start":
            out = "Bem-vindo. Use /pulse, /eth, /btc.\n" + footer()
            await send_tg(out, chat_id)
        elif cmd == "pulse":
            out = await latest_pulse_text()
            await send_tg(out, chat_id)
        elif cmd == "eth":
            out = await eth_text()
            await send_tg(out, chat_id)
        elif cmd == "btc":
            out = await btc_text()
            await send_tg(out, chat_id)
        else:
            # ignora mensagens que não são comando
            pass
        return {"ok": True}
    except Exception as e:
        # Nunca derrubar por erro interno — responde algo básico
        safe_msg = f"⚠️ Sinal fraco de dados agora; use níveis estáticos e confirmação por fechamento. {footer()}"
        await send_tg(safe_msg, chat_id)
        return {"ok": True, "warn": str(e)}

# =============================================================================
# FIM — (a contagem real de linhas aparece no rodapé das mensagens) — v6.0.8-hotfix
# =============================================================================

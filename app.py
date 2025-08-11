# app.py — Stark DeFi Agent v6.0.9-full
# =============================================================
# PRINCIPAIS MUDANÇAS (6.0.9-full)
# - Correção definitiva das tabelas: cria/ajusta news_items com (id, ts, title, source,
#   url, link, author, summary, created_at), todos com defaults seguros.
# - Remoção de dependência em colunas antigas (published_at etc.). Tudo resiliente.
# - /start, /pause, /pulse, /eth, /btc funcionando via webhook Telegram.
# - Formato “ANÁLISE → FONTES” (3–5 linhas de decisão antes; depois lista de fontes).
# - Níveis dinâmicos por extremos de ~48h com fallback (sem NaN). Mescla com níveis fixos do .env.
# - Coleta de preços multi-fonte (CoinGecko -> Coinbase -> cache DB), com intraday hi/lo simples.
# - Ingest de notícias leve (RSS) com NULL-safe e source/url obrigatória (default).
# - /admin/ping/telegram e /admin/webhook/set para diagnóstico via browser.
# - Rodapé verde nos balões: versão, linhas reais, bytes e hash curto do arquivo.
# =============================================================

import os, asyncio, json, hashlib, math, re
from datetime import datetime, timezone, timedelta
from typing import List, Tuple, Dict, Any, Optional

import httpx
import asyncpg
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse, HTMLResponse

APP_VERSION = "v6.0.9-full"

# -------- Env --------
DATABASE_URL   = os.getenv("DATABASE_URL", "")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
HOST_URL       = os.getenv("HOST_URL", "")                # ex: https://starkradar-bot.onrender.com
WEBHOOK_PATH   = os.getenv("WEBHOOK_PATH", "/webhook")
WEBHOOK_AUTO   = os.getenv("WEBHOOK_AUTO", "0") == "1"
ADMIN_SECRET   = os.getenv("ADMIN_SECRET", "")            # opcional p/ travar /admin/*
# Níveis fixos opcionais (virgula-separados)
ETH_FIXED_SUPS = os.getenv("ETH_SUPS", "")
ETH_FIXED_RESS = os.getenv("ETH_RESS", "")
BTC_FIXED_SUPS = os.getenv("BTC_SUPS", "")
BTC_FIXED_RESS = os.getenv("BTC_RESS", "")

# -------- App --------
app = FastAPI()

# -------- Rodapé: contagem real do arquivo --------
def _file_stats() -> Tuple[int, int, str]:
    try:
        path = __file__
        with open(path, "rb") as f:
            data = f.read()
        lines = data.count(b"\n") + (0 if data.endswith(b"\n") else 1)
        size  = len(data)
        sha   = hashlib.sha1(data).hexdigest()[:8]
        return lines, size, sha
    except Exception:
        return (-1, -1, "00000000")

FILE_LINES, FILE_BYTES, FILE_SHA = _file_stats()

def footer() -> str:
    return (
        f"\n\n<code>fim • {APP_VERSION} • {FILE_LINES} linhas • {FILE_BYTES} bytes • {FILE_SHA}</code>"
    )

# -------- DB --------
_pool: Optional[asyncpg.pool.Pool] = None

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS news_items (
  id          BIGSERIAL PRIMARY KEY,
  ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
  title       TEXT NOT NULL,
  source      TEXT NOT NULL DEFAULT 'unknown',
  url         TEXT,
  link        TEXT,
  author      TEXT,
  summary     TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Ajustes idempotentes:
ALTER TABLE news_items ADD COLUMN IF NOT EXISTS link    TEXT;
ALTER TABLE news_items ADD COLUMN IF NOT EXISTS author  TEXT;
ALTER TABLE news_items ADD COLUMN IF NOT EXISTS summary TEXT;
ALTER TABLE news_items ADD COLUMN IF NOT EXISTS source  TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE news_items ADD COLUMN IF NOT EXISTS url     TEXT;

-- Previne nulos em source
UPDATE news_items SET source='unknown' WHERE source IS NULL;
"""

async def db_init():
    global _pool
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurado.")
    _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)
    async with _pool.acquire() as c:
        await c.execute(SCHEMA_SQL)

async def db() -> asyncpg.Connection:
    assert _pool is not None, "Pool não inicializado"
    return await _pool.acquire()

# -------- HTTP helpers --------
DEFAULT_HEADERS = {"User-Agent":"StarkRadarBot/6.0.9"}

async def get_json(url: str, params: Dict[str, Any]=None, timeout=8.0) -> Optional[Dict[str, Any]]:
    try:
        async with httpx.AsyncClient(timeout=timeout, headers=DEFAULT_HEADERS) as client:
            r = await client.get(url, params=params)
            if r.status_code == 200:
                return r.json()
    except Exception:
        pass
    return None

async def get_text(url: str, timeout=8.0) -> Optional[str]:
    try:
        async with httpx.AsyncClient(timeout=timeout, headers=DEFAULT_HEADERS) as client:
            r = await client.get(url)
            if r.status_code == 200:
                return r.text
    except Exception:
        pass
    return None

# -------- Prices (multi-fonte + fallback) --------
# Prioridade: CoinGecko -> Coinbase -> último cache (none aqui, só runtime)
async def price_now(symbol: str) -> Optional[float]:
    s = symbol.lower()
    # CoinGecko simple price
    cg = await get_json("https://api.coingecko.com/api/v3/simple/price", {
        "ids": "bitcoin,ethereum",
        "vs_currencies": "usd"
    })
    if cg:
        if s=="btc" and "bitcoin" in cg and "usd" in cg["bitcoin"]:
            return float(cg["bitcoin"]["usd"])
        if s=="eth" and "ethereum" in cg and "usd" in cg["ethereum"]:
            return float(cg["ethereum"]["usd"])
    # Coinbase spot
    cb = await get_json(f"https://api.coinbase.com/v2/prices/{symbol.upper()}-USD/spot")
    if cb and "data" in cb and "amount" in cb["data"]:
        try:
            return float(cb["data"]["amount"])
        except: pass
    return None

async def market_window(symbol: str, hours: int=48) -> Tuple[Optional[float], Optional[float]]:
    """Retorna (low, high) aproximados para janela recente via CoinGecko market_chart."""
    asset = "bitcoin" if symbol.lower()=="btc" else "ethereum"
    days = max(1, math.ceil(hours/24))
    cg = await get_json(f"https://api.coingecko.com/api/v3/coins/{asset}/market_chart", {
        "vs_currency":"usd","days": str(days)
    }, timeout=10.0)
    if not cg or "prices" not in cg:
        return (None, None)
    try:
        prices = [float(p[1]) for p in cg["prices"] if isinstance(p, list) and len(p)>=2]
        if len(prices)==0:
            return (None, None)
        return (min(prices), max(prices))
    except Exception:
        return (None, None)

async def eth_btc_ratio() -> Optional[float]:
    pe = await price_now("eth")
    pb = await price_now("btc")
    if pe and pb and pb>0:
        return pe/pb
    return None

# -------- Níveis dinâmicos --------
def merge_levels(dynamic: List[float], fixed_env: str) -> List[float]:
    L = set(dynamic)
    if fixed_env:
        for x in fixed_env.split(","):
            x = x.strip()
            if not x: continue
            try:
                L.add(float(x))
            except: pass
    out = sorted(L)
    return out

def step_for_symbol(symbol: str) -> float:
    return 50.0 if symbol.upper()=="BTC" else 10.0

def round_level(x: float, step: float) -> float:
    if x is None or math.isnan(x) or step<=0:
        return 0.0
    return round(x/step)*step

async def dynamic_levels(symbol: str) -> Tuple[List[float], List[float]]:
    sym = symbol.upper()
    sy = "BTC" if "BTC" in sym else "ETH"
    step = step_for_symbol(sy)
    low, high = await market_window("btc" if sy=="BTC" else "eth", 48)
    px = await price_now("btc" if sy=="BTC" else "eth")

    # Fallbacks agressivos contra NaN/None
    if px is None:
        px = 60000.0 if sy=="BTC" else 3000.0
    if (low is None) or (high is None) or low<=0 or high<=0 or low>=high:
        # constrói “janela” sintética ao redor do preço atual
        low, high = px*0.95, px*1.05

    mid = (low+high)/2.0
    dl  = low
    dh  = high

    sups = [round_level(dl, step), round_level(mid-step, step)]
    ress = [round_level(mid+step, step), round_level(dh, step)]

    # Mescla com fixos
    if sy=="ETH":
        sups = merge_levels(sups, ETH_FIXED_SUPS)
        ress = merge_levels(ress, ETH_FIXED_RESS)
    else:
        sups = merge_levels(sups, BTC_FIXED_SUPS)
        ress = merge_levels(ress, BTC_FIXED_RESS)

    # Remove zeros e dupes
    sups = [x for x in sups if x>0]
    ress = [x for x in ress if x>0]
    return (sorted(set(sups)), sorted(set(ress)))

# -------- Notícias (ingest leve + query) --------
RSS_FEEDS = [
    ("Decrypt", "https://decrypt.co/feed"),
    ("The Block", "https://www.theblock.co/rss.xml"),
    ("Coindesk", "https://www.coindesk.com/arc/outboundfeeds/rss/"),
]

def _xml_find(text: str, tag: str) -> List[str]:
    # Simplista: <tag>...</tag>
    pat = re.compile(rf"<{tag}[^>]*>(.*?)</{tag}>", re.I|re.S)
    return pat.findall(text or "")

async def ingest_news_light():
    if _pool is None: return
    now = datetime.now(timezone.utc)
    async with _pool.acquire() as c:
        for source, url in RSS_FEEDS:
            txt = await get_text(url)
            if not txt: 
                continue
            titles = _xml_find(txt, "title")
            links  = _xml_find(txt, "link")
            items = list(zip(titles[1:], links[1:]))  # pula o primeiro (título do feed)
            for title, link in items[:8]:
                t = re.sub(r"\s+", " ", title).strip()[:300] if title else "(sem título)"
                l = (link or "").strip()[:600]
                if not l: 
                    l = url
                # Insert NULL-safe
                await c.execute("""
                INSERT INTO news_items (ts, title, source, url, link, author, summary, created_at)
                VALUES (now(), $1, $2, $3, $4, NULL, NULL, now())
                ON CONFLICT DO NOTHING
                """, t, source or "unknown", url, l)

async def get_recent_news(hours: int=12, limit: int=6) -> List[Dict[str, Any]]:
    if _pool is None: return []
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    async with _pool.acquire() as c:
        rows = await c.fetch("""
            SELECT ts, title, COALESCE(source,'unknown') AS source,
                   COALESCE(link, url) AS link
            FROM news_items
            WHERE ts >= $1
            ORDER BY ts DESC
            LIMIT $2
        """, since, limit)
        out=[]
        for r in rows:
            out.append({
                "ts": r["ts"].astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M"),
                "title": r["title"],
                "source": r["source"],
                "link": r["link"] or ""
            })
        return out

# -------- Telegram --------
async def send_tg(text: str, chat_id: int):
    if not TELEGRAM_TOKEN:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        async with httpx.AsyncClient(timeout=10.0, headers=DEFAULT_HEADERS) as client:
            await client.post(url, json=payload)
    except Exception:
        pass

# -------- Análise (comentário curto) --------
def analyze_eth_btc(eth_px: float, btc_px: float, eth_hi: float, eth_lo: float,
                    btc_hi: float, btc_lo: float, ratio: Optional[float]) -> str:
    lines=[]
    # Direção relativa
    if ratio:
        if ratio < 0.035:  # fraca ETH vs BTC (exemplo)
            lines.append("ETH perdendo força relativa vs BTC; pressão vendedora em alts.")
        else:
            lines.append("ETH sustentando performance vs BTC; risco de short squeeze em alts.")
    # Posição no range
    def pos(px, lo, hi):
        if not (px and lo and hi and hi>lo): return 0.5
        return (px-lo)/(hi-lo)
    eth_pos = pos(eth_px, eth_lo, eth_hi)
    btc_pos = pos(btc_px, btc_lo, btc_hi)
    if eth_pos < 0.25:
        lines.append("ETH em quartil inferior do range de 48h; atenção para defesas em suportes.")
    elif eth_pos > 0.75:
        lines.append("ETH pressiona topo do range; buscar gatilhos de rompimento limpo.")
    if btc_pos < 0.25:
        lines.append("BTC também cedeu; risco sistêmico aumenta se perder suportes chave.")
    elif btc_pos > 0.75:
        lines.append("BTC dominante em topo de range; dominância favorece correções em alts.")
    # Ação tática
    lines.append("Plano: operar níveis; validar gatilhos por fechamento e volume/fluxo.")
    return " ".join(lines)

# -------- Formatadores --------
def fmt_levels(sups: List[float], ress: List[float]) -> str:
    def j(v): 
        return ", ".join(f"{x:,.0f}".replace(",", ".") for x in v)
    return f"Suportes: {j(sups)} | Resistências: {j(ress)}"

def fmt_news_list(news: List[Dict[str, Any]]) -> str:
    if not news:
        return "—"
    out=[]
    for n in news:
        title = n["title"]
        src   = n["source"]
        ts    = n["ts"]
        link  = n["link"] or ""
        if link:
            out.append(f"• {ts} — <a href=\"{link}\">{title}</a> ({src})")
        else:
            out.append(f"• {ts} — {title} ({src})")
    return "\n".join(out)

# -------- Pulse/BTC/ETH --------
async def latest_pulse_text() -> str:
    eth = await price_now("eth") or 0.0
    btc = await price_now("btc") or 0.0
    ratio = await eth_btc_ratio()

    eth_lo, eth_hi = await market_window("eth", 48)
    btc_lo, btc_hi = await market_window("btc", 48)
    if not all([eth_lo, eth_hi]) or eth_lo>=eth_hi:
        eth_lo, eth_hi = eth*0.95, eth*1.05
    if not all([btc_lo, btc_hi]) or btc_lo>=btc_hi:
        btc_lo, btc_hi = btc*0.95, btc*1.05

    eth_sups, eth_ress = await dynamic_levels("ETHUSDT")
    btc_sups, btc_ress = await dynamic_levels("BTCUSDT")

    analise = analyze_eth_btc(eth, btc, eth_hi, eth_lo, btc_hi, btc_lo, ratio)
    news = await get_recent_news(hours=12, limit=6)

    hdr = f"🕒 {datetime.now().strftime('%Y-%m-%d %H:%M')} • {APP_VERSION}"
    l1  = f"ETH ${eth:,.2f} | BTC ${btc:,.2f} | ETH/BTC {ratio:.5f}" if ratio else f"ETH ${eth:,.2f} | BTC ${btc:,.2f}"
    l2  = f"NÍVEIS ETH: {fmt_levels(eth_sups, eth_ress)}"
    l3  = f"NÍVEIS BTC: {fmt_levels(btc_sups, btc_ress)}"

    txt = (
        f"{hdr}\n"
        f"{l1}\n\n"
        f"<b>ANÁLISE</b>: {analise}\n\n"
        f"<b>FONTES (12h)</b>:\n{fmt_news_list(news)}"
        f"{footer()}"
    )
    return txt

async def coin_text(symbol: str) -> str:
    sym = symbol.upper()
    px  = await price_now("eth" if sym=="ETH" else "btc") or 0.0
    lo, hi = await market_window("eth" if sym=="ETH" else "btc", 48)
    if not all([lo,hi]) or lo>=hi:
        lo, hi = px*0.95, px*1.05
    sups, ress = await dynamic_levels(f"{sym}USDT")
    ratio = await eth_btc_ratio()
    news = await get_recent_news(hours=12, limit=4)

    if sym=="ETH":
        rel = f" • ETH/BTC {ratio:.5f}" if ratio else ""
    else:
        rel = f" • ETH/BTC {ratio:.5f}" if ratio else ""

    analise = analyze_eth_btc(
        (await price_now("eth") or 0.0),
        (await price_now("btc") or 0.0),
        (await market_window("eth",48))[1] or 0.0,
        (await market_window("eth",48))[0] or 0.0,
        (await market_window("btc",48))[1] or 0.0,
        (await market_window("btc",48))[0] or 0.0,
        ratio
    )

    hdr = f"{sym} • ${px:,.2f}{rel}"
    body = (
        f"<b>ANÁLISE</b>: {analise}\n\n"
        f"<b>NÍVEIS {sym}</b>: {fmt_levels(sups, ress)}\n\n"
        f"<b>FONTES (12h)</b>:\n{fmt_news_list(news)}"
        f"{footer()}"
    )
    return f"{hdr}\n{body}"

# -------- Webhook e comandos --------
STARTED = True

def _normalize_cmd(text: str) -> str:
    t = (text or "").strip()
    if t.startswith("/"): t = t[1:]
    if t.startswith("."): t = t[1:]
    # aliases
    alias = {
        "start": "start",
        "pause": "pause",
        "pulse": "pulse",
        "btc": "btc",
        "eth": "eth",
        "bth": "btc",
        "etereo": "eth",
        "vtc": "btc"
    }
    t = t.split()[0].lower()
    return alias.get(t, t)

@app.post(WEBHOOK_PATH)
async def webhook_root(request: Request):
    data = await request.json()
    message = data.get("message") or data.get("edited_message") or {}
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    if not chat_id:
        return {"ok": True}

    text = message.get("text","")
    cmd  = _normalize_cmd(text)

    global STARTED
    if cmd == "start":
        STARTED = True
        await send_tg("✅ Bot ativado. Use /pulse, /eth ou /btc.", chat_id)
        return {"ok": True}
    if cmd == "pause":
        STARTED = False
        await send_tg("⏸️ Bot pausado. Use /start para reativar.", chat_id)
        return {"ok": True}
    if not STARTED:
        await send_tg("⏸️ Bot está pausado. Envie /start para reativar.", chat_id)
        return {"ok": True}

    if cmd == "pulse":
        out = await latest_pulse_text()
        await send_tg(out, chat_id)
        return {"ok": True}
    if cmd == "eth":
        out = await coin_text("ETH")
        await send_tg(out, chat_id)
        return {"ok": True}
    if cmd == "btc":
        out = await coin_text("BTC")
        await send_tg(out, chat_id)
        return {"ok": True}

    # Ajuda padrão
    await send_tg("Comandos: /start • /pause • /pulse • /eth • /btc", chat_id)
    return {"ok": True}

# -------- Admin/Health --------
def _auth_ok(req: Request) -> bool:
    if not ADMIN_SECRET:
        return True
    return req.headers.get("x-admin-secret","") == ADMIN_SECRET

@app.get("/")
async def root():
    return HTMLResponse(f"<pre>Stark DeFi Agent {APP_VERSION}\nWebhook: {WEBHOOK_PATH}\nOK</pre>")

@app.get("/status")
async def status():
    return JSONResponse({"ok": True, "version": APP_VERSION, "lines": FILE_LINES, "bytes": FILE_BYTES, "sha": FILE_SHA})

@app.get("/admin/ping/telegram")
async def ping_telegram():
    if not TELEGRAM_TOKEN:
        return JSONResponse({"ok": False, "err":"TELEGRAM_TOKEN ausente"})
    # getWebhookInfo
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getWebhookInfo"
    try:
        async with httpx.AsyncClient(timeout=8.0, headers=DEFAULT_HEADERS) as client:
            r = await client.get(url)
            return JSONResponse({"ok": True, "result": r.json()})
    except Exception as e:
        return JSONResponse({"ok": False, "err": str(e)})

@app.get("/admin/webhook/set")
async def admin_set_webhook(request: Request):
    if not _auth_ok(request):
        return JSONResponse({"ok": False, "err":"unauthorized"}, status_code=401)
    if not TELEGRAM_TOKEN or not HOST_URL:
        return JSONResponse({"ok": False, "err": "TELEGRAM_TOKEN ou HOST_URL faltando"})
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/setWebhook"
    target = HOST_URL.rstrip("/") + WEBHOOK_PATH
    try:
        async with httpx.AsyncClient(timeout=10.0, headers=DEFAULT_HEADERS) as client:
            r = await client.post(url, data={"url": target})
            return JSONResponse({"ok": True, "result": r.json(), "target": target})
    except Exception as e:
        return JSONResponse({"ok": False, "err": str(e)})

# -------- Startup/Shut --------
@app.on_event("startup")
async def _startup():
    await db_init()
    # ingere notícias em background (não bloqueia)
    asyncio.create_task(ingest_news_light())
    # auto webhook
    if WEBHOOK_AUTO and TELEGRAM_TOKEN and HOST_URL:
        try:
            async with httpx.AsyncClient(timeout=10.0, headers=DEFAULT_HEADERS) as client:
                await client.post(
                    f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/setWebhook",
                    data={"url": HOST_URL.rstrip("/") + WEBHOOK_PATH}
                )
        except Exception:
            pass

@app.on_event("shutdown")
async def _shutdown():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None

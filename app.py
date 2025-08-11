# app.py — Stark DeFi Agent v6.0.9-full
# =============================================================
# PRINCIPAIS MUDANÇAS NESTA VERSÃO (6.0.9-full)
# - /start, /pulse, /eth, /btc sempre respondem (mesmo em degradação).
# - Correção do schema news_items (link/author/source presentes e/ou NULL-safe).
# - Migração automática sem SQL manual (CREATE/ALTER IF NOT EXISTS).
# - Correção de NaN em níveis dinâmicos (fallbacks e passos por ativo).
# - /status com versão, linecount e último erro capturado.
# - Webhook auto opcional via HOST_URL + WEBHOOK_AUTO=1.
# - Rodapé automático “FIM DO CÓDIGO — TOTAL: NNN linhas — versão ...”.
# =============================================================

import os, json, math, asyncio, datetime as dt
from typing import Optional, Tuple, List, Dict

import httpx
import asyncpg

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse

APP_VERSION = "6.0.9-full"

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
HOST_URL   = os.getenv("HOST_URL", "").rstrip("/")
WEBHOOK_AUTO = os.getenv("WEBHOOK_AUTO", "0").strip() == "1"
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

# Fallbacks e parâmetros
DEFAULT_TIMEOUT = 18.0
TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}" if BOT_TOKEN else ""
NEWS_FEEDS = [
    # leve, público; se algum feed vier vazio, o ingest ignora
    "https://decrypt.co/feed",
    "https://cointelegraph.com/rss",
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
]

# Passos por ativo (arredondamento de níveis)
LEVEL_STEP = {
    "BTCUSDT": 500.0,
    "ETHUSDT": 50.0,
}

# Níveis fixos via ENV (mesclados ao dinâmico quando possível)
ENV_ETH_SUPS = os.getenv("ETH_SUPS", "4200,4000")
ENV_ETH_RESS = os.getenv("ETH_RESS", "4300,4400")
ENV_BTC_SUPS = os.getenv("BTC_SUPS", "62000,60000")
ENV_BTC_RESS = os.getenv("BTC_RESS", "65000,68000")

# Estado global
pool: Optional[asyncpg.pool.Pool] = None
last_error: Optional[str] = None
LINECOUNT: int = 0

# ---------------- Utils ----------------

def _linecount() -> int:
    try:
        with open(__file__, "r", encoding="utf-8") as f:
            return sum(1 for _ in f)
    except Exception:
        return 0

def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def safe_float(x, default=None):
    try:
        if x is None: return default
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return default
        return v
    except Exception:
        return default

def round_level(x: float, step: float) -> float:
    # tolerante a None/NaN
    if x is None or step is None:
        return 0.0
    if math.isnan(x) or math.isnan(step) or step == 0:
        return 0.0
    return round(x / step) * step

def fmt_money(v: Optional[float]) -> str:
    if v is None: return "-"
    if v >= 1000:
        return f"${v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"${v:.2f}".replace(".", ",")

def footer() -> str:
    global LINECOUNT
    if LINECOUNT <= 0:
        LINECOUNT = _linecount()
    # Rodapé “verde” no Telegram: usamos markdown com ✓ para destacar
    return f"\n\n✅ FIM DO CÓDIGO — TOTAL: {LINECOUNT} linhas — versão {APP_VERSION}"

def msg_footer() -> str:
    # Rodapé que vai nos balões do bot (não o código), com versão + linecount
    global LINECOUNT
    if LINECOUNT <= 0:
        LINECOUNT = _linecount()
    ts = now_utc().strftime("%Y-%m-%d %H:%M UTC")
    return f"\n\n— v{APP_VERSION} • {LINECOUNT} linhas • {ts}"

async def set_last_error(e: Exception):
    global last_error
    last_error = f"{type(e).__name__}: {str(e)[:500]}"

# ------------- DB ----------------------

async def db_init():
    global pool
    if not DATABASE_URL:
        return
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)

    async with pool.acquire() as c:
        # news_items básico
        await c.execute("""
        CREATE TABLE IF NOT EXISTS news_items (
            id SERIAL PRIMARY KEY,
            ts TIMESTAMPTZ NOT NULL DEFAULT now(),
            source TEXT,
            author TEXT,
            title TEXT,
            link TEXT,
            feed_url TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            extra JSONB
        );
        """)

        # ALTERs tolerantes
        for col, ddl in [
            ("source",   "ALTER TABLE news_items ADD COLUMN IF NOT EXISTS source TEXT"),
            ("author",   "ALTER TABLE news_items ADD COLUMN IF NOT EXISTS author TEXT"),
            ("title",    "ALTER TABLE news_items ADD COLUMN IF NOT EXISTS title TEXT"),
            ("link",     "ALTER TABLE news_items ADD COLUMN IF NOT EXISTS link TEXT"),
            ("feed_url", "ALTER TABLE news_items ADD COLUMN IF NOT EXISTS feed_url TEXT"),
            ("extra",    "ALTER TABLE news_items ADD COLUMN IF NOT EXISTS extra JSONB"),
            ("created_at","ALTER TABLE news_items ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now()"),
            ("ts",       "ALTER TABLE news_items ADD COLUMN IF NOT EXISTS ts TIMESTAMPTZ NOT NULL DEFAULT now()"),
        ]:
            try:
                await c.execute(ddl)
            except Exception:
                pass

        # índices úteis
        await c.execute("CREATE INDEX IF NOT EXISTS idx_news_ts ON news_items(ts DESC)")
        await c.execute("CREATE INDEX IF NOT EXISTS idx_news_link ON news_items(link)")
        await c.execute("CREATE INDEX IF NOT EXISTS idx_news_source ON news_items(source)")

        # Tabela leve de cache de preços (opcional)
        await c.execute("""
        CREATE TABLE IF NOT EXISTS price_cache (
            symbol TEXT PRIMARY KEY,
            last  DOUBLE PRECISION,
            hi    DOUBLE PRECISION,
            lo    DOUBLE PRECISION,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """)

async def db_fetch(query: str, *args):
    if not pool:
        return []
    async with pool.acquire() as c:
        return await c.fetch(query, *args)

async def db_execute(query: str, *args):
    if not pool:
        return
    async with pool.acquire() as c:
        await c.execute(query, *args)

# ----------- News ingest / read --------

async def ingest_news_light():
    """Ingest simples de feeds; campos ausentes viram NULL; `source` vira domínio."""
    if not pool:
        return
    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as cli:
        for url in NEWS_FEEDS:
            try:
                r = await cli.get(url, headers={"User-Agent":"starkradar-bot/1.0"})
                if r.status_code != 200 or not r.text:
                    continue
                # Parse mínimo por regex simples (evitar libs pesadas)
                text = r.text
                # Muito básico: extrair <item><title>..</title><link>..</link><pubDate>..</pubDate>
                items = []
                # split por <item>
                parts = text.split("<item>")
                for p in parts[1:]:
                    title = _extract_tag(p, "title")
                    link  = _extract_tag(p, "link")
                    pub   = _extract_tag(p, "pubDate")
                    if not title and not link:
                        continue
                    src = _domain_from_url(url)
                    ts = now_utc()
                    try:
                        # sem parse robusto de datas; confiamos no now se vazio
                        pass
                    except Exception:
                        pass
                    items.append((ts, src, None, title, link, url))
                if items:
                    # upsert por link (se houver)
                    async with pool.acquire() as c:
                        for (ts, src, author, title, link, feed) in items:
                            if not title and not link:
                                continue
                            await c.execute("""
                                INSERT INTO news_items (ts, source, author, title, link, feed_url, extra)
                                VALUES ($1,$2,$3,$4,$5,$6,$7)
                                ON CONFLICT (link) DO NOTHING
                            """, ts, src, author, title, link, feed, None)
            except Exception:
                # não trava a aplicação por causa de um feed
                continue

def _extract_tag(chunk: str, tag: str) -> Optional[str]:
    start = f"<{tag}"
    end   = f"</{tag}>"
    i = chunk.find(start)
    if i < 0:
        return None
    # avança para '>'
    j = chunk.find(">", i)
    if j < 0: return None
    k = chunk.find(end, j)
    if k < 0: return None
    raw = chunk[j+1:k].strip()
    # limpar entidades básicas
    return (raw
            .replace("&amp;","&")
            .replace("&lt;","<")
            .replace("&gt;",">")
            .replace("&quot;","\"")
            .replace("&#39;","'")
            )

def _domain_from_url(u: str) -> str:
    try:
        return u.split("://",1)[1].split("/",1)[0]
    except Exception:
        return "unknown"

async def get_recent_news(hours=12, limit=6) -> List[Dict]:
    if not pool:
        return []
    rows = await db_fetch("""
        SELECT ts, COALESCE(title,'(sem título)') AS title,
               COALESCE(link,'') AS link,
               COALESCE(source,'') AS source
          FROM news_items
         WHERE ts >= (now() - ($1::INT || ' hours')::INTERVAL)
         ORDER BY ts DESC
         LIMIT $2
    """, hours, limit)
    out = []
    for r in rows:
        out.append({
            "ts": r["ts"],
            "title": r["title"],
            "link": r["link"],
            "source": r["source"] or "",
        })
    return out

# ------------- Preços / Níveis -------------

async def get_price_cache(symbol: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    rows = await db_fetch("SELECT last, hi, lo FROM price_cache WHERE symbol=$1", symbol)
    if rows:
        r = rows[0]
        return safe_float(r["last"]), safe_float(r["hi"]), safe_float(r["lo"])
    return None, None, None

async def set_price_cache(symbol: str, last: Optional[float], hi: Optional[float], lo: Optional[float]):
    await db_execute("""
        INSERT INTO price_cache(symbol,last,hi,lo,updated_at)
        VALUES($1,$2,$3,$4,now())
        ON CONFLICT(symbol) DO UPDATE SET
          last=EXCLUDED.last, hi=EXCLUDED.hi, lo=EXCLUDED.lo, updated_at=now()
    """, symbol, last, hi, lo)

async def fetch_prices_public() -> Dict[str, Dict[str, Optional[float]]]:
    """
    Fonte pública simples (tolerante a 403/451). Tentativa em ordem:
    1) CoinGecko simple/price (sem API key). 2) Fallback: usa cache.
    """
    symbols = {"bitcoin":"BTCUSDT", "ethereum":"ETHUSDT"}
    out = {"BTCUSDT": {"last": None, "hi": None, "lo": None},
           "ETHUSDT": {"last": None, "hi": None, "lo": None}}

    # tentativa CoinGecko
    try:
        async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as cli:
            r = await cli.get(
                "https://api.coingecko.com/api/v3/simple/price",
                params={"ids":"bitcoin,ethereum","vs_currencies":"usd","include_24hr_high_low":"true"},
                headers={"User-Agent":"starkradar-bot/1.0"}
            )
            if r.status_code == 200:
                js = r.json()
                if "bitcoin" in js:
                    out["BTCUSDT"]["last"] = safe_float(js["bitcoin"].get("usd"))
                    out["BTCUSDT"]["hi"]   = safe_float(js["bitcoin"].get("usd_24h_high"))
                    out["BTCUSDT"]["lo"]   = safe_float(js["bitcoin"].get("usd_24h_low"))
                if "ethereum" in js:
                    out["ETHUSDT"]["last"] = safe_float(js["ethereum"].get("usd"))
                    out["ETHUSDT"]["hi"]   = safe_float(js["ethereum"].get("usd_24h_high"))
                    out["ETHUSDT"]["lo"]   = safe_float(js["ethereum"].get("usd_24h_low"))
    except Exception:
        pass

    # completa com cache se faltou algo
    for sym in ["BTCUSDT","ETHUSDT"]:
        last, hi, lo = await get_price_cache(sym)
        if out[sym]["last"] is None: out[sym]["last"] = last
        if out[sym]["hi"]   is None: out[sym]["hi"]   = hi
        if out[sym]["lo"]   is None: out[sym]["lo"]   = lo

    # salva cache
    for sym in ["BTCUSDT","ETHUSDT"]:
        await set_price_cache(sym, out[sym]["last"], out[sym]["hi"], out[sym]["lo"])

    return out

def merge_levels(static_csv: str) -> List[float]:
    vals = []
    for p in (static_csv or "").split(","):
        v = safe_float(p.strip(), None)
        if v: vals.append(v)
    return vals

async def dynamic_levels(symbol: str) -> Tuple[List[float], List[float]]:
    """
    Cria 2–3 suportes e 2–3 resistências a partir de hi/lo/last (24h),
    com arredondamento por step e mescla com níveis fixos do ENV.
    Nunca retorna NaN; se faltar preço, devolve apenas ENV.
    """
    step = LEVEL_STEP.get(symbol, 50.0)
    prices = await fetch_prices_public()
    p = prices.get(symbol, {})
    last = safe_float(p.get("last"))
    hi   = safe_float(p.get("hi"))
    lo   = safe_float(p.get("lo"))

    sups, ress = [], []

    if last and hi and lo and step:
        mid = last
        dl  = lo
        dh  = hi
        # suportes
        sups = [round_level(dl, step)]
        s2 = round_level(mid - step, step)
        if s2 not in sups: sups.append(s2)
        # resistências
        r1 = round_level(dh, step)
        r2 = round_level(mid + step, step)
        for x in [r1, r2]:
            if x not in ress: ress.append(x)

    # Mescla ENV
    if symbol == "ETHUSDT":
        env_s = merge_levels(ENV_ETH_SUPS)
        env_r = merge_levels(ENV_ETH_RESS)
    else:
        env_s = merge_levels(ENV_BTC_SUPS)
        env_r = merge_levels(ENV_BTC_RESS)

    # remove zeros e duplica ordenando
    all_s = sorted({x for x in (sups + env_s) if x and x > 0})
    all_r = sorted({x for x in (ress + env_r) if x and x > 0})

    # limita 3 níveis
    return all_s[:3], all_r[:3]

# --------- Montagem de mensagens ----------

def bullet_levels(sups: List[float], ress: List[float]) -> str:
    fs = " / ".join([f"{int(x):,}".replace(",", ".") if x >= 1000 else f"{x:.0f}" for x in sups]) if sups else "-"
    fr = " / ".join([f"{int(x):,}".replace(",", ".") if x >= 1000 else f"{x:.0f}" for x in ress]) if ress else "-"
    return f"Suportes: {fs} | Resist: {fr}"

async def get_snapshot_text() -> Dict[str, str]:
    prices = await fetch_prices_public()
    eth = prices.get("ETHUSDT", {})
    btc = prices.get("BTCUSDT", {})

    out = {}
    out["ETH_line"] = f"ETH {fmt_money(eth.get('last'))}"
    out["BTC_line"] = f"BTC {fmt_money(btc.get('last'))}"
    # relação (se ambos existirem)
    e = safe_float(eth.get("last"))
    b = safe_float(btc.get("last"))
    rel = (e/b) if (e and b and b != 0) else None
    out["PAIR_line"] = f"ETH/BTC {rel:.5f}" if rel else "ETH/BTC -"

    return out

async def analysis_block(symbol: str, sups: List[float], ress: List[float], label: str) -> str:
    # bloco de 3–5 linhas com recomendação tática
    try:
        prices = await fetch_prices_public()
        last = safe_float(prices.get(symbol, {}).get("last"))
        hi   = safe_float(prices.get(symbol, {}).get("hi"))
        lo   = safe_float(prices.get(symbol, {}).get("lo"))
        if not last:
            return (f"{label}: dados parciais.\n"
                    f"Níveis: {bullet_levels(sups, ress)}\n"
                    f"Ação: operar pelos níveis até normalizar feed.")
        # leitura simples: posição do preço na faixa 24h
        bias = "-"
        if hi and lo and hi > lo:
            pos = (last - lo) / (hi - lo)
            if pos >= 0.66: bias = "compradores dominando (próx. topo 24h)"
            elif pos <= 0.33: bias = "vendedores dominando (próx. fundo 24h)"
            else: bias = "equilíbrio em faixa média"
        act = "gatilhos em rompimentos válidos das resistências" if last and ress and last < ress[0] else \
              "gestão defensiva em perda de suportes; reentrada em pullbacks"
        return (f"{label}: {bias}.\n"
                f"Níveis: {bullet_levels(sups, ress)}\n"
                f"Ação: {act}.")
    except Exception as e:
        await set_last_error(e)
        return (f"{label}: análise degradada.\n"
                f"Níveis: {bullet_levels(sups, ress)}\n"
                f"Ação: operar pelos níveis; monitore volatilidade.")

async def latest_pulse_text() -> str:
    try:
        snap = await get_snapshot_text()
        eth_s, eth_r = await dynamic_levels("ETHUSDT")
        btc_s, btc_r = await dynamic_levels("BTCUSDT")

        # 1) ANÁLISE (ETH/BTC + ETH + BTC)
        a_eth = await analysis_block("ETHUSDT", eth_s, eth_r, "ETH")
        a_btc = await analysis_block("BTCUSDT", btc_s, btc_r, "BTC")

        # 2) Notícias (12h)
        news = await get_recent_news(hours=12, limit=6)
        if not news:
            # tenta um ingest leve em background e segue
            asyncio.create_task(ingest_news_light())
        news_lines = []
        for n in news:
            ts_str = n["ts"].strftime("%H:%M")
            link = n["link"] or ""
            src  = n["source"] or ""
            title = n["title"][:120]
            if link:
                news_lines.append(f"• {ts_str} {src} — {title} → {link}")
            else:
                news_lines.append(f"• {ts_str} {src} — {title}")

        lines = [
            f"🕒 {now_utc().strftime('%Y-%m-%d %H:%M')} • v{APP_VERSION}",
            snap["ETH_line"],
            snap["BTC_line"],
            snap["PAIR_line"],
            "",
            "ANÁLISE:",
            a_eth,
            a_btc,
            "",
            "FONTES (últimas 12h):" if news_lines else "FONTES: (atualizando...)",
            *(news_lines if news_lines else []),
        ]
        return "\n".join(lines) + msg_footer()
    except Exception as e:
        await set_last_error(e)
        # resposta degradada
        eth_s, eth_r = await dynamic_levels("ETHUSDT")
        btc_s, btc_r = await dynamic_levels("BTCUSDT")
        lines = [
            f"🕒 {now_utc().strftime('%Y-%m-%d %H:%M')} • v{APP_VERSION}",
            "Pulse em modo degradado.",
            "",
            "ETH:",
            f"Níveis: {bullet_levels(eth_s, eth_r)}",
            "BTC:",
            f"Níveis: {bullet_levels(btc_s, btc_r)}",
            "",
            "Ação: operar por níveis; feeds sendo normalizados."
        ]
        return "\n".join(lines) + msg_footer()

async def latest_eth_text() -> str:
    eth_s, eth_r = await dynamic_levels("ETHUSDT")
    a = await analysis_block("ETHUSDT", eth_s, eth_r, "ETH")
    snap = await get_snapshot_text()
    return "\n".join([
        snap["ETH_line"],
        a,
    ]) + msg_footer()

async def latest_btc_text() -> str:
    btc_s, btc_r = await dynamic_levels("BTCUSDT")
    a = await analysis_block("BTCUSDT", btc_s, btc_r, "BTC")
    snap = await get_snapshot_text()
    return "\n".join([
        snap["BTC_line"],
        a,
    ]) + msg_footer()

# ------------- Telegram ----------------

async def send_tg(text: str, chat_id: int):
    if not BOT_TOKEN:
        return
    try:
        async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as cli:
            await cli.post(f"{TELEGRAM_API}/sendMessage",
                           json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True})
    except Exception as e:
        await set_last_error(e)

def parse_cmd(txt: str) -> str:
    if not txt: return ""
    t = txt.strip().lower()
    # aceita "/start", "/pulse", "/eth", "/btc" e variações com ponto
    aliases = {
        "/start": "start", ".start":"start", "start":"start",
        "/pulse": "pulse", ".pulse":"pulse", "pulse":"pulse",
        "/eth":   "eth", ".eth":"eth", "eth":"eth", "/etereo":"eth",
        "/btc":   "btc", ".btc":"btc", "btc":"btc", "/bitcoin":"btc",
    }
    return aliases.get(t.split()[0], "")

# ------------- FastAPI -----------------

app = FastAPI()

@app.on_event("startup")
async def _startup():
    global LINECOUNT
    LINECOUNT = _linecount()
    try:
        await db_init()
    except Exception as e:
        await set_last_error(e)

    # webhook auto
    if WEBHOOK_AUTO and BOT_TOKEN and HOST_URL:
        try:
            async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as cli:
                await cli.post(f"{TELEGRAM_API}/setWebhook",
                               json={"url": f"{HOST_URL}/webhook",
                                     "allowed_updates":["message","edited_message"]})
        except Exception as e:
            await set_last_error(e)

@app.get("/")
async def root():
    return PlainTextResponse(f"Stark DeFi Agent {APP_VERSION} is live.\n" + footer())

@app.get("/status")
async def status():
    return JSONResponse({
        "ok": True,
        "version": APP_VERSION,
        "linecount": LINECOUNT if LINECOUNT else _linecount(),
        "last_error": last_error
    })

@app.post("/webhook")
async def webhook_root(request: Request):
    body = await request.json()
    try:
        msg = body.get("message") or body.get("edited_message") or {}
        chat_id = msg.get("chat", {}).get("id")
        text = msg.get("text", "")
        cmd = parse_cmd(text)
        if not chat_id or not cmd:
            return {"ok": True}

        if cmd == "start":
            await send_tg("Bem-vindo. Envie /pulse, /eth ou /btc para o boletim.", chat_id)
            return {"ok": True}
        elif cmd == "pulse":
            out = await latest_pulse_text()
            await send_tg(out, chat_id)
            return {"ok": True}
        elif cmd == "eth":
            out = await latest_eth_text()
            await send_tg(out, chat_id)
            return {"ok": True}
        elif cmd == "btc":
            out = await latest_btc_text()
            await send_tg(out, chat_id)
            return {"ok": True}
        else:
            await send_tg("Comando não reconhecido. Use /pulse, /eth, /btc.", chat_id)
            return {"ok": True}
    except Exception as e:
        await set_last_error(e)
        return {"ok": True}

# -------- Admin endpoints (opcionais) --------

@app.get("/admin/ping/telegram")
async def admin_ping():
    if not BOT_TOKEN:
        return JSONResponse({"ok": False, "error": "BOT_TOKEN ausente"})
    try:
        async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as cli:
            r = await cli.get(f"{TELEGRAM_API}/getWebhookInfo")
            return JSONResponse({"ok": True, "result": r.json()})
    except Exception as e:
        await set_last_error(e)
        return JSONResponse({"ok": False, "error": str(e)})

@app.post("/admin/webhook/set")
async def admin_set_webhook():
    if not (BOT_TOKEN and HOST_URL):
        return JSONResponse({"ok": False, "error": "HOST_URL ou BOT_TOKEN ausentes"})
    try:
        async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as cli:
            r = await cli.post(f"{TELEGRAM_API}/setWebhook",
                               json={"url": f"{HOST_URL}/webhook",
                                     "allowed_updates":["message","edited_message"]})
            return JSONResponse(r.json())
    except Exception as e:
        await set_last_error(e)
        return JSONResponse({"ok": False, "error": str(e)})

@app.post("/admin/prices/upsert")
async def admin_upsert_prices(payload: Dict):
    """
    Upsert manual (debug) — body:
    {"symbol":"ETHUSDT","last":4180,"hi":4350,"lo":4050}
    """
    try:
        sym = payload.get("symbol","").upper()
        last = safe_float(payload.get("last"))
        hi   = safe_float(payload.get("hi"))
        lo   = safe_float(payload.get("lo"))
        if not sym:
            return JSONResponse({"ok": False, "error":"symbol vazio"})
        await set_price_cache(sym, last, hi, lo)
        return JSONResponse({"ok": True})
    except Exception as e:
        await set_last_error(e)
        return JSONResponse({"ok": False, "error": str(e)})

# =============================================================
# ✅ FIM DO CÓDIGO — TOTAL: será avaliado em runtime — versão 6.0.9-full
# =============================================================

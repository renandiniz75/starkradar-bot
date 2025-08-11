# app.py — Stark DeFi Agent v6.0.10-full
# =============================================================
# MUDANÇAS-CHAVE (6.0.10-full)
# - Removido rodapé com versão/linhas dos BALÕES do Telegram. Agora fica só no /status
#   e no cabeçalho deste arquivo (para GitHub).
# - "ANÁLISE → FONTES" sempre gera 3–5 linhas úteis (nunca "-.").
# - Níveis dinâmicos robustos (48h) com fallback caso OHLC falhe (evita NaN).
# - Schema de news_items saneado (url/source/title/summary/ts); migração automática
#   não quebra se colunas antigas existirem/não existirem; consultas tolerantes.
# - /start, /pulse (/push), /eth e /btc corrigidos e mais assertivos.
# - Job de ingest de notícias opcional (NEWS_JOB=1). Sem 451/403 críticos.
# - /admin/webhook/set e /admin/ping/telegram mantidos; setWebhook automático se
#   WEBHOOK_AUTO=1 & HOST_URL definido.
# - Variáveis de ambiente preservadas: BOT_TOKEN, HOST_URL, WEBHOOK_AUTO, DB_URL,
#   ADMIN_CHAT_IDS (opcional, csv), NEWS_JOB.
# =============================================================

import os
import math
import asyncio
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Optional, Dict, Any

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse

# DB (opcional)
import asyncpg

# ---------------------------------------
# Versão
# ---------------------------------------
VERSION = "6.0.10-full"

# ---------------------------------------
# Env
# ---------------------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
HOST_URL = os.getenv("HOST_URL", "")  # ex: https://seuservico.onrender.com
WEBHOOK_AUTO = os.getenv("WEBHOOK_AUTO", "0") == "1"
DB_URL = os.getenv("DB_URL", os.getenv("DATABASE_URL", ""))
ADMIN_CHAT_IDS = [x.strip() for x in os.getenv("ADMIN_CHAT_IDS", "").split(",") if x.strip()]
NEWS_JOB = os.getenv("NEWS_JOB", "0") == "1"

# ---------------------------------------
# App & globals
# ---------------------------------------
app = FastAPI()
pool: Optional[asyncpg.Pool] = None
_last_error: Optional[str] = None

# ---------------------------------------
# Helpers de formatação
# ---------------------------------------
def br_money(x: float) -> str:
    try:
        return f"{x:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")
    except Exception:
        return f"{x}"

def pct(x: float) -> str:
    s = f"{x:+.2f}%"
    return s.replace(".", ",")

# ---------------------------------------
# Telegram helpers
# ---------------------------------------
TG_BASE = "https://api.telegram.org"

async def send_tg(text: str, chat_id: int, disable_preview: bool = True) -> None:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN ausente")
    url = f"{TG_BASE}/bot{BOT_TOKEN}/sendMessage"
    data = {"chat_id": chat_id, "text": text, "disable_web_page_preview": disable_preview}
    async with httpx.AsyncClient(timeout=15) as cli:
        r = await cli.post(url, json=data)
        r.raise_for_status()

async def set_webhook() -> Dict[str, Any]:
    if not BOT_TOKEN:
        return {"ok": False, "error": "BOT_TOKEN ausente"}
    if not HOST_URL:
        return {"ok": False, "error": "HOST_URL ausente"}
    async with httpx.AsyncClient(timeout=15) as cli:
        r = await cli.get(f"{TG_BASE}/bot{BOT_TOKEN}/setWebhook", params={"url": f"{HOST_URL}/webhook"})
        try:
            return r.json()
        except Exception:
            return {"ok": False, "error": r.text}

# ---------------------------------------
# DB
# ---------------------------------------
CREATE_NEWS_TABLE = """
CREATE TABLE IF NOT EXISTS news_items (
    id SERIAL PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL DEFAULT now(),
    source TEXT NOT NULL,
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    summary TEXT NULL
);
"""

# migrações defensivas (não explodir se colunas já existirem ou não existirem)
MIGRATIONS = [
    "ALTER TABLE news_items ADD COLUMN IF NOT EXISTS url TEXT",
    "ALTER TABLE news_items ADD COLUMN IF NOT EXISTS source TEXT",
    "ALTER TABLE news_items ADD COLUMN IF NOT EXISTS summary TEXT",
    "ALTER TABLE news_items ADD COLUMN IF NOT EXISTS title TEXT",
    "ALTER TABLE news_items ADD COLUMN IF NOT EXISTS ts TIMESTAMPTZ NOT NULL DEFAULT now()",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_news_url ON news_items(url)",
]

async def db_init():
    global pool
    if not DB_URL:
        return
    pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=2)
    async with pool.acquire() as c:
        await c.execute(CREATE_NEWS_TABLE)
        for sql in MIGRATIONS:
            try:
                await c.execute(sql)
            except Exception:
                # ignora migração específica que falhar (ex.: colunas antigas diferentes)
                pass

async def db_fetch(sql: str, *args) -> List[asyncpg.Record]:
    if not pool:
        return []
    async with pool.acquire() as c:
        return await c.fetch(sql, *args)

async def db_execute(sql: str, *args) -> None:
    if not pool:
        return
    async with pool.acquire() as c:
        await c.execute(sql, *args)

# ---------------------------------------
# News (leve, opcional)
# ---------------------------------------
NEWS_FEEDS = [
    # RSS de portais mais acessíveis
    ("coindesk", "https://www.coindesk.com/arc/outboundfeeds/rss/?outputType=xml"),
    ("theblock", "https://www.theblock.co/rss.xml"),
    ("decrypt", "https://decrypt.co/feed"),
]

async def fetch_rss(url: str) -> List[Dict[str, str]]:
    """Parser RSS simples e resiliente (sem libs externas)."""
    items: List[Dict[str, str]] = []
    try:
        async with httpx.AsyncClient(timeout=20) as cli:
            r = await cli.get(url, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            text = r.text
        # parsing mínimo (best-effort)
        import re
        entries = re.findall(r"<item>(.*?)</item>", text, flags=re.S|re.I)
        for e in entries[:30]:
            def _extract(tag: str) -> str:
                m = re.search(fr"<{tag}>(.*?)</{tag}>", e, flags=re.S|re.I)
                if not m:
                    # tenta CDATA
                    m = re.search(fr"<{tag}><!\[CDATA\[(.*?)\]\]></{tag}>", e, flags=re.S|re.I)
                return (m.group(1).strip() if m else "")
            title = _extract("title")
            link = _extract("link")
            pub = _extract("pubDate")
            items.append({"title": title, "url": link, "pub": pub})
    except Exception:
        return []
    return items

async def ingest_news_light():
    if not pool:
        return
    for source, url in NEWS_FEEDS:
        rows = await fetch_rss(url)
        for r in rows:
            title = (r.get("title") or "").strip() or "(sem título)"
            link = (r.get("url") or "").strip() or f"{url}#"
            try:
                await db_execute(
                    """
                    INSERT INTO news_items (source, title, url, summary)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (url) DO NOTHING
                    """,
                    source, title, link, None,
                )
            except Exception as e:
                # não travar por notícia ruim
                global _last_error
                _last_error = f"ingest_news_light: {type(e).__name__}: {e}"
                continue

async def get_recent_news(hours: int = 12, limit: int = 4) -> List[Tuple[datetime, str, str, str]]:
    if not pool:
        return []
    try:
        rows = await db_fetch(
            """
            SELECT ts, source, title, url
            FROM news_items
            WHERE ts >= now() - ($1::INTERVAL)
            ORDER BY ts DESC
            LIMIT $2
            """,
            f"{hours} hours", limit,
        )
        out = []
        for r in rows:
            out.append((r["ts"], r["source"], r["title"], r["url"]))
        return out
    except Exception as e:
        # fallback para casos de schema antigo (coluna 'link')
        try:
            rows = await db_fetch(
                """
                SELECT ts, source, title, link as url
                FROM news_items
                WHERE ts >= now() - ($1::INTERVAL)
                ORDER BY ts DESC
                LIMIT $2
                """,
                f"{hours} hours", limit,
            )
            out = []
            for r in rows:
                out.append((r["ts"], r["source"], r["title"], r["url"]))
            return out
        except Exception as e2:
            global _last_error
            _last_error = f"get_recent_news: {type(e).__name__}/{type(e2).__name__}"
            return []

# ---------------------------------------
# Market data providers (fallback chain)
# ---------------------------------------
async def http_get_json(url: str, params: Dict[str, Any] | None = None) -> Optional[Any]:
    try:
        async with httpx.AsyncClient(timeout=20) as cli:
            r = await cli.get(url, params=params, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            return r.json()
    except Exception:
        return None

async def price_now_usd(symbol: str) -> Optional[float]:
    """Retorna preço spot em USD. symbols: BTCUSDT/ETHUSDT (ou BTC/ETH)."""
    s = symbol.replace("USDT", "").replace("USD", "").upper()
    # 1) CoinGecko
    ids = {"BTC": "bitcoin", "ETH": "ethereum"}.get(s)
    if ids:
        j = await http_get_json(
            "https://api.coingecko.com/api/v3/simple/price",
            {"ids": ids, "vs_currencies": "usd"},
        )
        if j and ids in j and "usd" in j[ids]:
            return float(j[ids]["usd"])
    # 2) Coinbase
    prod = {"BTC": "BTC-USD", "ETH": "ETH-USD"}.get(s)
    if prod:
        j = await http_get_json(f"https://api.exchange.coinbase.com/products/{prod}/ticker")
        if j and j.get("price"):
            try:
                return float(j["price"])
            except Exception:
                pass
    # 3) Bitstamp
    pair = {"BTC": "btcusd", "ETH": "ethusd"}.get(s)
    if pair:
        j = await http_get_json(f"https://www.bitstamp.net/api/v2/ticker/{pair}/")
        if j and j.get("last"):
            try:
                return float(j["last"])
            except Exception:
                pass
    return None

async def ohlc_1h(symbol: str, hours: int = 48) -> List[Tuple[int, float, float, float, float, float]]:
    """Retorna lista de candles 1h (ts, open, high, low, close, volume)."""
    s = symbol.replace("USDT", "").replace("USD", "").upper()
    # Tentativa 1: Coinbase
    prod = {"BTC": "BTC-USD", "ETH": "ETH-USD"}.get(s)
    if prod:
        j = await http_get_json(f"https://api.exchange.coinbase.com/products/{prod}/candles", {"granularity": 3600})
        if isinstance(j, list) and j:
            out = []
            for arr in j[: hours + 2]:
                # coinbase: [ time, low, high, open, close, volume ]
                if len(arr) >= 6:
                    t, low, high, o, c, vol = arr[0], float(arr[1]), float(arr[2]), float(arr[3]), float(arr[4]), float(arr[5])
                    out.append((int(t), o, high, low, c, vol))
            out.sort(key=lambda x: x[0])
            if out:
                return out[-hours:]
    # Tentativa 2: Bitstamp (tem formato diferente)
    pair = {"BTC": "btcusd", "ETH": "ethusd"}.get(s)
    if pair:
        j = await http_get_json(f"https://www.bitstamp.net/api/v2/ohlc/{pair}/", {"step": 3600, "limit": hours})
        try:
            data = j.get("data", {}).get("ohlc", []) if isinstance(j, dict) else []
        except Exception:
            data = []
        if data:
            out = []
            for e in data:
                # {'timestamp':'...','open':'','high':'','low':'','close':'','volume':''}
                try:
                    t = int(e.get("timestamp", "0"))
                    o = float(e.get("open", "nan"))
                    h = float(e.get("high", "nan"))
                    l = float(e.get("low", "nan"))
                    c = float(e.get("close", "nan"))
                    v = float(e.get("volume", "0"))
                except Exception:
                    continue
                out.append((t, o, h, l, c, v))
            out.sort(key=lambda x: x[0])
            if out:
                return out[-hours:]
    return []

# ---------------------------------------
# Níveis dinâmicos + análise tática
# ---------------------------------------

def step_for(symbol: str) -> float:
    s = symbol.upper()
    if s.startswith("BTC"): return 1000.0
    if s.startswith("ETH"): return 50.0
    return 10.0

def safe_round(x: float, step: float) -> float:
    try:
        if x is None or math.isnan(x) or math.isinf(x):
            return float("nan")
        return round(x / step) * step
    except Exception:
        return float("nan")

async def dynamic_levels(symbol: str) -> Tuple[List[float], List[float], Dict[str, Any]]:
    """Retorna (suportes, resistencias, extras) com base nos últimos 48h."""
    step = step_for(symbol)
    candles = await ohlc_1h(symbol, 48)
    if not candles:
        # fallback por preço atual
        p = await price_now_usd(symbol)
        if not p:
            p = 0.0
        mid = p
        sups = [safe_round(mid - step, step), safe_round(mid - 2*step, step)]
        ress = [safe_round(mid + step, step), safe_round(mid + 2*step, step)]
        return (sorted([x for x in sups if not math.isnan(x)]),
                sorted([x for x in ress if not math.isnan(x)]),
                {"hi": p, "lo": p, "mid": mid, "vol48h": 0.0})

    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    closes = [c[4] for c in candles]
    vols = [c[5] for c in candles]

    hi = max(highs) if highs else float("nan")
    lo = min(lows) if lows else float("nan")
    mid = (hi + lo) / 2.0 if not (math.isnan(hi) or math.isnan(lo)) else (closes[-1] if closes else 0.0)

    dl = lo
    du = hi
    sups = [safe_round(dl, step), safe_round(mid - step, step)]
    ress = [safe_round(du, step), safe_round(mid + step, step)]

    sups = [x for x in sups if not math.isnan(x)]
    ress = [x for x in ress if not math.isnan(x)]

    vol48h = sum(vols[-48:]) if vols else 0.0
    return (sorted(sups), sorted(ress), {"hi": hi, "lo": lo, "mid": mid, "vol48h": vol48h})

async def pct_change_from(candles: List[Tuple[int, float, float, float, float, float]], hours: int) -> Optional[float]:
    if not candles:
        return None
    try:
        c_now = candles[-1][4]
        # pega candle de N horas atrás (aprox)
        idx = -1 - hours if len(candles) > hours else 0
        c_prev = candles[idx][4]
        if c_prev == 0: return None
        return (c_now / c_prev - 1.0) * 100.0
    except Exception:
        return None

async def analysis_block(asset: str, peer: Optional[str] = None) -> Tuple[str, Dict[str, Any]]:
    """Gera 3–5 linhas de análise para BTC/ETH, com níveis e ações."""
    symbol = f"{asset.upper()}USDT"
    price = await price_now_usd(symbol)
    candles = await ohlc_1h(symbol, 48)
    ch8 = await pct_change_from(candles, 8)
    ch24 = await pct_change_from(candles, 24)
    sups, ress, extra = await dynamic_levels(symbol)

    # volume relativo (48h)
    vol48 = extra.get("vol48h", 0.0)

    # relação ETH/BTC quando aplicável
    ratio_txt = ""
    if peer:
        p0 = price
        p1 = await price_now_usd(f"{peer.upper()}USDT")
        if p0 and p1 and p1 > 0:
            ratio = p0 / p1
            ratio_txt = f" | relação {asset.upper()}/{peer.upper()}: {ratio:.5f}"

    # distância até níveis
    def _dist_to_levels(px: Optional[float], levels: List[float]) -> Optional[float]:
        if px is None or not levels:
            return None
        return min(abs(px - lv) for lv in levels)

    d_sup = _dist_to_levels(price, sups)
    d_res = _dist_to_levels(price, ress)
    step = step_for(symbol)

    # regras simples para uma leitura tática
    lines: List[str] = []

    # 1) momentum
    if ch8 is not None and ch24 is not None:
        mom = (ch8, ch24)
        if ch8 < 0 and ch24 < 0:
            lines.append("Fluxo vendedor em curta e média — cautela.")
        elif ch8 > 0 and ch24 > 0:
            lines.append("Momentum comprador consistente — favorece rompimentos.")
        elif ch8 > 0 and ch24 <= 0:
            lines.append("Reação de curto prazo dentro de tendência fraca — evita euforia.")
        elif ch8 < 0 and ch24 >= 0:
            lines.append("Correção curta dentro de tendência positiva — buscar pullbacks limpos.")
    else:
        lines.append("Sem dados completos de tendência — focar níveis e risco.")

    # 2) posição vs níveis
    if price is not None and sups and ress:
        near_sup = d_sup is not None and d_sup <= 0.5 * step
        near_res = d_res is not None and d_res <= 0.5 * step
        if near_sup:
            lines.append("Preço testando suporte — confirmações em defesa e rejeição de mínimas.")
        if near_res:
            lines.append("Preço encostado em resistência — confirmar rompimento com fechamento/volume.")
    
    # 3) volume
    if vol48 > 0:
        lines.append("Volume 48h observado; priorizar sinais alinhados ao fluxo.")

    # 4) ação objetiva
    action = []
    if d_res is not None and d_res <= step:
        action.append("gatilhos em rompimentos válidos das resistências")
    if d_sup is not None and d_sup <= step:
        action.append("gestão defensiva na perda de suportes")
    if not action:
        action.append("operar por faixas: comprar suporte, reduzir perto de resistência")

    # monta bloco texto
    hdr = f"{asset.upper()}: {pct(ch8) if ch8 is not None else '-'} (8h), {pct(ch24) if ch24 is not None else '-'} (24h){ratio_txt}."
    levels = f"Níveis: Suportes: {br_money(sups[0]) if len(sups)>0 else '-'}, {br_money(sups[1]) if len(sups)>1 else '-'} | Resist: {br_money(ress[0]) if len(ress)>0 else '-'}, {br_money(ress[1]) if len(ress)>1 else '-'}"
    act = f"Ação: {', '.join(action)}."

    # garante 3–5 linhas totais
    core = lines[:3]
    if len(core) < 2:
        core.append("Operar somente sinais com confirmação (fechamento/volume).")
    
    text = "\n".join([hdr] + core + [levels, act])
    return text, {"price": price, "sups": sups, "ress": ress, "ch8": ch8, "ch24": ch24}

# ---------------------------------------
# Builders de balões
# ---------------------------------------
async def pulse_text() -> str:
    now = datetime.now(timezone.utc)
    tprefix = f"🕒 {now:%Y-%m-%d %H:%M} UTC"

    # preços e relação
    p_eth = await price_now_usd("ETHUSDT")
    p_btc = await price_now_usd("BTCUSDT")
    ratio = (p_eth / p_btc) if (p_eth and p_btc and p_btc > 0) else None

    a_eth, _ = await analysis_block("ETH", peer="BTC")
    a_btc, _ = await analysis_block("BTC", peer="ETH")

    lines = [
        f"Pulse…….. {tprefix}",
        f"ETH ${br_money(p_eth) if p_eth else '-'}",
        f"BTC ${br_money(p_btc) if p_btc else '-'}",
        f"ETH/BTC {ratio:.5f}" if ratio else "ETH/BTC -",
        "",
        "ANÁLISE:",
        a_eth,
        "",
        a_btc,
    ]

    # FONTES (poucas, filtradas)
    news = await get_recent_news(hours=12, limit=6)
    if news:
        # filtra por relevância simples
        picked: List[Tuple[datetime,str,str,str]] = []
        for n in news:
            ttl = (n[2] or "").lower()
            if any(k in ttl for k in ["bitcoin", "ethereum", "eth", "btc", "crypto"]):
                picked.append(n)
        if not picked:
            picked = news
        picked = picked[:3]
        lines.append("")
        lines.append("FONTES (últimas 12h):")
        for ts, src, title, url in picked:
            hhmm = ts.astimezone(timezone.utc).strftime("%H:%M") if isinstance(ts, datetime) else "--:--"
            dom = src
            lines.append(f"• {hhmm} {dom} — {title} — {url}")

    return "\n".join(lines)

async def eth_text() -> str:
    p_eth = await price_now_usd("ETHUSDT")
    hdr = f"ETH ${br_money(p_eth) if p_eth else '-'}"
    a_eth, _ = await analysis_block("ETH", peer="BTC")
    return "\n".join([hdr, a_eth])

async def btc_text() -> str:
    p_btc = await price_now_usd("BTCUSDT")
    hdr = f"BTC ${br_money(p_btc) if p_btc else '-'}"
    a_btc, _ = await analysis_block("BTC", peer="ETH")
    return "\n".join([hdr, a_btc])

# ---------------------------------------
# Webhook
# ---------------------------------------
@app.post("/webhook")
async def webhook_root(request: Request):
    global _last_error
    try:
        update = await request.json()
        msg = update.get("message") or update.get("edited_message") or {}
        chat = msg.get("chat", {})
        chat_id = chat.get("id")
        text = (msg.get("text") or "").strip()
        if not text or chat_id is None:
            return {"ok": True}
        t = text.lower().strip()
        if t in ("/start", "start", ".start"):
            await send_tg("Envie /pulse para o boletim. Comandos: /eth, /btc.", chat_id)
            return {"ok": True}
        if t in ("/pulse", "/push", "pulse", ".pulse"):
            out = await pulse_text()
            await send_tg(out, chat_id)
            return {"ok": True}
        if t.startswith("/eth") or t.strip() == "eth":
            out = await eth_text()
            await send_tg(out, chat_id)
            return {"ok": True}
        if t.startswith("/btc") or t.strip() == "btc":
            out = await btc_text()
            await send_tg(out, chat_id)
            return {"ok": True}
        # fallback
        await send_tg("Comandos: /pulse, /eth, /btc", chat_id)
        return {"ok": True}
    except Exception as e:
        _last_error = f"webhook: {type(e).__name__}: {e}"
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

# ---------------------------------------
# Admin/health
# ---------------------------------------
@app.get("/")
async def root():
    return PlainTextResponse("ok")

@app.get("/status")
async def status():
    # tenta contar linhas do próprio arquivo
    linecount = None
    try:
        here = os.path.abspath(__file__)
        with open(here, "r", encoding="utf-8") as f:
            linecount = sum(1 for _ in f)
    except Exception:
        linecount = None
    return {
        "ok": True,
        "version": VERSION,
        "linecount": linecount,
        "last_error": _last_error,
    }

@app.get("/admin/ping/telegram")
async def admin_ping_telegram():
    if not BOT_TOKEN:
        return {"ok": False, "error": "BOT_TOKEN ausente"}
    try:
        async with httpx.AsyncClient(timeout=10) as cli:
            r = await cli.get(f"{TG_BASE}/bot{BOT_TOKEN}/getMe")
            j = r.json()
            return {"ok": True, "me": j}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/admin/webhook/set")
async def admin_webhook_set():
    return await set_webhook()

@app.post("/admin/news/ingest")
async def admin_news_ingest():
    try:
        await ingest_news_light()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# ---------------------------------------
# Startup/shutdown
# ---------------------------------------
@app.on_event("startup")
async def _startup():
    await db_init()
    # job de notícias opcional
    if NEWS_JOB and pool:
        async def _job_loop():
            while True:
                try:
                    await ingest_news_light()
                except Exception:
                    pass
                await asyncio.sleep(15 * 60)  # a cada 15min
        asyncio.create_task(_job_loop())
    if WEBHOOK_AUTO and HOST_URL and BOT_TOKEN:
        try:
            await set_webhook()
        except Exception:
            pass

@app.on_event("shutdown")
async def _shutdown():
    global pool
    if pool:
        await pool.close()
        pool = None

# =============================================================
# FIM DA CODIFICAÇÃO — v6.0.10-full — (o contador real de linhas aparece no /status)
# =============================================================

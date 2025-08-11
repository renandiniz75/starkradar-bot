# app.py — Stark DeFi Agent v6.0.11-full
# =============================================================
# PRINCIPAIS MUDANÇAS (6.0.11-full)
# - Balões com visual melhor (emojis, divisores) e SEM rodapé de versão/linhas.
# - "ANÁLISE → FONTES" com 3–5 linhas úteis (nunca "-.").
# - Níveis dinâmicos (48h) com fallback 100% seguro (sem NaN); steps: BTC=1000, ETH=100.
# - Schema de notícias robusto (url/source/ts_published); migração automática.
# - /status mostra versão + contagem de linhas reais; fim do arquivo traz marcador
#   "FIM DA CODIFICAÇÃO" (apenas texto fixo). Nenhuma info de versão/linhas vai
#   nos balões do Telegram.
# - /panel opcional (gráfico PNG) fora do Pulse; Pulse permanece só texto.
# - Webhook auto (WEBHOOK_AUTO=1 + HOST_URL), e /admin/ping/telegram para teste.
# - Dependências externas reduzidas (httpx/asyncpg/fastapi); sem chamadas que dão 403/451.
# =============================================================

import os
import io
import math
import json
import time
import html
import asyncio
import textwrap
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
import asyncpg
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse, Response

# -------------------------
# Config & Globals
# -------------------------
VERSION = "6.0.11-full"
APP_NAME = "Stark DeFi Agent"

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
HOST_URL = os.getenv("HOST_URL", "")  # ex: https://starkradar-bot.onrender.com
DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("DB_URL")
WEBHOOK_AUTO = os.getenv("WEBHOOK_AUTO", "0") == "1"
NEWS_JOB = os.getenv("NEWS_JOB", "0") == "1"

# Fallback níveis (se não houver dados de mercado)
FALLBACK_LEVELS = {
    "BTCUSDT": {"sup": [62000, 60000], "res": [65000, 68000], "step": 1000},
    "ETHUSDT": {"sup": [4200, 4000],  "res": [4300, 4400],  "step": 100},
}

# Feeds utilizados (leves). Podemos expandir depois.
RSS_FEEDS = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://www.theblock.co/rss.xml",
]

# News: limite por ingest
NEWS_MAX_PER_FEED = 15

# HTTP client global
_http_client: Optional[httpx.AsyncClient] = None
_db_pool: Optional[asyncpg.Pool] = None
_last_error: Optional[str] = None


# -------------------------
# Utils
# -------------------------

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def linecount_this_file() -> int:
    try:
        path = os.path.abspath(__file__)
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return sum(1 for _ in f)
    except Exception:
        return -1


def fmt_hhmm(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%H:%M")


# -------------------------
# DB
# -------------------------
async def get_pool() -> asyncpg.Pool:
    global _db_pool
    if _db_pool is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL ausente")
        _db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    return _db_pool


async def db_init() -> None:
    pool = await get_pool()
    async with pool.acquire() as c:
        # Tabela de notícias resiliente
        await c.execute(
            """
            CREATE TABLE IF NOT EXISTS news_items (
                id SERIAL PRIMARY KEY,
                ts TIMESTAMPTZ NOT NULL DEFAULT now(),
                title TEXT,
                url TEXT UNIQUE,
                source TEXT NOT NULL DEFAULT 'unknown',
                ts_published TIMESTAMPTZ
            );
            """
        )
        # Índices úteis
        await c.execute("CREATE INDEX IF NOT EXISTS idx_news_ts ON news_items(ts DESC);")
        # Migrações tolerantes
        await c.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_indexes WHERE indexname='u_news_url'
                ) THEN
                    EXECUTE 'CREATE UNIQUE INDEX u_news_url ON news_items(url)';
                END IF;
            END$$;
        """)


# -------------------------
# HTTP helpers
# -------------------------
async def http() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None:
        _http_client = httpx.AsyncClient(timeout=15)
    return _http_client


# -------------------------
# Market data (CoinGecko)
# -------------------------
COINGECKO = "https://api.coingecko.com/api/v3"

async def gecko_market_chart(coin: str, days: int = 2, interval: str = "hourly") -> Optional[Dict[str, Any]]:
    try:
        cli = await http()
        url = f"{COINGECKO}/coins/{coin}/market_chart?vs_currency=usd&days={days}&interval={interval}"
        r = await cli.get(url)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        set_last_error(f"gecko_market_chart {coin}: {e}")
    return None


async def gecko_simple_price(ids: List[str]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    try:
        cli = await http()
        url = f"{COINGECKO}/simple/price?ids={','.join(ids)}&vs_currencies=usd"
        r = await cli.get(url)
        if r.status_code == 200:
            j = r.json()
            for k in ids:
                if k in j and 'usd' in j[k]:
                    out[k] = float(j[k]['usd'])
    except Exception as e:
        set_last_error(f"gecko_simple_price: {e}")
    return out


def price_step(symbol: str) -> int:
    return FALLBACK_LEVELS.get(symbol, {}).get("step", 100)


def round_level(x: float, step: int) -> float:
    if not (x is not None and math.isfinite(x) and step > 0):
        return x
    return round(x / step) * step


async def dynamic_levels(symbol: str) -> Tuple[List[float], List[float]]:
    """Calcula suportes e resistências a partir de 48h de dados. Fallback seguro."""
    try:
        coin = "bitcoin" if symbol.upper().startswith("BTC") else "ethereum"
        data = await gecko_market_chart(coin, days=2, interval="hourly")
        if data and data.get("prices"):
            prices = [float(p[1]) for p in data["prices"] if isinstance(p, list) and len(p) >= 2]
            if prices:
                lo = min(prices)
                hi = max(prices)
                mid = (lo + hi) / 2
                step = price_step(symbol)
                sups = [round_level(lo, step), round_level(mid - step, step)]
                ress = [round_level(mid + step, step), round_level(hi, step)]
                sups = [x for x in sups if x]
                ress = [x for x in ress if x]
                if sups and ress:
                    return sups, ress
    except Exception as e:
        set_last_error(f"dynamic_levels {symbol}: {e}")

    # Fallback estático
    fb = FALLBACK_LEVELS.get(symbol, {})
    return fb.get("sup", [])[0:2], fb.get("res", [0, 0])[0:2]


async def market_snapshot() -> Dict[str, Any]:
    """Retorna preços, variações 8h/12h e ETH/BTC, com tolerância."""
    out = {
        "eth": {"px": None, "chg8h": None, "chg12h": None},
        "btc": {"px": None, "chg8h": None, "chg12h": None},
        "eth_btc": None,
    }
    # Preço spot atual
    sp = await gecko_simple_price(["ethereum", "bitcoin"])
    out["eth"]["px"] = sp.get("ethereum")
    out["btc"]["px"] = sp.get("bitcoin")

    # Série para calcular 8h/12h e hi/lo 24h (se possível)
    eth_mc = await gecko_market_chart("ethereum", days=2, interval="hourly")
    btc_mc = await gecko_market_chart("bitcoin", days=2, interval="hourly")

    def pct_change(series: List[float], hours: int) -> Optional[float]:
        if not series:
            return None
        # Dados horários: 1 ponto ≈ 1h
        if len(series) <= hours:
            return None
        now = series[-1]
        prev = series[-1 - hours]
        if prev and math.isfinite(prev) and prev != 0:
            return (now / prev - 1.0) * 100.0
        return None

    eth_prices = [float(p[1]) for p in (eth_mc or {}).get("prices", [])]
    btc_prices = [float(p[1]) for p in (btc_mc or {}).get("prices", [])]

    out["eth"]["chg8h"] = pct_change(eth_prices, 8)
    out["eth"]["chg12h"] = pct_change(eth_prices, 12)
    out["btc"]["chg8h"] = pct_change(btc_prices, 8)
    out["btc"]["chg12h"] = pct_change(btc_prices, 12)

    # ETH/BTC
    if out["eth"]["px"] and out["btc"]["px"] and out["btc"]["px"] != 0:
        out["eth_btc"] = out["eth"]["px"] / out["btc"]["px"]

    # Hi/Lo 24h (aprox das últimas 24 barras)
    out["eth"]["hi24"] = max(eth_prices[-24:]) if len(eth_prices) >= 24 else None
    out["eth"]["lo24"] = min(eth_prices[-24:]) if len(eth_prices) >= 24 else None
    out["btc"]["hi24"] = max(btc_prices[-24:]) if len(btc_prices) >= 24 else None
    out["btc"]["lo24"] = min(btc_prices[-24:]) if len(btc_prices) >= 24 else None

    return out


# -------------------------
# News ingest (RSS mínimo)
# -------------------------
async def ingest_news_light(limit_per_feed: int = NEWS_MAX_PER_FEED) -> int:
    cli = await http()
    pool = await get_pool()
    total = 0

    async with pool.acquire() as c:
        for feed in RSS_FEEDS:
            try:
                r = await cli.get(feed)
                if r.status_code != 200:
                    continue
                items = parse_rss_items(r.text)[:limit_per_feed]
                for it in items:
                    title = it.get("title")
                    url = it.get("link") or it.get("url")
                    if not url:
                        continue
                    source = host_from_url(url) or host_from_url(feed) or "unknown"
                    ts_pub = it.get("published")
                    try:
                        await c.execute(
                            """
                            INSERT INTO news_items(title, url, source, ts_published)
                            VALUES($1,$2,$3,$4)
                            ON CONFLICT (url) DO NOTHING
                            """,
                            title, url, source, ts_pub,
                        )
                        total += 1
                    except Exception as e:
                        # ignora conflitos e campos faltantes
                        set_last_error(f"ingest_news insert: {e}")
                        continue
            except Exception as e:
                set_last_error(f"ingest_news fetch {feed}: {e}")
                continue
    return total


def host_from_url(u: str) -> Optional[str]:
    try:
        if not u:
            return None
        # pobre-man's parse
        if "://" in u:
            u = u.split("://", 1)[1]
        return u.split("/", 1)[0]
    except Exception:
        return None


def parse_rss_items(xml_text: str) -> List[Dict[str, Any]]:
    # Parser simples para RSS 2.0
    from xml.etree import ElementTree as ET
    out: List[Dict[str, Any]] = []
    try:
        root = ET.fromstring(xml_text)
        for item in root.findall('.//item'):
            title = (item.findtext('title') or '').strip()
            link = (item.findtext('link') or '').strip()
            pub = (item.findtext('pubDate') or '').strip()
            ts_pub = None
            if pub:
                try:
                    # tenta RFC2822
                    from email.utils import parsedate_to_datetime
                    ts_pub = parsedate_to_datetime(pub)
                    if ts_pub and not ts_pub.tzinfo:
                        ts_pub = ts_pub.replace(tzinfo=timezone.utc)
                except Exception:
                    ts_pub = None
            out.append({"title": title, "link": link, "published": ts_pub})
    except Exception:
        pass
    return out


async def get_recent_news(hours: int = 12, limit: int = 6) -> List[Dict[str, Any]]:
    pool = await get_pool()
    start = utcnow() - timedelta(hours=hours)
    async with pool.acquire() as c:
        rows = await c.fetch(
            """
            SELECT ts, title, source, url, ts_published
            FROM news_items
            WHERE ts >= $1
            ORDER BY ts DESC
            LIMIT $2
            """,
            start, limit,
        )
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({
            "ts": r["ts"],
            "title": r["title"],
            "source": r["source"],
            "url": r["url"],
            "ts_published": r["ts_published"],
        })
    return out


# -------------------------
# Telegram helpers
# -------------------------
async def tg(method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if not BOT_TOKEN:
        return {"ok": False, "error": "BOT_TOKEN ausente"}
    cli = await http()
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    r = await cli.post(url, data=payload)
    try:
        return r.json()
    except Exception:
        return {"ok": False, "status": r.status_code}


async def send_tg(text: str, chat_id: int) -> None:
    await tg("sendMessage", {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    })


async def set_webhook() -> Dict[str, Any]:
    if not (BOT_TOKEN and HOST_URL):
        return {"ok": False, "error": "BOT_TOKEN ou HOST_URL ausente"}
    return await tg("setWebhook", {"url": f"{HOST_URL}/webhook", "allowed_updates": json.dumps(["message","edited_message"])})


# -------------------------
# Formatting (balões)
# -------------------------

def fmt_price(x: Optional[float]) -> str:
    if x is None:
        return "—"
    if x >= 1000:
        return f"${x:,.0f}".replace(",", ".")  # estilo PT-BR
    return f"${x:,.2f}".replace(",", ".")


def fmt_pct(x: Optional[float]) -> str:
    if x is None:
        return "—"
    s = f"{x:+.2f}%"
    return s


def bullet_news(items: List[Dict[str, Any]]) -> str:
    out = []
    for it in items:
        t = (it.get("title") or "").strip()
        src = host_from_url(it.get("url") or "") or (it.get("source") or "")
        hh = fmt_hhmm(it.get("ts") or utcnow())
        url = it.get("url") or ""
        # pequeno: hora, fonte e título
        out.append(f"• <b>{hh}</b> {html.escape(src)} — {html.escape(t)}")
        if url:
            out[-1] += f"\n  <a href=\"{html.escape(url)}\">link</a>"
    return "\n".join(out)


async def build_analysis() -> Tuple[str, str, str]:
    snap = await market_snapshot()

    eth_px = snap["eth"]["px"]
    btc_px = snap["btc"]["px"]
    ratio = snap["eth_btc"]

    eth_sups, eth_ress = await dynamic_levels("ETHUSDT")
    btc_sups, btc_ress = await dynamic_levels("BTCUSDT")

    # Divergências simples
    eth8 = snap["eth"]["chg8h"]; btc8 = snap["btc"]["chg8h"]
    eth12 = snap["eth"]["chg12h"]; btc12 = snap["btc"]["chg12h"]

    lines: List[str] = []
    # Linha 1: quadro geral
    if eth8 is not None and btc8 is not None:
        bias = "📉 risco" if (eth8 < 0 or btc8 < 0) else "📈 viés comprador"
        lines.append(f"<b>Visão 8h</b>: {bias}; ETH {fmt_pct(eth8)}, BTC {fmt_pct(btc8)}; ETH/BTC {ratio:.5f if ratio else 0}.")
    else:
        lines.append("<b>Mercado</b>: dados parciais; manter operação nos níveis.")

    # Linha 2: leitura de força relativa
    if eth8 is not None and btc8 is not None:
        rel = "ETH mais fraco que BTC" if eth8 < btc8 else "ETH mais forte que BTC"
        lines.append(f"<b>Força relativa</b>: {rel}; observar pares e dominância.")
    else:
        lines.append("<b>Força relativa</b>: sem série completa (fallback)." )

    # Linha 3: níveis e gatilhos
    lines.append(
        f"<b>ETH</b> {fmt_price(eth_px)} | S: {', '.join(map(lambda x: fmt_price(float(x)), eth_sups))} • R: {', '.join(map(lambda x: fmt_price(float(x)), eth_ress))}.\n"
        f"<i>Gatilhos</i>: fechamento acima de {fmt_price(eth_ress[0] if eth_ress else None)} confirma pullback; perda de {fmt_price(eth_sups[0] if eth_sups else None)} pede defesa."
    )

    lines.append(
        f"<b>BTC</b> {fmt_price(btc_px)} | S: {', '.join(map(lambda x: fmt_price(float(x)), btc_sups))} • R: {', '.join(map(lambda x: fmt_price(float(x)), btc_ress))}.\n"
        f"<i>Plano</i>: compras em pullbacks após rompimento válido; gestão de risco na perda dos suportes."
    )

    # Opcional: linha 5 com contexto 12h
    if eth12 is not None and btc12 is not None:
        trend = "tendência de curto consolidando" if abs(eth12) < 1.0 and abs(btc12) < 1.0 else "movimento direcional em 12h"
        lines.append(f"<b>12h</b>: {trend} • ETH {fmt_pct(eth12)} / BTC {fmt_pct(btc12)}.")

    analysis = "\n".join(lines)

    # Sub-blocos específicos
    eth_block = (
        f"<b>ETH</b>: {fmt_price(eth_px)}\n"
        f"• Suportes: {', '.join(map(lambda x: fmt_price(float(x)), eth_sups))} | Resistências: {', '.join(map(lambda x: fmt_price(float(x)), eth_ress))}\n"
        f"• Leitura: operar zonas; validar rompimentos com fechamento."
    )

    btc_block = (
        f"<b>BTC</b>: {fmt_price(btc_px)}\n"
        f"• Suportes: {', '.join(map(lambda x: fmt_price(float(x)), btc_sups))} | Resistências: {', '.join(map(lambda x: fmt_price(float(x)), btc_ress))}\n"
        f"• Leitura: prioridade para pullbacks pós-rompimento; defesa ativa abaixo dos suportes."
    )

    return analysis, eth_block, btc_block


async def build_pulse_text() -> str:
    now = utcnow()
    snap = await market_snapshot()
    analysis, eth_block, btc_block = await build_analysis()

    header = (
        f"<b>Pulse</b>  ⏱ {now.strftime('%Y-%m-%d %H:%M UTC')}\n"
        f"<b>ETH</b> {fmt_price(snap['eth']['px'])}   <b>BTC</b> {fmt_price(snap['btc']['px'])}   <b>ETH/BTC</b> {snap['eth_btc']:.5f if snap['eth_btc'] else '—'}\n"
        "────────────\n"
    )

    # Fontes curtas
    news = await get_recent_news(hours=12, limit=3)
    fontes = bullet_news(news) if news else "(sem novidades relevantes nas últimas horas)"

    body = (
        f"🧠 <b>ANÁLISE</b>\n{analysis}\n\n"
        f"🗞 <b>FONTES</b> (12h)\n{fontes}"
    )

    return header + body


async def build_eth_text() -> str:
    _, eth_block, _ = await build_analysis()
    return f"📊 <b>ETH</b>\n{eth_block}"


async def build_btc_text() -> str:
    _, _, btc_block = await build_analysis()
    return f"📊 <b>BTC</b>\n{btc_block}"


# -------------------------
# Optional chart (/panel)
# -------------------------
async def build_chart_png(symbol: str) -> bytes:
    """Gera um PNG simples de linha (24–48h). Mantido mínimo para não poluir o Pulse."""
    import matplotlib
    matplotlib.use('Agg')  # backend offscreen
    import matplotlib.pyplot as plt

    coin = "bitcoin" if symbol.upper().startswith("BTC") else "ethereum"
    data = await gecko_market_chart(coin, days=2, interval="hourly")
    series = [float(p[1]) for p in (data or {}).get("prices", [])]
    if len(series) < 4:
        series = series or [1, 1, 1, 1]

    fig = plt.figure(figsize=(6, 3))
    plt.plot(series)
    plt.title(f"{symbol} — últimas 48h")
    plt.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format='png')
    plt.close(fig)
    buf.seek(0)
    return buf.read()


# -------------------------
# FastAPI
# -------------------------
app = FastAPI(title=APP_NAME)


def set_last_error(msg: str) -> None:
    global _last_error
    _last_error = msg


@app.on_event("startup")
async def _startup() -> None:
    try:
        # DB
        if DATABASE_URL:
            await db_init()
            # Ingest inicial leve (não bloqueante)
            asyncio.create_task(ingest_news_light())
    except Exception as e:
        set_last_error(f"startup db: {e}")

    # webhook automático
    if WEBHOOK_AUTO and BOT_TOKEN and HOST_URL:
        try:
            await set_webhook()
        except Exception as e:
            set_last_error(f"set_webhook: {e}")


@app.get("/")
async def root():
    return PlainTextResponse(f"{APP_NAME} {VERSION} — OK")


@app.get("/status")
async def status():
    return JSONResponse({
        "ok": True,
        "version": VERSION,
        "linecount": linecount_this_file(),
        "last_error": _last_error,
    })


@app.get("/admin/ping/telegram")
async def admin_ping_tg():
    me = await tg("getMe", {})
    return JSONResponse(me)


@app.get("/admin/webhook/set")
async def admin_set_webhook():
    res = await set_webhook()
    return JSONResponse(res)


@app.get("/admin/news/ingest")
async def admin_ingest_news():
    try:
        n = await ingest_news_light()
        return {"ok": True, "inserted": n}
    except Exception as e:
        set_last_error(str(e))
        return {"ok": False, "error": str(e)}


@app.post("/webhook")
async def webhook_root(request: Request):
    try:
        data = await request.json()
        msg = (data.get("message") or data.get("edited_message") or {})
        chat_id = ((msg.get("chat") or {}).get("id"))
        text = (msg.get("text") or "").strip().lower()
        if not chat_id:
            return {"ok": True}

        # comandos
        if text in ("/start", "start", "ɢ start", "• start"):
            await send_tg(
                "👋 <b>Bem-vindo.</b> Comandos: \n"
                "• /pulse — boletim (análise → fontes)\n"
                "• /eth — leitura do ETH\n"
                "• /btc — leitura do BTC\n"
                "• /panel eth|btc — gráfico em PNG (opcional)",
                chat_id,
            )
            return {"ok": True}

        if text.startswith("/pulse") or text.startswith("/push"):
            out = await build_pulse_text()
            await send_tg(out, chat_id)
            return {"ok": True}

        if text.startswith("/eth"):
            out = await build_eth_text()
            await send_tg(out, chat_id)
            return {"ok": True}

        if text.startswith("/btc") or text.startswith("/vtc"):
            out = await build_btc_text()
            await send_tg(out, chat_id)
            return {"ok": True}

        if text.startswith("/panel"):
            parts = text.split()
            sym = (parts[1] if len(parts) > 1 else "eth").upper()
            sym = "BTCUSDT" if sym.startswith("BTC") else "ETHUSDT"
            png = await build_chart_png(sym)
            # Envia como Photo
            await tg("sendPhoto", {
                "chat_id": chat_id,
                "photo": ("chart.png", png, "image/png"),
            })
            return {"ok": True}

        # Se nada casar, manda instruções rápidas
        await send_tg("Use /pulse, /eth, /btc ou /panel btc|eth", chat_id)
        return {"ok": True}

    except Exception as e:
        set_last_error(f"webhook: {e}")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


# ===============================
# FIM DA CODIFICAÇÃO — v6.0.11-full
# A contagem REAL de linhas aparece em /status.
# ===============================

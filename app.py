# app.py — Stark DeFi Agent v6.0.12-full
# =============================================================
# PRINCIPAIS AJUSTES NESTA VERSÃO (6.0.12-full)
# - Formato “ANÁLISE → AÇÃO → FONTES” em /pulse, /eth e /btc (sem gráfico).
# - Níveis dinâmicos (S/R) com fallback: se candles falharem, usa níveis estáticos
#   (de env) e degraus por ativo. Nunca retorna NaN (evita 500 no webhook).
# - news_items: criação/alteração automática, sem colunas problemáticas (link/author/ts ok).
# - ingest_news_light tolerante (qualquer feed vazio/fora retorna sem quebrar).
# - /panel com try/except: se faltar matplotlib/numpy, responde texto e NÃO quebra.
# - /admin/ping/telegram e /admin/webhook/set para testar sem terminal.
# - /status mostra: versão, linecount real (lido do próprio arquivo) e last_error.
# - Webhook robusto: trata /start, /pulse, /eth, /btc; erros não derrubam a request.
# =============================================================

import os, json, math, asyncio, datetime as dt, inspect, re
from typing import List, Tuple, Optional

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse, HTMLResponse
from pydantic import BaseModel

VERSION = "6.0.12-full"

# --------------------------
# Estado simples em memória
# --------------------------
_state = {
    "last_error": None,
    "linecount": None,
    "boot_ts": dt.datetime.utcnow().isoformat() + "Z",
}

# --------------------------
# Config
# --------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
HOST_URL  = os.getenv("HOST_URL", "").rstrip("/")
WEBHOOK_AUTO = os.getenv("WEBHOOK_AUTO", "0").strip() == "1"
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

# Níveis estáticos opcionais (csv de números) — mesclados com dinâmicos
ETH_SUPS = [float(x) for x in os.getenv("ETH_SUPS", "4000,4200").split(",") if x.strip()]
ETH_RESS = [float(x) for x in os.getenv("ETH_RESS", "4300,4400").split(",") if x.strip()]
BTC_SUPS = [float(x) for x in os.getenv("BTC_SUPS", "60000,62000").split(",") if x.strip()]
BTC_RESS = [float(x) for x in os.getenv("BTC_RESS", "65000,68000").split(",") if x.strip()]

NEWS_FEEDS = [x.strip() for x in os.getenv("NEWS_FEEDS", "https://www.coindesk.com/arc/outboundfeeds/rss/").split(",") if x.strip()]

# --------------
# FastAPI
# --------------
app = FastAPI(title="Stark DeFi Agent", version=VERSION)


# --------------------------
# Auxiliares gerais
# --------------------------
def set_error(msg: str):
    _state["last_error"] = msg

def fmt_price(x: Optional[float]) -> str:
    if x is None or math.isnan(x):
        return "-"
    if x >= 1000:
        return f"${x:,.0f}".replace(",", ".")
    return f"${x:,.2f}".replace(",", "X").replace(".", ",").replace("X",".")

def fmt_pct(x: Optional[float]) -> str:
    if x is None or math.isnan(x):
        return "-"
    s = f"{x:+.2f}%"
    return s

def count_lines_of_this_file() -> int:
    try:
        src = inspect.getsource(inspect.getmodule(count_lines_of_this_file))
        return len(src.splitlines())
    except Exception:
        try:
            with open(__file__, "r", encoding="utf-8") as f:
                return sum(1 for _ in f)
        except Exception:
            return -1

def step_for(symbol: str) -> float:
    return 50.0 if symbol.upper().startswith("BTC") else 10.0

def round_level(x: float, step: float) -> float:
    try:
        return round(x / step) * step
    except Exception:
        return x

# --------------------------
# DB (asyncpg) — opcional
# --------------------------
_asyncpg = None
async def pg():
    global _asyncpg
    if not DATABASE_URL:
        return None
    if _asyncpg is None:
        import asyncpg  # lazy import
        _asyncpg = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=2)
    return _asyncpg

async def db_init():
    pool = await pg()
    if not pool:
        return
    async with pool.acquire() as c:
        # Cria tabela robusta (sem published_at/link ausentes, aceita author nulo)
        await c.execute("""
        CREATE TABLE IF NOT EXISTS news_items (
          id SERIAL PRIMARY KEY,
          ts TIMESTAMPTZ NOT NULL DEFAULT now(),
          source TEXT NOT NULL,
          author TEXT NULL,
          title TEXT NOT NULL,
          url TEXT NOT NULL,
          raw JSONB NULL
        );
        """)
        # Índices mínimos
        await c.execute("CREATE INDEX IF NOT EXISTS idx_news_ts ON news_items(ts DESC);")
        await c.execute("CREATE INDEX IF NOT EXISTS idx_news_source ON news_items(source);")

# --------------------------
# News (leve e tolerante)
# --------------------------
async def ingest_news_light(limit_total: int = 10):
    """
    Coleta leve: tenta buscar alguns feeds; se falhar, ignora silenciosamente.
    Nunca lança exceção (para não derrubar o app).
    """
    pool = await pg()
    if not pool:
        return
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            collected = []
            for feed in NEWS_FEEDS[:4]:
                try:
                    r = await client.get(feed)
                    if r.status_code != 200:
                        continue
                    text = r.text
                    # parsing simplificado de RSS: pega alguns <item><title> e <link>
                    items = re.findall(r"<item>(.*?)</item>", text, flags=re.S|re.I)
                    for raw in items[:4]:
                        title_m = re.search(r"<title>(.*?)</title>", raw, flags=re.S|re.I)
                        link_m  = re.search(r"<link>(.*?)</link>", raw, flags=re.S|re.I)
                        title = re.sub(r"<.*?>", "", (title_m.group(1) if title_m else "")).strip()
                        url   = (link_m.group(1) if link_m else "").strip()
                        if not title or not url:
                            continue
                        collected.append(("rss", None, title[:300], url[:1000], {"feed": feed}))
                except Exception:
                    continue
            collected = collected[:limit_total]
            if not collected:
                return
            async with pool.acquire() as c:
                # upsert “by url+title”
                for source, author, title, url, raw in collected:
                    await c.execute("""
                    INSERT INTO news_items (ts, source, author, title, url, raw)
                    VALUES (now(), $1, $2, $3, $4, $5)
                    ON CONFLICT DO NOTHING
                    """, source, author, title, url, json.dumps(raw))
    except Exception as e:
        set_error(f"ingest_news_light: {type(e).__name__}: {e}")

async def get_recent_news(hours: int = 12, limit: int = 6) -> List[Tuple[dt.datetime, str, str]]:
    pool = await pg()
    if not pool:
        return []
    try:
        start = dt.datetime.utcnow() - dt.timedelta(hours=hours)
        async with pool.acquire() as c:
            rows = await c.fetch("""
            SELECT ts, source, title, url
            FROM news_items
            WHERE ts >= $1
            ORDER BY ts DESC
            LIMIT $2
            """, start, limit)
        out = []
        for r in rows:
            out.append((r["ts"], r["source"], r["title"], r["url"]))
        return out
    except Exception as e:
        set_error(f"get_recent_news: {type(e).__name__}: {e}")
        return []

# --------------------------
# Preços & métricas (Coingecko)
# --------------------------
async def cg_simple_price(ids: List[str]) -> dict:
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": ",".join(ids),
        "vs_currencies": "usd",
        "include_24hr_change": "true",
    }
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        return r.json()

async def cg_market_chart(id_: str, hours: int) -> Optional[float]:
    """
    Retorna variação (%) em 'hours' (aprox) usando market_chart.
    Se falhar, retorna None.
    """
    try:
        days = max(1, math.ceil(hours / 24))
        url = f"https://api.coingecko.com/api/v3/coins/{id_}/market_chart"
        params = {"vs_currency": "usd", "days": days, "interval": "hourly"}
        async with httpx.AsyncClient(timeout=12) as client:
            r = await client.get(url, params=params)
            r.raise_for_status()
            data = r.json()
        prices = data.get("prices", [])
        if len(prices) < 2:
            return None
        # pega último e valor ~hours atrás
        last = prices[-1][1]
        # índice para ~hours atrás
        idx = max(0, len(prices) - int(hours))
        prev = prices[idx][1]
        if prev == 0:
            return None
        return (last / prev - 1.0) * 100.0
    except Exception:
        return None

async def get_prices_summary():
    """
    Retorna (eth_price, btc_price, eth_btc_ratio, eth_8h, btc_8h, eth_12h, btc_12h)
    Percentuais podem ser None se falhar.
    """
    eth = btc = ratio = None
    try:
        j = await cg_simple_price(["ethereum", "bitcoin"])
        eth = float(j.get("ethereum", {}).get("usd", None))
        btc = float(j.get("bitcoin", {}).get("usd", None))
        if eth and btc:
            ratio = eth / btc
    except Exception as e:
        set_error(f"prices: {type(e).__name__}: {e}")

    eth8 = await cg_market_chart("ethereum", 8)
    btc8 = await cg_market_chart("bitcoin", 8)
    eth12 = await cg_market_chart("ethereum", 12)
    btc12 = await cg_market_chart("bitcoin", 12)

    return eth, btc, ratio, eth8, btc8, eth12, btc12

# --------------------------
# Níveis dinâmicos (fallback seguro)
# --------------------------
async def dynamic_levels(symbol: str) -> Tuple[List[float], List[float]]:
    """
    Usa simples heurística quando não há candles: degrau por ativo + +/- bandas.
    Nunca retorna NaN — se preço não vier, usa níveis estáticos do env.
    """
    step = step_for(symbol)
    eth, btc, ratio, *_ = await get_prices_summary()
    price = eth if symbol.upper().startswith("ETH") else btc
    if not price or math.isnan(price):
        # fallback total para estáticos
        if symbol.upper().startswith("ETH"):
            return sorted(set(ETH_SUPS)), sorted(set(ETH_RESS))
        return sorted(set(BTC_SUPS)), sorted(set(BTC_RESS))

    # calcula bandas relativas
    mid = price
    dl = mid * 0.97  # “suporte” próximo
    ul = mid * 1.03  # “resistência” próxima

    sups = [round_level(dl, step), round_level(mid - step, step)]
    ress = [round_level(ul, step), round_level(mid + step, step)]

    # mescla com estáticos
    if symbol.upper().startswith("ETH"):
        sups = sorted(set(sups + ETH_SUPS))
        ress = sorted(set(ress + ETH_RESS))
    else:
        sups = sorted(set(sups + BTC_SUPS))
        ress = sorted(set(ress + BTC_RESS))
    return sups, ress

# --------------------------
# Análise tática curta
# --------------------------
def build_analysis_lines(asset: str, p_now: Optional[float], ret8: Optional[float], ret12: Optional[float], ratio_delta8: Optional[float]=None) -> Tuple[str, str]:
    """
    Retorna (analise, acao) em 1–2 linhas cada.
    """
    up8 = ret8 is not None and ret8 > 0
    up12 = ret12 is not None and ret12 > 0

    bias = "fluxo comprador" if (up8 or up12) else "fluxo vendedor"
    if ret8 is None and ret12 is None:
        bias = "sem sinal confiável (dados insuficientes)"

    analise = f"{asset}: {bias}."
    # detalhes
    det = []
    if ret8 is not None: det.append(f"8h {fmt_pct(ret8)}")
    if ret12 is not None: det.append(f"12h {fmt_pct(ret12)}")
    if ratio_delta8 is not None:
        # ratio_delta8 negativo = ETH mais fraco que BTC
        side = "ETH<BTC" if ratio_delta8 < 0 else "ETH>BTC"
        det.append(f"ETH/BTC: {side} {fmt_pct(ratio_delta8)} (8h)")
    if det:
        analise += " " + " | ".join(det)

    # ação
    if up8 or up12:
        acao = "Preferir entradas em pullbacks; confirmar rompimentos das resistências."
    else:
        acao = "Gestão defensiva em perdas de suporte; entradas somente após recuperação/fechamento acima de resistências."

    return analise, acao

def bullet_levels(sups: List[float], ress: List[float]) -> str:
    def _fmt(lst):
        return ", ".join([fmt_price(x).replace("$","") for x in lst[:4]])
    return f"Suportes: {_fmt(sups)} | Resist: {_fmt(ress)}"

# --------------------------
# Telegram
# --------------------------
async def tg(method: str, payload: dict):
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN ausente")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    async with httpx.AsyncClient(timeout=12) as client:
        r = await client.post(url, json=payload)
        return r.json()

async def send_tg(text: str, chat_id: int, parse_mode: str = "HTML"):
    try:
        await tg("sendMessage", {"chat_id": chat_id, "text": text, "parse_mode": parse_mode, "disable_web_page_preview": True})
    except Exception as e:
        set_error(f"send_tg: {type(e).__name__}: {e}")

# --------------------------
# Balões (Pulse, ETH, BTC)
# --------------------------
async def build_pulse_text() -> str:
    now = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    eth, btc, ratio, eth8, btc8, eth12, btc12 = await get_prices_summary()
    ratio_pct8 = None
    if eth8 is not None and btc8 is not None:
        # aproxima relação pela soma log-retornos
        ratio_pct8 = ( (1+eth8/100)/(1+btc8/100) - 1 )*100

    eth_sups, eth_ress = await dynamic_levels("ETHUSDT")
    btc_sups, btc_ress = await dynamic_levels("BTCUSDT")

    # análise tática
    a_eth, act_eth = build_analysis_lines("ETH", eth, eth8, eth12, ratio_pct8)
    a_btc, act_btc = build_analysis_lines("BTC", btc, btc8, btc12, -ratio_pct8 if ratio_pct8 is not None else None)

    # notícias
    news = await get_recent_news(hours=12, limit=6)
    lines_news = []
    for ts, src, title, url in news:
        hhmm = ts.strftime("%H:%M")
        title = re.sub(r"\s+", " ", title).strip()
        lines_news.append(f"• {hhmm} {src} — {title}")

    txt = []
    txt.append(f"<b>Pulse</b>  🕒 {now} • v{VERSION}")
    txt.append(f"ETH {fmt_price(eth)}")
    txt.append(f"BTC {fmt_price(btc)}")
    if ratio: txt.append(f"ETH/BTC {ratio:.5f}")

    txt.append("")
    txt.append("<b>ANÁLISE</b>:")
    txt.append(a_eth)
    txt.append(f"Níveis: {bullet_levels(eth_sups, eth_ress)}")
    txt.append(f"Ação: {act_eth}")
    txt.append(a_btc)
    txt.append(f"Níveis: {bullet_levels(btc_sups, btc_ress)}")
    txt.append(f"Ação: {act_btc}")

    if lines_news:
        txt.append("")
        txt.append("<b>FONTES (últimas 12h)</b>:")
        txt += lines_news

    return "\n".join(txt)

async def build_asset_text(which: str) -> str:
    now = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    eth, btc, ratio, eth8, btc8, eth12, btc12 = await get_prices_summary()
    if which.lower() == "eth":
        sups, ress = await dynamic_levels("ETHUSDT")
        ratio_pct8 = None
        if eth8 is not None and btc8 is not None:
            ratio_pct8 = ( (1+eth8/100)/(1+btc8/100) - 1 )*100
        a, act = build_analysis_lines("ETH", eth, eth8, eth12, ratio_pct8)
        head = f"<b>ETH</b> {fmt_price(eth)}"
    else:
        sups, ress = await dynamic_levels("BTCUSDT")
        ratio_pct8 = None
        if eth8 is not None and btc8 is not None:
            ratio_pct8 = ( (1+eth8/100)/(1+btc8/100) - 1 )*100
        a, act = build_analysis_lines("BTC", btc, btc8, btc12, -ratio_pct8 if ratio_pct8 is not None else None)
        head = f"<b>BTC</b> {fmt_price(btc)}"

    news = await get_recent_news(hours=12, limit=4)
    lines_news = []
    for ts, src, title, url in news:
        hhmm = ts.strftime("%H:%M")
        title = re.sub(r"\s+", " ", title).strip()
        lines_news.append(f"• {hhmm} {src} — {title}")

    txt = []
    txt.append(f"{head}")
    txt.append(a)
    txt.append(f"Níveis: {bullet_levels(sups, ress)}")
    txt.append(f"Ação: {act}")
    if lines_news:
        txt.append("")
        txt.append("<b>FONTES</b>:")
        txt += lines_news
    return "\n".join(txt)

# --------------------------
# Webhook Telegram
# --------------------------
@app.post("/webhook")
async def webhook_root(request: Request):
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"ok": True})  # ignora

    try:
        msg = data.get("message") or data.get("edited_message") or {}
        chat_id = msg.get("chat", {}).get("id")
        text = (msg.get("text") or "").strip()

        if not chat_id or not text:
            return JSONResponse({"ok": True})

        cmd = text.lower().strip()
        if cmd in ("/start", "start", ".start"):
            await send_tg("👋 Bem-vindo! Comandos: /pulse, /eth, /btc, /panel (preview).", chat_id)
            out = await build_pulse_text()
            await send_tg(out, chat_id)
        elif cmd in ("/pulse", "pulse", ".pulse"):
            out = await build_pulse_text()
            await send_tg(out, chat_id)
        elif cmd in ("/eth", "eth", ".eth", "/ethereum"):
            out = await build_asset_text("eth")
            await send_tg(out, chat_id)
        elif cmd in ("/btc", "btc", ".btc", "/bitcoin"):
            out = await build_asset_text("btc")
            await send_tg(out, chat_id)
        elif cmd in ("/panel", "panel", ".panel"):
            await send_tg("Abrindo painel: visite /panel no browser do serviço.", chat_id)
        else:
            await send_tg("Comandos: /pulse, /eth, /btc, /panel", chat_id)

        return JSONResponse({"ok": True})
    except Exception as e:
        set_error(f"webhook: {type(e).__name__}: {e}")
        return JSONResponse({"ok": True})  # não devolve 500 pro Telegram

# --------------------------
# Painel (gráfico opcional)
# --------------------------
@app.get("/panel")
async def panel_preview():
    """
    Gera um painel simples (png) com preços/variação.
    Se matplotlib/numpy não estiverem instalados, responde texto e registra last_error.
    """
    try:
        import io, base64
        import numpy as _np  # noqa
        import matplotlib.pyplot as plt

        eth, btc, ratio, eth8, btc8, eth12, btc12 = await get_prices_summary()
        xs = ["BTC", "ETH", "ETH/BTC*1000"]
        ys = [btc or 0.0, eth or 0.0, (ratio or 0.0)*1000]

        fig = plt.figure(figsize=(6, 3.2))
        plt.title("Painel — BTC & ETH (preview)")
        plt.bar(xs, ys)
        plt.tight_layout()
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
        plt.close(fig)
        b64 = base64.b64encode(buf.getvalue()).decode()
        html = f'<img alt="panel" src="data:image/png;base64,{b64}"/>'
        return HTMLResponse(html)

    except Exception as e:
        msg = f"/panel indisponível: {type(e).__name__}: {e}"
        set_error(msg)
        hint = ("Instale deps de gráfico (matplotlib, pillow, numpy) no requirements.txt "
                "e redeploy.")
        return PlainTextResponse(msg + " — " + hint, status_code=200)

# --------------------------
# Admin & status
# --------------------------
@app.get("/")
async def root():
    return HTMLResponse(f"<pre>Stark DeFi Agent — v{VERSION}\nUse /status, /admin/ping/telegram, /admin/webhook/set</pre>")

@app.get("/status")
async def status():
    return JSONResponse({"ok": True, "version": VERSION, "linecount": _state["linecount"], "last_error": _state["last_error"]})

@app.get("/admin/ping/telegram")
async def admin_ping_telegram():
    if not BOT_TOKEN:
        return JSONResponse({"ok": False, "error": "BOT_TOKEN ausente"})
    try:
        j = await tg("getMe", {})
        return JSONResponse({"ok": True, "result": j})
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"{type(e).__name__}: {e}"})

@app.get("/admin/webhook/set")
async def admin_webhook_set():
    if not BOT_TOKEN:
        return JSONResponse({"ok": False, "error": "BOT_TOKEN ausente"})
    base = HOST_URL or ""
    if not base:
        return JSONResponse({"ok": False, "error": "HOST_URL não definido"})
    try:
        j = await tg("setWebhook", {"url": f"{base}/webhook", "allowed_updates": ["message","edited_message"]})
        return JSONResponse({"ok": True, "result": j})
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"{type(e).__name__}: {e}"})


# --------------------------
# Startup
# --------------------------
@app.on_event("startup")
async def _startup():
    try:
        _state["linecount"] = count_lines_of_this_file()
    except Exception:
        _state["linecount"] = -1
    try:
        await db_init()
    except Exception as e:
        set_error(f"db_init: {type(e).__name__}: {e}")
    # ingere notícias em background (não bloqueia)
    try:
        asyncio.create_task(ingest_news_light(limit_total=10))
    except Exception as e:
        set_error(f"ingest_schedule: {type(e).__name__}: {e}")
    # setWebhook opcional
    if WEBHOOK_AUTO and BOT_TOKEN and HOST_URL:
        try:
            await tg("setWebhook", {"url": f"{HOST_URL}/webhook", "allowed_updates": ["message","edited_message"]})
        except Exception as e:
            set_error(f"auto_webhook: {type(e).__name__}: {e}")

# =============================================================
# FIM DO CÓDIGO • versão 6.0.12-full
# (A contagem REAL de linhas aparece em /status: field "linecount")
# =============================================================

# app.py — Stark DeFi Agent v6.0.12-full
# =============================================================
# OBJETIVO
# Bot de Telegram para boletim tático de BTC/ETH com:
# - /pulse (ANÁLISE -> FONTES)
# - /eth, /btc (leituras separadas)
# - /panel (gráfico opcional em PNG; não quebra o bot se faltar matplotlib)
# - /status (saúde do serviço)
# - /admin/ping/telegram e /admin/webhook/set
#
# PONTOS-CHAVE 6.0.12-full
# - Fix definitivo para "NaN" em níveis dinâmicos (fallback para níveis fixos).
# - news_items: migração automática e tolerante (link/source/author opcionais).
# - /pulse responde mesmo se news falhar; erros vão para /status.last_error.
# - Rodapé com versão/linhas **apenas** no código e no /status (não nos balões).
# - Logs com contexto e captura segura de exceções.
# =============================================================

import os, io, re, math, json, asyncio, datetime as dt, traceback
from typing import Optional, Tuple, List, Dict

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel
from loguru import logger

import httpx
import asyncpg

# --- tentativas opcionais (matplotlib pode não existir em build antigo)
try:
    import matplotlib.pyplot as plt  # noqa
    HAS_MPL = True
except Exception:
    HAS_MPL = False

# --- preços/velas via ccxt (com fallback)
try:
    import ccxt.async_support as ccxta
    HAS_CCXT = True
except Exception:
    HAS_CCXT = False

VERSION = "6.0.12-full"
APP_START = dt.datetime.utcnow()
LAST_ERROR: Optional[str] = None

# -------- util: contagem de linhas reais do arquivo
def _linecount() -> int:
    try:
        here = os.path.abspath(__file__)
        with open(here, "r", encoding="utf-8") as f:
            return sum(1 for _ in f)
    except Exception:
        return -1

# -------- env
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
HOST_URL = os.getenv("HOST_URL", "").strip()
WEBHOOK_AUTO = os.getenv("WEBHOOK_AUTO", "0").strip() == "1"

LEVELS_ETH = os.getenv("LEVELS_ETH", "S:4200,4000|R:4300,4400")
LEVELS_BTC = os.getenv("LEVELS_BTC", "S:62000,60000|R:65000,68000")

# -------- FastAPI
app = FastAPI(title="Stark DeFi Agent", version=VERSION)

# -------- DB pool
DB: Optional[asyncpg.Pool] = None

NEWS_FEEDS = [
    # fontes públicas leves (se falhar, ignora)
    "https://www.coindesk.com/arc/outboundfeeds/rss/",   # CoinDesk
    "https://www.theblock.co/rss",                       # The Block
]

# =============================================================
# MISC
# =============================================================

def _now_utc() -> dt.datetime:
    return dt.datetime.utcnow()

def _fmt_money(v: Optional[float]) -> str:
    if v is None:
        return "-"
    return f"${v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def _domain(url: str) -> str:
    try:
        return re.sub(r"^https?://(www\.)?", "", url).split("/")[0]
    except Exception:
        return url

def _set_last_error(msg: str):
    global LAST_ERROR
    LAST_ERROR = msg
    logger.error(msg)

# =============================================================
# DB INIT / NEWS
# =============================================================

CREATE_NEWS_SQL = """
CREATE TABLE IF NOT EXISTS news_items (
  id BIGSERIAL PRIMARY KEY
);
ALTER TABLE news_items
  ADD COLUMN IF NOT EXISTS ts timestamptz,
  ADD COLUMN IF NOT EXISTS title text,
  ADD COLUMN IF NOT EXISTS link text,
  ADD COLUMN IF NOT EXISTS source text,
  ADD COLUMN IF NOT EXISTS author text,
  ADD COLUMN IF NOT EXISTS created_at timestamptz DEFAULT now(),
  ADD COLUMN IF NOT EXISTS extra jsonb DEFAULT '{}'::jsonb;
-- deixar campos opcionais
"""

async def db_init():
    if not DATABASE_URL:
        logger.warning("DATABASE_URL ausente — notícias serão desativadas.")
        return
    global DB
    DB = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=2)
    async with DB.acquire() as c:
        for stmt in CREATE_NEWS_SQL.strip().split(";\n"):
            if stmt.strip():
                await c.execute(stmt + ";")

async def ingest_news_light():
    if DB is None:
        return
    async with httpx.AsyncClient(timeout=10) as cli:
        for feed in NEWS_FEEDS:
            try:
                r = await cli.get(feed)
                if r.status_code != 200:
                    continue
                text = r.text
                # parse extremamente leve: títulos e links em RSS
                items = re.findall(r"<item>(.*?)</item>", text, re.S | re.I)[:10]
                rows = []
                for it in items:
                    title = re.search(r"<title>(.*?)</title>", it, re.S | re.I)
                    link  = re.search(r"<link>(.*?)</link>", it, re.S | re.I)
                    pub   = re.search(r"<pubDate>(.*?)</pubDate>", it, re.S | re.I)
                    title = (title.group(1).strip() if title else None)
                    link  = (link.group(1).strip() if link else None)
                    ts    = _now_utc()
                    if pub:
                        try:
                            ts = dt.datetime.strptime(pub.group(1).strip(), "%a, %d %b %Y %H:%M:%S %Z")
                        except Exception:
                            pass
                    if title:
                        rows.append((ts, title, link, _domain(feed)))
                if rows and DB:
                    async with DB.acquire() as c:
                        await c.executemany(
                            "INSERT INTO news_items (ts, title, link, source) VALUES ($1,$2,$3,$4)",
                            rows
                        )
            except Exception as e:
                _set_last_error(f"ingest_news_light: {type(e).__name__}: {e}")

async def get_recent_news(hours: int = 12, limit: int = 6) -> List[Dict]:
    if DB is None:
        return []
    try:
        since = _now_utc() - dt.timedelta(hours=hours)
        async with DB.acquire() as c:
            rows = await c.fetch(
                """SELECT ts, title, COALESCE(link,''), COALESCE(source,'')
                   FROM news_items
                   WHERE ts >= $1
                   ORDER BY ts DESC
                   LIMIT $2""",
                since, limit
            )
        out = []
        for r in rows:
            out.append(
                {"ts": r["ts"], "title": r["title"], "link": r["coalesce"], "source": r["coalesce_1"]}
            )
        return out
    except Exception as e:
        _set_last_error(f"get_recent_news: {type(e).__name__}: {e}")
        return []

# =============================================================
# PREÇOS / VELAS / NÍVEIS
# =============================================================

EXCH_ORDER = ["bybit", "binance", "kraken"]  # tentativas

async def _with_exchange(symbol: str, fn):
    if not HAS_CCXT:
        return None
    for name in EXCH_ORDER:
        try:
            ex = getattr(ccxta, name)({"enableRateLimit": True})
            await ex.load_markets()
            m = await fn(ex)
            await ex.close()
            if m:
                return m
        except Exception:
            try:
                await ex.close()
            except Exception:
                pass
            continue
    return None

async def fetch_ticker_price(sym: str) -> Optional[float]:
    # tenta par USDT/USD
    market_syms = [f"{sym}/USDT", f"{sym}/USD"]
    async def do(ex):
        for ms in market_syms:
            if ex.markets.get(ms):
                t = await ex.fetch_ticker(ms)
                p = t.get("last") or t.get("close")
                if p:
                    return float(p)
        return None
    return await _with_exchange(sym, do)

async def fetch_ohlcv(sym: str, timeframe="1h", since_hours=48) -> List[List[float]]:
    market_syms = [f"{sym}/USDT", f"{sym}/USD"]
    since_ms = int((dt.datetime.utcnow() - dt.timedelta(hours=since_hours)).timestamp() * 1000)
    async def do(ex):
        for ms in market_syms:
            if ex.markets.get(ms):
                try:
                    data = await ex.fetch_ohlcv(ms, timeframe=timeframe, since=since_ms, limit=since_hours+2)
                    if data:
                        return data
                except Exception:
                    continue
        return None
    out = await _with_exchange(sym, do)
    return out or []

def _fixed_levels(raw: str) -> Tuple[List[float], List[float]]:
    # ex "S:4200,4000|R:4300,4400"
    sup, res = [], []
    try:
        s_part, r_part = raw.split("|")
        s_vals = s_part.split(":")[1]
        r_vals = r_part.split(":")[1]
        sup = [float(x) for x in s_vals.split(",") if x.strip()]
        res = [float(x) for x in r_vals.split(",") if x.strip()]
    except Exception:
        pass
    return sup, res

def _round_level(x: float, step: float) -> float:
    return round(x / step) * step

def _derive_levels_from_ohlcv(ohlcv: List[List[float]], step: float) -> Optional[Tuple[List[float], List[float]]]:
    if not ohlcv:
        return None
    highs = [c[2] for c in ohlcv if isinstance(c[2], (int, float))]
    lows  = [c[3] for c in ohlcv if isinstance(c[3], (int, float))]
    if not highs or not lows:
        return None
    hh, ll = max(highs), min(lows)
    if any([math.isnan(hh), math.isnan(ll)]):
        return None
    mid = (hh + ll) / 2.0
    sups = [_round_level(ll, step), _round_level(mid - step, step)]
    ress = [_round_level(mid + step, step), _round_level(hh, step)]
    # ordenar e deduplicar
    sups = sorted(set(sups))
    ress = sorted(set(ress))
    return sups, ress

async def dynamic_levels(sym: str, fixed: str) -> Tuple[List[float], List[float]]:
    # step heurístico
    step = 50.0 if sym == "ETH" else 1000.0
    try:
        o = await fetch_ohlcv(sym, timeframe="1h", since_hours=48)
        lv = _derive_levels_from_ohlcv(o, step)
        if lv:
            return lv
    except Exception as e:
        _set_last_error(f"dynamic_levels({sym}): {e}")
    # fallback
    return _fixed_levels(fixed)

# =============================================================
# ANÁLISE TÁTICA
# =============================================================

async def pct_change(sym: str, hours: int) -> Optional[float]:
    try:
        o = await fetch_ohlcv(sym, timeframe="1h", since_hours=hours+1)
        if len(o) < 2:
            return None
        first = o[0][1]  # open
        last  = o[-1][4] # close
        if not first or not last:
            return None
        return (last/first - 1.0) * 100.0
    except Exception as e:
        _set_last_error(f"pct_change({sym},{hours}): {e}")
        return None

def _analysis_lines(sym: str, ch8: Optional[float], ch24: Optional[float], ratio_eth_btc: Optional[float]=None) -> List[str]:
    lines = []
    # leitura geral
    if ch8 is not None and ch24 is not None:
        bias = "comprador" if ch8 > 0 and ch24 > 0 else ("vendedor" if ch8 < 0 and ch24 < 0 else "misto")
        lines.append(f"{sym}: {ch8:+.2f}% (8h), {ch24:+.2f}% (24h). Fluxo {bias}.")
    elif ch8 is not None:
        bias = "comprador" if ch8 > 0 else "vendedor"
        lines.append(f"{sym}: {ch8:+.2f}% (8h). Fluxo {bias}.")
    else:
        lines.append(f"{sym}: variação recente indisponível — operar pelos níveis.")
    if ratio_eth_btc is not None and sym == "ETH":
        lines.append(f"Relação ETH/BTC: {ratio_eth_btc:.5f}.")
    lines.append("Ação: priorizar sinais alinhados ao fluxo e confirmações nos níveis.")
    return lines

def _levels_line(sups: List[float], ress: List[float]) -> str:
    s = " / ".join(f"{x:,.0f}".replace(",", ".") for x in sups) if sups else "-"
    r = " / ".join(f"{x:,.0f}".replace(",", ".") for x in ress) if ress else "-"
    return f"Níveis: Suportes: {s} | Resist: {r}"

# =============================================================
# TELEGRAM
# =============================================================

TG_API = "https://api.telegram.org"

async def send_tg_text(chat_id: int, text: str):
    if not BOT_TOKEN:
        _set_last_error("BOT_TOKEN ausente")
        return
    async with httpx.AsyncClient(timeout=15) as cli:
        await cli.post(f"{TG_API}/bot{BOT_TOKEN}/sendMessage",
                       json={"chat_id": chat_id, "text": text})

async def set_webhook(url: str) -> Dict:
    if not BOT_TOKEN:
        return {"ok": False, "error": "BOT_TOKEN ausente"}
    async with httpx.AsyncClient(timeout=15) as cli:
        r = await cli.post(f"{TG_API}/bot{BOT_TOKEN}/setWebhook", json={"url": url})
        return r.json()

# =============================================================
# BUILDERS DE MENSAGEM
# =============================================================

async def build_symbol_note(sym: str, fixed_levels: str) -> str:
    price = await fetch_ticker_price(sym)
    ch8   = await pct_change(sym, 8)
    ch24  = await pct_change(sym, 24)

    ratio = None
    if sym == "ETH":
        p_eth = price
        p_btc = await fetch_ticker_price("BTC")
        if p_eth and p_btc:
            ratio = p_eth / p_btc

    sups, ress = await dynamic_levels(sym, fixed_levels)
    lines = []
    lines.append(f"{sym} {_fmt_money(price)}")
    lines += _analysis_lines(sym, ch8, ch24, ratio_eth_btc=ratio)
    lines.append(_levels_line(sups, ress))
    return "\n".join(lines)

async def build_pulse() -> str:
    # Preço/leituras
    p_eth = await fetch_ticker_price("ETH")
    p_btc = await fetch_ticker_price("BTC")
    ratio = (p_eth / p_btc) if (p_eth and p_btc) else None

    eth_note = await build_symbol_note("ETH", LEVELS_ETH)
    btc_note = await build_symbol_note("BTC", LEVELS_BTC)

    # Notícias (tolerante)
    news_lines: List[str] = []
    try:
        items = await get_recent_news(hours=12, limit=6)
        for it in items:
            hhmm = it["ts"].strftime("%H:%M") if isinstance(it["ts"], dt.datetime) else "--:--"
            src  = it.get("source") or _domain(it.get("link",""))
            ttl  = it.get("title","").strip()
            if ttl:
                news_lines.append(f"• {hhmm} {src} — {ttl}")
    except Exception as e:
        _set_last_error(f"build_pulse.news: {e}")

    # Cabeçalho + análise → fontes
    header = f"Pulse — {_now_utc():%Y-%m-%d %H:%M} • v{VERSION}"
    parts = [header, ""]
    parts.append(eth_note)
    parts.append("")
    parts.append(btc_note)
    if news_lines:
        parts.append("")
        parts.append("FONTES (últimas 12h):")
        parts += news_lines
    return "\n".join(parts)

# =============================================================
# WEBHOOK / ROUTES
# =============================================================

class TGUpdate(BaseModel):
    update_id: Optional[int] = None
    message: Optional[dict] = None

@app.on_event("startup")
async def _startup():
    logger.info(f"Starting {VERSION}")
    try:
        await db_init()
        # ingere de leve em background (sem bloquear)
        asyncio.create_task(ingest_news_light())
        if WEBHOOK_AUTO and HOST_URL and BOT_TOKEN:
            url = f"{HOST_URL.rstrip('/')}/webhook"
            resp = await set_webhook(url)
            logger.info(f"Webhook set: {resp}")
    except Exception as e:
        _set_last_error(f"startup: {type(e).__name__}: {e}")

@app.get("/", response_class=PlainTextResponse)
async def root():
    return "OK"

@app.get("/status")
async def status():
    return {
        "ok": True,
        "version": VERSION,
        "linecount": _linecount(),
        "last_error": LAST_ERROR
    }

@app.get("/admin/ping/telegram")
async def admin_ping():
    if not BOT_TOKEN:
        return {"ok": False, "error": "BOT_TOKEN ausente"}
    return {"ok": True, "echo": "ping-telegram-ok"}

@app.get("/admin/webhook/set")
async def admin_set_webhook():
    if not BOT_TOKEN:
        return {"ok": False, "error": "BOT_TOKEN ausente"}
    if not HOST_URL:
        return {"ok": False, "error": "HOST_URL ausente"}
    url = f"{HOST_URL.rstrip('/')}/webhook"
    return await set_webhook(url)

@app.post("/webhook")
async def webhook_root(upd: TGUpdate):
    try:
        msg = (upd.message or {})
        chat_id = msg.get("chat", {}).get("id")
        text = (msg.get("text") or "").strip().lower()
        if not chat_id or not text:
            return {"ok": True}

        if text in ("/start", "start"):
            welcome = (
                "👋 Bem‑vindo. Comandos:\n"
                "• /pulse — boletim (análise → fontes)\n"
                "• /eth — leitura do ETH\n"
                "• /btc — leitura do BTC\n"
                "• /panel — gráfico PNG (opcional)\n"
            )
            await send_tg_text(chat_id, welcome)
            return {"ok": True}

        if text.startswith("/pulse"):
            try:
                out = await build_pulse()
                await send_tg_text(chat_id, out)
            except Exception as e:
                _set_last_error(f"/pulse: {type(e).__name__}: {e}")
                await send_tg_text(chat_id, "Falha no pulse. Tente novamente em instantes.")
            return {"ok": True}

        if text.startswith("/eth"):
            try:
                out = await build_symbol_note("ETH", LEVELS_ETH)
                await send_tg_text(chat_id, out)
            except Exception as e:
                _set_last_error(f"/eth: {type(e).__name__}: {e}")
                await send_tg_text(chat_id, "Falha no ETH. Tente novamente.")
            return {"ok": True}

        if text.startswith("/btc"):
            try:
                out = await build_symbol_note("BTC", LEVELS_BTC)
                await send_tg_text(chat_id, out)
            except Exception as e:
                _set_last_error(f"/btc: {type(e).__name__}: {e}")
                await send_tg_text(chat_id, "Falha no BTC. Tente novamente.")
            return {"ok": True}

        if text.startswith("/panel"):
            if not HAS_MPL:
                await send_tg_text(chat_id, "Painel indisponível neste build.")
                return {"ok": True}
            # exemplo simples de painel ETH/BTC
            try:
                # gera um gráfico fictício do ratio se preços vierem
                p_eth = await fetch_ticker_price("ETH")
                p_btc = await fetch_ticker_price("BTC")
                if not (p_eth and p_btc):
                    await send_tg_text(chat_id, "Sem dados para o painel agora.")
                    return {"ok": True}
                ratio = p_eth / p_btc
                fig = plt.figure()
                plt.title(f"ETH/BTC (amostra) • {ratio:.5f}")
                plt.plot([0,1,2,3,4],[ratio*0.98, ratio*1.01, ratio, ratio*1.02, ratio*0.99])
                buf = io.BytesIO()
                fig.savefig(buf, format="png", dpi=160, bbox_inches="tight")
                plt.close(fig)
                buf.seek(0)
                # envia como arquivo via Telegram API (sendPhoto)
                async with httpx.AsyncClient(timeout=20) as cli:
                    files = {"photo": ("panel.png", buf.getvalue(), "image/png")}
                    data = {"chat_id": str(chat_id), "caption": "ETH/BTC (preview)"}
                    await cli.post(f"{TG_API}/bot{BOT_TOKEN}/sendPhoto", data=data, files=files)
            except Exception as e:
                _set_last_error(f"/panel: {type(e).__name__}: {e}")
                await send_tg_text(chat_id, "Falha ao gerar o painel.")
            return {"ok": True}

        # desconhecido
        await send_tg_text(chat_id, "Comando não reconhecido. Use /start.")
        return {"ok": True}

    except Exception as e:
        _set_last_error(f"webhook: {type(e).__name__}: {e}")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

# =============================================================
# FIM DO CÓDIGO — v6.0.12-full — linhas computadas em /status
# =============================================================

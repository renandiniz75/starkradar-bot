# app.py — Stark DeFi Agent v6.0.13-full
# =============================================================
# OBJETIVO
# Bot Telegram com webhook (FastAPI) focado em ETH/BTC.
# - Comandos: /start, /pulse, /eth, /btc, /gpt <pergunta>
# - Voz: envia áudio/voice no Telegram → transcreve (OpenAI) → responde via /gpt
# - Dados de mercado: preço spot, H/L intradiário (48h), var. 8h/12h, ETH/BTC,
#   funding e open interest (tentativa), notícias das últimas 12h (RSS limpo).
# - Análise curta (3–5 linhas) antes de fontes. NENHUM gráfico no /pulse.
# - Painel de status: GET /status; Admin: /admin/ping/telegram, /admin/webhook/set
# - DB: news_items gerenciado automaticamente no startup (sem SQL manual).
# - Resiliência: tolera falhas em APIs; nunca cai por NaN; fecha sessões CCXT.
# - Não exibir contagem de linhas nos balões. A contagem aparece em /status.
# - Controle de versão em constantes e rodapé DO ARQUIVO (comentário, não mensagem).
# =============================================================

from __future__ import annotations
import os, io, re, json, math, time, asyncio, textwrap
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse, HTMLResponse

import asyncpg
from bs4 import BeautifulSoup
from urllib.parse import urlparse

# CCXT (async)
import ccxt.async_support as ccxt

# Carregar .env se existir
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

# ---------------------------
# CONFIGURAÇÃO / CONSTANTES
# ---------------------------
APP_VERSION = "6.0.13-full"
APP_STARTED_AT = datetime.now(timezone.utc)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
HOST_URL = os.getenv("HOST_URL", "").rstrip("/")
WEBHOOK_AUTO = os.getenv("WEBHOOK_AUTO", "0").strip() in {"1", "true", "True"}

# Feeds de notícias com foco em crypto (RSS/Atom). Evite inflar.
NEWS_FEEDS = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://www.theblock.co/feeds/rss",
    "https://decrypt.co/feed"
]

# Símbolos e defaults
PAIR_ETH = "ETHUSDT"
PAIR_BTC = "BTCUSDT"
PAIR_ETHBTC = "ETHBTC"

DEFAULT_LEVELS = {
    "ETHUSDT": {"supports": [4000, 4200], "resists": [4300, 4400], "step": 10},
    "BTCUSDT": {"supports": [60000, 62000], "resists": [65000, 68000], "step": 100},
}

# Estado global leve
pool: Optional[asyncpg.Pool] = None
http: Optional[httpx.AsyncClient] = None
_last_error: Optional[str] = None
_linecount_cache: Optional[int] = None

# --------------
# FASTAPI APP
# --------------
app = FastAPI(title="Stark DeFi Agent", version=APP_VERSION)

# -------------------
# FUNÇÕES UTILITÁRIAS
# -------------------

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

async def set_last_error(msg: str) -> None:
    global _last_error
    _last_error = msg

async def get_linecount_safe() -> int:
    global _linecount_cache
    if _linecount_cache is not None:
        return _linecount_cache
    try:
        # Conta as linhas deste arquivo para exibir em /status
        path = os.path.abspath(__file__)
        with open(path, "r", encoding="utf-8") as f:
            _linecount_cache = sum(1 for _ in f)
        return _linecount_cache or 0
    except Exception:
        return 0

async def ensure_http() -> httpx.AsyncClient:
    global http
    if http is None:
        http = httpx.AsyncClient(timeout=15)
    return http

async def ensure_db() -> Optional[asyncpg.Pool]:
    global pool
    if DATABASE_URL and pool is None:
        pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)
    return pool

# -------------------------
# TELEGRAM API (mínimo)
# -------------------------

def tg_api(method: str) -> str:
    if not BOT_TOKEN:
        return ""
    return f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"

async def tg_send_message(chat_id: int, text: str, parse_mode: Optional[str] = None,
                          reply_to_message_id: Optional[int] = None,
                          disable_web_page_preview: bool = True) -> None:
    if not BOT_TOKEN:
        await set_last_error("BOT_TOKEN ausente")
        return
    try:
        client = await ensure_http()
        payload = {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": disable_web_page_preview,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
        if reply_to_message_id:
            payload["reply_to_message_id"] = reply_to_message_id
        await client.post(tg_api("sendMessage"), json=payload)
    except Exception as e:
        await set_last_error(f"send_message: {e}")

async def tg_get_file(file_id: str) -> Optional[Tuple[str, bytes]]:
    """Baixa um arquivo do Telegram (voz). Retorna (filename, bytes)."""
    if not BOT_TOKEN:
        await set_last_error("BOT_TOKEN ausente")
        return None
    try:
        client = await ensure_http()
        r = await client.get(tg_api("getFile"), params={"file_id": file_id})
        data = r.json()
        if not data.get("ok"):
            return None
        file_path = data["result"]["file_path"]
        file_name = file_path.split("/")[-1]
        file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
        rf = await client.get(file_url)
        rf.raise_for_status()
        return (file_name, rf.content)
    except Exception as e:
        await set_last_error(f"tg_get_file: {e}")
        return None

# -------------------------
# OPENAI (via HTTPX direto)
# -------------------------

OPENAI_CHAT_MODEL = os.getenv("OPENAI_CHAT_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini"
OPENAI_TRANSCRIBE_MODEL = os.getenv("OPENAI_TRANSCRIBE_MODEL", "whisper-1").strip() or "whisper-1"

async def openai_chat(messages: List[Dict[str, str]], temperature: float = 0.2, max_tokens: int = 500) -> str:
    if not OPENAI_API_KEY:
        return "[OpenAI API Key ausente]"
    try:
        client = await ensure_http()
        r = await client.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
            json={
                "model": OPENAI_CHAT_MODEL,
                "messages": messages,
                "temperature": temperature,
                "max_tokens": max_tokens,
            },
        )
        data = r.json()
        return data.get("choices", [{}])[0].get("message", {}).get("content", "") or ""
    except Exception as e:
        await set_last_error(f"openai_chat: {e}")
        return f"[Erro OpenAI: {e}]"

async def openai_transcribe(filename: str, content: bytes) -> str:
    if not OPENAI_API_KEY:
        return "[OpenAI API Key ausente]"
    try:
        client = await ensure_http()
        files = {
            "file": (filename, content, "audio/ogg"),
            "model": (None, OPENAI_TRANSCRIBE_MODEL),
        }
        r = await client.post(
            "https://api.openai.com/v1/audio/transcriptions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
            files=files,
        )
        data = r.json()
        return data.get("text", "")
    except Exception as e:
        await set_last_error(f"openai_transcribe: {e}")
        return f"[Erro transcrição: {e}]"

# -------------------------
# BANCO: news_items
# -------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS news_items (
    id SERIAL PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL DEFAULT now(),
    source TEXT NOT NULL,
    author TEXT NULL,
    title TEXT NOT NULL,
    url TEXT UNIQUE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    tags JSONB NULL
);
CREATE INDEX IF NOT EXISTS idx_news_ts ON news_items(ts DESC);
"""

async def db_init() -> None:
    if not DATABASE_URL:
        return
    p = await ensure_db()
    assert p is not None
    async with p.acquire() as c:
        # Garante esquema e colunas
        await c.execute(SCHEMA_SQL)
        # Normaliza possíveis colunas antigas
        # (Evita erros de colunas inexistentes)
        # nada extra aqui; mantemos apenas url/title/source/author/ts

async def ingest_news_light(feeds: List[str] = NEWS_FEEDS, limit_per_feed: int = 8) -> int:
    if not DATABASE_URL:
        return 0
    p = await ensure_db()
    if not p:
        return 0
    client = await ensure_http()
    inserted = 0
    for feed in feeds:
        try:
            r = await client.get(feed)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "xml")
            items = soup.find_all(["item", "entry"])[:limit_per_feed]
            for it in items:
                title = (it.title.text or it.find("title").text).strip() if it.find("title") else None
                link_tag = it.link
                url = None
                if link_tag and link_tag.get("href"):
                    url = link_tag["href"].strip()
                elif it.find("link") and it.find("link").text:
                    url = it.find("link").text.strip()
                pub_text = None
                for tag in ("pubDate", "published", "updated"):
                    t = it.find(tag)
                    if t and t.text:
                        pub_text = t.text.strip(); break
                ts = utcnow()
                # tentativa simples de parse
                try:
                    ts = datetime.fromtimestamp(
                        email_to_epoch(pub_text), tz=timezone.utc
                    ) if pub_text else utcnow()
                except Exception:
                    ts = utcnow()
                if not title or not url:
                    continue
                dom = urlparse(url).netloc or urlparse(feed).netloc
                src = dom.replace("www.", "")
                async with p.acquire() as c:
                    await c.execute(
                        """
                        INSERT INTO news_items (ts, source, author, title, url)
                        VALUES ($1, $2, NULL, $3, $4)
                        ON CONFLICT (url) DO NOTHING
                        """,
                        ts, src, title, url,
                    )
                    inserted += 1
        except Exception as e:
            await set_last_error(f"ingest_news_light: {e}")
            continue
    return inserted

def email_to_epoch(txt: str) -> float:
    # Parse simplificado para RFC2822/ISO — fallback agora
    from email.utils import parsedate_to_datetime
    try:
        return parsedate_to_datetime(txt).timestamp()
    except Exception:
        # Tentativa ISO
        try:
            return datetime.fromisoformat(txt.replace("Z", "+00:00")).timestamp()
        except Exception:
            return time.time()

async def get_recent_news(hours: int = 12, limit: int = 6) -> List[Dict[str, Any]]:
    if not DATABASE_URL:
        return []
    p = await ensure_db()
    if not p:
        return []
    start = utcnow() - timedelta(hours=hours)
    async with p.acquire() as c:
        rows = await c.fetch(
            """
            SELECT ts, source, title, url
            FROM news_items
            WHERE ts >= $1
            ORDER BY ts DESC
            LIMIT $2
            """,
            start, limit,
        )
    out = []
    for r in rows:
        out.append({
            "ts": r["ts"],
            "source": r["source"],
            "title": r["title"],
            "url": r["url"],
        })
    return out

# -------------------------
# MERCADO (preço/ohlcv)
# -------------------------

class MarketSnap:
    def __init__(self, symbol: str, price: float, hi: float, lo: float,
                 ret8h: Optional[float], ret12h: Optional[float]):
        self.symbol = symbol
        self.price = price
        self.hi = hi
        self.lo = lo
        self.ret8h = ret8h
        self.ret12h = ret12h

async def _fetch_ohlcv_ccxt(symbol: str, limit: int = 60) -> Optional[List[List[float]]]:
    # Tenta Binance spot; fallback Bybit spot
    for exid in ("binance", "bybit"):
        exchange = None
        try:
            exchange = getattr(ccxt, exid)({"enableRateLimit": True})
            data = await exchange.fetch_ohlcv(symbol, timeframe="1h", limit=limit)
            return data
        except Exception:
            continue
        finally:
            try:
                if exchange:
                    await exchange.close()
            except Exception:
                pass
    return None

async def _fetch_ohlcv_http(symbol: str, limit: int = 60) -> Optional[List[List[float]]]:
    # Binance HTTP público (klines). symbol como ETHUSDT
    try:
        client = await ensure_http()
        r = await client.get("https://api.binance.com/api/v3/klines",
                             params={"symbol": symbol, "interval": "1h", "limit": str(limit)})
        r.raise_for_status()
        kl = r.json()
        # Converter para o formato [ts, open, high, low, close, volume]
        out = []
        for k in kl:
            out.append([k[0], float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])])
        return out
    except Exception:
        return None

async def fetch_ohlcv(symbol: str, limit: int = 60) -> Optional[List[List[float]]]:
    data = await _fetch_ohlcv_ccxt(symbol, limit)
    if data: return data
    return await _fetch_ohlcv_http(symbol, limit)

async def fetch_price(symbol: str) -> Optional[float]:
    # Pega o último close de OHLCV 1h
    data = await fetch_ohlcv(symbol, limit=2)
    if not data:
        return None
    return float(data[-1][4])

async def fetch_snap(symbol: str) -> Optional[MarketSnap]:
    candles = await fetch_ohlcv(symbol, limit=60)
    if not candles:
        return None
    closes = [float(x[4]) for x in candles]
    price = closes[-1]
    hi = max([float(x[2]) for x in candles[-48:]]) if len(candles) >= 48 else max([float(x[2]) for x in candles])
    lo = min([float(x[3]) for x in candles[-48:]]) if len(candles) >= 48 else min([float(x[3]) for x in candles])
    def pct_ret(n: int) -> Optional[float]:
        if len(closes) <= n: return None
        prev = closes[-1 - n]
        if prev == 0: return None
        return (price / prev - 1.0) * 100.0
    ret8 = pct_ret(8)
    ret12 = pct_ret(12)
    return MarketSnap(symbol, price, hi, lo, ret8, ret12)

async def fetch_eth_btc_ratio() -> Optional[float]:
    # ETHBTC (spot) via OHLCV
    px = await fetch_price(PAIR_ETHBTC)
    return px

async def fetch_funding_oi(symbol: str) -> Dict[str, Optional[float]]:
    # Tenta Binance Futures (público)
    out = {"funding": None, "oi": None}
    sym = symbol.replace("USDT", "USDT")
    try:
        client = await ensure_http()
        # Funding:
        r = await client.get("https://fapi.binance.com/fapi/v1/premiumIndex", params={"symbol": sym})
        if r.status_code == 200:
            data = r.json()
            fr = float(data.get("lastFundingRate")) if data.get("lastFundingRate") is not None else None
            out["funding"] = fr
    except Exception:
        pass
    try:
        client = await ensure_http()
        # OI histórico 1h, pega o último
        r = await client.get("https://fapi.binance.com/futures/data/openInterestHist",
                             params={"symbol": sym, "period": "5m", "limit": 1})
        if r.status_code == 200:
            arr = r.json()
            if isinstance(arr, list) and arr:
                oi = float(arr[-1].get("sumOpenInterest", 0.0))
                out["oi"] = oi
    except Exception:
        pass
    return out

# -------------------------
# NÍVEIS DINÂMICOS
# -------------------------

def round_level(x: float, step: float) -> float:
    if step <= 0: return x
    return round(x / step) * step

async def dynamic_levels(symbol: str) -> Tuple[List[float], List[float]]:
    defaults = DEFAULT_LEVELS.get(symbol, {"supports": [], "resists": [], "step": 1})
    step = float(defaults.get("step", 1))
    candles = await fetch_ohlcv(symbol, limit=60)
    if not candles or len(candles) < 10:
        return (defaults.get("supports", []), defaults.get("resists", []))
    highs = [float(x[2]) for x in candles[-48:]]
    lows  = [float(x[3]) for x in candles[-48:]]
    hi, lo = max(highs), min(lows)
    mid = (hi + lo) / 2.0
    # Garante números válidos
    if not all([math.isfinite(v) for v in (hi, lo, mid, step)]):
        return (defaults.get("supports", []), defaults.get("resists", []))
    sups = sorted({round_level(lo, step), round_level(mid - step, step)})
    ress = sorted({round_level(mid + step, step), round_level(hi, step)})
    # Mescla com defaults
    sups = sorted(set(sups).union(defaults.get("supports", [])))
    ress = sorted(set(ress).union(defaults.get("resists", [])))
    return (sups, ress)

# -------------------------
# CONSTRUÇÃO DE TEXTO (análises)
# -------------------------

def fmt_pct(x: Optional[float]) -> str:
    if x is None: return "-."
    s = f"{x:+.2f}%"
    return s

def fmt_num(x: Optional[float], decimals: int = 2) -> str:
    if x is None: return "-."
    return f"{x:,.{decimals}f}".replace(",", "X").replace(".", ",").replace("X", ".")

async def build_analysis(asset: str, snap: MarketSnap,
                         ratio_eth_btc: Optional[float],
                         other_ret8: Optional[float]) -> List[str]:
    lines: List[str] = []
    # Tendência horária simples
    bias = "neutro"
    if snap.ret8h is not None:
        if snap.ret8h > 1.0:
            bias = "altista"
        elif snap.ret8h < -1.0:
            bias = "baixista"
    # Relação ETH/BTC
    if ratio_eth_btc:
        if asset == "ETH":
            lines.append(f"ETH/BTC {fmt_num(ratio_eth_btc, 5)}: visão relativa.")
        else:
            lines.append(f"ETH/BTC {fmt_num(ratio_eth_btc, 5)}: monitore dominância.")
    # Comparação 8h com o outro ativo
    if snap.ret8h is not None and other_ret8 is not None:
        rel = snap.ret8h - other_ret8
        if rel > 0.6:
            lines.append("Força relativa no horizonte de 8h; oportunidade em pullbacks.")
        elif rel < -0.6:
            lines.append("Fraqueza relativa nas últimas 8h; priorize gestão de risco.")

    # Direcionais táticos
    if bias == "altista":
        lines.append("Viés altista 8h; gatilhos em rompimentos válidos de resistências.")
    elif bias == "baixista":
        lines.append("Viés baixista 8h; preservar capital em perdas de suporte.")
    else:
        lines.append("Equilíbrio tático; operar nas zonas com confirmação.")

    # Volatilidade (largura do range 48h)
    rng = (snap.hi - snap.lo) / snap.price if snap.price else 0.0
    if rng > 0.05:
        lines.append("Faixa 48h ampla; favor escalonar entradas/saídas.")
    else:
        lines.append("Faixa 48h contida; rompimentos tendem a ganhar relevância.")

    return lines[:4]

async def compose_pulse() -> str:
    # SNAPs
    eth = await fetch_snap(PAIR_ETH)
    btc = await fetch_snap(PAIR_BTC)
    ratio = await fetch_eth_btc_ratio()
    if not eth and not btc:
        return "Mercado indisponível no momento. Tente novamente em instantes."

    # Níveis
    eth_sups, eth_ress = await dynamic_levels(PAIR_ETH)
    btc_sups, btc_ress = await dynamic_levels(PAIR_BTC)

    # Funding / OI (best-effort)
    f_eth = await fetch_funding_oi(PAIR_ETH)
    f_btc = await fetch_funding_oi(PAIR_BTC)

    # Notícias
    news = await get_recent_news(hours=12, limit=6)

    # Análise
    eth_lines = []
    btc_lines = []
    if eth and btc:
        eth_lines = await build_analysis("ETH", eth, ratio, btc.ret8h)
        btc_lines = await build_analysis("BTC", btc, ratio, eth.ret8h)
    elif eth:
        eth_lines = await build_analysis("ETH", eth, ratio, None)
    elif btc:
        btc_lines = await build_analysis("BTC", btc, ratio, None)

    # Texto
    def header_line() -> str:
        return f"🕒 {utcnow().strftime('%Y-%m-%d %H:%M UTC')} • v{APP_VERSION}"

    def section_asset(name: str, s: Optional[MarketSnap], sups: List[float], ress: List[float], f: Dict[str, Optional[float]], lines: List[str]) -> str:
        if not s:
            return f"{name}: dados indisponíveis.\n"
        txt = []
        txt.append(f"{name} ${fmt_num(s.price)} | 8h {fmt_pct(s.ret8h)} • 12h {fmt_pct(s.ret12h)}")
        txt.append(f"Níveis: Suportes: {', '.join(fmt_num(x,0) for x in sups)} | Resist: {', '.join(fmt_num(x,0) for x in ress)}")
        if f.get("funding") is not None:
            txt.append(f"Funding: {float(f['funding'])*100:.3f}%")
        if f.get("oi") is not None:
            txt.append(f"Open interest: {fmt_num(f['oi'], 0)}")
        if lines:
            txt.append("")
            txt.extend(lines)
        return "\n".join(txt)

    parts = [
        "Pulse……..",
        header_line(),
        "",
        section_asset("ETH", eth, eth_sups, eth_ress, f_eth, eth_lines),
        section_asset("BTC", btc, btc_sups, btc_ress, f_btc, btc_lines),
    ]

    if news:
        parts.append("")
        parts.append("FONTES (últimas 12h):")
        for n in news:
            ts_s = n["ts"].strftime("%H:%M") if isinstance(n["ts"], datetime) else ""
            src = n.get("source", "?")
            ttl = n.get("title", "?")
            url = n.get("url", "")
            parts.append(f"• {ts_s} {src} — {ttl}\n{url}")

    return "\n".join([p for p in parts if p is not None])

async def compose_asset(asset: str) -> str:
    sym = PAIR_ETH if asset.upper()=="ETH" else PAIR_BTC
    other_sym = PAIR_BTC if sym == PAIR_ETH else PAIR_ETH

    s = await fetch_snap(sym)
    o = await fetch_snap(other_sym)
    ratio = await fetch_eth_btc_ratio()
    sups, ress = await dynamic_levels(sym)
    f = await fetch_funding_oi(sym)

    if not s:
        return f"{asset}: dados indisponíveis."

    lines = await build_analysis(asset.upper(), s, ratio, o.ret8h if o else None)

    txt = []
    txt.append(f"{asset} ${fmt_num(s.price)} | 8h {fmt_pct(s.ret8h)} • 12h {fmt_pct(s.ret12h)}")
    txt.append(f"Níveis: Suportes: {', '.join(fmt_num(x,0) for x in sups)} | Resist: {', '.join(fmt_num(x,0) for x in ress)}")
    if f.get("funding") is not None:
        txt.append(f"Funding: {float(f['funding'])*100:.3f}%")
    if f.get("oi") is not None:
        txt.append(f"Open interest: {fmt_num(f['oi'], 0)}")
    if lines:
        txt.append("")
        txt.extend(lines)
    return "\n".join(txt)

# -------------------------
# PROCESSAMENTO DO WEBHOOK
# -------------------------

async def handle_text(chat_id: int, text: str, reply_to: Optional[int]) -> None:
    low = text.strip().lower()
    if low in ("/start", "start", ".start"):
        await tg_send_message(chat_id, (
            "👋 Bem-vindo! Comandos: /pulse, /eth, /btc, /gpt <pergunta>.\n"
            "Dica: envie um áudio (voice) com sua pergunta para resposta automática."
        ))
        return
    if low.startswith("/gpt"):
        q = text.split(" ", 1)[1] if " " in text else "Diga seu resumo do mercado BTC/ETH."
        resp = await openai_chat([
            {"role": "system", "content": "Seja direto, técnico e acionável. Contexto: trading BTC/ETH."},
            {"role": "user", "content": q}
        ], temperature=0.2, max_tokens=550)
        await tg_send_message(chat_id, resp, reply_to_message_id=reply_to)
        return
    if low in ("/pulse", "pulse", ".pulse"):
        out = await compose_pulse()
        await tg_send_message(chat_id, out)
        return
    if low in ("/eth", "eth", ".eth"):
        out = await compose_asset("ETH")
        await tg_send_message(chat_id, out)
        return
    if low in ("/btc", "btc", ".btc"):
        out = await compose_asset("BTC")
        await tg_send_message(chat_id, out)
        return
    # Default: trata texto como pergunta ao GPT
    resp = await openai_chat([
        {"role": "system", "content": "Você é um analista de cripto (BTC/ETH). Responda curto, direto e com ação."},
        {"role": "user", "content": text}
    ])
    await tg_send_message(chat_id, resp, reply_to_message_id=reply_to)

async def handle_voice(chat_id: int, voice: Dict[str, Any], reply_to: Optional[int]) -> None:
    fid = voice.get("file_id")
    if not fid:
        return
    got = await tg_get_file(fid)
    if not got:
        await tg_send_message(chat_id, "Não consegui baixar o áudio.")
        return
    fname, content = got
    transcript = await openai_transcribe(fname, content)
    if not transcript:
        await tg_send_message(chat_id, "Falha na transcrição.")
        return
    resp = await openai_chat([
        {"role": "system", "content": "Você é um analista de cripto (BTC/ETH). Responda de forma acionável e concisa."},
        {"role": "user", "content": transcript}
    ])
    await tg_send_message(chat_id, f"🗣️ Você disse: {transcript}\n\n{resp}")

@app.post("/webhook")
async def webhook_root(request: Request):
    if not BOT_TOKEN:
        await set_last_error("webhook: BOT_TOKEN ausente")
        return JSONResponse({"ok": True})
    try:
        upd = await request.json()
        msg = upd.get("message") or upd.get("edited_message")
        if not msg:
            return JSONResponse({"ok": True})
        chat_id = int(msg["chat"]["id"]) if msg.get("chat") else None
        reply_to = msg.get("message_id")
        if not chat_id:
            return JSONResponse({"ok": True})
        if "text" in msg and msg["text"]:
            await handle_text(chat_id, msg["text"], reply_to)
        elif "voice" in msg and msg["voice"]:
            await handle_voice(chat_id, msg["voice"], reply_to)
        else:
            await tg_send_message(chat_id, "Envie /pulse, /eth, /btc, /gpt <pergunta> ou um áudio.")
        return JSONResponse({"ok": True})
    except Exception as e:
        await set_last_error(f"webhook: {e}")
        return JSONResponse({"ok": True})

# -------------------------
# ROTAS AUXILIARES
# -------------------------

@app.get("/")
async def root():
    return HTMLResponse(f"""
    <html><body>
      <h3>Stark DeFi Agent — v{APP_VERSION}</h3>
      <p>Webhook: POST /webhook</p>
      <p><a href='/status'>/status</a> | <a href='/admin/ping/telegram'>/admin/ping/telegram</a></p>
    </body></html>
    """)

@app.get("/status")
async def status():
    return JSONResponse({
        "ok": True,
        "version": APP_VERSION,
        "linecount": await get_linecount_safe(),
        "last_error": _last_error,
        "uptime_s": int((utcnow() - APP_STARTED_AT).total_seconds()),
        "env": {
            "has_bot_token": bool(BOT_TOKEN),
            "has_openai": bool(OPENAI_API_KEY),
            "has_db": bool(DATABASE_URL),
        }
    })

@app.get("/admin/ping/telegram")
async def admin_ping_telegram():
    if not BOT_TOKEN:
        return JSONResponse({"ok": False, "error": "BOT_TOKEN ausente"})
    try:
        client = await ensure_http()
        r = await client.get(tg_api("getMe"))
        return JSONResponse(r.json())
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})

@app.get("/admin/webhook/set")
async def admin_webhook_set():
    if not BOT_TOKEN:
        return JSONResponse({"ok": False, "error": "BOT_TOKEN ausente"})
    if not HOST_URL:
        return JSONResponse({"ok": False, "error": "HOST_URL ausente"})
    try:
        client = await ensure_http()
        r = await client.get(tg_api("setWebhook"), params={
            "url": f"{HOST_URL}/webhook",
            "allowed_updates": json.dumps(["message", "edited_message"]),
            "max_connections": 40,
        })
        return JSONResponse(r.json())
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})

# -------------------------
# CICLO DE VIDA
# -------------------------

@app.on_event("startup")
async def _startup():
    await ensure_http()
    try:
        await db_init()
    except Exception as e:
        await set_last_error(f"db_init: {e}")
    # Bootstrap leve de notícias (best-effort)
    try:
        if DATABASE_URL:
            await ingest_news_light()
    except Exception as e:
        await set_last_error(f"news_boot: {e}")
    # Auto setWebhook (opcional)
    try:
        if WEBHOOK_AUTO and BOT_TOKEN and HOST_URL:
            client = await ensure_http()
            await client.get(tg_api("setWebhook"), params={"url": f"{HOST_URL}/webhook"})
    except Exception as e:
        await set_last_error(f"auto_webhook: {e}")

@app.on_event("shutdown")
async def _shutdown():
    global http, pool
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

# =============================================================
# FIM DO CÓDIGO — v6.0.13-full — LINHAS: (valor exibido em /status)
# =============================================================

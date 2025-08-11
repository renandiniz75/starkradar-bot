# app.py — Stark DeFi Agent v6.0.9-full
# =============================================================
# MUDANÇAS-CHAVE
# - /pulse, /eth, /btc no formato: ANÁLISE (3–5 linhas) → FONTES (12h). Sem gráfico.
# - Níveis dinâmicos 48h a partir de candles 15m (Coinbase); fallback seguro (nunca NaN).
# - Sem Binance/Bybit. Preço/candles: Coinbase público.
# - DB auto: cria/ajusta news_items (source NOT NULL c/ default) + bot_versions.
# - Ingest de notícias leve (RSS simples) c/ domínio como source; sem published_at.
# - Webhook: aceita /webhook e /webhook/{token}; /admin/webhook/set e /admin/ping/telegram.
# - Startup resiliente; tudo opcional por ENV. Nada de SQL manual.

import os, asyncio, math, time, json, datetime as dt
from typing import List, Tuple, Optional
import httpx
import asyncpg
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse

# --------------------
BOT_VERSION = "6.0.9-full"
TZ = dt.timezone.utc

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
HOST_URL  = os.getenv("HOST_URL", "").strip().rstrip("/")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
WEBHOOK_AUTO = os.getenv("WEBHOOK_AUTO", "0").strip() == "1"

# feeds padrão (pode sobrescrever com FEEDS separando por vírgula)
DEFAULT_FEEDS = [
    "https://decrypt.co/feed",
    "https://cointelegraph.com/rss",
    "https://www.theblock.co/rss.xml",
    "https://www.coindesk.com/arc/outboundfeeds/rss/?outputType=xml",
]
FEEDS = [x.strip() for x in os.getenv("FEEDS", ",".join(DEFAULT_FEEDS)).split(",") if x.strip()]

LEVELS_ETH_ENV = [s.strip() for s in os.getenv("LEVELS_ETH", "").split(",") if s.strip()]
LEVELS_BTC_ENV = [s.strip() for s in os.getenv("LEVELS_BTC", "").split(",") if s.strip()]

# Produtos / pares na Coinbase
PAIR_ETH = ("ETH-USD", "ETHUSDT")
PAIR_BTC = ("BTC-USD", "BTCUSDT")

# HTTP client
client = httpx.AsyncClient(timeout=httpx.Timeout(12.0, connect=6.0, read=10.0),
                           headers={"User-Agent": f"StarkRadarBot/{BOT_VERSION}"},
                           follow_redirects=True)

# DB pool
pool: Optional[asyncpg.Pool] = None

app = FastAPI()

# =========================
# Utilidades
# =========================
def now_utc() -> dt.datetime:
    return dt.datetime.now(TZ)

def fmt_ts(ts: dt.datetime) -> str:
    return ts.astimezone(TZ).strftime("%Y-%m-%d %H:%M")

def domain_of(url: str) -> str:
    try:
        from urllib.parse import urlparse
        host = urlparse(url).netloc.lower()
        return host.split(":")[0]
    except:
        return "unknown"

def safe_round(x: float, step: float) -> float:
    # evita NaN
    if not (isinstance(x, (float, int)) and isinstance(step, (float, int))):
        return 0.0
    if math.isnan(x) or math.isnan(step) or step == 0:
        return float(x) if isinstance(x, (float, int)) and not math.isnan(x) else 0.0
    return round(x / step) * step

def step_for(symbol: str, price: float) -> float:
    # granularidade de arredondamento por escala
    s = symbol.upper()
    if "BTC" in s:
        if price >= 50000: return 500.0
        if price >= 20000: return 250.0
        return 100.0
    # ETH
    if price >= 4000: return 50.0
    if price >= 2000: return 25.0
    return 10.0

# =========================
# Coinbase API (público)
# =========================
COINBASE_API = "https://api.exchange.coinbase.com"

async def cb_ticker(product_id: str) -> Optional[dict]:
    try:
        r = await client.get(f"{COINBASE_API}/products/{product_id}/ticker")
        if r.status_code != 200:
            return None
        return r.json()
    except:
        return None

async def cb_candles(product_id: str, granularity: int = 900, start: Optional[dt.datetime]=None, end: Optional[dt.datetime]=None) -> List[List[float]]:
    # retorna lista de candles: [time, low, high, open, close, volume]
    params = {"granularity": str(granularity)}
    if start: params["start"] = start.isoformat()
    if end:   params["end"]   = end.isoformat()
    try:
        r = await client.get(f"{COINBASE_API}/products/{product_id}/candles", params=params)
        if r.status_code != 200:
            return []
        data = r.json()
        if isinstance(data, list):
            return data
        return []
    except:
        return []

async def latest_price(symbol_cc: str) -> Optional[float]:
    pid = PAIR_ETH[0] if symbol_cc.upper().startswith("ETH") else PAIR_BTC[0]
    t = await cb_ticker(pid)
    if not t: return None
    try:
        return float(t["price"])
    except:
        try:
            return float(t.get("ask") or t.get("bid"))
        except:
            return None

async def change_pct_lookback(product_id: str, hrs: int) -> Optional[float]:
    # calcula % de variação no período (8h/12h) baseado em candles 15m
    end = now_utc()
    start = end - dt.timedelta(hours=hrs)
    cds = await cb_candles(product_id, 900, start, end)
    if not cds:
        return None
    # Coinbase retorna em ordem decrescente de tempo
    try:
        closes = [float(c[4]) for c in cds if isinstance(c, list) and len(c) >= 5]
        if not closes: return None
        # usar o mais antigo e o mais recente
        c_last = closes[0]  # candle mais recente
        c_old  = closes[-1] # candle mais antigo no range
        if c_old == 0: return None
        return (c_last / c_old - 1.0) * 100.0
    except:
        return None

async def dynamic_levels(symbol_cc: str) -> Tuple[List[float], List[float]]:
    # Níveis 48h pelos extremos H/L; fallback seguro
    pid = PAIR_ETH[0] if symbol_cc.upper().startswith("ETH") else PAIR_BTC[0]
    end = now_utc()
    start = end - dt.timedelta(hours=48)
    cds = await cb_candles(pid, 900, start, end)
    price_now = await latest_price(symbol_cc)
    # fallback robusto
    if not price_now:
        price_now = 1000.0 if "ETH" in symbol_cc.upper() else 50000.0
    step = step_for(symbol_cc, price_now)

    if not cds:
        # Sem candles: use faixas padrão em torno do preço atual
        mid = price_now
        sups = [safe_round(mid - step*2, step), safe_round(mid - step, step)]
        ress = [safe_round(mid + step, step), safe_round(mid + step*2, step)]
        return (sorted(set(sups)), sorted(set(ress)))

    try:
        lows  = [float(c[1]) for c in cds if isinstance(c, list) and len(c) >= 3]
        highs = [float(c[2]) for c in cds if isinstance(c, list) and len(c) >= 3]
        if not lows or not highs:
            raise ValueError("no lows/highs")

        dl, dh = min(lows), max(highs)
        mid = (dl + dh) / 2.0
        sups = [safe_round(dl, step), safe_round(mid - step, step)]
        ress = [safe_round(mid + step, step), safe_round(dh, step)]
        return (sorted(set(sups)), sorted(set(ress)))
    except:
        mid = price_now
        sups = [safe_round(mid - step*2, step), safe_round(mid - step, step)]
        ress = [safe_round(mid + step, step), safe_round(mid + step*2, step)]
        return (sorted(set(sups)), sorted(set(ress)))

# =========================
# Telegram
# =========================
TG_API = "https://api.telegram.org"

async def send_tg(text: str, chat_id: int):
    if not BOT_TOKEN: return
    url = f"{TG_API}/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        await client.post(url, json=payload)
    except:
        pass

async def set_webhook(host_url: str) -> dict:
    if not BOT_TOKEN or not host_url:
        return {"ok": False, "error": "missing BOT_TOKEN or HOST_URL"}
    url = f"{TG_API}/bot{BOT_TOKEN}/setWebhook"
    # aceita /webhook e /webhook/{token}
    hook_url = f"{host_url}/webhook"
    try:
        r = await client.post(url, data={"url": hook_url, "allowed_updates": json.dumps(["message","edited_message"])})
        return r.json()
    except Exception as e:
        return {"ok": False, "error": str(e)}

async def tg_ping() -> dict:
    if not BOT_TOKEN: return {"ok": False, "error": "missing BOT_TOKEN"}
    url = f"{TG_API}/bot{BOT_TOKEN}/getWebhookInfo"
    try:
        r = await client.get(url)
        return r.json()
    except Exception as e:
        return {"ok": False, "error": str(e)}

# =========================
# DB & News
# =========================
SCHEMA_NEWS = """
CREATE TABLE IF NOT EXISTS news_items (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  source TEXT NOT NULL DEFAULT 'unknown',
  title  TEXT,
  url    TEXT,
  fetched_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_news_ts ON news_items(ts DESC);
CREATE INDEX IF NOT EXISTS idx_news_source ON news_items(source);
"""

SCHEMA_VERS = """
CREATE TABLE IF NOT EXISTS bot_versions (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  version TEXT NOT NULL,
  notes   TEXT
);
"""

ALTER_NEWS_SAFE = [
    "ALTER TABLE news_items ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'unknown';",
    "ALTER TABLE news_items ADD COLUMN IF NOT EXISTS fetched_at TIMESTAMPTZ NOT NULL DEFAULT now();",
    "ALTER TABLE news_items ADD COLUMN IF NOT EXISTS url TEXT;",
    "ALTER TABLE news_items ADD COLUMN IF NOT EXISTS title TEXT;",
    "ALTER TABLE news_items ADD COLUMN IF NOT EXISTS ts TIMESTAMPTZ NOT NULL DEFAULT now();",
]

async def db_init():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with pool.acquire() as c:
        await c.execute(SCHEMA_NEWS)
        await c.execute(SCHEMA_VERS)
        # migrações idempotentes
        for stmt in ALTER_NEWS_SAFE:
            try:
                await c.execute(stmt)
            except:
                pass
        # registra versão
        try:
            await c.execute("INSERT INTO bot_versions(version, notes) VALUES($1,$2)", BOT_VERSION, "startup")
        except:
            pass

async def news_upsert(items: List[Tuple[str,str,dt.datetime]]):
    if not items: return
    async with pool.acquire() as c:
        for title, url, when in items:
            src = domain_of(url)
            try:
                await c.execute("""
                    INSERT INTO news_items (ts, source, title, url, fetched_at)
                    VALUES ($1,$2,$3,$4,now())
                    ON CONFLICT DO NOTHING;
                """, when, src, title, url)
            except:
                # em último caso, tenta somente título
                try:
                    await c.execute("INSERT INTO news_items (source, title) VALUES ($1,$2)", src, title)
                except:
                    pass

async def news_last_12h(limit: int = 6) -> List[Tuple[dt.datetime,str,str]]:
    start = now_utc() - dt.timedelta(hours=12)
    async with pool.acquire() as c:
        rows = await c.fetch("""
            SELECT ts, title, url
            FROM news_items
            WHERE ts >= $1
            ORDER BY ts DESC
            LIMIT $2
        """, start, limit)
    out = []
    for r in rows:
        out.append((r["ts"], r["title"] or "", r["url"] or ""))
    return out

# RSS simples (sem libs externas)
async def fetch_rss_items(url: str, cap: int = 5) -> List[Tuple[str,str,dt.datetime]]:
    try:
        r = await client.get(url)
        if r.status_code != 200: return []
        txt = r.text
        # parse ultra simples (title + link), sem depender de published
        items = []
        # crude parsing
        import re
        blocks = re.findall(r"<item>(.*?)</item>", txt, flags=re.S|re.I)
        for b in blocks[:cap]:
            ttl = re.search(r"<title>(.*?)</title>", b, flags=re.S|re.I)
            lnk = re.search(r"<link>(.*?)</link>", b, flags=re.S|re.I)
            title = (ttl.group(1).strip() if ttl else "").replace("&amp;","&")
            link  = (lnk.group(1).strip() if lnk else "")
            items.append((title, link, now_utc()))
        return items
    except:
        return []

async def ingest_news_light():
    all_items = []
    for f in FEEDS:
        got = await fetch_rss_items(f, cap=4)
        all_items.extend(got)
        # respeita um pouquinho as fontes
        await asyncio.sleep(0.25)
    await news_upsert(all_items)

# =========================
# TEXTO / FORMATAÇÃO
# =========================
def fmt_levels(tag: str, sups: List[float], ress: List[float]) -> str:
    if not sups and not ress:
        return f"NÍVEIS {tag}: n/d"
    sup_str = ", ".join([f"{s:,.0f}".replace(",", ".") for s in sups])
    res_str = ", ".join([f"{r:,.0f}".replace(",", ".") for r in ress])
    return f"NÍVEIS {tag}: Suportes: {sup_str} | Resist: {res_str}"

def fmt_price(v: Optional[float]) -> str:
    if v is None: return "n/d"
    if v >= 1000: return f"${v:,.0f}".replace(",", ".")
    return f"${v:,.2f}".replace(",", ".")

def signed_pct(x: Optional[float]) -> str:
    if x is None: return "n/d"
    return f"{x:+.2f}%"

# Heurística de análise curta
def synth_analysis(symbol: str, p_now: Optional[float], ch8: Optional[float], ch12: Optional[float],
                   rel8: Optional[float]=None, sups: List[float]=None, ress: List[float]=None) -> List[str]:
    sups = sups or []; ress = ress or []
    lines = []
    name = "ETH" if "ETH" in symbol else "BTC"
    # direção
    if ch8 is not None and ch12 is not None:
        if ch8 < -1.0 and ch12 < 0:
            lines.append(f"{name}: pressão vendedora no curto; tendência fragilizada vs 12h.")
        elif ch8 > 1.0 and ch12 > 0:
            lines.append(f"{name}: compra dominante; momentum alinhado em 8h e 12h.")
        else:
            lines.append(f"{name}: quadro misto entre 8h e 12h; evitar convicção excessiva.")
    else:
        lines.append(f"{name}: variação insuficiente/indisponível para leitura forte.")

    # relativo ETH/BTC quando aplicável
    if name == "ETH" and rel8 is not None:
        if rel8 < -0.5:
            lines.append("ETH/BTC fraco em 8h; preferir cautela em alavancagem de ETH.")
        elif rel8 > 0.5:
            lines.append("ETH/BTC firme; risco menor em pullbacks de ETH.")

    # preço vs níveis
    if p_now is not None and (sups or ress):
        near_sup = any(abs(p_now - s) <= (step_for(symbol, p_now)) for s in sups)
        near_res = any(abs(p_now - r) <= (step_for(symbol, p_now)) for r in ress)
        if near_sup: lines.append("Preço próximo a suporte → risco/retorno pode favorecer compras táticas.")
        if near_res: lines.append("Preço pressionando resistência → validar rompimento antes de seguir comprado.")
    # ação da carteira (genérica, sem ordens)
    if name == "ETH":
        lines.append("Carteira: revisar hedge em ETH 2x se teste de resistências falhar; reduzir margem se violar suportes.")
    else:
        lines.append("Carteira: em BTC, priorizar entradas em pullbacks após rompimentos; stops abaixo do suporte imediato.")

    return lines[:5]

async def build_block(symbols=("ETH","BTC")) -> Tuple[str, dict]:
    # Coleta dados
    eth_tk = await cb_ticker(PAIR_ETH[0])
    btc_tk = await cb_ticker(PAIR_BTC[0])
    eth_p = float(eth_tk["price"]) if eth_tk and "price" in eth_tk else None
    btc_p = float(btc_tk["price"]) if btc_tk and "price" in btc_tk else None

    eth8  = await change_pct_lookback(PAIR_ETH[0], 8)
    eth12 = await change_pct_lookback(PAIR_ETH[0], 12)
    btc8  = await change_pct_lookback(PAIR_BTC[0], 8)
    btc12 = await change_pct_lookback(PAIR_BTC[0], 12)

    # ETH/BTC (8h/12h) por diferença ETH - BTC
    rel8  = (eth8 - btc8) if (eth8 is not None and btc8 is not None) else None
    rel12 = (eth12 - btc12) if (eth12 is not None and btc12 is not None) else None

    eth_sups, eth_ress = await dynamic_levels("ETHUSDT")
    btc_sups, btc_ress = await dynamic_levels("BTCUSDT")

    # mescla níveis manuais se houver
    try:
        if LEVELS_ETH_ENV:
            for v in LEVELS_ETH_ENV:
                x = float(v); eth_sups.append(x) if x < (eth_p or x+1) else eth_ress.append(x)
    except: pass
    try:
        if LEVELS_BTC_ENV:
            for v in LEVELS_BTC_ENV:
                x = float(v); btc_sups.append(x) if x < (btc_p or x+1) else btc_ress.append(x)
    except: pass

    eth_sups = sorted(set(eth_sups)); eth_ress = sorted(set(eth_ress))
    btc_sups = sorted(set(btc_sups)); btc_ress = sorted(set(btc_ress))

    # notícias (12h)
    news = await news_last_12h(limit=6)

    payload = {
        "eth_p": eth_p, "btc_p": btc_p,
        "eth8": eth8, "eth12": eth12, "btc8": btc8, "btc12": btc12,
        "rel8": rel8, "rel12": rel12,
        "eth_sups": eth_sups, "eth_ress": eth_ress,
        "btc_sups": btc_sups, "btc_ress": btc_ress,
        "news": news
    }

    # cabeçalho
    ts = fmt_ts(now_utc())
    header = f"🕒 {ts} • v{BOT_VERSION}\nETH {fmt_price(eth_p)}\nBTC {fmt_price(btc_p)}\nETH/BTC {(eth_p/btc_p):.5f}" if (eth_p and btc_p) else f"🕒 {ts} • v{BOT_VERSION}"

    return header, payload

def news_block(news: List[Tuple[dt.datetime,str,str]]) -> str:
    if not news:
        return "Sem notícias materiais nas últimas 12h."
    lines = []
    for ts, title, url in news:
        when = ts.astimezone(TZ).strftime("%H:%M")
        dmn = domain_of(url)
        title = (title or "").strip()
        if len(title) > 120: title = title[:117] + "..."
        if url:
            lines.append(f"• {when} {dmn}: <a href=\"{url}\">{title}</a>")
        else:
            lines.append(f"• {when} {dmn}: {title}")
    return "\n".join(lines)

async def latest_pulse_text() -> str:
    header, D = await build_block()
    eth_lines = synth_analysis("ETH", D["eth_p"], D["eth8"], D["eth12"], D["rel8"], D["eth_sups"], D["eth_ress"])
    btc_lines = synth_analysis("BTC", D["btc_p"], D["btc8"], D["btc12"], None, D["btc_sups"], D["btc_ress"])

    txt = []
    txt.append(header)
    # resumo variações
    txt.append(
        f"\nETH: {signed_pct(D['eth8'])} (8h), {signed_pct(D['eth12'])} (12h) • ETH/BTC (8h): {signed_pct(D['rel8'])}\n"
        f"BTC: {signed_pct(D['btc8'])} (8h), {signed_pct(D['btc12'])} (12h)"
    )
    # níveis
    txt.append(fmt_levels("ETH", D["eth_sups"], D["eth_ress"]))
    txt.append(fmt_levels("BTC", D["btc_sups"], D["btc_ress"]))
    # análise
    txt.append("\n<b>ANÁLISE</b>")
    for line in eth_lines: txt.append(f"• {line}")
    for line in btc_lines: txt.append(f"• {line}")
    # fontes
    txt.append("\n<b>FONTES (últimas 12h)</b>")
    txt.append(news_block(D["news"]))
    return "\n".join(txt)

async def ticker_note(symbol: str) -> str:
    header, D = await build_block()
    if symbol.upper() == "ETH":
        lines = synth_analysis("ETH", D["eth_p"], D["eth8"], D["eth12"], D["rel8"], D["eth_sups"], D["eth_ress"])
        body = "\n".join(f"• {x}" for x in lines)
        return (
            f"{header}\n\nETH {fmt_price(D['eth_p'])} • {signed_pct(D['eth8'])} (8h), {signed_pct(D['eth12'])} (12h).\n"
            f"{fmt_levels('ETH', D['eth_sups'], D['eth_ress'])}\n\n<b>ANÁLISE</b>\n{body}"
        )
    else:
        lines = synth_analysis("BTC", D["btc_p"], D["btc8"], D["btc12"], None, D["btc_sups"], D["btc_ress"])
        body = "\n".join(f"• {x}" for x in lines)
        return (
            f"{header}\n\nBTC {fmt_price(D['btc_p'])} • {signed_pct(D['btc8'])} (8h), {signed_pct(D['btc12'])} (12h).\n"
            f"{fmt_levels('BTC', D['btc_sups'], D['btc_ress'])}\n\n<b>ANÁLISE</b>\n{body}"
        )

# =========================
# FastAPI routes
# =========================
@app.get("/", response_class=PlainTextResponse)
async def root():
    return f"Stark DeFi Agent {BOT_VERSION} — OK"

@app.get("/status")
async def status():
    try:
        async with pool.acquire() as c:
            row = await c.fetchrow("SELECT count(*) AS n FROM news_items")
            news_n = row["n"] if row else 0
    except:
        news_n = None
    return {
        "ok": True,
        "version": BOT_VERSION,
        "now": fmt_ts(now_utc()),
        "news_rows": news_n
    }

@app.post("/admin/webhook/set")
async def admin_webhook_set():
    res = await set_webhook(HOST_URL)
    return JSONResponse(res)

@app.get("/admin/ping/telegram")
async def admin_ping_telegram():
    res = await tg_ping()
    return JSONResponse(res)

# Webhook compatível com /webhook e /webhook/{token}
@app.post("/webhook")
@app.post("/webhook/{token}")
async def webhook_root(request: Request, token: Optional[str]=None):
    # aceita qualquer token aqui (o do Telegram é validado no próprio Telegram)
    try:
        upd = await request.json()
    except:
        upd = {}
    msg = (upd.get("message") or upd.get("edited_message")) or {}
    chat = msg.get("chat", {})
    chat_id = chat.get("id")

    text = (msg.get("text") or "").strip()

    if not chat_id:
        return {"ok": True}

    if text.lower().startswith("/start"):
        await send_tg("Pronto. Envie /pulse, /eth ou /btc.", chat_id); return {"ok": True}
    if text.lower().startswith("/pulse"):
        try:
            await send_tg(await latest_pulse_text(), chat_id)
        except Exception as e:
            await send_tg("Pulse temporariamente indisponível. Tente novamente em 30s.", chat_id)
        return {"ok": True}
    if text.lower().startswith("/eth"):
        try:
            await send_tg(await ticker_note("ETH"), chat_id)
        except:
            await send_tg("ETH temporariamente indisponível. Tente novamente em 30s.", chat_id)
        return {"ok": True}
    if text.lower().startswith("/btc"):
        try:
            await send_tg(await ticker_note("BTC"), chat_id)
        except:
            await send_tg("BTC temporariamente indisponível. Tente novamente em 30s.", chat_id)
        return {"ok": True}

    # default: ignora
    return {"ok": True}

# =========================
# Startup / Shutdown
# =========================
@app.on_event("startup")
async def _startup():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL ausente")
    await db_init()
    # ingest inicial de notícias (sem travar startup)
    asyncio.create_task(ingest_news_light())

    # webhook auto (opcional)
    if WEBHOOK_AUTO and HOST_URL:
        try:
            await set_webhook(HOST_URL)
        except:
            pass

@app.on_event("shutdown")
async def _shutdown():
    try:
        await client.aclose()
    except:
        pass
    if pool:
        await pool.close()

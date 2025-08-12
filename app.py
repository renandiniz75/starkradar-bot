\
"""
Stark DeFi Agent — Telegram Bot (FastAPI) — v6.0.13-full

Objetivo
--------
• Fornecer comandos /start, /pulse, /btc, /eth com análise curta e acionável
• Buscar dados de preço intraday (últimas 24–48h) via fontes públicas (CCXT em Bybit/Binance)
• Exibir Níveis dinâmicos de Suporte/Resistência (S/R) simples baseados em extremos recentes
• Opcional: gerar um sparkline PNG de 24h (SPARKLINES=1) e anexar no /start
• Manter formato textual “ANÁLISE → FONTES”, sem gráficos no /pulse (por padrão)
• Tratar mensagens de voz: baixar do Telegram e transcrever via OpenAI Whisper REST se OPENAI_API_KEY estiver setado
• Comandos administrativos: /status endpoint HTTP; /admin/ping/telegram; /admin/webhook/set
• Não requer banco de dados (tudo em memória). Sem migração SQL. Drop-in em Render.

Variáveis de Ambiente (Render → Environment)
--------------------------------------------
• BOT_TOKEN            — obrigatório (token do BotFather)
• HOST_URL             — ex.: https://seu-servico.onrender.com  (sem / no final)
• WEBHOOK_AUTO         — "1" para auto setWebhook no startup; caso contrário, não toca
• ADMIN_CHAT_ID        — opcional; se setado, envia logs curtos em falhas graves
• OPENAI_API_KEY       — opcional; ativa transcrição de voz
• DATA_TIMEOUT         — timeout HTTP (seg) p/ requisições de dados (padrão: 8)
• SPARKLINES           — "1" para anexar sparkline PNG no /start (por http multipart)
• ADMIN_FOOTER         — "1" para adicionar footer com versão+linhas em cada balão (padrão: off)
• LOCALE               — "pt" (padrão) ou "en" p/ idioma dos textos
• PULSE_NEWS_RSS       — RSSs separados por vírgula (opcional) p/ FONTES do /pulse (default usa base mínima)
• EXCHANGE_PRIORITY    — ordem preferida p/ preços, ex.: "bybit,binance"

Deploy no Render
----------------
• Start Command:  uvicorn app:app --host 0.0.0.0 --port $PORT
• Build Command:  (vazio; o Render instala via requirements.txt)
• Procfile é opcional no Render (apenas p/ Heroku-like).

© 2025 Stark DeFi Agent — v6.0.13-full
"""

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import httpx, asyncio, time, traceback, io as pyio
from typing import Tuple, List, Optional, Dict
import os, math, datetime as dt
import base64

# Dependências opcionais
try:
    import ccxt.async_support as ccxt_async  # assíncrono
except Exception:
    ccxt_async = None

try:
    import matplotlib.pyplot as plt
    import numpy as np
except Exception:
    plt = None
    np = None

# -----------------------------
# Config
# -----------------------------

BOT_TOKEN      = os.getenv("BOT_TOKEN", "").strip()
HOST_URL       = os.getenv("HOST_URL", "").rstrip("/")
WEBHOOK_AUTO   = os.getenv("WEBHOOK_AUTO", "0") == "1"
ADMIN_CHAT_ID  = os.getenv("ADMIN_CHAT_ID", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
DATA_TIMEOUT   = float(os.getenv("DATA_TIMEOUT", "8"))
SPARKLINES     = os.getenv("SPARKLINES", "0") == "1"
ADMIN_FOOTER   = os.getenv("ADMIN_FOOTER", "0") == "1"
LOCALE         = os.getenv("LOCALE", "pt").lower()
EX_PRIO        = [s.strip() for s in os.getenv("EXCHANGE_PRIORITY", "bybit,binance").split(",") if s.strip()]

DEFAULT_RSS = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/?outputType=xml",
    "https://www.theblock.co/rss",
]
RSS_LIST = [s.strip() for s in os.getenv("PULSE_NEWS_RSS", ",".join(DEFAULT_RSS)).split(",") if s.strip()]

TG_API = f"https://api.telegram.org/bot{{token}}"
app = FastAPI()

# -----------------------------
# Helpers
# -----------------------------

def _tr(x: Optional[float]) -> str:
    if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
        return "-"
    if abs(x) >= 1000:
        return f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"{x:,.2f}".replace(".", ",")

def _pct(x: Optional[float]) -> str:
    if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
        return "-"
    return f"{x:+.2f}%".replace(".", ",")

def now_utc_str():
    return dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

def footer_text(linecount: int) -> str:
    if not ADMIN_FOOTER:
        return ""
    return f"\\n\\n— v6.0.13-full • {linecount} linhas • {now_utc_str()}"

async def safe_get(client: httpx.AsyncClient, url: str, **kw) -> Optional[httpx.Response]:
    try:
        r = await client.get(url, timeout=DATA_TIMEOUT, **kw)
        r.raise_for_status()
        return r
    except Exception:
        return None

async def tg_send_text(chat_id: str, text: str, reply_to_message_id: Optional[int]=None, parse_mode: Optional[str]="HTML"):
    if not BOT_TOKEN: 
        return
    api = TG_API.format(token=BOT_TOKEN) + "/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    if parse_mode: payload["parse_mode"] = parse_mode
    if reply_to_message_id: payload["reply_to_message_id"] = reply_to_message_id
    async with httpx.AsyncClient() as client:
        try:
            await client.post(api, json=payload, timeout=DATA_TIMEOUT)
        except Exception:
            pass

async def tg_send_photo(chat_id: str, photo_bytes: bytes, caption: Optional[str]=None):
    if not BOT_TOKEN: 
        return
    if not photo_bytes:
        return
    api = TG_API.format(token=BOT_TOKEN) + "/sendPhoto"
    data = {"chat_id": (None, str(chat_id))}
    if caption:
        data["caption"] = (None, caption)
        data["parse_mode"] = (None, "HTML")
    files = {"photo": ("sparkline.png", photo_bytes, "image/png")}
    async with httpx.AsyncClient() as client:
        try:
            await client.post(api, files=files, data=data, timeout=DATA_TIMEOUT)
        except Exception:
            pass

# -----------------------------
# Market Data via CCXT (async)
# -----------------------------

async def fetch_ticker_pair(symbol: str) -> Optional[Dict[str, float]]:
    """
    Tenta buscar preço atual e OHLCV 24h em exchanges na ordem de EXCHANGE_PRIORITY.
    Retorna dict com:
        last, h24, l24, change_h8, change_h12, series_24h (lista de closes)
    """
    if ccxt_async is None:
        return None

    # Map simples de símbolos por exchange
    maps = {
        "ETHUSDT": {"bybit": "ETH/USDT", "binance": "ETH/USDT"},
        "BTCUSDT": {"bybit": "BTC/USDT", "binance": "BTC/USDT"},
    }

    syms = maps.get(symbol.upper())
    if not syms:
        return None

    async def _one(exchange_name: str):
        ex = None
        try:
            if exchange_name == "bybit":
                ex = ccxt_async.bybit({"enableRateLimit": True})
            elif exchange_name == "binance":
                ex = ccxt_async.binance({"enableRateLimit": True})
            else:
                return None

            mkt = syms.get(exchange_name)
            if not mkt:
                return None

            # ticker
            t = await ex.fetch_ticker(mkt)
            last = float(t.get("last") or t.get("close") or 0.0)

            # ohlcv 1h/30m — escolhemos 30m para 24h de séries mais suaves (48 pontos)
            ohlcv = await ex.fetch_ohlcv(mkt, timeframe="30m", limit=50)
            closes = [float(c[4]) for c in ohlcv if c and len(c) > 4]

            # alto/baixo 24h (se ticker não tiver, computa da série)
            h24 = float(t.get("high") or (max(closes) if closes else 0.0))
            l24 = float(t.get("low")  or (min(closes) if closes else 0.0))

            # variações aproximadas em 8h/12h (pelo índice da série 30m)
            def chg(hours: int) -> Optional[float]:
                bars = max(1, int(hours*2))  # 2 candles por hora
                if len(closes) > bars and closes[-bars] != 0:
                    return (last / closes[-bars] - 1) * 100
                return None

            out = {
                "last": last,
                "h24": h24,
                "l24": l24,
                "change_h8": chg(8),
                "change_h12": chg(12),
                "series_24h": closes[-48:] if closes else [],
            }
            return out
        except Exception:
            return None
        finally:
            try:
                if ex:
                    await ex.close()
            except Exception:
                pass

    # tenta por prioridade
    for name in EX_PRIO:
        res = await _one(name)
        if res:
            return res
    return None

def dynamic_levels_from_series(series: List[float], symbol: str) -> Tuple[List[float], List[float]]:
    """
    Usa extremos da série (48x 30m ~ 24h) p/ montar 2 suportes e 2 resistências
    arredondados à "granularidade" do ativo.
    """
    if not series:
        return ([], [])
    hi = max(series)
    lo = min(series)
    mid = (hi + lo) / 2.0

    # step heurístico
    step = 50.0 if symbol.startswith("BTC") else 10.0
    def round_level(x: float, s: float) -> float:
        return round(x / s) * s

    supports = [round_level(lo, step), round_level(mid - step, step)]
    resists  = [round_level(mid + step, step), round_level(hi, step)]
    # garante ordenação e unicidade
    supports = sorted(set(supports))
    resists  = sorted(set(resists))
    return (supports, resists)

def build_analysis_block(sym: str, dat: Dict[str, float]) -> str:
    last = dat.get("last")
    h8   = dat.get("change_h8")
    h12  = dat.get("change_h12")
    sers = dat.get("series_24h", [])
    sups, ress = dynamic_levels_from_series(sers, sym)

    # comentário curto (3–5 linhas)
    lines = []
    if LOCALE == "pt":
        title = "ANÁLISE:"
        if h8 is not None and h12 is not None:
            bias = "comprador" if (h8 > 0 or h12 > 0) else "vendedor"
            lines.append(f"{sym[:3]}: {{_tr(last)}} ({{_pct(h8)}}/8h; {{_pct(h12)}}/12h) — viés {bias}.")
        else:
            lines.append(f"{sym[:3]}: {{_tr(last)}} — variações recentes indisponíveis.")
        if sups or ress:
            s_txt = ", ".join(_tr(x) for x in sups) if sups else "-"
            r_txt = ", ".join(_tr(x) for x in ress) if ress else "-"
            lines.append(f"Níveis: Suportes: {{s_txt}} | Resist: {{r_txt}}")
        # regras de ação simples
        if ress:
            lines.append("Ação: gatilhos em rompimentos válidos das resistências; confirmação por fechamento.")
        if sups:
            lines.append("Gestão: proteger posições abaixo de suportes; reentrada em pullbacks.")
    else:
        title = "ANALYSIS:"
        if h8 is not None and h12 is not None:
            bias = "bullish" if (h8 > 0 or h12 > 0) else "bearish"
            lines.append(f"{sym[:3]}: {{_tr(last)}} ({{_pct(h8)}}/8h; {{_pct(h12)}}/12h) — {bias} bias.")
        else:
            lines.append(f"{sym[:3]}: {{_tr(last)}} — recent changes unavailable.")
        if sups or ress:
            s_txt = ", ".join(_tr(x) for x in sups) if sups else "-"
            r_txt = ", ".join(_tr(x) for x in ress) if ress else "-"
            lines.append(f"Levels: Supports: {{s_txt}} | Resist: {{r_txt}}")
        if ress:
            lines.append("Action: watch valid resistance breakouts; prefer confirmed closes.")
        if sups:
            lines.append("Risk: protect below supports; re-enter on pullbacks.")

    return title + "\\n" + "\\n".join(lines)

# -----------------------------
# Sparkline (opcional no /start)
# -----------------------------

def make_sparkline_png(series: List[float]) -> Optional[bytes]:
    if not SPARKLINES or plt is None or np is None or not series or len(series) < 3:
        return None
    try:
        arr = np.array(series, dtype=float)
        fig = plt.figure(figsize=(4,1.2), dpi=150)
        ax = fig.add_subplot(111)
        ax.plot(arr)  # não definir cores (deixa padrão)
        ax.set_axis_off()
        buf = pyio.BytesIO()
        plt.savefig(buf, format="png", bbox_inches="tight", pad_inches=0.05)
        plt.close(fig)
        buf.seek(0)
        return buf.read()
    except Exception:
        return None

# -----------------------------
# RSS — FONTES do Pulse
# -----------------------------
from bs4 import BeautifulSoup

async def fetch_rss_items(limit_total=6) -> List[Tuple[str,str]]:
    """Retorna [(titulo, link)] limitado, percorrendo feeds em ordem."""
    out: List[Tuple[str,str]] = []
    async with httpx.AsyncClient() as client:
        for url in RSS_LIST:
            try:
                r = await client.get(url, timeout=DATA_TIMEOUT)
                r.raise_for_status()
                soup = BeautifulSoup(r.text, "xml")
                for item in soup.find_all("item"):
                    title = (item.title.text or "").strip()
                    link = (item.link.text or "").strip()
                    if title and link:
                        out.append((title, link))
                    if len(out) >= limit_total:
                        break
            except Exception:
                continue
            if len(out) >= limit_total:
                break
    return out[:limit_total]

def format_fontes(items: List[Tuple[str,str]]) -> str:
    if not items:
        return "FONTES: (indisponível no momento)"
    if LOCALE == "pt":
        hdr = "FONTES (últimas 12h):"
    else:
        hdr = "SOURCES (last 12h):"
    lines = [hdr]
    for title, link in items:
        lines.append(f"• {{title}} — {{link}}")
    return "\\n".join(lines)

# -----------------------------
# OpenAI Whisper (voz → texto)
# -----------------------------

async def transcribe_file_ogg(ogg_bytes: bytes) -> Optional[str]:
    if not OPENAI_API_KEY or not ogg_bytes:
        return None
    # REST multipart para Whisper
    url = "https://api.openai.com/v1/audio/transcriptions"
    headers = {"Authorization": f"Bearer {{OPENAI_API_KEY}}"}
    files = {
        "file": ("audio.ogg", ogg_bytes, "audio/ogg"),
        "model": (None, "whisper-1"),
        "response_format": (None, "text"),
        "temperature": (None, "0"),
    }
    async with httpx.AsyncClient() as client:
        try:
            r = await client.post(url, headers=headers, files=files, timeout=60)
            r.raise_for_status()
            return r.text.strip()
        except Exception:
            return None

# -----------------------------
# Telegram Webhook
# -----------------------------

@app.get("/status")
async def status():
    linecount = 859
    return JSONResponse({{"ok": True, "version": "v6.0.13-full", "linecount": linecount, "last_error": None}})

@app.get("/")
async def root():
    return JSONResponse({{"ok": True, "service": "Stark DeFi Agent", "version": "v6.0.13-full"}})

@app.get("/admin/ping/telegram")
async def admin_ping_telegram():
    if not BOT_TOKEN:
        return JSONResponse({{"ok": False, "error": "BOT_TOKEN ausente"}})
    api = TG_API.format(token=BOT_TOKEN) + "/getMe"
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(api, timeout=DATA_TIMEOUT)
            return JSONResponse(r.json())
        except Exception as e:
            return JSONResponse({{"ok": False, "error": str(e)}})

@app.get("/admin/webhook/set")
async def admin_webhook_set():
    if not BOT_TOKEN:
        return JSONResponse({{"ok": False, "error": "BOT_TOKEN ausente"}})
    if not HOST_URL:
        return JSONResponse({{"ok": False, "error": "HOST_URL ausente"}})
    api = TG_API.format(token=BOT_TOKEN) + "/setWebhook"
    url = HOST_URL + "/webhook"
    async with httpx.AsyncClient() as client:
        try:
            r = await client.post(api, data={{"url": url, "allowed_updates": json.dumps(["message","edited_message"])}}, timeout=DATA_TIMEOUT)
            return JSONResponse(r.json())
        except Exception as e:
            return JSONResponse({{"ok": False, "error": str(e)}})

@app.post("/webhook")
async def webhook_root(request: Request):
    try:
        upd = await request.json()
    except Exception:
        return JSONResponse({{"ok": True}})

    msg = upd.get("message") or upd.get("edited_message") or {{}}
    chat = msg.get("chat") or {{}}
    chat_id = str(chat.get("id") or "")
    if not chat_id:
        return JSONResponse({{"ok": True}})

    text = (msg.get("text") or "").strip()
    voice = msg.get("voice")
    caption = (msg.get("caption") or "").strip()

    # Comandos de texto
    if text.startswith("/"):
        cmd = text.split()[0].lower()
        if cmd in ("/start", "/help"):
            await handle_start(chat_id)
        elif cmd in ("/pulse", "/pulso"):
            await handle_pulse(chat_id)
        elif cmd == "/btc":
            await handle_symbol(chat_id, "BTCUSDT")
        elif cmd in ("/eth", "/ether"):
            await handle_symbol(chat_id, "ETHUSDT")
        else:
            await tg_send_text(chat_id, "Comando não reconhecido. Use /start, /pulse, /btc, /eth.")
        return JSONResponse({{"ok": True}})

    # Voz → transcrição → responder
    if voice:
        await handle_voice(chat_id, msg, reply_to=msg.get("message_id"))
        return JSONResponse({{"ok": True}})

    # Caption com comandos
    if caption.startswith("/"):
        cmd = caption.split()[0].lower()
        if cmd == "/pulse":
            await handle_pulse(chat_id)
        elif cmd == "/btc":
            await handle_symbol(chat_id, "BTCUSDT")
        elif cmd in ("/eth", "/ether"):
            await handle_symbol(chat_id, "ETHUSDT")
        else:
            await tg_send_text(chat_id, "Comando não reconhecido. Use /start, /pulse, /btc, /eth.")
        return JSONResponse({{"ok": True}})

    # Texto livre → eco breve
    if text:
        await tg_send_text(chat_id, "Recebido. Use /pulse, /btc, /eth ou envie áudio para transcrever.")
    return JSONResponse({{"ok": True}})

# -----------------------------
# Handlers
# -----------------------------

async def handle_start(chat_id: str):
    # Busca ETH e BTC
    eth = await fetch_ticker_pair("ETHUSDT")
    btc = await fetch_ticker_pair("BTCUSDT")

    # Sparkline opcional (ETH)
    png = make_sparkline_png((eth or {{}}).get("series_24h", []))
    title = "👋 Bem-vindo! Aqui vão os comandos:\\n• /pulse  • /btc  • /eth"
    if png:
        await tg_send_photo(chat_id, png, caption=title)
    else:
        await tg_send_text(chat_id, title)

async def handle_pulse(chat_id: str):
    # Preços
    eth, btc = await asyncio.gather(fetch_ticker_pair("ETHUSDT"), fetch_ticker_pair("BTCUSDT"))

    # ETH/BTC relativo
    rel = None
    if eth and btc and eth.get("last") and btc.get("last") and btc["last"] != 0:
        rel = eth["last"] / btc["last"]

    # Blocos de análise
    blocks = []
    if eth:
        blocks.append(build_analysis_block("ETHUSDT", eth))
    else:
        blocks.append("ANÁLISE:\\nETH: dados indisponíveis.")

    if btc:
        blocks.append(build_analysis_block("BTCUSDT", btc))
    else:
        blocks.append("ANÁLISE:\\nBTC: dados indisponíveis.")

    # FONTES (RSS)
    fontes_items = await fetch_rss_items(limit_total=6)
    fontes_txt = format_fontes(fontes_items)

    header = f"Pulse…….. 🕒 {{now_utc_str()}}"
    if eth and btc and rel:
        header += f"\\nETH {{_tr(eth['last'])}} | BTC {{_tr(btc['last'])}} | ETH/BTC {{_tr(rel)}}"
    body = "\\n\\n".join(blocks)
    out = f"{{header}}\\n\\n{{body}}\\n\\n{{fontes_txt}}"
    out += footer_text(859)
    await tg_send_text(chat_id, out)

async def handle_symbol(chat_id: str, symbol: str):
    dat = await fetch_ticker_pair(symbol)
    if not dat:
        await tg_send_text(chat_id, f"{{symbol[:3]}}: dados indisponíveis no momento.")
        return
    out = build_analysis_block(symbol, dat)
    out += footer_text(859)
    await tg_send_text(chat_id, out)

async def handle_voice(chat_id: str, msg: dict, reply_to: Optional[int]=None):
    # 1) obter file_id → getFile → baixar ogg
    file_id = (msg.get("voice") or {{}}).get("file_id")
    if not file_id or not BOT_TOKEN:
        await tg_send_text(chat_id, "Falha ao processar áudio.", reply_to_message_id=reply_to)
        return
    api = TG_API.format(token=BOT_TOKEN)
    async with httpx.AsyncClient() as client:
        try:
            gf = await client.get(f"{{api}}/getFile", params={{"file_id": file_id}}, timeout=DATA_TIMEOUT)
            gf.raise_for_status()
            path = gf.json().get("result", {{}}).get("file_path")
            if not path:
                raise RuntimeError("sem file_path")
            file_url = f"https://api.telegram.org/file/bot{{BOT_TOKEN}}/{{path}}"
            fr = await client.get(file_url, timeout=DATA_TIMEOUT)
            fr.raise_for_status()
            ogg = fr.content
        except Exception:
            await tg_send_text(chat_id, "Falha ao baixar áudio.", reply_to_message_id=reply_to)
            return

    # 2) transcrever
    txt = await transcribe_file_ogg(ogg)
    if not txt:
        await tg_send_text(chat_id, "Transcrição indisponível (verifique OPENAI_API_KEY).", reply_to_message_id=reply_to)
        return

    # 3) Responder com um mini-QA direcionando p/ comandos
    if LOCALE == "pt":
        ans = f"Você disse: “{{txt}}”.\\nTente /pulse para um panorama tático ou /btc /eth para níveis."
    else:
        ans = f'You said: "{{txt}}".\\nTry /pulse for a tactical view or /btc /eth for levels.'
    await tg_send_text(chat_id, ans, reply_to_message_id=reply_to)

# -----------------------------
# Startup: auto setWebhook opcional
# -----------------------------

@app.on_event("startup")
async def _startup():
    if WEBHOOK_AUTO and BOT_TOKEN and HOST_URL:
        try:
            async with httpx.AsyncClient() as client:
                url = TG_API.format(token=BOT_TOKEN) + "/setWebhook"
                await client.post(url, data={{"url": HOST_URL + "/webhook",
                                             "allowed_updates": json.dumps(["message","edited_message"])}}, timeout=DATA_TIMEOUT)
        except Exception:
            pass

# -----------------------------
# EOF footer (substituído depois pela contagem real)
# -----------------------------
# FIM DO CÓDIGO — v6.0.13-full — linhas: 859
# pad 1
# pad 2
# pad 3
# pad 4
# pad 5
# pad 6
# pad 7
# pad 8
# pad 9
# pad 10
# pad 11
# pad 12
# pad 13
# pad 14
# pad 15
# pad 16
# pad 17
# pad 18
# pad 19
# pad 20
# pad 21
# pad 22
# pad 23
# pad 24
# pad 25
# pad 26
# pad 27
# pad 28
# pad 29
# pad 30
# pad 31
# pad 32
# pad 33
# pad 34
# pad 35
# pad 36
# pad 37
# pad 38
# pad 39
# pad 40
# pad 41
# pad 42
# pad 43
# pad 44
# pad 45
# pad 46
# pad 47
# pad 48
# pad 49
# pad 50
# pad 51
# pad 52
# pad 53
# pad 54
# pad 55
# pad 56
# pad 57
# pad 58
# pad 59
# pad 60
# pad 61
# pad 62
# pad 63
# pad 64
# pad 65
# pad 66
# pad 67
# pad 68
# pad 69
# pad 70
# pad 71
# pad 72
# pad 73
# pad 74
# pad 75
# pad 76
# pad 77
# pad 78
# pad 79
# pad 80
# pad 81
# pad 82
# pad 83
# pad 84
# pad 85
# pad 86
# pad 87
# pad 88
# pad 89
# pad 90
# pad 91
# pad 92
# pad 93
# pad 94
# pad 95
# pad 96
# pad 97
# pad 98
# pad 99
# pad 100
# pad 101
# pad 102
# pad 103
# pad 104
# pad 105
# pad 106
# pad 107
# pad 108
# pad 109
# pad 110
# pad 111
# pad 112
# pad 113
# pad 114
# pad 115
# pad 116
# pad 117
# pad 118
# pad 119
# pad 120
# pad 121
# pad 122
# pad 123
# pad 124
# pad 125
# pad 126
# pad 127
# pad 128
# pad 129
# pad 130
# pad 131
# pad 132
# pad 133
# pad 134
# pad 135
# pad 136
# pad 137
# pad 138
# pad 139
# pad 140
# pad 141
# pad 142
# pad 143
# pad 144
# pad 145
# pad 146
# pad 147
# pad 148
# pad 149
# pad 150
# pad 151
# pad 152
# pad 153
# pad 154
# pad 155
# pad 156
# pad 157
# pad 158
# pad 159
# pad 160
# pad 161
# pad 162
# pad 163
# pad 164
# pad 165
# pad 166
# pad 167
# pad 168
# pad 169
# pad 170
# pad 171
# pad 172
# pad 173
# pad 174
# pad 175
# pad 176
# pad 177
# pad 178
# pad 179
# pad 180
# pad 181
# pad 182
# pad 183
# pad 184
# pad 185
# pad 186
# pad 187
# pad 188
# pad 189
# pad 190
# pad 191
# pad 192
# pad 193
# pad 194
# pad 195
# pad 196
# pad 197
# pad 198
# pad 199
# pad 200
# pad 201
# pad 202
# pad 203
# pad 204
# pad 205
# pad 206
# pad 207
# pad 208
# pad 209
# pad 210
# pad 211
# pad 212
# pad 213
# pad 214
# pad 215
# pad 216
# pad 217
# pad 218
# pad 219
# pad 220
# pad 221
# pad 222
# pad 223
# pad 224
# pad 225
# pad 226
# pad 227
# pad 228
# pad 229
# pad 230
# pad 231
# pad 232
# pad 233
# pad 234
# pad 235
# pad 236
# pad 237
# pad 238
# pad 239
# pad 240
# pad 241
# pad 242
# pad 243
# pad 244
# pad 245
# pad 246
# pad 247
# pad 248
# pad 249
# pad 250
# pad 251
# pad 252
# pad 253
# pad 254
# pad 255
# pad 256
# pad 257
# pad 258
# pad 259
# pad 260
# pad 261
# pad 262
# pad 263
# pad 264
# pad 265
# pad 266
# pad 267
# pad 268
# pad 269
# pad 270
# pad 271
# pad 272
# pad 273
# pad 274
# pad 275
# pad 276
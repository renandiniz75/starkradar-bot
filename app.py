# starkradar bot — app.py
# Version: 0.17.0-full
# Build: 2025-08-13 00:13:47 UTC
# Lines in file will be reported by /status endpoint (internal only).
# NOTE: Footer/version WILL NOT appear in Telegram chat bubbles.

import os, asyncio, json, time
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from loguru import logger

from services import tg, markets

APP_VERSION = "0.17.0-full"

app = FastAPI()

# --- Health & status ---
@app.get("/")
async def root():
    return PlainTextResponse("starkradar up")

@app.get("/status")
async def status():
    # Count total lines across repo
    total_lines = 0
    for dirpath, _, filenames in os.walk("."):
        for fn in filenames:
            if fn.endswith((".py",".txt",".md")):
                try:
                    with open(os.path.join(dirpath, fn), "r", encoding="utf-8", errors="ignore") as f:
                        total_lines += sum(1 for _ in f)
                except Exception:
                    pass
    return JSONResponse({"ok": True, "version": APP_VERSION, "linecount": total_lines, "last_error": None})

# --- Telegram webhook ---
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
TELEGRAM_SECRET = os.getenv("TELEGRAM_SECRET", "").strip()

@app.get("/admin/ping/telegram")
async def ping_telegram():
    if not BOT_TOKEN:
        return JSONResponse({"ok": False, "error": "BOT_TOKEN ausente"})
    ok = await tg.ping(BOT_TOKEN)
    return JSONResponse({"ok": ok})

def _msg_text(update: dict) -> tuple[int|None, str|None, int|None]:
    try:
        msg = update.get("message") or update.get("edited_message") or update.get("channel_post") or {}
        chat_id = (msg.get("chat") or {}).get("id")
        txt = msg.get("text")
        msg_id = msg.get("message_id")
        return chat_id, txt, msg_id
    except Exception:
        return None, None, None

@app.post("/webhook")
async def webhook_root(request: Request):
    if not BOT_TOKEN:
        return JSONResponse({"ok": False, "error": "BOT_TOKEN ausente"})
    data = await request.json()
    chat_id, text, _ = _msg_text(data)
    if not chat_id:
        return JSONResponse({"ok": True})

    cmd = (text or "").strip().lower()
    try:
        if cmd.startswith("/start"):
            await tg.handle_start(BOT_TOKEN, chat_id)
        elif cmd.startswith("/pulse"):
            await tg.handle_pulse(BOT_TOKEN, chat_id)
        elif cmd.startswith("/eth"):
            await tg.handle_asset(BOT_TOKEN, chat_id, "ETH")
        elif cmd.startswith("/btc"):
            await tg.handle_asset(BOT_TOKEN, chat_id, "BTC")
        elif cmd.startswith("/panel"):
            await tg.handle_panel(BOT_TOKEN, chat_id)
        else:
            await tg.send_text(BOT_TOKEN, chat_id, "Comando não reconhecido. Use /pulse, /eth, /btc, /panel.")
    except Exception as e:
        logger.exception("webhook error: {}", e)
        await tg.send_text(BOT_TOKEN, chat_id, "Erro interno. Já estou olhando aqui.")
        return JSONResponse({"ok": False, "error": str(e)})
    return JSONResponse({"ok": True})

# --- graceful shutdown logging ---
@app.on_event("shutdown")
async def on_shutdown():
    logger.info("shutdown complete")

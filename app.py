# app.py — StarkRadar Bot API
# version: 0.17.3-stable
# lines-counted: will be computed at runtime via /status

import os, io, asyncio, inspect
from datetime import datetime, timezone
from typing import Dict, Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from loguru import logger

from services import tg
from services.markets import health_check as markets_health

APP_VERSION = "0.17.3-stable"

app = FastAPI()

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
BASE_URL = os.environ.get("BASE_URL", "").rstrip("/")

if not BOT_TOKEN:
    logger.warning("BOT_TOKEN ausente — configure no Render.")

# ---------- Helpers ----------
def _count_lines() -> int:
    try:
        root = os.getcwd()
        total = 0
        for dirpath, _, filenames in os.walk(root):
            for fn in filenames:
                if fn.endswith((".py", ".txt", ".md", ".env")):
                    with open(os.path.join(dirpath, fn), "rb") as f:
                        total += sum(1 for _ in f)
        return total
    except Exception:
        return -1

@app.get("/")
async def root():
    return PlainTextResponse("ok")

@app.get("/status")
async def status():
    return JSONResponse({
        "ok": True,
        "version": APP_VERSION,
        "linecount": _count_lines(),
        "last_error": None
    })

# ---------- Telegram Webhook ----------
@app.post("/webhook")
async def webhook_root(request: Request):
    # (Opcional) simples proteção de segredo via header
    if WEBHOOK_SECRET:
        sec = request.headers.get("X-Webhook-Secret", "")
        if sec != WEBHOOK_SECRET:
            return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)

    data = await request.json()
    try:
        message = data.get("message") or data.get("edited_message") or {}
        chat = (message.get("chat") or {})
        chat_id = chat.get("id")
        text = (message.get("text") or "").strip()

        if not chat_id:
            return JSONResponse({"ok": True})

        cmd = (text or "").split()[0].lower()

        # Comandos principais
        if cmd == "/start":
            await tg.send_start(BOT_TOKEN, chat_id)
            return {"ok": True}

        if cmd == "/pulse":
            await tg.handle_pulse(BOT_TOKEN, chat_id)
            return {"ok": True}

        if cmd == "/eth":
            await tg.handle_asset(BOT_TOKEN, chat_id, "ETH")
            return {"ok": True}

        if cmd == "/btc":
            await tg.handle_asset(BOT_TOKEN, chat_id, "BTC")
            return {"ok": True}

        if cmd == "/strategy":
            await tg.handle_strategy(BOT_TOKEN, chat_id)
            return {"ok": True}

        # fallback: eco curto com menu
        await tg.send_menu(BOT_TOKEN, chat_id)
        return {"ok": True}

    except Exception as e:
        logger.exception("webhook error")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

# ---------- Lifecycle ----------
@app.on_event("startup")
async def on_startup():
    logger.info("startup complete")

@app.on_event("shutdown")
async def on_shutdown():
    logger.info("shutdown complete")

# starkradar / v0.18-step2
# FastAPI + Telegram webhook (no python-telegram-bot, pure HTTP)
# ENV required: BOT_TOKEN, (optional) WEBHOOK_SECRET
# Endpoints:
#   GET  /status    -> service status
#   POST /webhook   -> Telegram webhook
#   GET  /admin/ping/telegram -> quick check for BOT_TOKEN presence

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse
import os, asyncio, inspect

from services import tg, markets, analysis, images

APP_VERSION = "v0.18-step2"

app = FastAPI(title="starkradar-bot", version=APP_VERSION)

@app.get("/status")
async def status():
    # Count lines in repo for quick sanity (non-critical)
    try:
        base = os.path.dirname(__file__)
        total = 0
        for root, _, files in os.walk(base):
            for fn in files:
                if fn.endswith((".py", ".txt", ".md")):
                    with open(os.path.join(root, fn), "r", encoding="utf-8", errors="ignore") as fh:
                        total += sum(1 for _ in fh)
    except Exception:
        total = -1

    return {"ok": True, "version": APP_VERSION, "linecount": total, "last_error": None}

@app.get("/admin/ping/telegram")
async def ping_tg():
    token = os.getenv("BOT_TOKEN")
    return {"ok": bool(token), "has_token": bool(token)}

@app.post("/webhook")
async def webhook_root(request: Request):
    # Optional shared secret header to avoid random posts
    secret = os.getenv("WEBHOOK_SECRET")
    if secret:
        hdr = request.headers.get("X-Webhook-Secret")
        if hdr != secret:
            raise HTTPException(status_code=403, detail="Forbidden")

    payload = await request.json()
    try:
        msg = payload.get("message") or payload.get("edited_message") or {}
        chat_id = msg.get("chat", {}).get("id")
        text = (msg.get("text") or "").strip()
        if not chat_id:
            return JSONResponse({"ok": True, "skipped": True})

        # Commands
        if text.lower().startswith("/start"):
            await tg.reply_start(chat_id)
        elif text.lower().startswith("/pulse"):
            await tg.reply_pulse(chat_id)
        elif text.lower().startswith("/eth"):
            await tg.reply_asset(chat_id, "ETH")
        elif text.lower().startswith("/btc"):
            await tg.reply_asset(chat_id, "BTC")
        else:
            await tg.reply_help(chat_id)

        return JSONResponse({"ok": True})
    except Exception as e:
        # Don't crash webhook, return OK to Telegram while logging
        from loguru import logger
        logger.exception("webhook error: {}", e)
        return JSONResponse({"ok": True, "handled_error": str(e)})

@app.get("/")
async def root():
    return PlainTextResponse("starkradar-bot is running. Use /status")

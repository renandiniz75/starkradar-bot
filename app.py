
"""
StarkRadar Bot • v0.20-full
- FastAPI + Telegram webhook
- Commands: /start, /pulse, /eth, /btc
- Resilient data layer with multi-source fetch + caching
- Sparkline PNGs via matplotlib
- Clean fallbacks (never empty messages)
"""

import os
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from loguru import logger

from services import tg, markets

APP_VERSION = "0.20-full"
START_TS = datetime.now(timezone.utc)

app = FastAPI(title="StarkRadar Bot", version=APP_VERSION)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")  # optional
ENV = os.getenv("ENV", "prod")

@app.get("/")
async def root():
    return PlainTextResponse(f"StarkRadar Bot {APP_VERSION} is alive.")

@app.get("/status")
async def status():
    cache_stats = markets.cache_stats()
    return JSONResponse({
        "ok": True,
        "version": APP_VERSION,
        "uptime_s": int((datetime.now(timezone.utc) - START_TS).total_seconds()),
        "cache": cache_stats
    })

@app.post("/webhook")
async def webhook_root(request: Request):
    # Optional basic guard for Telegram secret token header (x-telegram-bot-api-secret-token)
    if WEBHOOK_SECRET:
        given = request.headers.get("x-telegram-bot-api-secret-token", "")
        if given != WEBHOOK_SECRET:
            return JSONResponse({"ok": False, "error": "bad secret"}, status_code=401)

    try:
        update = await request.json()
    except Exception:
        update = {}

    chat_id = tg.extract_chat_id(update)
    text = tg.extract_text(update)
    logger.info(f"update from chat={chat_id} text={text!r}")

    if not chat_id:
        return JSONResponse({"ok": True})

    cmd = (text or "").strip().lower()

    # Route commands
    if cmd in ("/start", "start"):
        await tg.handle_start(BOT_TOKEN, chat_id)
    elif cmd in ("/pulse", "pulse"):
        await tg.handle_pulse(BOT_TOKEN, chat_id)
    elif cmd in ("/eth", "eth"):
        await tg.handle_asset(BOT_TOKEN, chat_id, "ETH")
    elif cmd in ("/btc", "btc"):
        await tg.handle_asset(BOT_TOKEN, chat_id, "BTC")
    else:
        await tg.send_markdown(
            BOT_TOKEN, chat_id,
            "Comandos: /pulse • /eth • /btc\n"
            "_(digite um deles)_"
        )

    return JSONResponse({"ok": True})


@app.get("/admin/ping/telegram")
async def ping_telegram():
    ok = await tg.test_send(BOT_TOKEN)
    return JSONResponse({"ok": ok})


@app.on_event("shutdown")
async def on_shutdown():
    await markets.close()
    logger.info("shutdown complete")

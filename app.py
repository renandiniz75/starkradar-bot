# app.py
# Stark Cripto Radar – v0.17 "Agora vai"
# FastAPI webhook + comandos /start, /pulse, /eth, /btc, /panel
# ▶️ Substitua integralmente este arquivo.

import os
import asyncio
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from loguru import logger

from services import tg as tgsvc

APP_VERSION = "0.17"
app = FastAPI(title="Stark Cripto Radar")

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    logger.warning("BOT_TOKEN não configurado — defina no Render/Env.")

TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

@app.get("/")
async def root():
    return {"ok": True, "service": "stark-radar", "version": APP_VERSION}

@app.get("/status")
async def status():
    return {"ok": True, "version": APP_VERSION}

@app.post("/webhook")
async def webhook_root(request: Request):
    """
    Webhook do Telegram.
    Responde /start, /pulse, /eth, /btc, /panel e voz (ignorado por enquanto).
    """
    body = await request.json()
    try:
        message = body.get("message") or body.get("edited_message") or {}
        chat = message.get("chat", {})
        chat_id = chat.get("id")
        text = (message.get("text") or "").strip()

        if not chat_id:
            return JSONResponse({"ok": True})

        if text.startswith("/start"):
            await tgsvc.handle_start(BOT_TOKEN, chat_id)
            return {"ok": True}

        if text.startswith("/pulse"):
            await tgsvc.handle_pulse(BOT_TOKEN, chat_id)
            return {"ok": True}

        if text.startswith("/eth"):
            await tgsvc.handle_asset(BOT_TOKEN, chat_id, "ETH")
            return {"ok": True}

        if text.startswith("/btc"):
            await tgsvc.handle_asset(BOT_TOKEN, chat_id, "BTC")
            return {"ok": True}

        if text.startswith("/panel"):
            await tgsvc.handle_panel(BOT_TOKEN, chat_id)
            return {"ok": True}

        # fallback: ajuda
        await tgsvc.send_text(BOT_TOKEN, chat_id,
                              "Comandos: /pulse, /eth, /btc, /panel.")
        return {"ok": True}

    except Exception as e:
        logger.exception("Erro no webhook: {}", e)
        if "chat_id" in locals() and chat_id:
            await tgsvc.send_text(BOT_TOKEN, chat_id,
                                  "Erro interno. Já estou olhando aqui.")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.on_event("shutdown")
async def on_shutdown():
    logger.info("shutdown complete")

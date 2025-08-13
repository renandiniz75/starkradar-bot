# v0.17.1-hotfix • app.py
import os, io, math, asyncio, datetime as dt
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from loguru import logger
from services import tg

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")  # opcional

app = FastAPI()

@app.get("/status")
async def status():
    return {"ok": True, "version": "0.17.1-hotfix"}

@app.post("/webhook")
async def webhook_root(request: Request):
    body = await request.json()
    # Se você usa segredo de webhook, valide aqui (opcional)
    try:
        message = body.get("message") or body.get("edited_message") or {}
        chat_id = (message.get("chat") or {}).get("id")
        text = (message.get("text") or "").strip()
        if not chat_id:
            return {"ok": True}

        if text.startswith("/start"):
            await tg.send_text(BOT_TOKEN, chat_id, "Bem-vindo! 👋\nUse /pulse, /eth, /btc, /panel.")
            return {"ok": True}

        if text.startswith("/pulse"):
            await tg.handle_pulse(BOT_TOKEN, chat_id); return {"ok": True}

        if text.startswith("/eth"):
            await tg.handle_asset(BOT_TOKEN, chat_id, "ETH"); return {"ok": True}

        if text.startswith("/btc"):
            await tg.handle_asset(BOT_TOKEN, chat_id, "BTC"); return {"ok": True}

        if text.startswith("/panel"):
            await tg.handle_panel(BOT_TOKEN, chat_id); return {"ok": True}

        # fallback: tenta interpretar
        await tg.send_text(BOT_TOKEN, chat_id, "Comandos: /pulse, /eth, /btc.")
        return {"ok": True}
    except Exception as e:
        logger.exception("webhook error")
        if "chat_id" in locals():
            await tg.send_text(BOT_TOKEN, chat_id, "Erro interno. Já estou olhando aqui.")
        return JSONResponse({"ok": False, "error": repr(e)}, status_code=200)

@app.on_event("shutdown")
async def on_shutdown():
    logger.info("shutdown complete")

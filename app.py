# app.py — StarkRadar Bot API
# version: 0.18.3-stable

import os
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from loguru import logger
from services import tg  # pacote local

APP_VERSION = "0.18.3-stable"

app = FastAPI()
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
BASE_URL = os.environ.get("BASE_URL", "").rstrip("/")

def _count_lines() -> int:
    import os
    total = 0
    for d,_,fs in os.walk(os.getcwd()):
        for f in fs:
            if f.endswith((".py",".txt",".md",".env",".ini")):
                try:
                    with open(os.path.join(d,f),"rb") as h:
                        total += sum(1 for _ in h)
                except:
                    pass
    return total

@app.get("/")
async def root():
    return PlainTextResponse("ok")

@app.get("/status")
async def status():
    return JSONResponse({"ok": True, "version": APP_VERSION, "linecount": _count_lines(), "last_error": None})

@app.post("/webhook")
async def webhook_root(request: Request):
    if WEBHOOK_SECRET and request.headers.get("X-Webhook-Secret","") != WEBHOOK_SECRET:
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)

    data = await request.json()
    msg = data.get("message") or data.get("edited_message") or {}
    chat = msg.get("chat") or {}
    chat_id = chat.get("id")
    text = (msg.get("text") or "").strip()
    if not chat_id:
        return {"ok": True}

    cmd = (text.split()[0] if text else "").lower()

    try:
        if cmd == "/start":
            await tg.send_start(BOT_TOKEN, chat_id)
        elif cmd == "/pulse":
            await tg.handle_pulse(BOT_TOKEN, chat_id)
        elif cmd == "/eth":
            await tg.handle_asset(BOT_TOKEN, chat_id, "ETH")
        elif cmd == "/btc":
            await tg.handle_asset(BOT_TOKEN, chat_id, "BTC")
        elif cmd == "/strategy":
            await tg.handle_strategy(BOT_TOKEN, chat_id)
        else:
            await tg.send_menu(BOT_TOKEN, chat_id)
        return {"ok": True}
    except Exception as e:
        logger.exception("webhook error")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.on_event("startup")
async def on_startup():
    logger.info("startup complete")

@app.on_event("shutdown")
async def on_shutdown():
    logger.info("shutdown complete")

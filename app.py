
"""StarkRadar Bot — v0.20 (stabilized)"""
import os
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from loguru import logger
from services import markets, tg

VERSION = "0.20"; BUILD = "full"
BOT_TOKEN = os.getenv("BOT_TOKEN","").strip()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET","").strip()
app = FastAPI()

@app.get("/") async def root(): return {"ok":True,"version":f"{VERSION}-{BUILD}"}
@app.get("/status") async def status(): return {"ok":True,"version":f"{VERSION}-{BUILD}"}

@app.post("/webhook")
async def webhook_root(request: Request):
    try: body = await request.json()
    except Exception: return JSONResponse({"ok":False,"error":"invalid json"}, status_code=400)
    if WEBHOOK_SECRET and request.headers.get("x-hook-secret")!=WEBHOOK_SECRET:
        return JSONResponse({"ok":False,"error":"unauthorized"}, status_code=401)
    msg = body.get("message") or body.get("edited_message") or {}
    chat_id = str(((msg.get("chat") or {}).get("id")) or body.get("chat_id") or "")
    text = (msg.get("text") or "").strip()
    if not chat_id: return {"ok": True}
    if text in ("/start","start","/menu"): await tg.send_menu(BOT_TOKEN, chat_id); return {"ok":True}
    if text.lower().startswith("/pulse"): await tg.send_pulse(BOT_TOKEN, chat_id); return {"ok":True}
    if text.lower().startswith("/eth"): await tg.handle_asset(BOT_TOKEN, chat_id, "ETH"); return {"ok":True}
    if text.lower().startswith("/btc"): await tg.handle_asset(BOT_TOKEN, chat_id, "BTC"); return {"ok":True}
    await tg.send_menu(BOT_TOKEN, chat_id); return {"ok":True}

@app.on_event("shutdown")
async def on_shutdown(): logger.info("shutdown complete")

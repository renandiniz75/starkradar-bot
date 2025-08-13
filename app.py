# app.py
# v0.17.3 – FastAPI + Webhook com /start, /pulse, /eth, /btc
from __future__ import annotations
from fastapi import FastAPI, Request
from os import getenv
from datetime import datetime, timezone

from services import tg

app = FastAPI()

def _utcnow():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

@app.get("/status")
async def status():
    return {"ok": True, "version": "0.17.3", "time": _utcnow()}

@app.post("/webhook")
async def webhook_root(request: Request):
    data = await request.json()
    msg = data.get("message") or data.get("edited_message") or {}
    chat_id = (msg.get("chat") or {}).get("id")
    text = (msg.get("text") or "").strip().lower()

    if not chat_id:
        return {"ok": True}

    if text.startswith("/start"):
        await tg.send_message(chat_id, "Bem-vindo! 👋\nUse /pulse, /eth, /btc, /panel.")
        return {"ok": True}

    if text.startswith("/pulse"):
        # usa build_asset_text para ETH e BTC e devolve mini sumário
        eth_txt, _ = await tg.build_asset_text("ETH")
        btc_txt, _ = await tg.build_asset_text("BTC")
        head = f"Pulse ⏱ { _utcnow() }\n"
        await tg.send_message(chat_id, head + eth_txt.splitlines()[0] + "\n" + btc_txt.splitlines()[0] + "\n\nAnálise: foco nos gatilhos e gestão de risco.")
        return {"ok": True}

    if text.startswith("/eth"):
        await tg.handle_asset(chat_id, "ETH"); return {"ok": True}

    if text.startswith("/btc"):
        await tg.handle_asset(chat_id, "BTC"); return {"ok": True}

    # fallback
    await tg.send_message(chat_id, "Comandos: /pulse, /eth, /btc.")
    return {"ok": True}

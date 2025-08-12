from __future__ import annotations
import os, json, asyncio, inspect
from typing import Dict, Any
from fastapi import FastAPI, Request
from loguru import logger
from dotenv import load_dotenv
from utils import VERSION, utcnow_iso, linecount_of_files
from services import tg

load_dotenv()

app = FastAPI(title="Stark Cripto Radar")

@app.get("/status")
async def status():
    files = ["app.py","utils.py","services/tg.py","services/markets.py","services/news.py"]
    lc = linecount_of_files(files)
    return {"ok": True, "version": VERSION, "linecount": lc, "last_error": None}

@app.post("/webhook")
async def webhook_root(request: Request):
    body = await request.json()
    # Telegram webhook payload
    msg = body.get("message") or body.get("edited_message") or {}
    chat = msg.get("chat") or {}
    chat_id = chat.get("id")
    text = (msg.get("text") or "").strip()

    if not chat_id:
        return {"ok": True}

    if text in ("/start", "/start@starkradar_bot"):
        await send_start(chat_id)
        return {"ok": True}

    if text.startswith("/pulse"):
        await tg.handle_pulse(chat_id); return {"ok": True}
    if text.startswith("/eth"):
        await tg.handle_asset(chat_id, "ETH"); return {"ok": True}
    if text.startswith("/btc"):
        await tg.handle_asset(chat_id, "BTC"); return {"ok": True}
    if text.startswith("/panel"):
        await tg.handle_panel(chat_id); return {"ok": True}

    # Voice support (optional)
    voice = msg.get("voice")
    if voice:
        await handle_voice(chat_id, msg)
        return {"ok": True}

    # Fallback
    await tg.send_message(chat_id, "Use /pulse, /eth, /btc, /panel.")
    return {"ok": True}

async def send_start(chat_id: int):
    text = "Bem-vindo! Use /pulse, /eth, /btc, /panel."
    await tg.send_message(chat_id, text)

# ---------------- Voice + GPT ----------------
async def handle_voice(chat_id: int, msg: Dict[str, Any]):
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    if not (BOT_TOKEN and OPENAI_API_KEY):
        await tg.send_message(chat_id, "Voz indisponível: configure OPENAI_API_KEY.")
        return
    # Get file path
    file_id = msg["voice"]["file_id"]
    import httpx, tempfile, os
    async with httpx.AsyncClient(timeout=30) as client:
        # getFile
        r = await client.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getFile", params={"file_id": file_id})
        fpath = r.json()["result"]["file_path"]
        url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{fpath}"
        audio = await client.get(url)
        # Transcribe
        from openai import OpenAI
        client_oa = OpenAI(api_key=OPENAI_API_KEY)
        with tempfile.NamedTemporaryFile(suffix=".ogg", delete=False) as tmp:
            tmp.write(audio.content); tmp.flush()
            with open(tmp.name, "rb") as f:
                tr = client_oa.audio.transcriptions.create(model="gpt-4o-transcribe", file=f)
        question = tr.text
        # Answer
        sysmsg = "Você é um analista de cripto. Responda curto, objetivo, com níveis, riscos e ações."
        resp = client_oa.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[{"role":"system","content":sysmsg},{"role":"user","content":question}],
            temperature=0.2
        )
        answer = resp.choices[0].message.content.strip()
        await tg.send_message(chat_id, f"Pergunta: {question}\n\n{answer}")

# ---------------- Initialization ------------
@app.on_event("startup")
async def _on_start():
    logger.info("Stark Radar online")


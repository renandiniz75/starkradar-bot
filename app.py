from __future__ import annotations
import os, json, asyncio, datetime as dt, inspect
from typing import Any, Dict
from fastapi import FastAPI, Request, Header
from fastapi.responses import PlainTextResponse, JSONResponse
from loguru import logger

from services import config, db, migrations, tg, news as news_svc, data as data_svc

app = FastAPI(title="Stark DeFi Agent", version=config.VERSION)
LAST_ERROR = None

@app.on_event("startup")
async def _startup():
    try:
        await migrations.run_all()
        if os.getenv("WEBHOOK_AUTO","0") == "1" and config.HOST_URL and config.BOT_TOKEN:
            try:
                resp = await tg.set_webhook(config.HOST_URL)
                logger.info(f"setWebhook: {resp}")
            except Exception as e:
                logger.warning(f"setWebhook fail: {e}")
        # best-effort warm-up
        asyncio.create_task(news_svc.fetch_news() if config.FETCH_NEWS else asyncio.sleep(0))
        asyncio.create_task(data_svc.prices_snapshot())
    except Exception as e:
        global LAST_ERROR
        LAST_ERROR = f"startup: {e}"
        logger.exception(e)

@app.get("/", response_class=PlainTextResponse)
async def root():
    return "ok"

@app.get("/status")
async def status():
    code = inspect.getsource(app.__class__)
    # linecount for whole project (rough estimate): sum file lengths
    try:
        base = os.path.dirname(__file__)
        total = 0
        for dirpath, _, filenames in os.walk(base):
            for f in filenames:
                if f.endswith((".py",".md",".txt",".procfile","Procfile")):
                    try:
                        p = os.path.join(dirpath,f)
                        with open(p,"r",encoding="utf-8",errors="ignore") as fh:
                            total += len(fh.readlines())
                    except Exception:
                        pass
    except Exception:
        total = None
    return {"ok": True, "version": config.VERSION, "linecount": total, "last_error": LAST_ERROR}

@app.post("/admin/webhook/set")
async def admin_webhook_set():
    try:
        if not (config.HOST_URL and config.BOT_TOKEN):
            return {"ok": False, "error": "HOST_URL/BOT_TOKEN missing"}
        resp = await tg.set_webhook(config.HOST_URL)
        return resp
    except Exception as e:
        global LAST_ERROR; LAST_ERROR = f"set_webhook: {e}"; raise

@app.get("/selftest")
async def selftest(x_admin_secret: str = Header(default=None)):
    if x_admin_secret != (os.getenv("ADMIN_SECRET") or config.ADMIN_SECRET):
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
    from tests.selftest import run
    out = await run()
    return {"ok": True, "version": config.VERSION, "result": out}

@app.post("/webhook")
async def webhook_root(request: Request):
    body = await request.json()
    try:
        msg = body.get("message") or body.get("edited_message")
        if not msg:
            return {"ok": True}
        chat_id = msg.get("chat",{}).get("id")
        text = (msg.get("text") or "").strip()
        if not text:
            return {"ok": True}
        # Commands
        if text.lower().startswith("/start"):
            await tg.handle_start(chat_id)
        elif text.lower().startswith("/pulse"):
            await tg.handle_pulse(chat_id)
        elif text.lower().startswith("/eth"):
            await tg.handle_asset(chat_id, "ETH")
        elif text.lower().startswith("/btc"):
            await tg.handle_asset(chat_id, "BTC")
        elif text.lower().startswith("/panel"):
            await tg.handle_pulse(chat_id)
        elif text.lower().startswith("/selftest"):
            # Defensive: allow inline selftest for owner (simple check: presence of ADMIN_SECRET env)
            if os.getenv("ADMIN_SECRET"):
                from tests.selftest import run
                res = await run()
                await tg.tg_send_text(chat_id, "<b>Selftest</b>\n" + json.dumps(res, indent=2))
        else:
            await tg.tg_send_text(chat_id, "Comandos: /pulse /eth /btc /panel")
        return {"ok": True}
    except Exception as e:
        global LAST_ERROR
        LAST_ERROR = f"webhook: {e}"
        logger.exception(e)
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

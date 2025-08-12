from __future__ import annotations
import asyncio, os
from loguru import logger
from services import config, db, data, charts

async def run() -> dict:
    out = {"env": {}, "db": None, "prices": None, "sparkline": None}
    # Env check
    needed = ["BOT_TOKEN"]
    out["env"] = {k: bool(os.getenv(k)) for k in needed}
    # DB check
    try:
        await db.ensure_db()
        out["db"] = "ok" if config.DATABASE_URL else "skipped"
    except Exception as e:
        out["db"] = f"fail: {e}"
    # Prices
    try:
        p = await data.prices_snapshot()
        out["prices"] = {k: bool(v and v.get("last")) for k,v in p.items()}
    except Exception as e:
        out["prices"] = f"fail: {e}"
    # Sparkline
    try:
        img = charts.make_sparkline_png([1,2,1,3,2,4,3,5])
        out["sparkline"] = "ok" if img and len(img)>1000 else "small"
    except Exception as e:
        out["sparkline"] = f"fail: {e}"
    return out

if __name__ == "__main__":
    print(asyncio.run(run()))

from __future__ import annotations
import os

VERSION = "6.0.16-full"

def env(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name, default)
    if v is not None and isinstance(v, str):
        v = v.strip()
    return v

BOT_TOKEN = env("BOT_TOKEN")
HOST_URL = env("HOST_URL")
DATABASE_URL = env("DATABASE_URL")
ADMIN_SECRET = env("ADMIN_SECRET", "dev-secret-change-me")
WEBHOOK_AUTO = env("WEBHOOK_AUTO", "0") == "1"

NEWS_SOURCES = [s.strip() for s in (env("NEWS_SOURCES") or "").split(",") if s.strip()]
TZ = env("TZ", "UTC")

# Feature flags
FETCH_NEWS = True
USE_CCXT = True

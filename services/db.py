from __future__ import annotations
import asyncpg
from typing import Optional, Any
from . import config

_POOL: Optional[asyncpg.Pool] = None

CREATE_BASE = [
    """
    CREATE TABLE IF NOT EXISTS _migrations (
        id TEXT PRIMARY KEY,
        applied_at TIMESTAMPTZ DEFAULT now()
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS news_items (
        id BIGSERIAL PRIMARY KEY,
        ts TIMESTAMPTZ DEFAULT now(),
        title TEXT,
        url TEXT,
        source TEXT,
        summary TEXT,
        tags TEXT,
        extra JSONB DEFAULT '{}'::jsonb
    );
    """
]

MIGRATIONS = [
    # Add future migrations here with unique IDs
    ("2025-08-12-add-index", """
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_indexes WHERE indexname = 'idx_news_ts'
            ) THEN
                CREATE INDEX idx_news_ts ON news_items (ts DESC);
            END IF;
        END $$;
    """)
]

async def pool() -> asyncpg.Pool:
    global _POOL
    if _POOL is None:
        _POOL = await asyncpg.create_pool(dsn=config.DATABASE_URL) if config.DATABASE_URL else None
    return _POOL

async def ensure_db():
    if not config.DATABASE_URL:
        return
    p = await pool()
    async with p.acquire() as c:
        for stmt in CREATE_BASE:
            await c.execute(stmt)
        await c.execute("CREATE TABLE IF NOT EXISTS _migrations (id TEXT PRIMARY KEY, applied_at TIMESTAMPTZ DEFAULT now())")
        rows = await c.fetch("SELECT id FROM _migrations")
        applied = {r["id"] for r in rows}
        for mid, sql in MIGRATIONS:
            if mid not in applied:
                await c.execute(sql)
                await c.execute("INSERT INTO _migrations (id) VALUES ($1)", mid)

async def insert_news(items: list[dict]):
    if not items or not config.DATABASE_URL:
        return 0
    p = await pool()
    async with p.acquire() as c:
        q = """
        INSERT INTO news_items (ts, title, url, source, summary, tags, extra)
        VALUES (COALESCE($1, now()), $2, $3, $4, $5, $6, $7)
        ON CONFLICT DO NOTHING;
        """
        count = 0
        for it in items:
            try:
                await c.execute(q,
                    it.get("ts"),
                    it.get("title"),
                    it.get("url"),
                    it.get("source"),
                    it.get("summary"),
                    it.get("tags"),
                    it.get("extra") or {}
                )
                count += 1
            except Exception:
                # Tolerate wrong rows
                pass
        return count

async def get_recent_news(hours: int = 12, limit: int = 6):
    if not config.DATABASE_URL:
        return []
    p = await pool()
    async with p.acquire() as c:
        q = """
        SELECT ts, title, url, source, summary
        FROM news_items
        WHERE ts >= now() - ($1::int || ' hours')::interval
        ORDER BY ts DESC
        LIMIT $2
        """
        return await c.fetch(q, hours, limit)

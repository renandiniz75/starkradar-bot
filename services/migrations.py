from __future__ import annotations
from . import db

async def run_all():
    await db.ensure_db()

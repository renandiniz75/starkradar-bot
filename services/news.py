from __future__ import annotations
import datetime as dt
from typing import List, Dict
import httpx
from bs4 import BeautifulSoup
from loguru import logger
from . import db, config

DEFAULT_SOURCES = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://www.theblock.co/rss.xml"
]

async def fetch_news() -> List[Dict]:
    sources = config.NEWS_SOURCES or DEFAULT_SOURCES
    items: List[Dict] = []
    async with httpx.AsyncClient(timeout=10) as client:
        for url in sources:
            try:
                r = await client.get(url, follow_redirects=True)
                soup = BeautifulSoup(r.text, "lxml-xml")
                for it in soup.select("item")[:10]:
                    title = (it.title.text or "").strip()
                    link = (it.link.text or "").strip()
                    pub = it.pubDate.text.strip() if it.pubDate else None
                    ts = None
                    try:
                        ts = dt.datetime.strptime(pub, "%a, %d %b %Y %H:%M:%S %Z") if pub else None
                    except Exception:
                        ts = None
                    items.append({
                        "ts": ts,
                        "title": title[:280] if title else None,
                        "url": link or None,
                        "source": url.split('/')[2] if '://' in url else url,
                        "summary": None,
                        "tags": None,
                        "extra": {}
                    })
            except Exception as e:
                logger.warning(f"news fetch fail {url}: {e}")
    if items:
        try:
            await db.insert_news(items)
        except Exception as e:
            logger.warning(f"insert_news fail: {e}")
    return items[:12]

from __future__ import annotations
import os, datetime, time
from urllib.parse import urlparse
import httpx
from bs4 import BeautifulSoup
from loguru import logger

FEEDS = (os.getenv("NEWS_FEEDS") or "https://www.theblock.co/rss;https://www.coindesk.com/arc/outboundfeeds/rss/?outputType=xml").split(";")

async def fetch_feed_items(hours=12, limit=6):
    cutoff = time.time() - hours*3600
    items = []
    async with httpx.AsyncClient(timeout=12) as client:
        for url in FEEDS:
            try:
                r = await client.get(url)
                r.raise_for_status()
                soup = BeautifulSoup(r.text, "xml")
                for it in soup.select("item")[:10]:
                    title = (it.title.text or "").strip()
                    link = (it.link.text or "").strip()
                    pub = it.pubDate.text if it.pubDate else ""
                    ts = None
                    try:
                        ts = datetime.datetime.strptime(pub, "%a, %d %b %Y %H:%M:%S %Z").timestamp()
                    except Exception:
                        ts = time.time()
                    if ts >= cutoff and title:
                        items.append({"title": title, "link": link, "ts": ts, "host": urlparse(link).hostname})
            except Exception as e:
                logger.warning(f"feed fail {url}: {e}")
    items.sort(key=lambda x: x["ts"], reverse=True)
    return items[:limit]

def summarize_items(items):
    bullets = []
    for it in items:
        host = it.get("host","")
        bullets.append(f"• {host} — {it['title']}")
    return "\n".join(bullets) if bullets else "—"

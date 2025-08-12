from __future__ import annotations
import io, time, textwrap, os, math, base64
from datetime import datetime, timezone
from loguru import logger
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

VERSION = "0.16-full"
START_TS = time.time()

def utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

def linecount_of_files(paths):
    total = 0
    for p in paths:
        try:
            with open(p, "r", encoding="utf-8") as f:
                total += sum(1 for _ in f)
        except Exception:
            pass
    return total

def fmt_price(v):
    try:
        return f"${float(v):,.2f}"
    except Exception:
        return "—"

def spark_png(series, width=600, height=220, label=None):
    fig, ax = plt.subplots(figsize=(width/100, height/100), dpi=100)
    ax.plot(series if series else [0,0])
    ax.set_xticks([]); ax.set_yticks([])
    ax.grid(True, alpha=.15)
    if label:
        ax.set_title(label, loc="left")
    buf = io.BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png")
    plt.close(fig)
    buf.seek(0)
    return buf.read()

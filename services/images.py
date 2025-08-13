# services/images.py
# Geração de PNGs (sparklines) simples

import io
from typing import List, Optional
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

def sparkline_png(series: Optional[List[float]], title: str) -> bytes:
    fig = plt.figure(figsize=(5.5, 2.4), dpi=160)
    ax = fig.add_subplot(111)
    if series and len(series) >= 2:
        ax.plot(series, linewidth=2)
    ax.set_title(title, fontsize=10)
    ax.grid(alpha=0.25)
    ax.set_xlabel("")
    ax.set_ylabel("")
    buf = io.BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    return buf.getvalue()

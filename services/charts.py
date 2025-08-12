from __future__ import annotations
import io
from typing import List
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

def make_sparkline_png(series: List[float]) -> bytes:
    fig, ax = plt.subplots(figsize=(3.0, 0.6), dpi=200)
    ax.plot(series if series else [0,0,0])
    ax.set_axis_off()
    buf = io.BytesIO()
    plt.tight_layout(pad=0.1)
    fig.savefig(buf, format="png", bbox_inches="tight", pad_inches=0)
    plt.close(fig)
    buf.seek(0)
    return buf.read()

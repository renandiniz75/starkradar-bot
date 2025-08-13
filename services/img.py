# utils/img.py — geração de sparkline PNG
import io
from typing import List
import matplotlib
matplotlib.use("Agg")  # backend offscreen
import matplotlib.pyplot as plt

def render_sparkline_png(series: List[float], label: str = "") -> bytes:
    if not series:
        series = [0, 0]
    fig = plt.figure(figsize=(6, 2.1), dpi=180)
    ax = fig.add_subplot(111)
    ax.plot(series)
    ax.set_title(label, fontsize=9)
    ax.grid(True, alpha=0.25)
    ax.set_xticks([])
    ax.set_yticks([])
    fig.tight_layout(pad=0.5)
    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    return buf.getvalue()

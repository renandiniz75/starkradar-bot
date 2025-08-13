
from typing import List, Tuple
import io
import matplotlib
matplotlib.use("Agg")  # headless
import matplotlib.pyplot as plt

def render_sparkline_png(series: List[Tuple[int, float]]) -> bytes:
    xs = [t/1000 for t, _ in series] if series else [0,1]
    ys = [p for _, p in series] if series else [0,0]
    fig = plt.figure(figsize=(5,1.6), dpi=200)
    ax = fig.add_subplot(111)
    ax.plot(xs, ys)
    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_visible(False)
    buf = io.BytesIO()
    plt.tight_layout()
    fig.savefig(buf, format="png")
    plt.close(fig)
    return buf.getvalue()

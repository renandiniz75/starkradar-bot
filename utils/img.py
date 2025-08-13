
from typing import List, Tuple
import io
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

def render_sparkline_png(series: List[Tuple[int,float]], label: str="") -> bytes:
    xs = [i for i,_ in enumerate(series)]
    ys = [p for _,p in series]
    fig = plt.figure(figsize=(5,2), dpi=150)
    ax = plt.gca()
    if ys:
        ax.plot(xs, ys, linewidth=2)
        ax.fill_between(xs, ys, min(ys), alpha=0.1)
        ax.set_title(label)
    ax.set_xticks([]); ax.set_yticks([])
    for s in ["top","right","left","bottom"]: ax.spines[s].set_visible(False)
    import io as _io
    buf = _io.BytesIO(); plt.tight_layout(); plt.savefig(buf, format="png"); plt.close(fig)
    return buf.getvalue()

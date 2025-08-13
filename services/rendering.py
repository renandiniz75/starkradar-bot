# services/rendering.py
import io
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

def sparkline_png(series, title=""):
    # series: list of [ts, price]
    ys = [p for _, p in series] if series else []
    fig = plt.figure(figsize=(6,2), dpi=150)
    if ys:
        plt.plot(ys)
    plt.title(title)
    plt.xticks([]); plt.yticks([])
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format="png")
    plt.close(fig)
    return buf.getvalue()

# services/images.py
# Sparkline PNG generation
import io
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

def sparkline_png(series, width=640, height=200):
    fig = plt.figure(figsize=(width/100, height/100), dpi=100)
    ax = fig.add_subplot(111)
    if series:
        ax.plot(series)
        ax.fill_between(range(len(series)), series, alpha=0.1)
    ax.set_xticks([]); ax.set_yticks([])
    for spine in ["top","right","left","bottom"]:
        ax.spines[spine].set_visible(False)
    bio = io.BytesIO()
    plt.tight_layout()
    fig.savefig(bio, format="png")
    plt.close(fig)
    bio.seek(0)
    return bio.getvalue()

# services/analysis.py
# Lightweight levels + narrative based on last 24h
from typing import Dict, List
import statistics as stats

def key_levels(series: List[float]):
    if not series:
        return {"support": "-", "resist": "-", "pivot":"-"}
    high = max(series)
    low  = min(series)
    pivot = (high + low) / 2
    # simple bucketed levels
    support = round((low + pivot)/2, 2)
    resist  = round((high + pivot)/2, 2)
    return {"support": support, "resist": resist, "pivot": round(pivot,2), "high": round(high,2), "low": round(low,2)}

def narrative(asset: str, price: float, change_24h: float, levels: Dict[str, float]):
    if price is None:
        return "Dados indisponíveis agora."
    ch = "-" if change_24h is None else f"{change_24h:+.2f}%"
    sup = levels.get("support","-"); res = levels.get("resist","-"); piv = levels.get("pivot","-")
    txt = []
    txt.append(f"{asset} ${price:,.2f}  ({ch})")
    txt.append(f"Níveis • S:{sup}  P:{piv}  R:{res}")
    if isinstance(res,(int,float)) and price>res:
        txt.append("⚠️ Acima da resistência intraday → confirma força; pullbacks até R podem ser compra tática.")
    elif isinstance(sup,(int,float)) and price<sup:
        txt.append("⚠️ Abaixo do suporte → gestão defensiva; aguardar reconquista de S para reentrada.")
    else:
        txt.append("⏳ Em range entre suporte e resistência; aguardar rompimento válido com volume.")
    return "\n".join(txt)

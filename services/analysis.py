# services/analysis.py
# Geração de texto analítico e recomendações

from typing import Dict, Any, Tuple

def _fmt(v, n=2, prefix="$"):
    if v is None:
        return "-"
    return f"{prefix}{v:,.{n}f}"

def _fmt_pct(v, n=2):
    if v is None:
        return "-"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.{n}f}%"

def asset_analysis(s: Dict[str, Any]) -> Tuple[str, str]:
    """
    Retorna (headline, recomendacao)
    """
    px = _fmt(s.get("price"))
    chg = _fmt_pct(s.get("chg24"))
    high = _fmt(s.get("high")); low = _fmt(s.get("low"))
    lv = s.get("levels") or {}
    S1, S2, R1, R2 = (lv.get("S1"), lv.get("S2"), lv.get("R1"), lv.get("R2"))
    rsi = s.get("rsi")
    ma7 = (s.get("ma") or {}).get("ma7")
    ma21 = (s.get("ma") or {}).get("ma21")
    rel = s.get("rel_eth_btc")

    head = (
        f"{s['asset']} {px} ({chg} 24h)\n"
        f"Níveis S:{_fmt(S1)}, {_fmt(S2)} | R:{_fmt(R1)}, {_fmt(R2)}"
    )

    # Regras simples de ação
    tips = []
    if rsi is not None:
        if rsi > 70:
            tips.append("RSI > 70 (sobrecomprado) — evitar entradas atrasadas; avaliar hedge parcial.")
        elif rsi < 30:
            tips.append("RSI < 30 (sobrevendido) — atenção a reversões; gatilhos de compra com confirmação.")
    if ma7 and ma21:
        if ma7 > ma21:
            tips.append("Tendência de curto acima da média (MA7>MA21).")
        else:
            tips.append("Curto abaixo da média (MA7<MA21) — prudência.")
    if S1 and s.get("price"):
        dist_s1 = (s["price"] - S1) / S1 * 100
        if dist_s1 < 1.0:
            tips.append("Preço próximo de S1 — apertar stops e reduzir risco se perder S1.")
    if R1 and s.get("price"):
        dist_r1 = (R1 - s["price"]) / s["price"] * 100
        if dist_r1 < 1.0:
            tips.append("Romper R1 com volume é gatilho de continuação.")

    if rel and s["asset"] == "ETH":
        tips.append(f"ETH/BTC ~ {rel:.5f} — força relativa {'alta' if rel>0.06 else 'neutra/baixa'}.")

    rec = " ".join(tips) if tips else "Acompanhar gatilhos em rompimentos válidos e gerir risco nas perdas de suporte."
    return head, rec

def portfolio_hint(asset: str, s: Dict[str, Any]) -> str:
    """
    Recomendação específica considerando seu contexto (ETHT 2x, Aave, BTC spot).
    """
    price = s.get("price")
    lv = s.get("levels") or {}
    S1, R1 = lv.get("S1"), lv.get("R1")
    rsi = s.get("rsi")

    if asset == "ETH":
        hints = []
        if R1 and price and price > R1:
            hints.append("Acima de R1 — mantenha exposição; hedge leve (5–10%) se RSI esticado.")
        if S1 and price and price < S1:
            hints.append("Abaixo de S1 — reduza alavancagem do ETHT 2x e reponha após pullback.")
        if rsi and rsi > 72:
            hints.append("RSI>72 — bom lugar para travar parte do ganho no ETHT com short perp mínimo.")
        return " ".join(hints) or "Sem mudança estrutural — manter plano e ajustar hedge se tocar R1/S1."

    if asset == "BTC":
        if S1 and price and price < S1:
            return "Se perder S1 com confirmação, reduzir risco do portfólio e reentrar em pullback."
        return "BTC neutro para risco — usar apenas como referência de beta e hedge cruzado."
    return ""

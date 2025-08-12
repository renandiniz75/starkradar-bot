# Stark DeFi Agent v6.0.15-full

Bot Telegram com webhook (FastAPI) + análise tática de BTC/ETH, gráficos sparkline,
níveis dinâmicos robustos, ingest de notícias leve e voz (áudio → texto via OpenAI)
e comando `/ask` para perguntas livres.

## Comandos
- `/start` — boas‑vindas + painéis rápidos.
- `/pulse` — visão tática (preços, ETH/BTC, variações 8h/12h, S/R, ações).
- `/eth`, `/btc` — foco no ativo.
- `/panel` — painel compacto + gráfico sparkline.
- **Voz:** envie áudio; o bot transcreve e responde.
- `/ask <pergunta>` — pergunta livre com resposta sintetizada.

## Deploy rápido (Render)
1. Web Service → Upload do ZIP deste repo.
2. Start command: `uvicorn app:app --host 0.0.0.0 --port $PORT`
3. Defina variáveis de ambiente do `.env.example`.
4. (Opcional) `WEBHOOK_AUTO=1` para setar automaticamente no startup.
5. Teste `/status` e `/admin/setwebhook`.

## Observações
- Fechamento garantido de clientes CCXT (`await ex.close()`).
- Migrations idempotentes para `news_items`.
- Fallbacks seguros quando dados externos falharem (sem NaN).


# StarkRadar Bot v0.20-full

Telegram bot com FastAPI e webhook. Comandos: `/start`, `/pulse`, `/eth`, `/btc`.

## Como rodar local
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.sample .env  # edite BOT_TOKEN
uvicorn app:app --reload --port 8080
```

## Render
- Tipo: Web Service
- Start Command: `uvicorn app:app --host 0.0.0.0 --port $PORT`
- Variáveis: `BOT_TOKEN`, `WEBHOOK_SECRET`(opcional), `COINGECKO_API_KEY`(opcional)

Após deploy, configure o webhook:
```
curl -sS "https://api.telegram.org/bot$BOT_TOKEN/setWebhook" \
  -d "url=https://SEUAPP.onrender.com/webhook" \
  -d "secret_token=$WEBHOOK_SECRET"
```

## O que há de novo (v0.20)
- Camada de mercado resiliente: CoinGecko (com chave opcional) + fallbacks Coinbase/Bitstamp
- Séries de 24h via Bybit Kline (com tolerância a 403/timeout) e fallback sintético
- Cache in-memory com TTL (evita 429/limites)
- Sparkline PNG com matplotlib
- Mensagens Markdown + menu estático

## TODO próximos passos
- Persistência em Postgres (Railway) para histórico e métricas
- Funding/OI snapshots (derivados públicos) com backoff
- Comando /panel com análise ampliada (risco, gatilhos)

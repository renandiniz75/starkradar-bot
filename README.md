
# StarkRadar Bot — 6.0.13-hotfix1

## Variáveis obrigatórias
- BOT_TOKEN: Token do Telegram Bot
- HOST_URL: URL pública do serviço (https://...)

## Variáveis opcionais
- DATABASE_URL: Postgres (para cache de notícias)
- WEBHOOK_AUTO=1: define webhook no startup
- SPARKLINES=1: liga sparkline PNG no /start (requer matplotlib)
- PROVIDERS: bybit,okx,binance (default)
- NEWS_SOURCES: CSV com feeds RSS

## Deploy Render
1) Novo Web Service
2) Build Command: `pip install -r requirements.txt`
3) Start Command: `uvicorn app:app --host 0.0.0.0 --port $PORT`
4) Set envs e Deploy.

## Endpoints úteis
- GET /status
- GET /admin/ping/telegram
- GET /admin/webhook/set
- POST /admin/db/migrate
- POST /webhook  (Telegram)

## Comandos do Bot
/start, /pulse, /btc, /eth, /help

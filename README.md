# Stark Cripto Radar — v0.16-full

Bot de Telegram com webhook em FastAPI + gráficos, análise e transcrição de áudio.

## Comandos

- `/start` — boas-vindas + instruções
- `/pulse` — boletim com preços, variação, níveis e síntese de notícias (12h)
- `/eth` — leitura rápida do ETH
- `/btc` — leitura rápida do BTC
- `/panel` — mini painel PNG (ETH, BTC e ETH/BTC)

Suporte a **áudio**: envie uma mensagem de voz com a pergunta. Se `OPENAI_API_KEY` estiver configurada, o bot transcreve com Whisper e responde via GPT com foco em cripto.

## Variáveis de ambiente

```
BOT_TOKEN= # token do @BotFather
WEBHOOK_URL= # https://seuapp.onrender.com/webhook
OPENAI_API_KEY= # opcional (voz e /gpt)
NEWS_FEEDS=https://www.theblock.co/rss;https://www.coindesk.com/arc/outboundfeeds/rss/?outputType=xml
```

## Deploy (Render)

- `requirements.txt` já inclui `uvicorn`.
- Command: `uvicorn app:app --host 0.0.0.0 --port $PORT`

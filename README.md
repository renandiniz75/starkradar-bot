# StarkRadar Bot 0.17.0-full

## Endpoints
- `/` health
- `/status` status + linecount
- `/webhook` Telegram webhook (POST from Telegram)

## Environment
```
BOT_TOKEN=123:ABC
TELEGRAM_SECRET=optional
PORT=10000
```
Set webhook once:
```
curl -sS "https://api.telegram.org/bot$BOT_TOKEN/setWebhook?url=https://YOUR-RENDER-URL/webhook"
```

## Commands
- `/start` – boas-vindas + sparkline ETH
- `/pulse` – ETH/BTC resumo e análise curta
- `/eth` – snapshot com níveis e sparkline
- `/btc` – snapshot com níveis e sparkline
- `/panel` – ETH e BTC em sequência

## Notes
- Versão e contagem de linhas **não** aparecem nos balões. Ver só em `/status`.
- Preços via Bybit público com fallback CoinGecko. Klíne 24h via Bybit.
- Sem dependência de `python-telegram-bot` para evitar conflitos; enviamos direto na API do Telegram.

# starkradar v0.18-step2

## Como usar
1. Suba estes arquivos no GitHub/Render.
2. Defina as variáveis de ambiente:
   - `BOT_TOKEN` (obrigatório)
   - `WEBHOOK_SECRET` (opcional, e use o mesmo header na configuração do webhook no Telegram/Render)
3. Aponte o webhook do Telegram para `POST https://SEU_DOMINIO/webhook`.

### Comandos
- `/start` – boas-vindas e menu
- `/pulse` – visão rápida (ETH, BTC, ETH/BTC)
- `/eth` – detalhe do Ethereum
- `/btc` – detalhe do Bitcoin

### Observações
- Usa CoinGecko (sem API key) para *spot, 24h high/low e série intraday*.
- Evita Binance/Bybit para contornar 403/451.
- Gráficos via Matplotlib (sparkline).

### Roadmap rápido
- Funding/Open Interest (Coinglass/Glassnode) – precisa de API keys.
- Alertas e gatilhos automáticos.
- Painéis com múltiplos tempos gráficos.

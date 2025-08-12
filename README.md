# Stark DeFi Agent v6.0.16-full

**Objetivo:** Bot Telegram com FastAPI, análise tática de BTC/ETH, níveis dinâmicos, notícias e autotestes.  
**Comandos:** `/start`, `/pulse`, `/eth`, `/btc`, `/panel`, `/selftest` (privado).

## Variáveis de ambiente
- `BOT_TOKEN` (obrigatório) — token do bot do Telegram
- `HOST_URL` (obrigatório p/ webhook) — ex: `https://seuservico.onrender.com`
- `DATABASE_URL` (opcional) — Postgres: `postgres://user:pass@host:5432/db`
- `OPENAI_API_KEY` (opcional) — para STT/TTS no futuro (placeholder)
- `WEBHOOK_AUTO=1` (opcional) — auto-configura webhook no startup
- `NEWS_SOURCES` (opcional) — lista separada por vírgula (RSS/HTML), ex: `https://www.coindesk.com/arc/outboundfeeds/rss/`
- `TZ` (opcional) — timezone, ex: `UTC`

## Deploy rápido
1. Suba o repositório (estes arquivos) no GitHub.
2. Render: Python 3.11, build padrão. Porta `$PORT`.
3. **requirements.txt** inclui `uvicorn`.
4. Configure as envs acima; salve.
5. Abra `/status` — se OK, rode `/selftest` no bot ou `GET /selftest` (com `X-Admin-Secret`, ver abaixo).
6. Mande `/start` no Telegram. Depois `/pulse`, `/eth`, `/btc`.

## Rotas HTTP
- `GET /` — ping
- `GET /status` — estado, versão, contagem de linhas e último erro
- `POST /webhook` — webhook Telegram
- `POST /admin/webhook/set` — define webhook (env `HOST_URL` + `BOT_TOKEN`)
- `GET /selftest` — autoteste (precisa `X-Admin-Secret` = `ADMIN_SECRET` env)

## Banco de dados
Migrações pequenas são executadas automaticamente (tabela `_migrations`).  
Tabela `news_items`: colunas tolerantes a `NULL` (sem NOT NULL rígido) e defaults.

## Notas
- Fechamento de clientes CCXT é **garantido** (sem *Unclosed session*).
- Níveis S/R baseados em extremos de 48h, com *guards* para dados ausentes.
- Sem rodapé nos balões (rodapé só no código e no `/status`).

---
**Versão:** v6.0.16-full

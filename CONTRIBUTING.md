# Contributing

Thanks for your interest! This started as a graduation thesis, but issues and pull requests are welcome.

## Development setup

```bash
git clone --recurse-submodules https://github.com/minhnhat1206/streamhouse-violence-detection.git
cd streamhouse-violence-detection
cp docker/.env.example docker/.env          # never commit docker/.env
docker network create violence-detection-net
docker compose -f docker/docker-compose.yml --profile streaming --profile gateway up -d
```

## Conventions

- **Commits:** [Conventional Commits](https://www.conventionalcommits.org/) — `feat:`, `fix:`, `docs:`, `refactor:`, `chore:`, `perf:`.
- **Python:** `snake_case`, type hints on public functions, one-line docstrings.
- **Kafka topics:** `kebab-case` (e.g. `urban-safety-alerts`); messages must satisfy the [data contract](docs/data-contracts.md).
- **SQL / Flink tables:** `snake_case`, no layer prefix. Quote reserved words with backticks (`` `timestamp` ``, `` `year` ``).
- **Flink:** keep checkpointing on (30 s) — Paimon only commits on checkpoints.
- **Docker:** no hard-coded credentials (use `${VAR}` from `.env`); every service needs a `healthcheck` and `deploy.resources.limits` (memory **and** cpus); put optional services behind a profile.
- **Frontend:** functional components + hooks, Tailwind utilities, dark theme by default.

## Adding things

- **A Flink job:** add `scripts/transform/<job>.py`, register it in `STREAMING_JOBS` in `pipeline_manager.py`, and document any change to the data flow in `docs/architecture.md`.
- **A chatbot capability:** update schema metadata in `scripts/chatbot/components/schema_registry.py` and layer routing in `components/trino_client.py`; check the boundaries (45 min → HOT, 2 h → WARM, 8 d → COLD).

## Secrets

Never commit API keys or passwords. If a key leaks, revoke it at the provider first, then rotate it in `docker/.env`.

## Pull requests

Describe the change and how you verified it (logs, `curl` output, or screenshots). Keep PRs focused.

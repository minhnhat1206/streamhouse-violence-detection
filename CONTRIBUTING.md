# Contributing Guide

## Development Setup

```bash
git clone --recurse-submodules https://github.com/minhnhat1206/realtime-violence-detection.git
cd realtime-violence-detection
cp docker/.env.example docker/.env
docker network create violence-detection-net
cd docker && docker compose up -d
```

## Code Conventions

### Python

- **Style**: snake_case for functions and variables, PascalCase for classes
- **Type hints**: required for all public function signatures
- **Docstrings**: one-line summary for public functions; skip for internal helpers
- **Imports**: stdlib → third-party → local, separated by blank lines

```python
def validate_event(event: dict) -> tuple[bool, list[str]]:
    """Validate a Kafka event against data contract rules."""
    violations = []
    ...
    return len(violations) == 0, violations
```

### Flink SQL / Python

- Table names: `snake_case` (e.g., `hot_violence_alerts`, `daily_incident_stats`)
- Watermark: `WATERMARK FOR timestamp AS timestamp - INTERVAL '5' SECOND`
- Reserved keywords: always quote with backticks (`` `year` ``, `` `month` ``, `` `day` ``)
- Exactly-once: always use checkpointing; checkpoint interval 30s for Paimon

### Docker

- No hardcoded credentials — use `${VAR_NAME}` from `.env`
- Every service needs `healthcheck` with `interval`, `timeout`, `retries`
- Every service needs `deploy.resources.limits` (memory + cpus)
- New services > 512MB RAM must reduce another service to compensate (16GB machine budget)

### JavaScript / React

- Functional components + hooks only (no class components)
- File naming: `PascalCase.jsx` for components
- Tailwind utility classes; avoid custom CSS
- Default to dark theme (security monitoring UI)

### SQL

- Table naming: `snake_case` with descriptive names (no layer prefix — layer is the catalog)
- Always use `LIMIT N` for Fluss HOT queries (not `COUNT(*)` — streaming aggregates return 0 for historical data)

## Commit Messages

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
feat:     new feature
fix:      bug fix
refactor: code change without behavior change
docs:     documentation only
chore:    build, cleanup, dependencies
test:     adding or updating tests
perf:     performance improvement
```

Examples:
```
feat(chatbot): add evidence image retrieval endpoint
fix(flink): quote reserved keywords in dim_time DDL
docs(readme): add troubleshooting section for Paimon queries
chore: remove outdated sink_to_fluss.py (superseded by sink_to_fluss_enriched.py)
```

## Branch Strategy

- `main` — stable, tested, thesis-ready
- `devNhat` — Nguyễn Ngọc Minh Nhật's work branch
- Feature branches: `feat/<short-description>` off `devNhat`

PR to `main` requires:
1. E2E test suite passes (target: 22+/23)
2. No hardcoded secrets
3. DEVELOPER_LOG.md updated

## Adding New Flink Jobs

1. Create `scripts/transform/your_job.py`
2. Add to `STREAMING_JOBS` dict in `pipeline_manager.py` (if it should run continuously)
3. Add resource limits in `docker/docker-compose.yml` if it needs a dedicated service
4. Update `docs/agent-guides/architecture.md` if it changes data flow

## Adding New Chatbot Capabilities

1. Add schema metadata to `scripts/chatbot/ingest.py`
2. Re-run ingestion: `docker exec chatbot python /app/ingest.py`
3. Update layer routing logic in `scripts/chatbot/trino_client.py` if needed
4. Test with time-boundary queries (45min → HOT, 2h → WARM, 8d → COLD)

## Secrets Management

- **Never** commit secrets to git — use `docker/.env` (gitignored)
- **Never** log API keys, even in debug mode
- If a key is exposed: revoke immediately at the provider, generate new, update `.env` only
- Template: `docker/.env.example` — contains variable names with placeholder values only

## Testing

### E2E tests

```bash
# Full 23-test suite
bash run-e2e-tests.sh

# Critical path only (faster)
bash test-critical.sh
```

### Manual chatbot tests

```bash
# HOT routing (<1h)
curl -X POST http://localhost:5002/api/chat \
  -d '{"query": "Trong 45 phút qua có bao nhiêu vụ?"}'

# WARM routing (1h-7d)
curl -X POST http://localhost:5002/api/chat \
  -d '{"query": "Trong 2 giờ qua có bao nhiêu vụ?"}'

# Evidence NOT triggered by "cảnh báo" keyword
curl -X POST http://localhost:5002/api/chat \
  -d '{"query": "Cảnh báo nào được phát trong 30 phút qua?"}'
```

### Flink job verification

```bash
# All 3 jobs must be running
curl http://localhost:8081/jobs | python -m json.tool | grep '"status"'

# Check job metrics
curl http://localhost:8081/jobs/overview
```

## Resource Budget (16GB machine)

Core services use ~9.6GB RAM. Before adding a new service:

```
Current budget: ~9.6GB core + up to 3.6GB for optional profiles
Hard limit: 12GB total (leave 4GB for OS + Docker overhead)
```

If your service needs >512MB:
1. Check `resource-limits.md` for current allocations
2. Reduce an existing non-critical service
3. Consider putting it in an optional profile

## Documentation

- Keep `DEVELOPER_LOG.md` updated at end of each session (state, bugs fixed, next steps)
- Architecture changes → update `docs/agent-guides/architecture.md`
- New endpoints → update README.md API table
- Performance data → update E2E test reports in `docs/`

## Questions?

- Check `docs/agent-guides/` for detailed architecture documentation
- Check `DEVELOPER_LOG.md` for history and known issues
- Check `docs/PROJECT_CONTEXT.md` for current system state

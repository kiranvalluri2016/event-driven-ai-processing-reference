# Event-Driven AI Processing Reference

Local-only reference POC for durable SQLite event processing. It demonstrates at-least-once delivery, tenant-scoped idempotent results, sequential bounded worker execution, retry/backoff, dead-letter events, controlled replay, correlation IDs, and audit records. The processor is deterministic and synthetic; no LLM, cloud service, or GitHub Actions workflow is used.

```bash
uv sync --group dev
uv run event-processing enqueue tenant-a evt-1 --ticket T-1
uv run event-processing work
uv run event-processing inspect tenant-a
```

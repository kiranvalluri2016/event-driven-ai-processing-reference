# Architecture

`SQLite queue → bounded async worker → deterministic processor → idempotent result/audit`; failures are scheduled with bounded retry and enter tenant-scoped DLQ. Replay is explicit and restart-safe.

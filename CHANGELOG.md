# changelog

## [0.1.1] - 2026-10-30

### fixed
- implement actual gcra burst tolerance eligibility condition in global rate limiter.
- automatic major resource identifier extraction and cross-guild fairness resolution for intercepted discord.py routes.

## [0.1.0] - 2026-10-20

### initial release
- core async `requestflow` engine with bounded queues and clean lifecycle (`start`, `close`).
- gcra rate-limit engine with idle-burst protection, default conservative 40 req/s, and 50 req/s hard ceiling.
- cross-guild fairness via deficit round-robin so one spammy guild never starves other guilds.
- interaction endpoint detection (bypasses pacing to preserve discord's 3-second deadline).
- non-blocking route tracking so cooling-down routes never block unrelated endpoints.
- bounded streaming metrics (p50/p95 latency and queue wait percentiles, zero telemetry).
- optional in-flight deduplication and lru ttl caching.
- mode a (managed `flow.submit`) and mode b (transparent `flow.attach(bot)`).

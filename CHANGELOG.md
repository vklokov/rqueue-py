# Changelog

## 0.3.1 - 2026-05-26

### Added
- Lifecycle hooks: `Server.on_startup` and `Server.on_shutdown` register async callbacks for server startup and shutdown
  - Usable as decorators or called directly with a callable argument
  - Startup hooks run after the Redis connection is verified, before the consumer starts
  - Shutdown hooks run after the consumer is stopped, before the Redis connection is closed
  - Exceptions in hooks are logged and do not crash the server

---

## 0.3.0 - 2026-05-21

### Added
- Retry mechanism for failed jobs
  - `Client.enqueue` accepts `retry_count` (default: `1`) and `backoff_coefficient` (default: `1.5`)
  - Both values are stored on the `Job` model and travel with the job through the queue
  - On failure, the consumer re-enqueues the job with an exponential backoff delay (`backoff_coefficient ** attempt` seconds) until retries are exhausted
  - Once all retries are exhausted the job is marked failed as before
  - Jobs log a warning on each retry attempt and an error only when permanently failed

---

## 0.2.8 - 2026-05-20

### Changed
- `Server.add_worker` now accepts a worker **instance** (`Performable`) — the same instance is reused across all jobs of that type
- `Client.enqueue` accepts a worker **class** (`type[Performable]`) — no instantiation cost at enqueue time, only the class name is needed to build the job
- Healthcheck rewritten with FastAPI + uvicorn, replacing the manual raw TCP/HTTP server
  - `GET /live` — always returns 200, used as a liveness probe
  - `GET /ready` — checks consumer heartbeat and Redis connectivity (`store.ping`); returns 503 with reason on failure
- Job IDs switched from `uuid4` (stdlib) to `uuid7` (`uuid_extensions`), providing time-sortable identifiers

### Fixed
- `config.queue` was logged as a bound method object; now called correctly as `config.queue()`

### Dependencies
- Added `fastapi>=0.136.1`
- Added `uvicorn>=0.34.0`

---

## 0.2.6 - 2026-05-20

### Changed
- All Redis operations extracted into a new internal `Store` class (`rqueue/store.py`)
- Redis key names (`rqueue:queue:*`, `rqueue:stats:*`) are now centralized in `Store`
- `Consumer` and `Client` no longer interact with Redis directly — all calls go through `Store`
- `asyncio.to_thread` wrapping for blocking Redis calls moved from `Consumer` into `Store`

---

## 0.2.5 - 2026-05-20

### Changed
- Workers are now registered on `Server` via `add_worker()` instead of being passed to `Config`
- `Server` lazily initializes the consumer on `start()`, decoupling construction from Redis connection
- `Consumer` receives workers directly rather than reading them from `Config`

### Added
- `Server.uptime()` returns a human-readable duration (e.g. `"1 day, 4 hours"`) via the `humanize` library
- `Status` response includes an `uptime` field
- `Server.start()` raises `RuntimeError` if no workers have been registered

### Fixed
- `Consumer.is_ok` converted from a method to a property, fixing incorrect truthy evaluation in `status()`

---

## 0.2.4 — 2026-05-19

### Changed
- Queue names are now raw identifiers (e.g. `"default"`, `"emails"`); the full Redis key is constructed internally as `rqueue:queue:{name}`
- Stats counters moved from a Redis hash to separate keys (`rqueue:stats:processed`, `rqueue:stats:failed`), incremented with `INCR`

---

## 0.2.3 — 2026-05-19

### Added
- `Client.stats()` returns a `Stats` model with cumulative `processed` and `failed` job counts
- Consumer increments `rqueue.stats.processed` / `rqueue.stats.failed` in Redis after each job completes or raises

---

## 0.2.2 — 2026-05-19

### Added
- `Client.pending()` returns a list of jobs currently waiting in the queue (non-destructive)
# Changelog

## 0.2.7 - 2026-05-20

### Fixed
- `config.queue` is now called as `config.queue()` when logging server startup, matching its callable interface

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
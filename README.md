# rqueue-py

A Redis-backed background job queue for Python.

## Usage

### Defining a worker

Implement the `Performable` protocol:

```python
class SendEmailWorker:
    async def perform(self, payload: dict) -> None:
        recipient = payload["to"]
        # ... send email
```

### Running the server

```python
import asyncio
from rqueue import Server, Config
from your_workers import SendEmailWorker

config = Config(
    redis_url="redis://localhost:6379",
    queue="default",               # optional, default: "default"
    concurrency=5,                 # optional, default: 1
    healthcheck_port=3055,         # optional, default: 3055
)

server = Server(config)
server.add_worker(SendEmailWorker()) # i.e. Performable instance

asyncio.run(server.start())
```

### Enqueueing jobs

```python
from rqueue import Client
from your_workers import SendEmailWorker

client = Client(
    redis_url="redis://localhost:6379",
    queue="default",  # optional, must match the server queue
)

jid = client.enqueue(SendEmailWorker, {"to": "user@example.com"})

# with custom retry settings
jid = client.enqueue(
    SendEmailWorker,
    {"to": "user@example.com"},
    retry_count=3,
    backoff_coefficient=2.0,
)
```

`enqueue` returns the job ID (`jid`) that can be used for tracing.

#### Retries

`retry_count` (default `1`) sets how many times a failed job is retried before being marked as permanently failed. `backoff_coefficient` (default `1.5`) controls the exponential delay between attempts:

| Attempt | Delay |
|---------|-------|
| 1st retry | `backoff_coefficient ** 1` seconds |
| 2nd retry | `backoff_coefficient ** 2` seconds |
| … | … |

With the defaults a job gets one retry after 1.5 seconds. Set `retry_count=0` to disable retries entirely.

Queue names are raw identifiers (e.g. `"default"`, `"emails"`). The client constructs the full Redis key internally as `rqueue:queue:{name}`.

### Inspecting the queue

```python
# Jobs waiting to be processed (non-destructive)
jobs = client.pending()

# Cumulative counters
stats = client.stats()
print(stats.processed, stats.failed)
```

### Custom logger

Pass any object implementing the `Loggable` protocol to `Config`:

```python
from rqueue import Loggable

class MyLogger:
    def info(self, msg: str, extra: dict | None = None): ...
    def error(self, msg: str, extra: dict | None = None): ...
    def debug(self, msg: str, extra: dict | None = None): ...
    def warning(self, msg: str, extra: dict | None = None): ...

config = Config(redis_url=..., logger=MyLogger())
```

### Healthcheck

The server exposes an HTTP healthcheck on the configured port (default `3055`):

```
GET http://localhost:3055
```

Response:

```json
{
  "ok": true,
  "last_ping": "2026-05-18 12:34:56 UTC",
  "concurrency": 5,
  "uptime": "1 day, 4 hours and 32 minutes"
}
```

Returns `200` when healthy, `500` when the Redis connection has been lost.
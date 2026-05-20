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
server.add_worker(SendEmailWorker)

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
```

`enqueue` returns the job ID (`jid`) that can be used for tracing.

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

from typing import cast, Optional

import uuid
from redis import Redis

from rqueue.schemas import Job, Stats, Performable
from rqueue.config import Config, _default_queue


class Client:
    def __init__(self, redis_url: str, queue: Optional[str] = None):
        self._redis = Redis.from_url(redis_url)
        self._queue = Config._rqueue_name(queue or _default_queue)

    def enqueue(self, worker: type[Performable], payload: dict) -> str:
        job = Job(
            jid=str(uuid.uuid4()),
            worker=worker.__name__,
            payload=payload,
        )

        self._redis.rpush(self._queue, job.model_dump_json())
        return job.jid

    def pending(self) -> list[Job]:
        """Returns all jobs waiting in the queue without consuming them."""
        raw_jobs = cast(list[bytes], self._redis.lrange(self._queue, 0, -1))
        return [Job.model_validate_json(raw) for raw in raw_jobs]

    def stats(self) -> Stats:
        """Returns cumulative processed and failed job counters."""
        processed = cast(bytes | None, self._redis.get("rqueue:stats:processed"))
        failed = cast(bytes | None, self._redis.get("rqueue:stats:failed"))
        return Stats(
            processed=int(processed) if processed else 0,
            failed=int(failed) if failed else 0,
        )

    def close(self):
        self._redis.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

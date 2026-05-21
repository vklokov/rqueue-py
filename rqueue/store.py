import asyncio
from typing import cast

from redis import Redis

from rqueue.schemas import Job, Stats


class Store:
    _QUEUE_PREFIX = "rqueue:queue:"
    _PROCESSED_KEY = "rqueue:stats:processed"
    _FAILED_KEY = "rqueue:stats:failed"

    @classmethod
    def queue_key(cls, name: str) -> str:
        return f"{cls._QUEUE_PREFIX}{name}"

    def __init__(self, redis_url: str, queue: str):
        self._redis = Redis.from_url(redis_url)
        self._queue = self.queue_key(queue)

    def ping(self) -> None:
        self._redis.ping()

    def push(self, job: Job) -> None:
        self._redis.rpush(self._queue, job.model_dump_json())

    def pending(self) -> list[Job]:
        raw_jobs = cast(list[bytes], self._redis.lrange(self._queue, 0, -1))
        return [Job.model_validate_json(raw) for raw in raw_jobs]

    def stats(self) -> Stats:
        processed = cast(bytes | None, self._redis.get(self._PROCESSED_KEY))
        failed = cast(bytes | None, self._redis.get(self._FAILED_KEY))
        return Stats(
            processed=int(processed) if processed else 0,
            failed=int(failed) if failed else 0,
        )

    def close(self) -> None:
        self._redis.close()

    async def pop(self, timeout: int) -> bytes | None:
        result = cast(
            tuple[str | bytes, str | bytes] | None,
            await asyncio.to_thread(self._redis.blpop, self._queue, timeout=timeout),
        )
        if result is None:
            return None
        _, raw = result
        return cast(bytes, raw)

    async def increment_processed(self) -> None:
        await asyncio.to_thread(self._redis.incr, self._PROCESSED_KEY)

    async def increment_failed(self) -> None:
        await asyncio.to_thread(self._redis.incr, self._FAILED_KEY)

    async def push_async(self, job: "Job") -> None:
        await asyncio.to_thread(self._redis.rpush, self._queue, job.model_dump_json())

    async def ping_async(self) -> None:
        await asyncio.to_thread(self._redis.ping)
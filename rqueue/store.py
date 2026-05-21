import asyncio
from typing import cast

from redis import Redis
from redis.exceptions import RedisError

from rqueue.schemas import Job, Stats


class StoreError(Exception):
    pass


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
        try:
            self._redis.ping()
        except RedisError as e:
            raise StoreError(str(e)) from e

    def push(self, job: Job) -> None:
        try:
            self._redis.rpush(self._queue, job.model_dump_json())
        except RedisError as e:
            raise StoreError(str(e)) from e

    def pending(self) -> list[Job]:
        try:
            raw_jobs = cast(list[bytes], self._redis.lrange(self._queue, 0, -1))
            return [Job.model_validate_json(raw) for raw in raw_jobs]
        except RedisError as e:
            raise StoreError(str(e)) from e

    def stats(self) -> Stats:
        try:
            processed = cast(bytes | None, self._redis.get(self._PROCESSED_KEY))
            failed = cast(bytes | None, self._redis.get(self._FAILED_KEY))
            return Stats(
                processed=int(processed) if processed else 0,
                failed=int(failed) if failed else 0,
            )
        except RedisError as e:
            raise StoreError(str(e)) from e

    def close(self) -> None:
        self._redis.close()

    async def pop(self, timeout: int) -> bytes | None:
        try:
            result = cast(
                tuple[str | bytes, str | bytes] | None,
                await asyncio.to_thread(self._redis.blpop, self._queue, timeout=timeout),
            )
        except RedisError as e:
            raise StoreError(str(e)) from e
        if result is None:
            return None
        _, raw = result
        return cast(bytes, raw)

    async def increment_processed(self) -> None:
        try:
            await asyncio.to_thread(self._redis.incr, self._PROCESSED_KEY)
        except RedisError as e:
            raise StoreError(str(e)) from e

    async def increment_failed(self) -> None:
        try:
            await asyncio.to_thread(self._redis.incr, self._FAILED_KEY)
        except RedisError as e:
            raise StoreError(str(e)) from e

    async def push_async(self, job: Job) -> None:
        try:
            await asyncio.to_thread(self._redis.rpush, self._queue, job.model_dump_json())
        except RedisError as e:
            raise StoreError(str(e)) from e

    async def ping_async(self) -> None:
        try:
            await asyncio.to_thread(self._redis.ping)
        except RedisError as e:
            raise StoreError(str(e)) from e
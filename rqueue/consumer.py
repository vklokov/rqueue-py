import asyncio
import json
from typing import cast
from datetime import datetime, timezone

from redis import Redis
from redis.exceptions import RedisError

from rqueue.schemas import Job, Performable
from rqueue.config import Config


class Consumer:
    def __init__(
        self,
        redis: Redis,
        config: Config,
        workers: dict[str, Performable],
    ):
        self._redis = redis
        self.config = config
        self.logger = config.logger
        self._semaphore = asyncio.Semaphore(config.concurrency)
        self._last_heartbeat = datetime.now(timezone.utc)
        self._workers = workers

    async def consume(self):
        while True:
            await self._semaphore.acquire()

            try:
                job = cast(
                    tuple[str | bytes, str | bytes] | None,
                    await asyncio.to_thread(
                        self._redis.blpop,
                        self.config.queue(),
                        timeout=self.config.redis_ping_timeout,
                    ),
                )
            except RedisError as err:
                self._semaphore.release()
                self.logger.error(
                    "[RQueueServer] redis error", extra={"error": str(err)}
                )

                await asyncio.sleep(self.config.redis_reconnect_delay)
                continue

            await self._ping()

            if not job:
                self._semaphore.release()
                continue

            _, raw = job
            try:
                payload = json.loads(raw)
                asyncio.create_task(self._run_job(payload))
            except json.JSONDecodeError as err:
                self._semaphore.release()
                self.logger.error(
                    "[RQueueServer] failed to parse payload",
                    extra={"payload": raw, "error": str(err)},
                )

    async def _run_job(self, envelop: dict):
        try:
            job = Job.model_validate(envelop)
            worker = self._workers.get(job.worker)
            if not worker:
                raise RuntimeError(f"worker not found: {job.worker}")

            self.logger.info(f"[RqueueServer] jid={job.jid} started")

            await worker.perform(job.payload)

            self.logger.info(f"[RqueueServer] jid={job.jid} done")
            await asyncio.to_thread(self._redis.incr, "rqueue:stats:processed")

        except Exception as err:
            self.logger.error("[RQueueServer] error", extra={"error": str(err)})
            await asyncio.to_thread(self._redis.incr, "rqueue:stats:failed")
        finally:
            self._semaphore.release()

    @property
    def is_ok(self) -> bool:
        elapsed = (datetime.now(timezone.utc) - self._last_heartbeat).total_seconds()
        return elapsed < self.config.redis_ping_timeout * 2

    @property
    def last_ping(self) -> str:
        return self._last_heartbeat.strftime("%Y-%m-%d %H:%M:%S UTC")

    async def _ping(self):
        try:
            await asyncio.to_thread(self._redis.ping)
            self._last_heartbeat = datetime.now(timezone.utc)
        except RedisError as e:
            self.logger.error(
                "[RQueueServer] redis ping failed", extra={"error": str(e)}
            )

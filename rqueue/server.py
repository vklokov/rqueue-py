import asyncio
import signal
from datetime import datetime, timezone, timedelta
from typing import Optional

import humanize
from redis.exceptions import RedisError

from rqueue.healthcheck import Healthchecker
from rqueue.config import Config
from rqueue.consumer import Consumer
from rqueue.store import Store
from rqueue.schemas import Status, Performable


class Server:
    def __init__(self, config: Config):
        self.config = config
        self.logger = config.logger
        self._store = Store(config.redis_url, config._queue)
        self._workers: dict[str, Performable] = {}
        self._started_at: Optional[datetime] = None
        self._consumer: Optional[Consumer] = None

    def add_worker(self, worker: Performable):
        self._workers[worker.__class__.__name__] = worker

    async def start(self):
        if self._workers_count == 0:
            raise RuntimeError(
                "No workers registered. Register them with add_worker() before starting the server."
            )

        try:
            self._store.ping()
        except RedisError as e:
            raise RuntimeError(f"Redis connection failed: {e}") from e

        self._consumer = Consumer(
            store=self._store,
            config=self.config,
            workers=self._workers,
        )

        self._started_at = datetime.now(timezone.utc)

        checker = Healthchecker(port=self.config.healthcheck_port, app=self, store=self._store)

        tasks: list[asyncio.Task] = [
            asyncio.create_task(checker.run(), name="healthcheck"),
            asyncio.create_task(self._consumer.consume(), name="consumer"),
        ]

        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()

        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop_event.set)

        self.logger.info(
            "[RQueueServer] server starting...",
            extra={
                "queue": self.config.queue(),
                "concurrency": self.config.concurrency,
            },
        )

        stop_task = asyncio.create_task(stop_event.wait(), name="stop")
        done, pending = await asyncio.wait(
            [*tasks, stop_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in done:
            if task is not stop_task and (exc := task.exception()):
                self.logger.error(
                    "[RQueueServer] task failed",
                    extra={"task": task.get_name(), "error": str(exc)},
                )

        self.logger.info("[RQueueServer] shutting down")

        for task in pending:
            task.cancel()

        await asyncio.gather(*pending, return_exceptions=True)

        self._store.close()

    def uptime(self) -> str:
        if not self._started_at:
            return ""

        seconds = int((datetime.now(timezone.utc) - self._started_at).total_seconds())
        return humanize.precisedelta(timedelta(seconds=seconds))

    def status(self) -> Status:
        if not self._consumer:
            return Status(
                ok=False,
                last_ping="",
                concurrency=self.config.concurrency,
                uptime="",
            )

        return Status(
            ok=self._consumer.is_ok,
            last_ping=self._consumer.last_ping,
            concurrency=self.config.concurrency,
            uptime=self.uptime(),
        )

    @property
    def _workers_count(self) -> int:
        return len(self._workers.keys())

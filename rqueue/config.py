from typing import Optional

from rqueue.schemas import Loggable
from rqueue.logger import Logger


_default_concurrency = 1
_default_queue = "default"
_default_redis_ping_timeout = 30
_default_redis_reconnect_delay = 5
_default_healthcheck_port = 3055


class Config:
    @classmethod
    def _rqueue_name(cls, name: str) -> str:
        return f"rqueue:queue:{name}"

    def __init__(
        self,
        redis_url: str,
        queue: Optional[str] = None,
        concurrency: Optional[int] = None,
        redis_ping_timeout: Optional[int] = None,
        redis_reconnect_delay: Optional[int] = None,
        logger: Optional[Loggable] = None,
        healthcheck_port: Optional[int] = None,
    ):
        self.redis_url = redis_url
        self._queue = queue or _default_queue
        self.concurrency = concurrency or _default_concurrency
        self.redis_ping_timeout = redis_ping_timeout or _default_redis_ping_timeout
        self.redis_reconnect_delay = (
            redis_reconnect_delay or _default_redis_reconnect_delay
        )
        self.logger = logger or Logger()
        self.healthcheck_port = healthcheck_port or _default_healthcheck_port

    def queue(self) -> str:
        return self.__class__._rqueue_name(self._queue)

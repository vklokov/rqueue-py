from typing import Optional

from uuid_extensions import uuid7str

from rqueue.schemas import Job, Stats, Performable
from rqueue.config import _default_queue
from rqueue.store import Store


class Client:
    def __init__(self, redis_url: str, queue: Optional[str] = None):
        self._store = Store(redis_url, queue or _default_queue)

    def enqueue(self, worker: Performable, payload: dict) -> str:
        job = Job(
            jid=uuid7str(),
            worker=worker.__class__.__name__,
            payload=payload,
        )
        self._store.push(job)
        return job.jid

    def pending(self) -> list[Job]:
        return self._store.pending()

    def stats(self) -> Stats:
        return self._store.stats()

    def close(self):
        self._store.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

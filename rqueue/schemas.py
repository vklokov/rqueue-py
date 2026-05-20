from typing import Optional, Protocol, runtime_checkable
from pydantic import BaseModel


@runtime_checkable
class Performable(Protocol):
    async def perform(self, payload: dict) -> None: ...


@runtime_checkable
class Loggable(Protocol):
    def info(self, msg: str, extra: Optional[dict] = None): ...
    def error(self, msg: str, extra: Optional[dict] = None): ...
    def debug(self, msg: str, extra: Optional[dict] = None): ...
    def warning(self, msg: str, extra: Optional[dict] = None): ...


class Status(BaseModel):
    ok: bool
    last_ping: str
    concurrency: int
    uptime: str


class Observable(Protocol):
    def status(self) -> Status: ...


class Job(BaseModel):
    jid: str
    worker: str
    payload: dict


class Stats(BaseModel):
    processed: int = 0
    failed: int = 0

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock

from redis.exceptions import RedisError

from rqueue.consumer import Consumer
from rqueue.config import Config


@pytest.fixture
def mock_config():
    config = MagicMock(spec=Config)
    config.concurrency = 1
    config.queue = "rqueue:default"
    config.redis_ping_timeout = 30
    config.redis_reconnect_delay = 5
    config.logger = MagicMock()
    return config


@pytest.fixture
def mock_redis():
    return MagicMock()


@pytest.fixture
def consumer(mock_redis, mock_config):
    return Consumer(redis=mock_redis, config=mock_config, workers={})


# --- is_ok ---


def test_is_ok_returns_true_when_heartbeat_is_fresh(consumer):
    assert consumer.is_ok is True


def test_is_ok_returns_false_when_heartbeat_is_stale(consumer, mock_config):
    stale = datetime.now(timezone.utc) - timedelta(
        seconds=mock_config.redis_ping_timeout * 2 + 1
    )
    consumer._last_heartbeat = stale
    assert consumer.is_ok is False


# --- last_ping ---


def test_last_ping_format(consumer):
    consumer._last_heartbeat = datetime(2026, 5, 18, 12, 34, 56, tzinfo=timezone.utc)
    assert consumer.last_ping == "2026-05-18 12:34:56 UTC"


# --- _ping ---


async def test_ping_updates_heartbeat_on_success(consumer, mock_redis):
    before = consumer._last_heartbeat
    mock_redis.ping = MagicMock(return_value=True)
    await consumer._ping()
    assert consumer._last_heartbeat > before


async def test_ping_logs_error_and_keeps_heartbeat_on_redis_error(consumer, mock_redis):
    before = consumer._last_heartbeat
    mock_redis.ping = MagicMock(side_effect=RedisError("connection refused"))
    await consumer._ping()
    assert consumer._last_heartbeat == before
    consumer.logger.error.assert_called_once()


# --- _run_job ---


async def test_run_job_calls_worker_perform(consumer):
    worker = MagicMock()
    worker.perform = AsyncMock()
    consumer._workers = {"MyWorker": worker}

    await consumer._semaphore.acquire()
    await consumer._run_job({"jid": "abc", "worker": "MyWorker", "payload": {"x": 1}})

    worker.perform.assert_awaited_once_with({"x": 1})


async def test_run_job_logs_start_and_done(consumer):
    worker = MagicMock()
    worker.perform = AsyncMock()
    consumer._workers = {"MyWorker": worker}

    await consumer._semaphore.acquire()
    await consumer._run_job({"jid": "abc", "worker": "MyWorker", "payload": {}})

    assert consumer.logger.info.call_count == 2


async def test_run_job_logs_error_when_worker_not_found(consumer):
    await consumer._semaphore.acquire()
    await consumer._run_job({"jid": "abc", "worker": "Missing", "payload": {}})

    consumer.logger.error.assert_called_once()


async def test_run_job_logs_error_when_perform_raises(consumer):
    worker = MagicMock()
    worker.perform = AsyncMock(side_effect=RuntimeError("boom"))
    consumer._workers = {"MyWorker": worker}

    await consumer._semaphore.acquire()
    await consumer._run_job({"jid": "abc", "worker": "MyWorker", "payload": {}})

    consumer.logger.error.assert_called_once()


async def test_run_job_releases_semaphore_on_success(consumer):
    worker = MagicMock()
    worker.perform = AsyncMock()
    consumer._workers = {"MyWorker": worker}

    await consumer._semaphore.acquire()
    assert consumer._semaphore._value == 0
    await consumer._run_job({"jid": "abc", "worker": "MyWorker", "payload": {}})
    assert consumer._semaphore._value == 1


async def test_run_job_releases_semaphore_on_error(consumer):
    await consumer._semaphore.acquire()
    assert consumer._semaphore._value == 0
    await consumer._run_job({"jid": "abc", "worker": "Missing", "payload": {}})
    assert consumer._semaphore._value == 1

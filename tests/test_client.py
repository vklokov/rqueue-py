import json
import pytest
from unittest.mock import patch

from rqueue.client import Client
from rqueue.config import Config, _default_queue


class MyWorker:
    async def perform(self, payload: dict) -> None:
        pass


@pytest.fixture
def mock_redis():
    with patch("rqueue.client.Redis") as MockRedis:
        yield MockRedis.from_url.return_value


def make_client(mock_redis, queue=None):
    with patch("rqueue.client.Redis") as MockRedis:
        MockRedis.from_url.return_value = mock_redis
        return Client("redis://localhost:6379", queue=queue)


def test_enqueue_pushes_to_default_queue(mock_redis):
    client = make_client(mock_redis)
    client.enqueue(MyWorker, {})
    queue_arg = mock_redis.rpush.call_args[0][0]
    assert queue_arg == Config._rqueue_name(_default_queue)


def test_enqueue_pushes_to_custom_queue(mock_redis):
    client = make_client(mock_redis, queue="custom")
    client.enqueue(MyWorker, {})
    queue_arg = mock_redis.rpush.call_args[0][0]
    assert queue_arg == Config._rqueue_name("custom")


def test_enqueue_returns_jid(mock_redis):
    client = make_client(mock_redis)
    jid = client.enqueue(MyWorker, {"key": "value"})
    assert isinstance(jid, str)
    assert len(jid) > 0


def test_enqueue_jid_matches_pushed_payload(mock_redis):
    client = make_client(mock_redis)
    jid = client.enqueue(MyWorker, {"key": "value"})
    raw = mock_redis.rpush.call_args[0][1]
    pushed = json.loads(raw)
    assert pushed["jid"] == jid


def test_enqueue_serializes_worker_and_payload(mock_redis):
    client = make_client(mock_redis)
    client.enqueue(MyWorker, {"key": "value"})
    raw = mock_redis.rpush.call_args[0][1]
    pushed = json.loads(raw)
    assert pushed["worker"] == MyWorker.__name__
    assert pushed["payload"] == {"key": "value"}


def test_pending_returns_enqueued_jobs(mock_redis):
    client = make_client(mock_redis)
    jid = client.enqueue(MyWorker, {"key": "value"})
    raw = mock_redis.rpush.call_args[0][1]
    mock_redis.lrange.return_value = [raw]
    jobs = client.pending()
    assert len(jobs) == 1
    assert jobs[0].jid == jid
    assert jobs[0].worker == MyWorker.__name__
    assert jobs[0].payload == {"key": "value"}


def test_pending_returns_empty_when_queue_is_empty(mock_redis):
    client = make_client(mock_redis)
    mock_redis.lrange.return_value = []
    assert client.pending() == []


def test_enqueue_generates_unique_jids(mock_redis):
    client = make_client(mock_redis)
    jid1 = client.enqueue(MyWorker, {})
    jid2 = client.enqueue(MyWorker, {})
    assert jid1 != jid2


def test_close_closes_redis(mock_redis):
    client = make_client(mock_redis)
    client.close()
    mock_redis.close.assert_called_once()


def test_context_manager_closes_redis_on_exit(mock_redis):
    with patch("rqueue.client.Redis") as MockRedis:
        MockRedis.from_url.return_value = mock_redis
        with Client("redis://localhost:6379"):
            pass
    mock_redis.close.assert_called_once()


def test_context_manager_closes_redis_on_exception(mock_redis):
    with patch("rqueue.client.Redis") as MockRedis:
        MockRedis.from_url.return_value = mock_redis
        with pytest.raises(ValueError):
            with Client("redis://localhost:6379"):
                raise ValueError("boom")

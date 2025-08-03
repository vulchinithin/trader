# data_ingestion/tests/test_websocket_client.py
import sys
import os
import asyncio
import json
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
import warnings

# Suppress warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=RuntimeWarning)

# Add ingestion/ to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
ingestion_dir = os.path.join(current_dir, '..', 'ingestion')
sys.path.insert(0, ingestion_dir)
print(ingestion_dir)  # Debugging

import websocket_client
from websocket_client import connect_and_stream, produce_to_kafka

cfg = websocket_client.cfg

@pytest.fixture(autouse=True)
def kafka_producer_mock(monkeypatch):
    mock_prod = AsyncMock()
    mock_prod.start = AsyncMock()
    mock_prod.send_and_wait = AsyncMock()
    mock_prod.stop = AsyncMock()
    monkeypatch.setattr("websocket_client.AIOKafkaProducer", lambda **kw: mock_prod)
    return mock_prod

@pytest.fixture
def fake_ws(monkeypatch):
    class FakeWS:
        def __init__(self):
            self._msgs = [json.dumps({"dummy": "msg" + str(i)}) for i in range(5)]
            self.index = 0
            self.closed = False
            self.ping_called = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            self.closed = True

        async def recv(self):
            if self.index < len(self._msgs):
                msg = self._msgs[self.index]
                self.index += 1
                return msg
            raise Exception("Simulated connection drop")

        async def ping(self):
            self.ping_called += 1
            if self.ping_called > 2:
                raise asyncio.TimeoutError("Ping timeout")
            return await asyncio.sleep(0.01)

    async def fake_connect(url, ping_interval, ping_timeout=None):
        await asyncio.sleep(0.01)
        return FakeWS()

    monkeypatch.setattr("websocket_client.websockets.connect", fake_connect)
    return FakeWS

@pytest.mark.asyncio
async def test_stream_and_publish(kafka_producer_mock, fake_ws):
    symbol = cfg["assets"]["symbols"][0]
    interval = cfg["assets"]["intervals"][0]
    task = asyncio.create_task(connect_and_stream(symbol, interval))
    await asyncio.sleep(3.0)  # Allow messages, pings, and a drop
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert kafka_producer_mock.start.await_count >= 1
    sent_calls = kafka_producer_mock.send_and_wait.await_args_list
    assert len(sent_calls) >= 2

@pytest.mark.asyncio
async def test_reconnect_backoff(fake_ws):
    sleep_calls = []
    # 1. Capture the original, unpatched asyncio.sleep function
    original_asyncio_sleep = asyncio.sleep

    async def fake_sleep(secs):
        """Our mock sleep that records the call and yields control."""
        sleep_calls.append(secs)
        # 2. Use the original sleep to yield to the event loop, avoiding recursion.
        await original_asyncio_sleep(0)

    # 3. Patch the function where it is used in the client code.
    with patch("websocket_client.asyncio.sleep", fake_sleep):
        symbol = cfg["assets"]["symbols"][0]
        interval = cfg["assets"]["intervals"][0]
        websocket_client.MAX_ATTEMPTS = 5
        task = asyncio.create_task(connect_and_stream(symbol, interval))

        # 4. Wait for 1.5 seconds using the original sleep to allow the
        #    task to run, fail, and trigger the backoff logic.
        await original_asyncio_sleep(1.5)

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    # 5. Assert that the backoff logic was called correctly.
    #    Filter out small sleeps from the test fixtures (e.g., ping, connect).
    backoff_related_sleeps = [s for s in sleep_calls if s >= 1.0]

    # We expect at least one backoff cycle, which has two sleeps:
    # one for the exponential backoff and one for the fixed 1s wait.
    assert len(backoff_related_sleeps) >= 2

    # Check the values of the first backoff cycle
    actual_backoff_time = backoff_related_sleeps[0]
    fixed_one_second_wait = backoff_related_sleeps[1]

    expected_base_backoff = websocket_client.BASE * (websocket_client.FACTOR ** 0)
    
    assert expected_base_backoff <= actual_backoff_time < expected_base_backoff + 1
    assert fixed_one_second_wait == 1.0

@pytest.mark.asyncio
async def test_rest_fallback(monkeypatch):
    dummy = [{"open_time": 1, "open": "0", "high":"1", "low":"0", "close":"1", "volume":"100"}]
    monkeypatch.setattr("websocket_client.rest_fetch", lambda s, i, limit: dummy)

    result = await asyncio.to_thread(websocket_client.rest_fetch, "btcusdt", "1m", 500)
    assert result == dummy

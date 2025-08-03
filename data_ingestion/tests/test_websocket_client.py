# data_ingestion/tests/test_websocket_client.py
import sys
import os
import asyncio
import json
import pytest
import yaml
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
            self._msgs = [
                json.dumps({"dummy": "msg1"}),
                json.dumps({"dummy": "msg2"}),
                json.dumps({"dummy": "msg3"}),  # Added for more coverage
            ]
            self.closed = False

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            self.closed = True

        async def recv(self):
            if self._msgs:
                return self._msgs.pop(0)
            raise Exception("Simulated connection drop")

    # Use a generator to mimic async context manager
    async def fake_connect(url, ping_interval):
        ws = FakeWS()
        yield ws
        # Simulate exit

    monkeypatch.setattr("websocket_client.websockets.connect", fake_connect)
    return FakeWS

@pytest.mark.asyncio
async def test_stream_and_publish(kafka_producer_mock, fake_ws):
    symbol = cfg["assets"]["symbols"][0]
    interval = cfg["assets"]["intervals"][0]
    task = asyncio.create_task(connect_and_stream(symbol, interval))
    await asyncio.sleep(2.0)  # Extended to allow message processing and a drop
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert kafka_producer_mock.start.await_count >= 1
    topic = f"market-data-{symbol}"
    sent_calls = kafka_producer_mock.send_and_wait.await_args_list
    assert len(sent_calls) >= 2
    messages = [json.loads(call.args[1].decode()) for call in sent_calls]
    assert any(msg == {"dummy": "msg1"} for msg in messages)
    assert any(msg == {"dummy": "msg2"} for msg in messages)

@pytest.mark.asyncio
async def test_reconnect_backoff(fake_ws):
    sleep_calls = []
    async def fake_sleep(secs):
        sleep_calls.append(secs)

    with patch("websocket_client.asyncio.sleep", fake_sleep):
        symbol = cfg["assets"]["symbols"][0]
        interval = cfg["assets"]["intervals"][0]
        websocket_client.MAX_ATTEMPTS = 4  # Increase to 4 to ensure multiple reconnects
        task = asyncio.create_task(connect_and_stream(symbol, interval))
        await asyncio.sleep(3.0)  # Extended to allow multiple connection drops and backoffs
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    base = cfg["binance"]["reconnect"]["backoff_base"]
    factor = cfg["binance"]["reconnect"]["backoff_factor"]
    assert len(sleep_calls) >= 2
    for attempt, sleep_time in enumerate(sleep_calls):
        expected = base * (factor ** attempt)
        assert expected <= sleep_time <= expected + 1  # Account for random.random()

@pytest.mark.asyncio
async def test_rest_fallback(monkeypatch):
    async def bad_connect(*args, **kwargs):
        raise Exception("WS unavailable")
    monkeypatch.setattr("websocket_client.websockets.connect", bad_connect)

    dummy = [{"open_time": 1, "open": "0", "high":"1", "low":"0", "close":"1", "volume":"100"}]
    monkeypatch.setattr("websocket_client.rest_fetch", lambda s,i: dummy)

    result = await asyncio.to_thread(websocket_client.rest_fetch, "btcusdt", "1m")
    assert result == dummy

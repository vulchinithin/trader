import asyncio
import json
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
import websockets
from aiokafka import AIOKafkaProducer # Import AIOKafkaProducer

# It's better to run pytest from the project root so that imports work naturally.
from data_ingestion.ingestion import websocket_client

# Mock configuration for all tests
MOCK_CONFIG = {
    "binance": {
        "instrument_urls": {
            "spot": "wss://spot.test.url",
            "futures_usdm": "wss://futures.test.url"
        },
        "streams": ["{symbol}@kline_{interval}"],
        "reconnect": {"max_attempts": 3, "backoff_base": 0.1, "backoff_factor": 2},
        "ping_interval": 5,
        "rate_limit_backoff": 10
    },
    "selection": {
        "instruments": ["spot", "futures_usdm"],
        "symbols": ["BTCUSDT", "ETHUSDT"],
        "frequencies": ["1m"]
    },
    "kafka": {
        "bootstrap_servers": "mock:9092",
        "num_partitions": 1,
        "replication_factor": 1
    }
}

@pytest.fixture(autouse=True)
def mock_settings(monkeypatch):
    """Mock the configuration loader for all tests."""
    monkeypatch.setattr(websocket_client, "cfg", MOCK_CONFIG)
    # Also patch the constants that are set at module level
    monkeypatch.setattr(websocket_client, "MAX_ATTEMPTS", MOCK_CONFIG["binance"]["reconnect"]["max_attempts"])
    monkeypatch.setattr(websocket_client, "BASE", MOCK_CONFIG["binance"]["reconnect"]["backoff_base"])
    monkeypatch.setattr(websocket_client, "FACTOR", MOCK_CONFIG["binance"]["reconnect"]["backoff_factor"])
    monkeypatch.setattr(websocket_client, "RATE_LIMIT_BACKOFF", MOCK_CONFIG["binance"]["rate_limit_backoff"])

@pytest.fixture
def mock_producer():
    """Provides a mock AIOKafkaProducer."""
    producer = AsyncMock(spec=AIOKafkaProducer) # Corrected spec
    producer.send_and_wait = AsyncMock()
    return producer

@pytest.fixture
def mock_websockets(monkeypatch):
    """Fixture to mock the websockets.connect call."""
    mock_ws = AsyncMock(spec=websockets.WebSocketClientProtocol)
    mock_ws.recv.side_effect = [json.dumps({"data": "test"}), asyncio.TimeoutError] # Send one message then timeout

    async def connect_mock(*args, **kwargs):
        return mock_ws

    # Use a context manager for the mock
    mock_connect = MagicMock()
    mock_connect.__aenter__.return_value = mock_ws
    monkeypatch.setattr(websockets, "connect", lambda *args, **kwargs: mock_connect)
    return mock_connect, mock_ws

@pytest.mark.asyncio
async def test_connect_and_stream_produces_to_kafka(mock_producer, mock_websockets):
    """Test that a successful connection produces a message to Kafka."""
    await websocket_client.connect_and_stream(mock_producer, "spot", "BTCUSDT", "1m")

    # Check that connect was called with the right URL
    mock_connect, _ = mock_websockets
    expected_url = "wss://spot.test.url/btcusdt@kline_1m"
    mock_connect.assert_called_with(expected_url, ping_interval=None, ping_timeout=None)

    # Check that a message was produced
    mock_producer.send_and_wait.assert_called_once()
    topic_arg = mock_producer.send_and_wait.call_args[0][0]
    message_arg = json.loads(mock_producer.send_and_wait.call_args[0][1].decode())

    assert topic_arg == "market-data-spot-btcusdt"
    assert message_arg["data"] == "test"
    assert message_arg["instrument_type"] == "spot"

@pytest.mark.asyncio
async def test_rate_limit_handling(mock_producer, mock_websockets):
    """Test that rate limit errors trigger a longer backoff."""
    mock_connect, mock_ws = mock_websockets

    # Simulate a rate limit error
    rate_limit_exc = websockets.exceptions.ConnectionClosed(code=1013, reason="Rate limited")
    mock_connect.side_effect = rate_limit_exc

    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        await websocket_client.connect_and_stream(mock_producer, "spot", "BTCUSDT", "1m")

        # Check that the long backoff was triggered
        mock_sleep.assert_any_call(MOCK_CONFIG["binance"]["rate_limit_backoff"])

@pytest.mark.asyncio
async def test_main_loop_creates_tasks(monkeypatch):
    """Test that the main function creates tasks for all configured streams."""
    mock_connect = AsyncMock()
    monkeypatch.setattr(websocket_client, "connect_and_stream", mock_connect)

    # Mock producer and its lifecycle
    mock_producer_instance = AsyncMock()
    mock_AIOKafkaProducer = MagicMock(return_value=mock_producer_instance)
    monkeypatch.setattr(websocket_client, "AIOKafkaProducer", mock_AIOKafkaProducer)

    # Mock ensure_topics
    monkeypatch.setattr(websocket_client, "ensure_topics", MagicMock())

    await websocket_client.main()

    num_instruments = len(MOCK_CONFIG["selection"]["instruments"])
    num_symbols = len(MOCK_CONFIG["selection"]["symbols"])
    num_freqs = len(MOCK_CONFIG["selection"]["frequencies"])
    expected_calls = num_instruments * num_symbols * num_freqs

    assert mock_connect.call_count == expected_calls
    mock_connect.assert_any_call(mock_producer_instance, "futures_usdm", "ETHUSDT", "1m")
    
    # Check producer lifecycle
    mock_producer_instance.start.assert_called_once()
    mock_producer_instance.stop.assert_called_once()

@pytest.mark.asyncio
async def test_rest_fallback_is_called(mock_producer, mock_websockets, monkeypatch):
    """Test that the REST fallback is called after max reconnect attempts."""
    mock_connect, _ = mock_websockets
    mock_connect.side_effect = Exception("Connection failed")

    mock_rest_fetch = AsyncMock(return_value=[{"data": "fallback"}])
    monkeypatch.setattr(websocket_client, "rest_fetch", mock_rest_fetch)

    with patch("asyncio.sleep", new_callable=AsyncMock):
        await websocket_client.connect_and_stream(mock_producer, "spot", "BTCUSDT", "1m")

    # Check that rest_fetch was called with the correct arguments
    mock_rest_fetch.assert_called_once()
    # aio.to_thread passes args as a list, so we check the args of the inner call
    call_args = mock_rest_fetch.call_args[0]
    assert call_args[1] == "spot"  # instrument_type
    assert call_args[2] == "BTCUSDT" # symbol

    # Check that the fallback data was produced to Kafka
    mock_producer.send_and_wait.assert_called_once_with(
        "market-data-spot-btcusdt",
        json.dumps({"data": "fallback", "instrument_type": "spot"}).encode()
    )

import asyncio
import json
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
import websockets
from aiokafka import AIOKafkaProducer
import redis

# It's better to run pytest from the project root so that imports work naturally.
from data_ingestion.ingestion import websocket_client

# Base mock configuration
MOCK_CONFIG = {
    "binance": {
        "instrument_urls": {"spot": "wss://spot.test.url", "futures_usdm": "wss://futures.test.url"},
        "streams": ["{symbol}@kline_{interval}"],
        "reconnect": {"max_attempts": 3, "backoff_base": 0.1, "backoff_factor": 2},
        "ping_interval": 5,
        "rate_limit_backoff": 10
    },
    "selection": {
        "instruments": ["spot"], "symbols": ["BTCUSDT"], "frequencies": ["1m"]
    },
    "kafka": {"bootstrap_servers": "mock:9092", "num_partitions": 1, "replication_factor": 1},
    "redis": {"host": "mockhost", "port": 6379, "db": 0},
    "messaging": {"broker": "kafka"} # Default broker
}

@pytest.fixture
def mock_config(monkeypatch):
    """Mocks the configuration for a test."""
    def _mock_config(broker_type):
        config = MOCK_CONFIG.copy()
        config["messaging"]["broker"] = broker_type
        monkeypatch.setattr(websocket_client, "cfg", config)
        monkeypatch.setattr(websocket_client, "MESSAGING_BROKER", broker_type)
        monkeypatch.setattr(websocket_client, "MAX_ATTEMPTS", config["binance"]["reconnect"]["max_attempts"])
        monkeypatch.setattr(websocket_client, "BASE", config["binance"]["reconnect"]["backoff_base"])
        monkeypatch.setattr(websocket_client, "FACTOR", config["binance"]["reconnect"]["backoff_factor"])
        monkeypatch.setattr(websocket_client, "RATE_LIMIT_BACKOFF", config["binance"]["rate_limit_backoff"])
        return config
    return _mock_config

@pytest.fixture
def mock_producer():
    """Provides a mock AIOKafkaProducer."""
    producer = AsyncMock(spec=AIOKafkaProducer)
    producer.send_and_wait = AsyncMock()
    return producer

@pytest.fixture
def mock_redis_client(monkeypatch):
    """Provides a mock Redis client."""
    mock_client = MagicMock(spec=redis.Redis)
    monkeypatch.setattr(websocket_client, "get_redis_client", lambda cfg: mock_client)
    mock_publish = MagicMock()
    monkeypatch.setattr(websocket_client, "publish_to_redis_stream", mock_publish)
    return mock_client, mock_publish

@pytest.fixture
def mock_websockets(monkeypatch):
    """Fixture to mock the websockets.connect call."""
    mock_ws = AsyncMock(spec=websockets.WebSocketClientProtocol)
    mock_ws.recv.side_effect = [json.dumps({"data": "test"}), asyncio.TimeoutError("Stop loop")]
    mock_connect = MagicMock()
    mock_connect.__aenter__.return_value = mock_ws
    monkeypatch.setattr(websockets, "connect", lambda *args, **kwargs: mock_connect)
    return mock_connect, mock_ws


@pytest.mark.parametrize("broker, kafka_called, redis_called", [
    ("kafka", 1, 0),
    ("redis", 0, 1),
    ("both", 1, 1),
])
@pytest.mark.asyncio
async def test_broker_publishing(broker, kafka_called, redis_called, mock_config, mock_producer, mock_redis_client, mock_websockets, monkeypatch):
    """Test that messages are published to the correct broker based on config."""
    mock_config(broker)
    mock_redis_cli, mock_redis_publish = mock_redis_client

    # Mock the rest_fetch to prevent real network calls and fix the TypeError
    monkeypatch.setattr(websocket_client, "rest_fetch", MagicMock(return_value=[]))

    with patch.object(websocket_client, 'produce_to_kafka', new_callable=AsyncMock) as mock_kafka_publish:
        await websocket_client.connect_and_stream(mock_producer, mock_redis_cli, "spot", "BTCUSDT", "1m")

        # Check that the correct underlying publish functions were called
        assert mock_kafka_publish.call_count == kafka_called
        assert mock_redis_publish.call_count == redis_called

        if kafka_called:
            mock_kafka_publish.assert_called_once()
        if redis_called:
            mock_redis_publish.assert_called_once()


@pytest.mark.asyncio
async def test_main_loop_initializes_correct_clients(monkeypatch):
    """Test that the main function initializes the correct clients based on config."""
    mock_connect = AsyncMock()
    monkeypatch.setattr(websocket_client, "connect_and_stream", mock_connect)

    mock_producer_instance = AsyncMock()
    mock_AIOKafkaProducer = MagicMock(return_value=mock_producer_instance)
    monkeypatch.setattr(websocket_client, "AIOKafkaProducer", mock_AIOKafkaProducer)

    mock_redis_instance = MagicMock()
    mock_get_redis_client = MagicMock(return_value=mock_redis_instance)
    monkeypatch.setattr(websocket_client, "get_redis_client", mock_get_redis_client)

    monkeypatch.setattr(websocket_client, "ensure_topics", MagicMock())

    # Test with 'both'
    monkeypatch.setattr(websocket_client, "MESSAGING_BROKER", "both")
    await websocket_client.main()
    mock_AIOKafkaProducer.assert_called_once()
    mock_get_redis_client.assert_called_once()
    mock_producer_instance.start.assert_called_once()
    mock_producer_instance.stop.assert_called_once()
    mock_redis_instance.close.assert_called_once()

    # Reset mocks and test with 'redis'
    mock_AIOKafkaProducer.reset_mock()
    mock_get_redis_client.reset_mock()
    monkeypatch.setattr(websocket_client, "MESSAGING_BROKER", "redis")
    await websocket_client.main()
    mock_AIOKafkaProducer.assert_not_called()
    mock_get_redis_client.assert_called_once()

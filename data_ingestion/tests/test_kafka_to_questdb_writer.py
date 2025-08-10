import pytest
from unittest.mock import MagicMock, patch, call, AsyncMock
import asyncio
import json
import time

from data_ingestion.ingestion import kafka_to_questdb_writer

# Sample Kafka message for testing
SAMPLE_KLINE_MESSAGE = {
    "e": "kline", "s": "BTCUSDT",
    "k": {"t": 1678886400000, "c": "50050.00", "v": "100.0"},
    "instrument_type": "spot"
}

@pytest.fixture
def mock_consumer(monkeypatch):
    """Mocks the AIOKafkaConsumer."""
    mock_consumer = AsyncMock()
    monkeypatch.setattr(
        "data_ingestion.ingestion.kafka_to_questdb_writer.AIOKafkaConsumer",
        lambda *args, **kwargs: mock_consumer
    )
    return mock_consumer

@pytest.fixture
def mock_redis(monkeypatch):
    """Mocks the Redis client."""
    mock_client = MagicMock()
    monkeypatch.setattr(
        "data_ingestion.ingestion.kafka_to_questdb_writer.get_redis_client",
        lambda config: mock_client
    )
    return mock_client

def test_cache_price_function_directly(mock_redis):
    """Directly test the Redis caching function."""
    mock_pipe = MagicMock()
    mock_redis.pipeline.return_value = mock_pipe

    kafka_to_questdb_writer.cache_price_to_redis(
        redis_client=mock_redis, symbol="BTCUSDT", price="50050.00", cache_size=1000
    )

    mock_redis.pipeline.assert_called_once()
    mock_pipe.lpush.assert_called_once_with("prices:BTCUSDT", "50050.00")
    mock_pipe.ltrim.assert_called_once_with("prices:BTCUSDT", 0, 999)
    mock_pipe.execute.assert_called_once()

@pytest.mark.asyncio
async def test_writer_caches_to_redis(mock_consumer, mock_redis):
    """Test that the writer correctly calls the Redis caching function."""
    mock_message = MagicMock()
    mock_message.value = json.dumps(SAMPLE_KLINE_MESSAGE).encode('utf-8')
    # The return_value of __aiter__ should be a simple iterable. AsyncMock handles the rest.
    mock_consumer.__aiter__.return_value = [mock_message]

    with patch("data_ingestion.ingestion.kafka_to_questdb_writer.send_to_questdb"):
        await kafka_to_questdb_writer.consume_and_write()

        mock_redis.pipeline.assert_called_once()
        pipe_mock = mock_redis.pipeline.return_value
        pipe_mock.lpush.assert_called_once_with("prices:BTCUSDT", "50050.00")

@pytest.mark.asyncio
async def test_writer_sends_to_questdb(mock_consumer, mock_redis):
    """Test that the writer correctly formats and sends data to QuestDB."""
    mock_message = MagicMock()
    mock_message.value = json.dumps(SAMPLE_KLINE_MESSAGE).encode('utf-8')
    mock_consumer.__aiter__.return_value = [mock_message]

    with patch("data_ingestion.ingestion.kafka_to_questdb_writer.send_to_questdb") as mock_send:
        await kafka_to_questdb_writer.consume_and_write()

        mock_send.assert_called_once()
        sent_lines = mock_send.call_args[0][0]

        kline = SAMPLE_KLINE_MESSAGE['k']
        ts = int(kline['t']) * 1_000_000
        expected_line = (
            f"market_data,symbol=BTCUSDT,instrument_type=spot "
            f"price={kline['c']},volume={kline['v']} {ts}\n"
        )
        assert sent_lines == expected_line

@pytest.mark.asyncio
async def test_batching_by_size(mock_consumer, mock_redis, monkeypatch):
    """Test that data is sent when the batch size is reached."""
    monkeypatch.setattr(kafka_to_questdb_writer, "BATCH_SIZE", 3)

    mock_message = MagicMock()
    mock_message.value = json.dumps(SAMPLE_KLINE_MESSAGE).encode('utf-8')
    # Provide 4 messages to the consumer
    mock_consumer.__aiter__.return_value = [mock_message] * 4

    with patch("data_ingestion.ingestion.kafka_to_questdb_writer.send_to_questdb") as mock_send:
        await kafka_to_questdb_writer.consume_and_write()

        assert mock_send.call_count == 2
        assert mock_send.call_args_list[0][0][0].count('\n') == 3
        assert mock_send.call_args_list[1][0][0].count('\n') == 1

import pytest
from unittest.mock import MagicMock, patch, call, AsyncMock
import asyncio
import json

from feature_engineering import feature_writer

# Sample enriched Kafka message
SAMPLE_FEATURE_MESSAGE = {
    "e": "kline", "s": "BTCUSDT",
    "k": {"t": 1678886400000, "c": "50050.00", "v": "100.0"},
    "instrument_type": "spot",
    "features": {
        "close": 50050.0,
        "RSI_14": 75.27,
        "MACD_12_26_9": 150.5,
        "MACDs_12_26_9": 140.2
    }
}

@pytest.fixture
def mock_consumer(monkeypatch):
    """Mocks the AIOKafkaConsumer for the feature writer."""
    mock_consumer = AsyncMock()
    monkeypatch.setattr(
        "feature_engineering.feature_writer.AIOKafkaConsumer",
        lambda *args, **kwargs: mock_consumer
    )
    return mock_consumer

@pytest.mark.asyncio
async def test_feature_writer_integration(mock_consumer):
    """Test the main loop of the feature writer."""
    mock_message = MagicMock()
    mock_message.value = json.dumps(SAMPLE_FEATURE_MESSAGE).encode('utf-8')
    # The return_value of __aiter__ must be a standard iterable.
    mock_consumer.__aiter__.return_value = [mock_message]

    with patch("feature_engineering.feature_writer.send_to_questdb") as mock_send:
        await feature_writer.consume_and_write_features()

        mock_send.assert_called_once()
        sent_lines = mock_send.call_args[0][0]

        ts = SAMPLE_FEATURE_MESSAGE['k']['t'] * 1_000_000
        features = SAMPLE_FEATURE_MESSAGE['features']

        expected_fields = (
            f"rsi={features['RSI_14']},"
            f"macd={features['MACD_12_26_9']},"
            f"macd_signal={features['MACDs_12_26_9']}"
        )
        expected_line = f"feature_data,symbol=BTCUSDT {expected_fields} {ts}\n"

        assert sent_lines == expected_line

        mock_consumer.start.assert_called_once()
        mock_consumer.stop.assert_called_once()

@pytest.mark.asyncio
async def test_writer_handles_missing_features(mock_consumer):
    """Test that the writer skips messages with no feature data."""
    message_no_features = SAMPLE_FEATURE_MESSAGE.copy()
    del message_no_features["features"]

    mock_message = MagicMock()
    mock_message.value = json.dumps(message_no_features).encode('utf-8')
    mock_consumer.__aiter__.return_value = [mock_message]

    with patch("feature_engineering.feature_writer.send_to_questdb") as mock_send:
        await feature_writer.consume_and_write_features()

        mock_send.assert_not_called()

import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock, AsyncMock
import sys
import os
import json

# --- Path Setup ---
if __package__ is None or __package__ == '':
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

# We need to import the app and the consumer instance we want to mock
from api_backend.main import app
from api_backend import kafka_consumer

# --- Test Setup ---
@pytest.fixture
def client():
    """Provides a TestClient for the FastAPI app."""
    return TestClient(app)

# --- Test Cases ---
def test_health_check(client):
    """Tests the /health endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

def test_websocket_endpoint(client):
    """Tests the /ws/market-data WebSocket endpoint."""

    # Create some mock messages
    mock_msg1 = json.dumps({"symbol": "BTCUSDT", "price": 50000}).encode('utf-8')
    mock_msg2 = json.dumps({"symbol": "ETHUSDT", "price": 4000}).encode('utf-8')

    # Mock the async generator from the Kafka consumer
    async def mock_message_generator():
        yield mock_msg1
        yield mock_msg2

    # We use patch to replace the kafka_consumer's message_generator in the main module
    with patch.object(kafka_consumer, 'message_generator', new=mock_message_generator):
        # The TestClient's websocket_connect is a context manager
        with client.websocket_connect("/ws/market-data") as websocket:
            # Receive the two messages we yielded from our mock generator
            data1 = websocket.receive_text()
            assert json.loads(data1) == {"symbol": "BTCUSDT", "price": 50000}

            data2 = websocket.receive_text()
            assert json.loads(data2) == {"symbol": "ETHUSDT", "price": 4000}

def test_websocket_disconnect(client):
    """Tests that the server handles client disconnection."""
    with patch.object(kafka_consumer, 'message_generator') as mock_gen:
        # Mock the generator to be an empty one for this test
        async def empty_generator():
            if False: # This construct makes it an async generator
                yield
        mock_gen.return_value = empty_generator()

        # Connect and immediately disconnect
        with client.websocket_connect("/ws/market-data") as websocket:
            # The connection manager should have 1 connection
            from api_backend.main import manager
            assert len(manager.active_connections) == 1

        # After the 'with' block, the client disconnects
        # The connection manager should now have 0 connections
        assert len(manager.active_connections) == 0

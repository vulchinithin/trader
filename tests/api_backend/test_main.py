import pytest
from fastapi.testclient import TestClient

# --- Test Setup ---
import sys
import os

# Add the project root to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Now we can import the app
from api_backend.main import app

@pytest.fixture(scope="module")
def client():
    """Create a TestClient instance for the FastAPI app."""
    # The TestClient will automatically handle the startup and shutdown events of the app.
    # However, this means it will try to connect to Kafka.
    # For a simple health check, this is okay, but it highlights a dependency issue.
    # Let's mock the kafka_consumer to prevent this.
    from unittest.mock import patch, AsyncMock

    # Mock the consumer's start and stop methods to do nothing
    mock_kafka = AsyncMock()
    mock_kafka.start.return_value = None
    mock_kafka.stop.return_value = None

    with patch('api_backend.main.kafka_consumer', mock_kafka):
        with TestClient(app) as c:
            yield c

@pytest.mark.asyncio
async def test_health_check(client):
    """Test the /health endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

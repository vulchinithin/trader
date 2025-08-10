import pytest
from fastapi.testclient import TestClient
import sys
import os

# --- Path Setup ---
if __package__ is None or __package__ == '':
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

from api_backend.main import app

# Create a TestClient instance for the app
client = TestClient(app)

def test_health_check():
    """
    Tests the /health endpoint to ensure it's working correctly.
    """
    response = client.get("/health")

    # Assert that the status code is 200 OK
    assert response.status_code == 200

    # Assert that the response body is as expected
    assert response.json() == {"status": "ok"}

def test_read_main_root_is_not_found():
    """
    Tests that the root path (/) returns a 404 Not Found error,
    as we haven't defined it. This is a good sanity check.
    """
    response = client.get("/")
    assert response.status_code == 404

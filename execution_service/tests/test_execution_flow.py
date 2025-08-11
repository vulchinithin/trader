import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock

# Add project root to path to allow importing from other services
import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from execution_service.main import app

@pytest.fixture
def client():
    """Create a TestClient instance for the FastAPI app."""
    # We use a context manager to ensure startup/shutdown events are run.
    with TestClient(app) as c:
        yield c

@patch('execution_service.main.get_exchange_client')
def test_execute_order_approved(mock_get_exchange, client):
    """
    Test the full execution flow when the risk manager approves the trade.
    """
    # --- Mock Setup ---
    # 1. Mock the exchange client
    mock_exchange = MagicMock()
    mock_get_exchange.return_value = mock_exchange
    app.state.exchange_client = mock_exchange # Ensure the app state is updated

    # 2. Mock the requests.post call to the risk management service
    mock_risk_response = MagicMock()
    mock_risk_response.status_code = 200
    mock_risk_response.json.return_value = {"approved": True, "position_size": 0.5}

    # 3. Mock the order creation function
    mock_order_result = {"id": "12345", "symbol": "BTC/USDT", "amount": 0.5}

    # --- Test Execution ---
    with patch('requests.post', return_value=mock_risk_response) as mock_post:
        with patch('execution_service.main.create_market_buy_order', return_value=mock_order_result) as mock_create_order:

            response = client.post("/execute-order", json={"symbol": "BTCUSDT", "side": "buy"})

            # --- Assertions ---
            assert response.status_code == 200
            assert response.json() == {"status": "success", "order_details": mock_order_result}

            # Verify that the risk manager was called
            mock_post.assert_called_once()

            # Verify that the exchange order function was called with the correct size
            mock_create_order.assert_called_once()
            call_args = mock_create_order.call_args[1]
            assert call_args['symbol'] == "BTCUSDT"
            assert call_args['amount'] == 0.5

@patch('execution_service.main.get_exchange_client')
def test_execute_order_rejected(mock_get_exchange, client):
    """
    Test the execution flow when the risk manager rejects the trade.
    """
    # --- Mock Setup ---
    mock_exchange = MagicMock()
    mock_get_exchange.return_value = mock_exchange
    app.state.exchange_client = mock_exchange

    mock_risk_response = MagicMock()
    mock_risk_response.status_code = 200
    mock_risk_response.json.return_value = {"approved": False}

    # --- Test Execution ---
    with patch('requests.post', return_value=mock_risk_response) as mock_post:
        with patch('execution_service.main.create_market_buy_order') as mock_create_order:

            response = client.post("/execute-order", json={"symbol": "BTCUSDT", "side": "buy"})

            # --- Assertions ---
            assert response.status_code == 400
            assert "Trade rejected by risk management" in response.json()['detail']

            # Verify that the order creation was NOT called
            mock_create_order.assert_not_called()

@patch('execution_service.main.get_exchange_client')
def test_risk_service_unavailable(mock_get_exchange, client):
    """
    Test the execution flow when the risk management service is down.
    """
    # --- Mock Setup ---
    mock_exchange = MagicMock()
    mock_get_exchange.return_value = mock_exchange
    app.state.exchange_client = mock_exchange

    # --- Test Execution ---
    with patch('requests.post', side_effect=requests.exceptions.ConnectionError) as mock_post:
        response = client.post("/execute-order", json={"symbol": "BTCUSDT", "side": "buy"})

        # --- Assertions ---
        assert response.status_code == 503
        assert "Risk management service is unavailable" in response.json()['detail']

import pytest
from unittest.mock import MagicMock, patch
import sys
import os
import logging

# --- Path Setup ---
if __package__ is None or __package__ == '':
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

from model_selection.selector import select_best_model

@pytest.fixture
def mock_mlflow_client(monkeypatch):
    """Mocks the MlflowClient."""
    mock_client = MagicMock()

    mv1 = MagicMock()
    mv1.name = "test_model"
    mv1.version = "1"

    mv2 = MagicMock()
    mv2.name = "test_model"
    mv2.version = "2"

    mock_client.search_model_versions.return_value = [mv1, mv2]
    monkeypatch.setattr("model_selection.selector.MlflowClient", lambda: mock_client)
    return mock_client

@pytest.fixture
def mock_backtester(monkeypatch):
    """Mocks the backtesting function."""
    mock_backtest = MagicMock()

    def side_effect(*args, **kwargs):
        model_version = kwargs.get('model_stage')
        if model_version == "1":
            return {"Sharpe Ratio": 1.5, "Total Return [%]": 25.0}
        elif model_version == "2":
            return {"Sharpe Ratio": 2.1, "Total Return [%]": 35.0}
        return None

    mock_backtest.side_effect = side_effect
    monkeypatch.setattr("model_selection.selector.run_model_based_backtest", mock_backtest)
    return mock_backtest

def test_select_best_model(mock_mlflow_client, mock_backtester, caplog):
    """
    Test the main model selection logic.
    """
    # Set the log level for the correct logger name
    with caplog.at_level(logging.INFO, logger="model_selector"):
        select_best_model(
            model_base_name="test_model",
            symbol="TEST_SYMBOL",
            start_date="2023-01-01",
            end_date="2023-01-31",
            metric="Sharpe Ratio"
        )

    # 1. Verify that the backtester was called for each model version
    assert mock_backtester.call_count == 2

    # 2. Verify that the correct champion was selected from the logs
    assert "Champion model is Version 2" in caplog.text
    assert "with a Sharpe Ratio of 2.1000" in caplog.text

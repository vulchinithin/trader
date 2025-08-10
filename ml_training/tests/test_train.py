import pytest
import polars as pl
from unittest.mock import patch, MagicMock
from datetime import datetime
import numpy as np
import argparse

# --- Path Setup ---
if __package__ is None or __package__ == '':
    import sys, os
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

from ml_training.train import prepare_regression_data, main as train_main

@pytest.fixture
def sample_training_df():
    """Provides a realistic DataFrame for testing."""
    return pl.DataFrame({
        "ts": pl.date_range(start=datetime(2023, 1, 1), end=datetime(2023, 1, 10), interval="1d", eager=True),
        "close": [100.0, 110.0, 121.0, 108.9, 119.79, 115.0, 112.0, 118.0, 120.0, 122.0],
        "volume": [10, 12, 11, 13, 12, 14, 15, 16, 17, 18],
        "rsi": [50.0, 60.0, 70.0, 40.0, 55.0, 52.0, 51.0, 58.0, 62.0, 65.0],
        "macd": [0.5, 0.6, 0.7, 0.4, 0.55, 0.52, 0.51, 0.58, 0.62, 0.65],
        "macd_signal": [0.4, 0.5, 0.6, 0.5, 0.52, 0.51, 0.50, 0.53, 0.55, 0.58]
    })

@pytest.fixture
def mock_mlflow(monkeypatch):
    """Mocks all MLflow calls."""
    mock_mlflow = MagicMock()
    monkeypatch.setattr("ml_training.train.mlflow", mock_mlflow)
    return mock_mlflow

def test_prepare_regression_data(sample_training_df):
    """Test the data preparation for regression."""
    df = prepare_regression_data(sample_training_df, target_col="close", horizon=2)
    assert len(df) == len(sample_training_df) - 2

def test_xgboost_single_run_mlflow(monkeypatch, sample_training_df, mock_mlflow):
    """Test the XGBoost single run flow with MLflow logging."""
    monkeypatch.setattr("ml_training.train.load_training_data", MagicMock(return_value=sample_training_df))

    mock_xgb = MagicMock()
    # After prepare_data(horizon=5), df has 5 rows. test_size=0.2 -> 1 test row.
    mock_xgb.predict.return_value = np.array([0.0])
    monkeypatch.setattr("ml_training.train.xgb.XGBRegressor", lambda **kwargs: mock_xgb)

    args = argparse.Namespace(
        symbol="TEST_XGB", model_type="xgboost_regressor", tune=False,
        start_date="2023-01-01", end_date="2023-01-31"
    )
    train_main(args)

    mock_mlflow.start_run.assert_called_once()
    mock_mlflow.log_params.assert_called_once()
    mock_mlflow.log_metric.assert_called_once()
    mock_mlflow.xgboost.log_model.assert_called_once()

def test_tuning_pipeline_mlflow(monkeypatch, sample_training_df, mock_mlflow):
    """Test that the tuning pipeline correctly uses the MLflow callback."""
    monkeypatch.setattr("ml_training.train.load_training_data", MagicMock(return_value=sample_training_df))

    mock_tuner_instance = MagicMock()
    mock_result = MagicMock()
    mock_result.metrics = {'mse': 0.01}
    mock_result.config = {'n_estimators': 100}
    mock_tuner_instance.fit.return_value.get_best_result.return_value = mock_result
    monkeypatch.setattr("ml_training.train.tune.Tuner", MagicMock(return_value=mock_tuner_instance))

    mock_callback = MagicMock()
    monkeypatch.setattr("ml_training.train.MLflowLoggerCallback", mock_callback)

    args = argparse.Namespace(
        symbol="TEST_TUNE", model_type="xgboost_regressor", tune=True,
        start_date="2023-01-01", end_date="2023-01-31"
    )
    train_main(args)

    mock_callback.assert_called_once()
    mock_mlflow.log_params.assert_called_once()
    mock_mlflow.log_metric.assert_called_once()

def test_anomaly_detector_mlflow(monkeypatch, sample_training_df, mock_mlflow):
    """Test the Isolation Forest training flow with MLflow logging."""
    monkeypatch.setattr("ml_training.train.load_training_data", MagicMock(return_value=sample_training_df))

    mock_iso_forest = MagicMock()
    mock_iso_forest.predict.return_value = np.ones(10, dtype=int)
    monkeypatch.setattr("ml_training.train.IsolationForest", lambda **kwargs: mock_iso_forest)

    args = argparse.Namespace(
        symbol="TEST_ISO", model_type="anomaly_detector", tune=False,
        start_date="2023-01-01", end_date="2023-01-31"
    )
    train_main(args)

    mock_mlflow.start_run.assert_called_once()
    mock_mlflow.log_params.assert_called_once()
    mock_mlflow.log_metric.assert_called_once_with("num_anomalies", 0)
    mock_mlflow.sklearn.log_model.assert_called_once()

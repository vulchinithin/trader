import pytest
import polars as pl
from polars.testing import assert_frame_equal
import sys
import os
from unittest.mock import patch, MagicMock
from datetime import datetime
import numpy as np
import argparse

# --- Path Setup ---
if __package__ is None or __package__ == '':
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

from ml_training.train import prepare_regression_data, main as train_main

@pytest.fixture
def sample_training_df():
    """Provides a realistic DataFrame as returned by the data loader."""
    return pl.DataFrame({
        "ts": pl.date_range(start=datetime(2023, 1, 1), end=datetime(2023, 1, 10), interval="1d", eager=True),
        "close": [100.0, 110.0, 121.0, 108.9, 119.79, 115.0, 112.0, 118.0, 120.0, 122.0],
        "volume": [10, 12, 11, 13, 12, 14, 15, 16, 17, 18],
        "rsi": [50.0, 60.0, 70.0, 40.0, 55.0, 52.0, 51.0, 58.0, 62.0, 65.0],
        "macd": [0.5, 0.6, 0.7, 0.4, 0.55, 0.52, 0.51, 0.58, 0.62, 0.65],
        "macd_signal": [0.4, 0.5, 0.6, 0.5, 0.52, 0.51, 0.50, 0.53, 0.55, 0.58]
    })

def test_prepare_regression_data(sample_training_df):
    """Test the data preparation for regression."""
    horizon = 2
    df = prepare_regression_data(sample_training_df, target_col="close", horizon=horizon)
    assert len(df) == len(sample_training_df) - horizon
    expected_val = (121.0 / 100.0) - 1
    assert abs(df["target"][0] - expected_val) < 1e-9

def test_xgboost_pipeline(monkeypatch, sample_training_df):
    """Test the XGBoost regressor training flow."""
    mock_loader = MagicMock(return_value=sample_training_df)
    monkeypatch.setattr("ml_training.train.load_training_data", mock_loader)

    mock_xgb = MagicMock()
    mock_xgb.predict.return_value = np.array([0.0])
    monkeypatch.setattr("ml_training.train.xgb.XGBRegressor", lambda **kwargs: mock_xgb)

    args = argparse.Namespace(
        symbol="TEST_XGB", start_date="2023-01-01", end_date="2023-01-31",
        model_type="xgboost_regressor"
    )

    train_main(args)

    mock_loader.assert_called_once()
    mock_xgb.fit.assert_called_once()
    mock_xgb.save_model.assert_called_once_with("TEST_XGB_xgboost_regressor.xgb")

def test_anomaly_detector_pipeline(monkeypatch, sample_training_df):
    """Test the Isolation Forest training flow."""
    mock_loader = MagicMock(return_value=sample_training_df)
    monkeypatch.setattr("ml_training.train.load_training_data", mock_loader)

    mock_iso_forest = MagicMock()
    # The input X will have 10 rows. `predict` should return an array of that length.
    mock_iso_forest.predict.return_value = np.ones(10, dtype=int)
    monkeypatch.setattr("ml_training.train.IsolationForest", lambda **kwargs: mock_iso_forest)

    mock_joblib_dump = MagicMock()
    monkeypatch.setattr("ml_training.train.joblib.dump", mock_joblib_dump)

    args = argparse.Namespace(
        symbol="TEST_ISO", start_date="2023-01-01", end_date="2023-01-31",
        model_type="anomaly_detector"
    )

    train_main(args)

    mock_loader.assert_called_once()
    mock_iso_forest.fit.assert_called_once()

    mock_joblib_dump.assert_called_once()
    saved_filename = mock_joblib_dump.call_args[0][1]
    assert saved_filename == "TEST_ISO_isolation_forest.joblib"

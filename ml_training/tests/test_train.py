import pytest
import polars as pl
from polars.testing import assert_frame_equal
import sys
import os
from unittest.mock import patch, MagicMock
from datetime import datetime
import numpy as np

# --- Path Setup ---
if __package__ is None or __package__ == '':
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

from ml_training.train import prepare_data, train_model

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

def test_prepare_data(sample_training_df):
    """Test the prepare_data function for creating the target variable."""
    horizon = 2
    df = prepare_data(sample_training_df, target_col="close", horizon=horizon)

    assert len(df) == len(sample_training_df) - horizon
    expected_val = (121.0 / 100.0) - 1
    assert abs(df["target"][0] - expected_val) < 1e-9

def test_train_model_pipeline(monkeypatch, sample_training_df):
    """
    Test the main train_model function by mocking the data loader and XGBoost model.
    """
    mock_loader = MagicMock(return_value=sample_training_df)
    monkeypatch.setattr("ml_training.train.load_training_data", mock_loader)

    mock_xgb = MagicMock()
    # The test set size will be 1 (20% of 5 rows from prepare_data), so predict should return 1 value.
    # Let's make it more robust by getting the length of the test set.
    # prepare_data drops 5 rows, leaving 5. test_size=0.2 -> test set is 1 row.
    # So predict should return an array of length 1.
    # Ah, the split is 80/20. 20% of 8 is 1.6, which rounds to 2. Let's recheck the split logic.
    # `train_test_split` on 5 rows with test_size=0.2 results in 4 train, 1 test.
    # Let's adjust the sample data to be cleaner. 10 rows -> 5 after prepare -> 4 train, 1 test.
    # OK, the previous error was `[1, 2]`. This means y_test was 1, predictions was 2.
    # My hardcoded `np.array([0.0, 0.0])` was the problem.
    # The split of 8 rows is 6 train, 2 test. So y_test is length 2.
    # My mock should have been correct. What did I miss?

    # Ah, `prepare_data` on 10 rows with horizon 5 leaves 5 rows.
    # `train_test_split` on 5 rows with test_size=0.2 gives 4 train and 1 test.
    # So `y_test` has length 1. My mock should return an array of length 1.
    mock_xgb.predict.return_value = np.array([0.0])
    monkeypatch.setattr("xgboost.XGBRegressor", lambda **kwargs: mock_xgb)

    train_model("TEST_SYMBOL", "2023-01-01", "2023-01-31")

    mock_loader.assert_called_once()
    mock_xgb.fit.assert_called_once()

    # Check that the model was saved
    mock_xgb.save_model.assert_called_once_with("TEST_SYMBOL_model.xgb")

import pytest
import polars as pl
from polars.testing import assert_frame_equal
import sys
import os

# --- Path Setup ---
# This allows the script to be run directly and find the train module
if __package__ is None or __package__ == '':
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

from ml_training.train import prepare_data

@pytest.fixture
def sample_feature_df():
    """Provides a sample DataFrame with a 'close' price column."""
    return pl.DataFrame({
        "close": [100.0, 110.0, 121.0, 108.9, 119.79],
        "rsi": [50.0, 60.0, 70.0, 40.0, 55.0]
    })

def test_prepare_data(sample_feature_df):
    """
    Test the prepare_data function for creating the target variable.
    """
    horizon = 2
    df = prepare_data(sample_feature_df, target_col="close", horizon=horizon)

    # 1. Check that the last `horizon` rows were dropped
    assert len(df) == len(sample_feature_df) - horizon

    # 2. Check the calculated target values
    # target = (close_price_in_2_steps / current_close_price) - 1

    # For the first row (close=100.0), target should be (121.0 / 100.0) - 1 = 0.21
    assert abs(df["target"][0] - 0.21) < 1e-9

    # For the second row (close=110.0), target should be (108.9 / 110.0) - 1 = -0.01
    assert abs(df["target"][1] - -0.01) < 1e-9

    # For the third row (close=121.0), target should be (119.79 / 121.0) - 1
    expected_val = (119.79 / 121.0) - 1
    assert abs(df["target"][2] - expected_val) < 1e-9

    # 3. Check that the original columns are still there
    assert "close" in df.columns
    assert "rsi" in df.columns

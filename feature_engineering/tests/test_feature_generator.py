import pytest
import polars as pl
from polars.testing import assert_frame_equal
import numpy as np
from unittest.mock import patch

# This allows the script to be run directly and find the feature_generator module
if __package__ is None or __package__ == '':
    import sys, os
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    from feature_generator import calculate_rsi, calculate_macd, calculate_features
else:
    from ..feature_generator import calculate_rsi, calculate_macd, calculate_features

@pytest.fixture
def sample_price_series():
    """Provides a sample Polars Series of prices."""
    return pl.Series("close", [10, 12, 15, 14, 13, 16, 18, 17, 19, 22, 25, 23, 24, 21, 20])

def test_calculate_rsi_properties():
    """Test the mathematical properties of the RSI calculation."""
    # 1. For a constantly increasing series, RSI should be 100
    increasing_series = pl.Series("close", range(30), dtype=pl.Float64)
    rsi_inc = calculate_rsi(increasing_series, length=14)
    # The non-null values should all be 100
    assert rsi_inc.drop_nulls().min() == 100.0
    assert rsi_inc.drop_nulls().max() == 100.0

    # 2. For a constantly decreasing series, RSI should be 0
    decreasing_series = pl.Series("close", range(30, 0, -1), dtype=pl.Float64)
    rsi_dec = calculate_rsi(decreasing_series, length=14)
    # The non-null values should all be 0
    assert rsi_dec.drop_nulls().min() == 0.0
    assert rsi_dec.drop_nulls().max() == 0.0

    # 3. For a mixed series, values should be between 0 and 100
    mixed_series = pl.Series("close", [44.34, 44.09, 44.15, 43.61, 44.33, 44.83, 45.10, 45.42, 45.84], dtype=pl.Float64)
    rsi_mix = calculate_rsi(mixed_series, length=3)
    assert rsi_mix.drop_nulls().min() >= 0.0
    assert rsi_mix.drop_nulls().max() <= 100.0

def test_calculate_macd(sample_price_series):
    """Test the manual MACD calculation."""
    macd_df = calculate_macd(sample_price_series.cast(pl.Float64), fast=5, slow=10, signal=3)

    assert f"MACD_5_10_3" in macd_df.columns
    assert f"MACDs_5_10_3" in macd_df.columns
    assert macd_df[f"MACD_5_10_3"].is_not_null().sum() > 0

def test_calculate_features_integration(sample_price_series):
    """Test the main calculate_features function."""
    df = pl.DataFrame(sample_price_series.cast(pl.Float64))

    with patch("feature_engineering.feature_generator.RSI_LENGTH", 14), \
         patch("feature_engineering.feature_generator.MACD_FAST", 5), \
         patch("feature_engineering.feature_generator.MACD_SLOW", 10), \
         patch("feature_engineering.feature_generator.MACD_SIGNAL", 3), \
         patch("feature_engineering.feature_generator.WINDOW_SIZE", 100):

        output_df = calculate_features(df)

        assert f"RSI_14" in output_df.columns
        assert f"MACD_5_10_3" in output_df.columns
        assert "close" in output_df.columns
        # Check that the RSI values are in the valid 0-100 range
        assert output_df[f"RSI_14"].drop_nulls().min() >= 0.0
        assert output_df[f"RSI_14"].drop_nulls().max() <= 100.0

def test_calculate_features_not_enough_data():
    """Test that it returns an unchanged frame if data is too short."""
    small_df = pl.DataFrame({"close": [10.0, 12.0, 15.0]})

    with patch("feature_engineering.feature_generator.MACD_SLOW", 10):
        output_df = calculate_features(small_df)

        assert_frame_equal(small_df, output_df)

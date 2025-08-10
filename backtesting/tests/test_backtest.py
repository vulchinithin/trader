import pytest
import pandas as pd
import vectorbt as vbt
import sys
import os

# This allows the script to be run directly and find the backtest module
if __package__ is None or __package__ == '':
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

@pytest.fixture
def sample_price_series():
    """
    Creates a pandas Series with a clear SMA crossover event.
    - Crossover should happen at index 5.
    """
    prices = pd.Series([10, 11, 12, 10, 9, 15, 16], name="close")
    return prices

def test_sma_crossover_signal_generation(sample_price_series):
    """
    Tests that the vectorbt functions for SMA crossover generate correct signals.
    """
    fast_window = 2
    slow_window = 3

    fast_ma = vbt.MA.run(sample_price_series, window=fast_window)
    slow_ma = vbt.MA.run(sample_price_series, window=slow_window)

    entries = fast_ma.ma_crossed_above(slow_ma)

    # --- Manual Calculation for Verification ---
    # Prices:    [10, 11, 12,   10,    9,   15,   16]
    # Fast MA(2): [NaN, 10.5, 11.5, 11.0, 9.5, 12.0, 15.5]
    # Slow MA(3): [NaN, NaN, 11.0, 11.0, 10.33, 11.33, 13.33]
    #
    # Entry check at i=5:
    # i-1=4: fast_ma[4] (9.5) <= slow_ma[4] (10.33) -> True
    # i=5:   fast_ma[5] (12.0) > slow_ma[5] (11.33) -> True
    # Crossover is at index 5.

    assert entries.iloc[5] == True
    assert entries.drop(5).any() == False

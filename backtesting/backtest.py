import logging
import polars as pl
import vectorbt as vbt
import sys
import os
import argparse

# --- Path Setup ---
if __package__ is None or __package__ == '':
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    from data_ingestion.config.config_loader import load_config
    from backtesting.data_loader import load_historical_data
else:
    from ..data_ingestion.config.config_loader import load_config
    from .data_loader import load_historical_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("backtester")

def run_sma_crossover_backtest(symbol: str, start_date: str, end_date: str, fast_window: int, slow_window: int):
    """
    Runs a backtest for a simple moving average crossover strategy.
    """
    logger.info(f"Running SMA Crossover backtest for {symbol}...")

    # 1. Load Data
    config = load_config()
    price_data = load_historical_data(config, symbol, start_date, end_date)

    if price_data.is_empty():
        logger.error("No historical data loaded. Aborting backtest.")
        return

    # vectorbt works best with pandas Series, so we'll convert the close prices
    close_prices = price_data.to_pandas().set_index('ts')['close']

    # 2. Define Strategy & Generate Signals
    logger.info(f"Generating signals for SMA crossover ({fast_window} / {slow_window})...")
    fast_ma = vbt.MA.run(close_prices, window=fast_window, short_name='fast')
    slow_ma = vbt.MA.run(close_prices, window=slow_window, short_name='slow')

    entries = fast_ma.ma_crossed_above(slow_ma)
    exits = fast_ma.ma_crossed_below(slow_ma)

    # 3. Run Backtest
    logger.info("Running portfolio simulation...")
    portfolio = vbt.Portfolio.from_signals(close_prices, entries, exits, init_cash=100000, freq='1D')

    # 4. Print Results
    stats = portfolio.stats()
    logger.info("--- Backtest Results ---")
    print(stats)

    # In a local environment, you could also plot the results:
    # portfolio.plot().show()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Vectorized Backtester")
    parser.add_argument("--symbol", type=str, default="BTCUSDT", help="Asset symbol to backtest")
    parser.add_argument("--start-date", type=str, default="2023-01-01", help="Start date for backtest data")
    parser.add_argument("--end-date", type=str, default="2023-12-31", help="End date for backtest data")
    parser.add_argument("--fast", type=int, default=10, help="Fast moving average window")
    parser.add_argument("--slow", type=int, default=30, help="Slow moving average window")

    args = parser.parse_args()

    # vectorbt works best with pandas, so we ensure the pandas string extension is available
    try:
        import pandas as pd
        pd.options.mode.string_storage = "pyarrow"
    except ImportError:
        pass

    run_sma_crossover_backtest(args.symbol, args.start_date, args.end_date, args.fast, args.slow)

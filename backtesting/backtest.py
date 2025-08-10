import logging
import polars as pl
import vectorbt as vbt
import sys
import os
import argparse
import pandas as pd

# --- Path Setup ---
if __package__ is None or __package__ == '':
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

from data_ingestion.config.config_loader import load_config
from backtesting.data_loader import load_historical_data # Corrected loader
from ml_training.data_loader import load_training_data # Needed for model-based
from model_selection.utils import load_model_from_registry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("backtester")

def run_sma_crossover_backtest(symbol: str, start_date: str, end_date: str, **kwargs):
    """Runs a backtest for a simple moving average crossover strategy."""
    logger.info(f"Running SMA Crossover backtest for {symbol}...")
    config = load_config()
    # Use the correct data loader for historical OHLCV data
    price_data = load_historical_data(config, symbol, start_date, end_date)
    if price_data.is_empty(): return

    close_prices = price_data.to_pandas().set_index('ts')['close']

    fast_window = kwargs.get('fast', 10)
    slow_window = kwargs.get('slow', 30)

    fast_ma = vbt.MA.run(close_prices, window=fast_window, short_name='fast')
    slow_ma = vbt.MA.run(close_prices, window=slow_window, short_name='slow')

    entries = fast_ma.ma_crossed_above(slow_ma)
    exits = fast_ma.ma_crossed_below(slow_ma)

    portfolio = vbt.Portfolio.from_signals(close_prices, entries, exits, init_cash=100000, freq='1D')
    return portfolio.stats()

def run_model_based_backtest(symbol: str, start_date: str, end_date: str, **kwargs):
    """Runs a backtest using signals from a trained ML model."""
    logger.info(f"Running Model-Based backtest for {symbol}...")

    model_name = kwargs.get('model_name')
    if not model_name:
        logger.error("`model_name` must be provided.")
        return

    model = load_model_from_registry(model_name, kwargs.get('model_stage', 'None'))
    if not model: return

    config = load_config()
    # The model was trained on feature data, so we load that for prediction
    data_df = load_training_data(config, symbol, start_date, end_date)
    if data_df.is_empty(): return

    feature_cols = [c for c in data_df.columns if c not in ["ts", "close", "volume"]]
    features = data_df[feature_cols].to_pandas()

    logger.info(f"Generating predictions with model {model_name}...")
    predictions = model.predict(features)
    predictions = pd.Series(predictions, index=data_df['ts'].to_pandas())

    entries = predictions > kwargs.get('entry_threshold', 0.01)
    exits = predictions < kwargs.get('exit_threshold', -0.01)

    close_prices = data_df.to_pandas().set_index('ts')['close']
    portfolio = vbt.Portfolio.from_signals(close_prices, entries, exits, init_cash=100000, freq='1D')
    return portfolio.stats()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Vectorized Backtester")
    parser.add_argument("--symbol", type=str, default="BTCUSDT")
    parser.add_argument("--start-date", type=str, default="2023-01-01")
    parser.add_argument("--end-date", type=str, default="2023-12-31")
    parser.add_argument("--strategy", type=str, default="sma_crossover", choices=['sma_crossover', 'model_based'])

    # Strategy-specific arguments
    parser.add_argument("--fast", type=int, default=10)
    parser.add_argument("--slow", type=int, default=30)
    parser.add_argument("--model-name", type=str, default="BTCUSDT_xgboost_regressor")
    parser.add_argument("--model-stage", type=str, default="None")

    args = parser.parse_args()

    stats = None
    if args.strategy == 'sma_crossover':
        stats = run_sma_crossover_backtest(
            args.symbol, args.start_date, args.end_date,
            fast=args.fast, slow=args.slow
        )
    elif args.strategy == 'model_based':
        stats = run_model_based_backtest(
            args.symbol, args.start_date, args.end_date,
            model_name=args.model_name, model_stage=args.model_stage
        )

    if stats is not None:
        logger.info(f"--- Backtest Results for '{args.strategy}' on '{args.symbol}' ---")
        print(stats)
    else:
        logger.error("Backtest failed to produce stats.")

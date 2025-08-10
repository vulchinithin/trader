import logging
import polars as pl
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import sys
import os

# --- Path Setup ---
if __package__ is None or __package__ == '':
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

from data_ingestion.config.config_loader import load_config
from ml_training.data_loader import load_feature_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("trainer")

def prepare_data(df: pl.DataFrame, target_col: str, horizon: int) -> pl.DataFrame:
    """
    Prepares the data for training by creating a target variable and shifting features.
    """
    df = df.with_columns(
        target=(pl.col(target_col).shift(-horizon) / pl.col(target_col) - 1)
    )
    df = df.drop_nulls()
    return df

def train_model(symbol: str, start_date: str, end_date: str):
    """
    Main training pipeline for a given symbol and date range.
    """
    logger.info(f"Starting training pipeline for {symbol}...")

    config = load_config()

    logger.warning("Using dummy data for training pipeline flow. Implement actual data loading.")
    import numpy as np
    import pandas as pd
    feature_df = pl.DataFrame({
        "ts": pd.to_datetime(np.arange(100), unit='D', origin='2023-01-01'),
        "close": 100 + np.random.randn(100).cumsum(),
        "rsi": np.random.rand(100) * 100,
        "macd": np.random.randn(100),
    })

    if feature_df.is_empty():
        logger.error("No data loaded. Aborting training.")
        return

    prepared_df = prepare_data(feature_df, target_col="close", horizon=5)

    if prepared_df.is_empty():
        logger.error("Not enough data to create features and target. Aborting.")
        return

    feature_cols = [col for col in prepared_df.columns if col not in ["ts", "target", "close"]]
    X = prepared_df[feature_cols].to_pandas()
    y = prepared_df["target"].to_pandas()

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

    logger.info(f"Training XGBoost model on {len(X_train)} samples...")
    model = xgb.XGBRegressor(
        objective='reg:squarederror', n_estimators=100, learning_rate=0.1,
        max_depth=3, random_state=42
    )

    model.fit(X_train, y_train)

    predictions = model.predict(X_test)
    mse = mean_squared_error(y_test, predictions)
    logger.info(f"Model evaluation complete. Test MSE: {mse:.6f}")

    model_filename = f"{symbol}_model.xgb"
    model.save_model(model_filename)
    logger.info(f"Model saved to {model_filename}")

if __name__ == '__main__':
    train_model("BTCUSDT", "2023-01-01", "2023-01-31")

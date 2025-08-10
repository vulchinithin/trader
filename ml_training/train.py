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
# Updated import to get the new data loading function
from ml_training.data_loader import load_training_data

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

    # 1. Load Data
    config = load_config()
    # Replace dummy data with a call to the enhanced data loader
    training_df = load_training_data(config, symbol, start_date, end_date)

    if training_df.is_empty():
        logger.error("No data loaded from QuestDB. Aborting training.")
        return

    # 2. Prepare Data
    # The 'close' column for creating the target is now in the loaded data
    prepared_df = prepare_data(training_df, target_col="close", horizon=5)

    if prepared_df.is_empty():
        logger.error("Not enough data to create features and target after preparation. Aborting.")
        return

    # 3. Train Model
    # Define feature columns (everything except timestamp, target, and the original price)
    feature_cols = [col for col in prepared_df.columns if col not in ["ts", "target", "close", "volume"]]
    X = prepared_df[feature_cols].to_pandas()
    y = prepared_df["target"].to_pandas()

    # Simple time-based split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False, random_state=42)

    logger.info(f"Training XGBoost model on {len(X_train)} samples...")
    model = xgb.XGBRegressor(
        objective='reg:squarederror', n_estimators=100, learning_rate=0.1,
        max_depth=3, random_state=42
    )

    model.fit(X_train, y_train)

    # 4. Evaluate Model
    predictions = model.predict(X_test)
    mse = mean_squared_error(y_test, predictions)
    logger.info(f"Model evaluation complete. Test MSE: {mse:.6f}")

    # 5. Save Model
    model_filename = f"{symbol}_model.xgb"
    model.save_model(model_filename)
    logger.info(f"Model saved to {model_filename}")

if __name__ == '__main__':
    # This example will now attempt to connect to QuestDB and load real data.
    # It will fail if the database is empty or unavailable.
    logger.info("Running example training pipeline...")
    logger.info("This requires the full data pipeline to be running and populating QuestDB.")
    train_model("BTCUSDT", "2023-01-01", "2023-01-31")

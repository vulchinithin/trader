import logging
import polars as pl
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.ensemble import IsolationForest # <--- ADD THIS IMPORT
import sys
import os
import argparse
import joblib

# --- Path Setup ---
if __package__ is None or __package__ == '':
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

from data_ingestion.config.config_loader import load_config
from ml_training.data_loader import load_training_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("trainer")

# --- Data Preparation ---
def prepare_regression_data(df: pl.DataFrame, target_col: str, horizon: int) -> pl.DataFrame:
    """Prepares data for a regression task."""
    df = df.with_columns(
        target=(pl.col(target_col).shift(-horizon) / pl.col(target_col) - 1)
    )
    df = df.drop_nulls()
    return df

# --- Model Training Functions ---
def train_xgboost_regressor(symbol: str, data: pl.DataFrame):
    """Trains, evaluates, and saves an XGBoost regressor model."""
    logger.info("--- Starting XGBoost Regressor Training ---")

    prepared_df = prepare_regression_data(data, target_col="close", horizon=5)
    if prepared_df.is_empty():
        logger.error("Not enough data for XGBoost training.")
        return

    feature_cols = [col for col in prepared_df.columns if col not in ["ts", "target", "close", "volume"]]
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
    logger.info(f"XGBoost evaluation complete. Test MSE: {mse:.6f}")

    model_filename = f"{symbol}_xgboost_regressor.xgb"
    model.save_model(model_filename)
    logger.info(f"Model saved to {model_filename}")

def train_anomaly_detector(symbol: str, data: pl.DataFrame):
    """Trains and saves an Isolation Forest anomaly detection model."""
    logger.info("--- Starting Isolation Forest Anomaly Detector Training ---")

    feature_cols = [col for col in data.columns if col not in ["ts", "close", "volume"]]

    data = data.drop_nulls(subset=feature_cols)

    if data.is_empty():
        logger.error("Not enough data for Isolation Forest training after dropping nulls.")
        return

    X = data[feature_cols].to_pandas()

    logger.info(f"Training Isolation Forest model on {len(X)} samples...")
    model = IsolationForest(n_estimators=100, contamination='auto', random_state=42)

    model.fit(X)

    anomalies = model.predict(X)
    num_anomalies = (anomalies == -1).sum()
    logger.info(f"Isolation Forest training complete. Found {num_anomalies} anomalies in the training data.")

    model_filename = f"{symbol}_isolation_forest.joblib"
    joblib.dump(model, model_filename)
    logger.info(f"Model saved to {model_filename}")

# --- Main Training Orchestrator ---
def main(args):
    """
    Main training pipeline orchestrator.
    """
    logger.info(f"Starting training pipeline for {args.symbol} with model type '{args.model_type}'...")

    config = load_config()
    training_df = load_training_data(config, args.symbol, args.start_date, args.end_date)

    if training_df.is_empty():
        logger.error("No data loaded from QuestDB. Aborting training.")
        return

    if args.model_type == 'xgboost_regressor':
        train_xgboost_regressor(args.symbol, training_df)
    elif args.model_type == 'anomaly_detector':
        train_anomaly_detector(args.symbol, training_df)
    else:
        logger.error(f"Unknown model type: {args.model_type}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Pluggable ML Model Trainer")
    parser.add_argument("--symbol", type=str, default="BTCUSDT", help="Asset symbol to train on")
    parser.add_argument("--start-date", type=str, default="2023-01-01", help="Start date for training data")
    parser.add_argument("--end-date", type=str, default="2023-01-31", help="End date for training data")
    parser.add_argument(
        "--model-type",
        type=str,
        default="xgboost_regressor",
        choices=['xgboost_regressor', 'anomaly_detector'],
        help="The type of model to train"
    )

    args = parser.parse_args()
    main(args)

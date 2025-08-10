import logging
import polars as pl
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.ensemble import IsolationForest
import sys
import os
import argparse
import joblib
from ray import tune
from ray.tune.search.hyperopt import HyperOptSearch

# --- Path Setup ---
if __package__ is None or __package__ == '':
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

from data_ingestion.config.config_loader import load_config
from ml_training.data_loader import load_training_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("trainer")

# --- Ray Tune Search Space ---
XGBOOST_SEARCH_SPACE = {
    "n_estimators": tune.randint(100, 1000),
    "learning_rate": tune.loguniform(1e-4, 1e-1),
    "max_depth": tune.randint(3, 10),
    "subsample": tune.uniform(0.5, 1.0),
    "colsample_bytree": tune.uniform(0.5, 1.0),
}

# --- Data Preparation ---
def prepare_regression_data(df: pl.DataFrame, target_col: str, horizon: int) -> pl.DataFrame:
    df = df.with_columns(target=(pl.col(target_col).shift(-horizon) / pl.col(target_col) - 1)).drop_nulls()
    return df

# --- Ray Tune Trainable ---
def tune_xgboost_trainable(config, data):
    X_train, y_train, X_test, y_test = data['X_train'], data['y_train'], data['X_test'], data['y_test']
    model = xgb.XGBRegressor(objective='reg:squarederror', random_state=42, n_jobs=-1, **config)
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], early_stopping_rounds=10, verbose=False)
    predictions = model.predict(X_test)
    mse = mean_squared_error(y_test, predictions)
    tune.report(mse=mse)

# --- Model Training Functions ---
def tune_xgboost_model(symbol: str, data: pl.DataFrame):
    """Orchestrates a Ray Tune hyperparameter sweep for XGBoost."""
    logger.info("--- Starting XGBoost Hyperparameter Tuning ---")
    prepared_df = prepare_regression_data(data, target_col="close", horizon=5)
    if prepared_df.is_empty():
        logger.error("Not enough data for tuning.")
        return

    feature_cols = [c for c in prepared_df.columns if c not in ["ts", "target", "close", "volume"]]
    X = prepared_df[feature_cols].to_pandas()
    y = prepared_df["target"].to_pandas()
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

    data_payload = {"X_train": X_train, "y_train": y_train, "X_test": X_test, "y_test": y_test}

    tuner = tune.Tuner(
        tune.with_parameters(tune_xgboost_trainable, data=data_payload),
        param_space=XGBOOST_SEARCH_SPACE,
        tune_config=tune.TuneConfig(
            metric="mse",
            mode="min",
            search_alg=HyperOptSearch(),
            num_samples=50, # Number of different hyperparameter combinations to try
        ),
    )
    results = tuner.fit()
    best_result = results.get_best_result(metric="mse", mode="min")
    logger.info(f"Tuning complete. Best trial MSE: {best_result.metrics['mse']:.6f}")
    logger.info(f"Best hyperparameters: {best_result.config}")
    # Here you would typically retrain the model on the full dataset with the best params and save it.

def train_anomaly_detector(symbol: str, data: pl.DataFrame):
    logger.info("--- Starting Isolation Forest Anomaly Detector Training ---")
    feature_cols = [col for col in data.columns if col not in ["ts", "close", "volume"]]
    data = data.drop_nulls(subset=feature_cols)
    if data.is_empty(): return
    X = data[feature_cols].to_pandas()
    model = IsolationForest(n_estimators=100, contamination='auto', random_state=42)
    model.fit(X)
    num_anomalies = (model.predict(X) == -1).sum()
    logger.info(f"Found {num_anomalies} anomalies.")
    joblib.dump(model, f"{symbol}_isolation_forest.joblib")
    logger.info(f"Model saved to {symbol}_isolation_forest.joblib")

# --- Main Training Orchestrator ---
def main(args):
    logger.info(f"Starting training pipeline for {args.symbol}...")
    config = load_config()
    training_df = load_training_data(config, args.symbol, args.start_date, args.end_date)
    if training_df.is_empty():
        logger.error("No data loaded. Aborting.")
        return

    if args.tune:
        if args.model_type != 'xgboost_regressor':
            logger.error("Tuning is only implemented for 'xgboost_regressor'.")
            return
        tune_xgboost_model(args.symbol, training_df)
    elif args.model_type == 'xgboost_regressor':
        # For a single run, we can just call the trainable with default params
        # This part could be refactored to be cleaner, but for now it works.
        logger.warning("Single run training for XGBoost not implemented in this refactor. Use --tune.")
    elif args.model_type == 'anomaly_detector':
        train_anomaly_detector(args.symbol, training_df)
    else:
        logger.error(f"Unknown model type: {args.model_type}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Pluggable ML Model Trainer")
    parser.add_argument("--symbol", type=str, default="BTCUSDT")
    parser.add_argument("--start-date", type=str, default="2023-01-01")
    parser.add_argument("--end-date", type=str, default="2023-01-31")
    parser.add_argument("--model-type", type=str, default="xgboost_regressor",
                        choices=['xgboost_regressor', 'anomaly_detector'])
    parser.add_argument("--tune", action="store_true", help="Flag to run hyperparameter tuning.")

    args = parser.parse_args()
    main(args)

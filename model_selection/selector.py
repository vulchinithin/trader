import logging
import mlflow
from mlflow.tracking import MlflowClient
import sys
import os
import pandas as pd

# --- Path Setup ---
if __package__ is None or __package__ == '':
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

from backtesting.backtest import run_model_based_backtest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("model_selector")

def select_best_model(model_base_name: str, symbol: str, start_date: str, end_date: str, metric: str = "Sharpe Ratio"):
    """
    Finds the best version of a model by backtesting all versions.
    """
    logger.info(f"--- Starting model selection for '{model_base_name}' ---")

    client = MlflowClient()

    try:
        model_versions = client.search_model_versions(f"name='{model_base_name}'")
    except mlflow.exceptions.MlflowException:
        logger.error(f"Could not find registered model with name '{model_base_name}'. Aborting.")
        return

    if not model_versions:
        logger.warning(f"No versions found for model '{model_base_name}'.")
        return

    results = []
    for mv in model_versions:
        logger.info(f"Backtesting model version: {mv.version}")

        stats = run_model_based_backtest(
            symbol=symbol, start_date=start_date, end_date=end_date,
            model_name=mv.name, model_stage=mv.version
        )

        if stats is not None:
            performance = stats.get(metric)
            if performance is not None:
                results.append({"version": mv.version, "metric": performance})
                logger.info(f"Version {mv.version} -> {metric}: {performance:.4f}")
            else:
                logger.warning(f"Metric '{metric}' not found in backtest stats for version {mv.version}.")
        else:
            logger.warning(f"Backtest failed for model version {mv.version}.")

    if not results:
        logger.error("No backtests succeeded. Cannot select a champion model.")
        return

    champion = max(results, key=lambda x: x['metric'])

    logger.info("\n--- Model Selection Complete ---")
    logger.info(f"Champion model is Version {champion['version']} with a {metric} of {champion['metric']:.4f}")

if __name__ == '__main__':
    select_best_model(
        model_base_name="BTCUSDT_xgboost_regressor",
        symbol="BTCUSDT",
        start_date="2023-01-01",
        end_date="2023-12-31"
    )

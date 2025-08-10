import mlflow
import logging
import sys
import os

logger = logging.getLogger(__name__)

# --- Path Setup ---
if __package__ is None or __package__ == '':
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

def load_model_from_registry(model_name: str, model_stage: str = "None"):
    """
    Loads a model from the MLflow Model Registry.
    """
    try:
        model_uri = f"models:/{model_name}/{model_stage}"
        logger.info(f"Loading model from URI: {model_uri}")

        loaded_model = mlflow.pyfunc.load_model(model_uri)

        logger.info("Model loaded successfully.")
        return loaded_model

    except mlflow.exceptions.MlflowException as e:
        logger.error(f"Failed to load model '{model_name}' from registry: {e}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading the model: {e}")
        return None

if __name__ == '__main__':
    model_name = "BTCUSDT_xgboost_regressor"
    print(f"Attempting to load the latest version of model '{model_name}'...")
    model = load_model_from_registry(model_name, model_stage="None")
    if model:
        print("Model loaded successfully!")
        print("Model metadata:", model.metadata)
    else:
        print("Failed to load model.")

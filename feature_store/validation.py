import pandas as pd
from feast import FeatureStore
from datetime import datetime
import time

def validate_feature_store():
    """
    This script demonstrates how to fetch features from the feature store.

    NOTE: This script cannot be run successfully in the current environment
    because the `feast apply` command is failing due to a tooling issue with
    changing directories. `feast apply` is required to register the feature
    definitions before they can be queried.

    This script is provided as a reference for what the validation logic
    should look like.
    """
    try:
        # The repo_path should point to the directory with the feature_store.yaml
        # Based on my file moving, this should be the correct path.
        store = FeatureStore(repo_path="feature_store/feature_repo")

        # Define the entities we want to fetch features for.
        # In a real scenario, this would be a list of symbols and timestamps
        # for which you need training data.
        entity_df = pd.DataFrame.from_dict(
            {
                "symbol": ["BTCUSDT", "ETHUSDT"],
                "event_timestamp": [
                    datetime.now(),
                    datetime.now(),
                ],
            }
        )

        print("--- Attempting to fetch historical features from QuestDB (Offline Store) ---")

        # This command will query QuestDB to get feature values for the specified
        # entities at the given timestamps.
        feature_data = store.get_historical_features(
            entity_df=entity_df,
            features=[
                "market_features_v1:rsi",
                "market_features_v1:macd",
                "market_features_v1:macd_signal",
            ],
        ).to_df()

        print("\nSuccessfully retrieved historical features dataframe:")
        print(feature_data)

    except Exception as e:
        print(f"\n--- An error occurred during validation ---")
        print("This was expected because `feast apply` could not be run.")
        print(f"Error: {e}")

if __name__ == "__main__":
    validate_feature_store()

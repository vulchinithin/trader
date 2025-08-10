from datetime import timedelta
from feast import Entity, FeatureView, Field
from feast.infra.offline_stores.contrib.postgres_source import PostgreSQLSource
from feast.types import Float32

# 1. Define an entity for the asset symbol.
# An entity is a primary key used to look up features.
asset = Entity(
    name="symbol",
    description="A financial asset symbol, e.g., BTCUSDT"
)

# 2. Define a data source pointing to our QuestDB feature table.
# Feast will connect to QuestDB via its PostgreSQL wire protocol.
feature_data_source = PostgreSQLSource(
    name="questdb_feature_source",
    table="feature_data",
    timestamp_field="ts",
    # QuestDB doesn't have a separate created_timestamp, so we use the event timestamp.
    # This is common for time-series data.
    created_timestamp_column="ts",
)

# 3. Define the Feature View.
# A Feature View groups related features and connects them to a data source.
market_features_v1 = FeatureView(
    name="market_features_v1",
    entities=[asset],
    # How long a feature value is valid in the online store.
    ttl=timedelta(days=1),
    # The schema of the features in this view.
    schema=[
        Field(name="rsi", dtype=Float32),
        Field(name="macd", dtype=Float32),
        Field(name="macd_signal", dtype=Float32),
    ],
    # Make these features available for low-latency serving.
    online=True,
    source=feature_data_source,
    # Tags are for organization and discovery.
    tags={"owner": "data_science_team"},
)

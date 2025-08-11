import asyncio
import json
import logging
import os
import sys
import polars as pl
import yaml

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

# --- Path Setup ---
if __package__ is None or __package__ == '':
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

from data_ingestion.config.config_loader import load_config as load_main_config
from common.logging_setup import setup_logging

# --- Setup Logging ---
setup_logging('feature-generator')
logger = logging.getLogger(__name__)

# --- Configuration ---
def load_feature_config():
    config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
    with open(config_path) as f:
        return yaml.safe_load(f)

main_cfg = load_main_config()
feature_cfg = load_feature_config()

KAFKA_SERVERS = main_cfg.get("kafka", {}).get("bootstrap_servers", "localhost:9092")
WINDOW_SIZE = feature_cfg.get("feature_generator", {}).get("rolling_window_size", 2000)
RSI_LENGTH = feature_cfg.get("feature_generator", {}).get("rsi_length", 14)
MACD_FAST = feature_cfg.get("feature_generator", {}).get("macd_fast", 12)
MACD_SLOW = feature_cfg.get("feature_generator", {}).get("macd_slow", 26)
MACD_SIGNAL = feature_cfg.get("feature_generator", {}).get("macd_signal", 9)
OUTPUT_TOPIC = "features-data"

# --- In-Memory Data Store ---
symbol_data = {}

# --- Feature Calculation ---
def calculate_rsi(series: pl.Series, length: int) -> pl.Series:
    delta = series.diff()
    gain = delta.clip(lower_bound=0)
    loss = (-delta).clip(lower_bound=0)
    avg_gain = gain.ewm_mean(com=length - 1, adjust=False)
    avg_loss = loss.ewm_mean(com=length - 1, adjust=False)
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))

def calculate_macd(series: pl.Series, fast: int, slow: int, signal: int) -> pl.DataFrame:
    ema_fast = series.ewm_mean(span=fast, adjust=False)
    ema_slow = series.ewm_mean(span=slow, adjust=False)
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm_mean(span=signal, adjust=False)
    return pl.DataFrame({
        f"MACD_{fast}_{slow}_{signal}": macd_line,
        f"MACDs_{fast}_{slow}_{signal}": signal_line,
    })

def calculate_features(df: pl.DataFrame) -> pl.DataFrame:
    if len(df) < MACD_SLOW:
        return df

    rsi = calculate_rsi(df['close'], RSI_LENGTH)
    macd = calculate_macd(df['close'], MACD_FAST, MACD_SLOW, MACD_SIGNAL)

    df = df.with_columns(
        rsi.alias(f"RSI_{RSI_LENGTH}")
    ).with_columns(macd)

    if len(df) > WINDOW_SIZE:
        df = df.tail(WINDOW_SIZE)

    return df

# --- Main Consumer/Producer Loop ---
async def consume_and_generate():
    consumer = AIOKafkaConsumer(
        "market-data-.*", bootstrap_servers=KAFKA_SERVERS,
        group_id="feature-generator-group", auto_offset_reset="earliest"
    )
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_SERVERS)

    await consumer.start()
    await producer.start()
    logger.info("Feature generator started.")

    try:
        async for msg in consumer:
            try:
                data = json.loads(msg.value.decode('utf-8'))
                if 'k' in data:
                    kline = data['k']
                    symbol = data['s']

                    if symbol not in symbol_data:
                        symbol_data[symbol] = pl.DataFrame({
                            "close": pl.Series([], dtype=pl.Float64),
                        })

                    new_row = pl.DataFrame({"close": [float(kline['c'])]})
                    symbol_data[symbol] = pl.concat([symbol_data[symbol], new_row])

                    symbol_data[symbol] = calculate_features(symbol_data[symbol])

                    latest_features = symbol_data[symbol].tail(1).to_dicts()[0]

                    if latest_features and latest_features.get(f"RSI_{RSI_LENGTH}") is not None:
                        logger.info(f"Symbol: {symbol}, Features: {latest_features}")
                        enriched_data = {**data, "features": latest_features}
                        await producer.send_and_wait(OUTPUT_TOPIC, json.dumps(enriched_data).encode('utf-8'))

            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)
    finally:
        logger.info("Stopping feature generator...")
        await consumer.stop()
        await producer.stop()

if __name__ == "__main__":
    try:
        asyncio.run(consume_and_generate())
    except KeyboardInterrupt:
        logger.info("Feature generator stopped by user.")

import asyncio
import json
import logging
import socket
import sys
import os
import time

from aiokafka import AIOKafkaConsumer

# --- Path Setup ---
if __package__ is None or __package__ == '':
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

from data_ingestion.config.config_loader import load_config
from common.logging_setup import setup_logging

# --- Setup Logging ---
setup_logging('feature-writer')
logger = logging.getLogger(__name__)

# --- Configuration ---
cfg = load_config()
KAFKA_SERVERS = cfg.get("kafka", {}).get("bootstrap_servers", "localhost:9092")
QUESTDB_HOST = cfg.get("questdb", {}).get("host", "localhost")
QUESTDB_ILP_PORT = cfg.get("questdb", {}).get("ilp_port", 9009)
INPUT_TOPIC = "features-data"
BATCH_SIZE = 1000
BATCH_TIMEOUT = 1.0

# --- ILP Sender ---
def send_to_questdb(lines):
    """Sends a batch of InfluxDB Line Protocol lines to QuestDB."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((QUESTDB_HOST, QUESTDB_ILP_PORT))
            sock.sendall(lines.encode('utf-8'))
            logger.info(f"Sent {lines.count('\\n')} feature lines to QuestDB.")
    except socket.error as e:
        logger.error(f"Socket error sending features to QuestDB: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred when sending features to QuestDB: {e}")

# --- Main Consumer Loop ---
async def consume_and_write_features():
    """Consumes feature messages from Kafka and writes them to QuestDB."""
    consumer = AIOKafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        group_id="feature-writer-group",
        auto_offset_reset="earliest",
    )

    await consumer.start()
    logger.info(f"Feature writer consumer started, subscribed to '{INPUT_TOPIC}'.")

    batch = []
    last_send_time = time.time()

    try:
        async for msg in consumer:
            try:
                data = json.loads(msg.value.decode('utf-8'))
                features = data.get("features", {})

                if not features or features.get('RSI_14') is None:
                    continue

                ts = int(data['k']['t']) * 1_000_000
                symbol = data['s']

                rsi_val = features.get('RSI_14')
                macd_val = features.get('MACD_12_26_9')
                macds_val = features.get('MACDs_12_26_9')

                fields = []
                if rsi_val is not None: fields.append(f"rsi={rsi_val}")
                if macd_val is not None: fields.append(f"macd={macd_val}")
                if macds_val is not None: fields.append(f"macd_signal={macds_val}")

                if not fields:
                    continue

                line = f"feature_data,symbol={symbol} {','.join(fields)} {ts}\n"
                batch.append(line)

                current_time = time.time()
                if len(batch) >= BATCH_SIZE or (current_time - last_send_time) >= BATCH_TIMEOUT:
                    if batch:
                        send_to_questdb("".join(batch))
                        batch = []
                        last_send_time = current_time

            except Exception as e:
                logger.error(f"Error processing feature message: {e}", exc_info=True)

    finally:
        if batch:
            send_to_questdb("".join(batch))

        logger.info("Stopping feature writer...")
        await consumer.stop()

if __name__ == "__main__":
    try:
        asyncio.run(consume_and_write_features())
    except KeyboardInterrupt:
        logger.info("Feature writer stopped by user.")

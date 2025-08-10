import asyncio
import json
import logging
import socket
import sys
import os
import time

from aiokafka import AIOKafkaConsumer

# This allows the script to be run directly for development
if __package__ is None or __package__ == '':
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    from data_ingestion.config.config_loader import load_config
else:
    from ..config.config_loader import load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("questdb_writer")

# --- Configuration ---
cfg = load_config()
KAFKA_SERVERS = cfg.get("kafka", {}).get("bootstrap_servers", "localhost:9092")
QUESTDB_HOST = cfg.get("questdb", {}).get("host", "localhost")
QUESTDB_ILP_PORT = cfg.get("questdb", {}).get("ilp_port", 9009)
BATCH_SIZE = 1000
BATCH_TIMEOUT = 1.0  # seconds

# --- ILP Sender ---
def send_to_questdb(lines):
    """Sends a batch of InfluxDB Line Protocol lines to QuestDB."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((QUESTDB_HOST, QUESTDB_ILP_PORT))
            sock.sendall(lines.encode('utf-8'))
            logger.info(f"Sent {lines.count('\\n')} lines to QuestDB.")
    except socket.error as e:
        logger.error(f"Socket error sending to QuestDB: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred when sending to QuestDB: {e}")

# --- Main Consumer Loop ---
async def consume_and_write():
    """Consumes messages from Kafka and writes them to QuestDB."""
    consumer = AIOKafkaConsumer(
        "market-data-.*",
        bootstrap_servers=KAFKA_SERVERS,
        group_id="questdb-writer-group",
        auto_offset_reset="earliest",
    )

    await consumer.start()
    logger.info("Kafka consumer started for QuestDB writer.")

    batch = []
    last_send_time = time.time()

    try:
        async for msg in consumer:
            try:
                data = json.loads(msg.value)

                # Determine the table and timestamp from the message type
                # This assumes kline data format for now
                if 'k' in data: # Kline data
                    kline = data['k']
                    table = 'market_data'
                    ts = int(kline['t']) * 1_000_000 # Convert ms to ns
                    tags = f"symbol={data['s']},instrument_type={data.get('instrument_type', 'spot')}"
                    fields = f"price={kline['c']},volume={kline['v']}"
                    line = f"{table},{tags} {fields} {ts}\n"
                    batch.append(line)

                # Check if batch is ready to be sent
                current_time = time.time()
                if len(batch) >= BATCH_SIZE or (current_time - last_send_time) >= BATCH_TIMEOUT:
                    if batch:
                        send_to_questdb("".join(batch))
                        batch = []
                        last_send_time = current_time

            except json.JSONDecodeError:
                logger.warning(f"Could not decode message from {msg.topic}: {msg.value}")
            except KeyError as e:
                logger.warning(f"Message from {msg.topic} is missing expected key: {e}. Message: {data}")

    finally:
        # Send any remaining messages in the batch before shutting down
        if batch:
            send_to_questdb("".join(batch))

        logger.info("Stopping consumer...")
        await consumer.stop()

if __name__ == "__main__":
    try:
        asyncio.run(consume_and_write())
    except KeyboardInterrupt:
        logger.info("Writer stopped by user.")

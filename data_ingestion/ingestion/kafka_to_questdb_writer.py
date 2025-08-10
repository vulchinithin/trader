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
    from data_ingestion.ingestion.redis_client import get_redis_client
else:
    from ..config.config_loader import load_config
    from .redis_client import get_redis_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("questdb_writer")

# --- Configuration ---
cfg = load_config()
KAFKA_SERVERS = cfg.get("kafka", {}).get("bootstrap_servers", "localhost:9092")
QUESTDB_HOST = cfg.get("questdb", {}).get("host", "localhost")
QUESTDB_ILP_PORT = cfg.get("questdb", {}).get("ilp_port", 9009)
REDIS_CACHE_SIZE = cfg.get("redis", {}).get("price_cache_size", 1000)
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

# --- Redis Caching ---
def cache_price_to_redis(redis_client, symbol, price, cache_size):
    """Caches the latest price to a capped list in Redis."""
    if not redis_client:
        return
    try:
        key = f"prices:{symbol}"
        pipe = redis_client.pipeline()
        pipe.lpush(key, price)
        pipe.ltrim(key, 0, cache_size - 1)
        pipe.execute()
        logger.debug(f"Cached price for {symbol} to Redis.")
    except Exception as e:
        logger.error(f"Failed to cache price for {symbol} in Redis: {e}")

# --- Main Consumer Loop ---
async def consume_and_write():
    """Consumes messages from Kafka and writes them to QuestDB and Redis."""
    consumer = AIOKafkaConsumer(
        "market-data-.*",
        bootstrap_servers=KAFKA_SERVERS,
        group_id="questdb-writer-group",
        auto_offset_reset="earliest",
    )

    redis_client = get_redis_client(cfg)
    if not redis_client:
        logger.warning("Could not connect to Redis. Price caching will be disabled.")

    await consumer.start()
    logger.info("Kafka consumer started for QuestDB writer.")

    batch = []
    last_send_time = time.time()

    try:
        async for msg in consumer:
            try:
                data = json.loads(msg.value.decode('utf-8')) # Decode bytes to string

                if 'k' in data: # Kline data
                    kline = data['k']
                    symbol = data['s']
                    price = kline['c']

                    cache_price_to_redis(redis_client, symbol, price, REDIS_CACHE_SIZE)

                    table = 'market_data'
                    ts = int(kline['t']) * 1_000_000
                    tags = f"symbol={symbol},instrument_type={data.get('instrument_type', 'spot')}"
                    fields = f"price={price},volume={kline['v']}"
                    line = f"{table},{tags} {fields} {ts}\n"
                    batch.append(line)

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
        if batch:
            send_to_questdb("".join(batch))

        logger.info("Stopping consumer...")
        await consumer.stop()
        if redis_client:
            redis_client.close()

if __name__ == "__main__":
    try:
        asyncio.run(consume_and_write())
    except KeyboardInterrupt:
        logger.info("Writer stopped by user.")

# data_ingestion/ingestion/websocket_client.py
import asyncio
import json
import logging
import random
import time
import yaml
import os
from aiokafka import AIOKafkaProducer
import websockets
from rest_fallback import fetch_historical as rest_fetch

from kafka.admin import KafkaAdminClient, NewTopic

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ws_client")

def load_config():
    module_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(module_dir, '..', 'config', 'websocket_config.yaml')
    try:
        with open(config_path) as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Config file not found at {config_path}.")

cfg = load_config()

BINANCE_URL = cfg["binance"]["base_url"]
STREAMS = cfg["binance"]["streams"]
MAX_ATTEMPTS = cfg["binance"]["reconnect"]["max_attempts"]
BASE = cfg["binance"]["reconnect"]["backoff_base"]
FACTOR = cfg["binance"]["reconnect"]["backoff_factor"]
PING_INTERVAL = cfg["binance"]["ping_interval"]  # e.g., 180 for 3 minutes

KAFKA_BOOTSTRAP = "localhost:9092"

# Singleton producer
_producer = None
_producer_lock = asyncio.Lock()

async def get_producer():
    global _producer
    async with _producer_lock:
        if _producer is None:
            _producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
            await _producer.start()
            logger.info("Kafka producer initialized")
        return _producer

async def produce_to_kafka(topic, message):
    try:
        producer = await get_producer()
        await producer.send_and_wait(topic, json.dumps(message).encode())
        logger.info(f"Published to {topic}")
    except Exception as e:
        logger.error(f"Failed to publish: {e}")

async def send_ping(ws):
    while True:
        await asyncio.sleep(PING_INTERVAL)
        try:
            await ws.ping()
            logger.debug("Sent ping to keep connection alive")
        except Exception as e:
            logger.warning(f"Ping failed: {e}")
            break

async def connect_and_stream(symbol, interval):
    stream_names = [s.format(symbol=symbol, interval=interval) for s in STREAMS]
    url = f"{BINANCE_URL}/{'/'.join(stream_names)}"
    attempt = 0
    while attempt < MAX_ATTEMPTS:
        try:
            async with websockets.connect(url, ping_interval=PING_INTERVAL) as ws:
                logger.info(f"Connected to {url}")
                ping_task = asyncio.create_task(send_ping(ws))  # Start ping task
                async for raw in ws:
                    data = json.loads(raw)
                    topic = f"market-data-{symbol}"
                    asyncio.create_task(produce_to_kafka(topic, data))
                ping_task.cancel()  # Clean up ping task
        except Exception as e:
            wait = BASE * (FACTOR ** attempt) + random.random()
            logger.warning(f"Connection lost ({e}), retrying in {wait:.1f}s")
            await asyncio.sleep(wait)
            attempt += 1
            await asyncio.sleep(1)  # Extra delay to avoid rate limits
    # REST fallback after max attempts
    logger.error(f"Max attempts reached for {symbol}. Falling back to REST.")
    historical = rest_fetch(symbol, interval, limit=500)  # Fetch last 500 klines
    for item in historical:
        asyncio.create_task(produce_to_kafka(f"market-data-{symbol}", item))

def ensure_topics(symbols):
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
        existing_topics = admin_client.list_topics()
        for symbol in symbols:
            topic = f"market-data-{symbol}"
            if topic not in existing_topics:
                new_topic = NewTopic(name=topic, num_partitions=1, replication_factor=1)
                admin_client.create_topics([new_topic])
                logger.info(f"Created topic: {topic}")
        admin_client.close()
    except Exception as e:
        logger.error(f"Failed to ensure topics: {e}")

async def main():
    ensure_topics(cfg["assets"]["symbols"])
    tasks = []
    for symbol in cfg["assets"]["symbols"]:
        for interval in cfg["assets"]["intervals"]:
            tasks.append(connect_and_stream(symbol, interval))
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        if _producer is not None:
            await _producer.stop()

if __name__ == "__main__":
    asyncio.run(main())

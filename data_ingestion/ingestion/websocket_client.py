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
PING_INTERVAL = cfg["binance"]["ping_interval"]

KAFKA_BOOTSTRAP = "localhost:9092"

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

async def send_heartbeat(ws):
    while True:
        try:
            pong = await asyncio.wait_for(ws.ping(), timeout=10)
            await pong
            logger.debug("Heartbeat ping successful")
        except asyncio.TimeoutError:
            logger.warning("Ping timeout - forcing reconnection")
            raise Exception("Ping timeout - reconnecting")
        await asyncio.sleep(PING_INTERVAL)

async def connect_and_stream(symbol, interval):
    stream_names = [s.format(symbol=symbol, interval=interval) for s in STREAMS]
    url = f"{BINANCE_URL}/{'/'.join(stream_names)}"
    attempt = 0
    while attempt < MAX_ATTEMPTS:
        try:
            ws = await websockets.connect(url, ping_interval=PING_INTERVAL, ping_timeout=20)
            logger.info(f"Connected to {url}")
            heartbeat_task = asyncio.create_task(send_heartbeat(ws))
            while True:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=30)  # Detect silence
                    data = json.loads(raw)
                    topic = f"market-data-{symbol}"
                    asyncio.create_task(produce_to_kafka(topic, data))
                except asyncio.TimeoutError:
                    logger.warning("Recv timeout detected for {symbol} - forcing reconnection")
                    raise Exception("Recv timeout - reconnecting")
            heartbeat_task.cancel()
        except Exception as e:
            if 'heartbeat_task' in locals():
                heartbeat_task.cancel()
            wait = BASE * (FACTOR ** attempt) + random.random()
            logger.warning(f"Connection lost for {symbol} ({e}), retrying in {wait:.1f}s (attempt {attempt+1}/{MAX_ATTEMPTS})")
            await asyncio.sleep(wait)
            attempt += 1
            await asyncio.sleep(1)  # Avoid rate limits
    logger.error(f"Max attempts reached for {symbol}. Falling back to REST.")
    historical = rest_fetch(symbol, interval, limit=500)
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

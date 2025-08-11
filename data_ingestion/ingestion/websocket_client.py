import asyncio
import json
import logging
import random
import time
import sys
import os
from aiokafka import AIOKafkaProducer
import websockets
from kafka.admin import KafkaAdminClient, NewTopic

# This allows the script to be run directly for development, while also supporting package imports.
if __package__ is None or __package__ == '':
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    from data_ingestion.config.config_loader import load_config
    from data_ingestion.ingestion.rest_fallback import fetch_historical as rest_fetch
    from data_ingestion.ingestion.redis_client import get_redis_client, publish_to_redis_stream
    from data_ingestion.selection.asset_selector import AssetSelector
    from common.logging_setup import setup_logging
else:
    from ..config.config_loader import load_config
    from .rest_fallback import fetch_historical as rest_fetch
    from .redis_client import get_redis_client, publish_to_redis_stream
    from ..selection.asset_selector import AssetSelector
    from common.logging_setup import setup_logging


# --- Setup Logging ---
setup_logging('data-ingestion-ws')
logger = logging.getLogger(__name__)

cfg = load_config()

# Constants from config
STREAMS = cfg["binance"]["streams"]
MAX_ATTEMPTS = cfg["binance"]["reconnect"]["max_attempts"]
BASE = cfg["binance"]["reconnect"]["backoff_base"]
FACTOR = cfg["binance"]["reconnect"]["backoff_factor"]
PING_INTERVAL = cfg["binance"]["ping_interval"]
KAFKA_BOOTSTRAP_SERVERS = cfg['kafka']['bootstrap_servers']
RATE_LIMIT_BACKOFF = cfg.get("binance", {}).get("rate_limit_backoff", 60)
MESSAGING_BROKER = cfg.get("messaging", {}).get("broker", "kafka")


async def publish_message(kafka_producer, redis_client, topic, message):
    """Publishes a message to the configured message broker(s)."""
    if MESSAGING_BROKER in ["kafka", "both"] and kafka_producer:
        await produce_to_kafka(kafka_producer, topic, message)
    if MESSAGING_BROKER in ["redis", "both"] and redis_client:
        await asyncio.to_thread(publish_to_redis_stream, redis_client, topic, message)

async def produce_to_kafka(producer, topic, message):
    """Sends a message to a Kafka topic."""
    try:
        await producer.send_and_wait(topic, json.dumps(message).encode())
        logger.info(f"Published to Kafka topic {topic}")
    except Exception as e:
        logger.error(f"Failed to publish to Kafka topic {topic}: {e}")

async def send_heartbeat(ws):
    """Sends a periodic heartbeat to the WebSocket connection."""
    while True:
        try:
            await asyncio.sleep(PING_INTERVAL)
            await ws.ping()
            logger.debug("Heartbeat ping sent")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"Heartbeat failed: {e}. Forcing reconnection.")
            raise

async def connect_and_stream(kafka_producer, redis_client, instrument_type, symbol, interval):
    """Connects to a WebSocket stream, handles data, and manages reconnection."""
    base_url = cfg['binance']['instrument_urls'].get(instrument_type)
    if not base_url:
        logger.error(f"Base URL for instrument '{instrument_type}' not found in config.")
        return

    stream_names = [s.format(symbol=symbol.lower(), interval=interval) for s in STREAMS]
    url = f"{base_url}/{'/'.join(stream_names)}"
    attempt = 0
    topic = f"market-data-{instrument_type}-{symbol.lower()}"

    while attempt < MAX_ATTEMPTS:
        try:
            async with websockets.connect(url, ping_interval=None, ping_timeout=None) as ws:
                logger.info(f"Connected to {url}")
                heartbeat_task = asyncio.create_task(send_heartbeat(ws))
                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=PING_INTERVAL * 2)
                        data = json.loads(raw)
                        data['instrument_type'] = instrument_type
                        asyncio.create_task(publish_message(kafka_producer, redis_client, topic, data))
                    except asyncio.TimeoutError:
                        logger.warning(f"No message from {symbol} for {PING_INTERVAL * 2}s. Reconnecting.")
                        raise
        except websockets.exceptions.ConnectionClosed as e:
            if 'heartbeat_task' in locals() and not heartbeat_task.done():
                heartbeat_task.cancel()
            if e.code == 1013 or "rate limit" in str(e.reason).lower() or "429" in str(e.reason):
                logger.error(f"Rate limit for {symbol} ({instrument_type}). Backing off for {RATE_LIMIT_BACKOFF}s. Reason: {e.reason}")
                await asyncio.sleep(RATE_LIMIT_BACKOFF)
            else:
                wait = BASE * (FACTOR ** attempt) + random.uniform(0, 1)
                logger.warning(f"Conn closed for {symbol} ({instrument_type}): {e}. Retrying in {wait:.1f}s")
                await asyncio.sleep(wait)
                attempt += 1
        except Exception as e:
            if 'heartbeat_task' in locals() and not heartbeat_task.done():
                heartbeat_task.cancel()
            wait = BASE * (FACTOR ** attempt) + random.uniform(0, 1)
            logger.warning(f"Unhandled exception for {symbol} ({instrument_type}): {e}. Retrying in {wait:.1f}s")
            await asyncio.sleep(wait)
            attempt += 1

    logger.error(f"Max attempts for {symbol} ({instrument_type}). Falling back to REST.")
    historical = await asyncio.to_thread(rest_fetch, cfg, instrument_type, symbol, interval, limit=500)
    for item in historical:
        item['instrument_type'] = instrument_type
        asyncio.create_task(publish_message(kafka_producer, redis_client, topic, item))

def ensure_topics(instruments, symbols):
    """Ensures that the necessary Kafka topics exist if Kafka is the broker."""
    if MESSAGING_BROKER not in ["kafka", "both"]:
        return
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        existing_topics = admin_client.list_topics()
        num_partitions = cfg.get("kafka", {}).get("num_partitions", 4)
        replication_factor = cfg.get("kafka", {}).get("replication_factor", 1)

        topics_to_create = []
        for instrument in instruments:
            for symbol in symbols:
                topic = f"market-data-{instrument}-{symbol.lower()}"
                if topic not in existing_topics:
                    topics_to_create.append(NewTopic(name=topic, num_partitions=num_partitions, replication_factor=replication_factor))

        if topics_to_create:
            admin_client.create_topics(topics_to_create)
            for t in topics_to_create:
                logger.info(f"Created topic: {t.name}")
        admin_client.close()
    except Exception as e:
        logger.error(f"Failed to ensure Kafka topics: {e}")

async def main():
    """Main function to set up and run the WebSocket client."""
    kafka_producer = None
    redis_client = None

    try:
        # Initialize clients based on config
        if MESSAGING_BROKER in ["kafka", "both"]:
            kafka_producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                compression_type="zstd"
            )
            await kafka_producer.start()

        if MESSAGING_BROKER in ["redis", "both"] or cfg.get("selection", {}).get("mode") == "autonomous":
            redis_client = get_redis_client(cfg)
            if not redis_client:
                logger.error("Could not establish Redis connection. Aborting.")
                if kafka_producer: await kafka_producer.stop()
                return

        # Select assets
        asset_selector = AssetSelector(cfg, redis_client)
        symbols, frequencies, instruments = asset_selector.get_selected_assets()

        if not all([instruments, symbols, frequencies]):
            logger.error("No assets selected to subscribe. Check your configuration and data.")
            return

        if MESSAGING_BROKER in ["kafka", "both"]:
            ensure_topics(instruments, symbols)

        tasks = []
        # The new selector returns a list of symbols and a parallel list of frequencies
        for instrument in instruments:
            for i, symbol in enumerate(symbols):
                interval = frequencies[i] if i < len(frequencies) else "1m"
                tasks.append(connect_and_stream(kafka_producer, redis_client, instrument, symbol, interval))

        if tasks:
            await asyncio.gather(*tasks)
        else:
            logger.info("No tasks to run.")

    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        if kafka_producer:
            await kafka_producer.stop()
            logger.info("Kafka producer stopped.")
        if redis_client:
            redis_client.close()
            logger.info("Redis client closed.")

if __name__ == "__main__":
    asyncio.run(main())

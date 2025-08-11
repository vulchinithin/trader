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
from typing import Dict, Any

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

# --- Configuration ---
cfg = load_config()
MAX_ATTEMPTS = cfg["reconnect"]["max_attempts"]
BASE = cfg["reconnect"]["backoff_base"]
FACTOR = cfg["reconnect"]["backoff_factor"]
PING_INTERVAL = cfg["ping_interval"]
KAFKA_BOOTSTRAP_SERVERS = cfg['kafka']['bootstrap_servers']
RATE_LIMIT_BACKOFF = cfg.get("rate_limit_backoff", 60)
MESSAGING_BROKER = cfg.get("messaging", {}).get("broker", "kafka")


def normalize_data(exchange: str, raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalizes data from different exchanges into a common internal format.
    """
    if exchange == 'binance':
        if raw_data.get('e') == 'kline':
            k = raw_data['k']
            return {
                'exchange': exchange,
                'symbol': raw_data['s'],
                'timestamp': k['t'],
                'open': k['o'],
                'high': k['h'],
                'low': k['l'],
                'close': k['c'],
                'volume': k['v'],
                'is_final': k['x']
            }
    elif exchange == 'coinbase':
        if raw_data.get('type') == 'ticker':
            return {
                'exchange': exchange,
                'symbol': raw_data['product_id'],
                'timestamp': int(time.time() * 1000), # Coinbase ticker does not have a timestamp
                'open': raw_data.get('open_24h'),
                'high': raw_data.get('high_24h'),
                'low': raw_data.get('low_24h'),
                'close': raw_data.get('price'),
                'volume': raw_data.get('volume_24h'),
                'is_final': False
            }
    # Return raw data if no normalization rule matches
    return raw_data


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

async def connect_and_stream(kafka_producer, redis_client, portfolio: Dict[str, Any]):
    """Connects to a WebSocket stream for a given portfolio, handles data, and manages reconnection."""
    exchange = portfolio['exchange']
    exchange_cfg = cfg['exchanges'][exchange]

    attempt = 0
    while attempt < MAX_ATTEMPTS:
        try:
            # --- Connection and Subscription ---
            # For now, we only support one instrument type per portfolio for simplicity
            instrument = portfolio['instruments'][0]
            base_url = exchange_cfg['instrument_urls'][instrument]

            if exchange_cfg['stream_format'] == 'path':
                # Binance-style: streams are in the URL path
                stream_names = []
                for i, symbol in enumerate(portfolio['symbols']):
                    interval = portfolio['frequencies'][i]
                    # For now, just use the kline stream for simplicity
                    stream_names.append(f"{symbol.lower()}@kline_{interval}")
                url = f"{base_url}/{'/'.join(stream_names)}"

                async with websockets.connect(url, ping_interval=PING_INTERVAL) as ws:
                    logger.info(f"Connected to {exchange} at {url}")
                    # No separate subscription message needed for Binance
                    await receive_loop(ws, kafka_producer, redis_client, portfolio)

            elif exchange_cfg['stream_format'] == 'json_message':
                # Coinbase-style: connect first, then send a subscription message
                async with websockets.connect(base_url, ping_interval=PING_INTERVAL) as ws:
                    logger.info(f"Connected to {exchange} at {base_url}")
                    sub_msg = exchange_cfg['subscription_message'].copy()
                    sub_msg['product_ids'] = portfolio['symbols']
                    await ws.send(json.dumps(sub_msg))
                    logger.info(f"Sent subscription message to {exchange}: {sub_msg}")
                    await receive_loop(ws, kafka_producer, redis_client, portfolio)

            # If the loop exits cleanly, break the retry loop
            break

        except websockets.exceptions.ConnectionClosed as e:
            wait = BASE * (FACTOR ** attempt) + random.uniform(0, 1)
            logger.warning(f"Conn closed for {exchange}: {e}. Retrying in {wait:.1f}s")
            await asyncio.sleep(wait)
            attempt += 1
        except Exception as e:
            wait = BASE * (FACTOR ** attempt) + random.uniform(0, 1)
            logger.warning(f"Unhandled exception for {exchange}: {e}. Retrying in {wait:.1f}s", exc_info=True)
            await asyncio.sleep(wait)
            attempt += 1

    logger.error(f"Max reconnection attempts reached for portfolio on exchange '{exchange}'.")
    # Note: Fallback to REST is more complex with multiple exchanges and is omitted for now.

async def receive_loop(ws, kafka_producer, redis_client, portfolio):
    """The main loop to receive and process messages from a WebSocket."""
    exchange = portfolio['exchange']
    while True:
        raw = await ws.recv()
        data = json.loads(raw)

        normalized_data = normalize_data(exchange, data)

        if normalized_data and 'symbol' in normalized_data:
            # Construct topic from normalized data
            topic = f"market-data-{exchange}-{normalized_data['symbol'].lower()}"
            asyncio.create_task(publish_message(kafka_producer, redis_client, topic, normalized_data))
        else:
            logger.debug(f"Received non-trade message from {exchange}: {data}")

def ensure_topics(portfolios: List[Dict[str, Any]]):
    """Ensures that the necessary Kafka topics exist if Kafka is the broker."""
    if MESSAGING_BROKER not in ["kafka", "both"]:
        return
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        existing_topics = admin_client.list_topics()
        num_partitions = cfg.get("kafka", {}).get("num_partitions", 4)
        replication_factor = cfg.get("kafka", {}).get("replication_factor", 1)

        topics_to_create = []
        for p in portfolios:
            for symbol in p['symbols']:
                topic = f"market-data-{p['exchange']}-{symbol.lower()}"
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
            kafka_producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, compression_type="zstd")
            await kafka_producer.start()

        if MESSAGING_BROKER in ["redis", "both"] or cfg.get("selection", {}).get("mode") == "autonomous":
            redis_client = get_redis_client(cfg)

        # Select assets
        asset_selector = AssetSelector(cfg, redis_client)
        selected_portfolios = asset_selector.get_selected_assets()

        if not selected_portfolios:
            logger.error("No portfolios selected to subscribe. Check your configuration and data.")
            return

        ensure_topics(selected_portfolios)

        # Create a connection task for each portfolio
        tasks = [connect_and_stream(kafka_producer, redis_client, p) for p in selected_portfolios]

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

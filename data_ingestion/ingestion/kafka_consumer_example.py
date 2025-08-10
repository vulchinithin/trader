import asyncio
import json
import logging
import sys
import os

from aiokafka import AIOKafkaConsumer

# This allows the script to be run directly for development, while also supporting package imports.
if __package__ is None or __package__ == '':
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    from data_ingestion.config.config_loader import load_config
else:
    from ..config.config_loader import load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("kafka_consumer_example")

async def consume():
    """
    Connects to Kafka and consumes messages from the market data topics.
    """
    cfg = load_config()
    kafka_servers = cfg.get("kafka", {}).get("bootstrap_servers", "localhost:9092")

    # Using a consumer group allows multiple instances of this script to run in parallel,
    # with Kafka distributing the topic partitions among them.
    # To demonstrate, run this script in two separate terminals with the same group_id.
    consumer = AIOKafkaConsumer(
        "market-data-.*",  # Subscribe to all market-data topics using a regex pattern
        bootstrap_servers=kafka_servers,
        group_id="my-market-data-consumers",
        # To replay messages from the beginning of a topic, set auto_offset_reset to 'earliest'.
        # The default is 'latest', which only shows new messages.
        auto_offset_reset="earliest",
    )

    await consumer.start()
    logger.info("Kafka consumer started. Subscribed to 'market-data-.*' topics.")

    try:
        async for msg in consumer:
            try:
                # Assuming the message value is a JSON string
                data = json.loads(msg.value.decode("utf-8"))
                logger.info(
                    f"Consumed from {msg.topic}: partition={msg.partition}, offset={msg.offset}, "
                    f"key={msg.key}, value={data}"
                )
            except json.JSONDecodeError:
                logger.warning(f"Could not decode message from {msg.topic}: {msg.value}")
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user.")
    finally:
        logger.info("Stopping consumer...")
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(consume())

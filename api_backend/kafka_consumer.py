import asyncio
import logging
from aiokafka import AIOKafkaConsumer
import sys
import os

# --- Path Setup ---
if __package__ is None or __package__ == '':
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    from data_ingestion.config.config_loader import load_config
else:
    from ..data_ingestion.config.config_loader import load_config

logger = logging.getLogger(__name__)

# --- Configuration ---
cfg = load_config()
KAFKA_SERVERS = cfg.get("kafka", {}).get("bootstrap_servers", "localhost:9092")

class KafkaConsumer:
    def __init__(self):
        # The consumer is now created in the start method
        self.consumer = None
        self._running = False

    async def start(self):
        """Creates and starts the Kafka consumer."""
        logger.info("Starting Kafka consumer for API backend...")
        self.consumer = AIOKafkaConsumer(
            "market-data-.*",
            bootstrap_servers=KAFKA_SERVERS,
            group_id="api-backend-group",
            auto_offset_reset="latest",
        )
        await self.consumer.start()
        self._running = True
        logger.info("Kafka consumer started.")

    async def stop(self):
        """Stops the Kafka consumer."""
        if self.consumer:
            logger.info("Stopping Kafka consumer for API backend...")
            await self.consumer.stop()
            self._running = False
            logger.info("Kafka consumer stopped.")

    async def message_generator(self):
        """An async generator that yields messages from the consumer."""
        if not self._running or not self.consumer:
            logger.warning("Consumer is not running. Cannot generate messages.")
            return

        try:
            async for msg in self.consumer:
                yield msg.value
        except Exception as e:
            logger.error(f"Kafka consumer error: {e}", exc_info=True)

# The global instance is removed. The app will create and manage the instance.

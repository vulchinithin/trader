# data_ingestion/ingestion/kafka_consumer.py
import asyncio
import json
import logging
import yaml
import os
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("kafka_consumer")

KAFKA_BOOTSTRAP = "localhost:9092"

# Load config (same as in websocket_client.py)
def load_config():
    module_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(module_dir, '..', 'config', 'websocket_config.yaml')
    try:
        with open(config_path) as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Config file not found at {config_path}. Please create it in data_ingestion/config/")

cfg = load_config()  # Load cfg here

def create_consumer(topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset='earliest',  # Start from beginning for testing
        enable_auto_commit=True,
        group_id='trading-group',  # For parallel consumers
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    return consumer

async def consume_and_process(topic):
    consumer = create_consumer(topic)
    for message in consumer:
        data = message.value
        logger.info(f"Received from {topic}: {data}")
        # Process data (e.g., save to DB or feature engineering)
        # Example: await save_to_db(data)  # Integrate with Epic 2

async def main():
    tasks = []
    for symbol in cfg["assets"]["symbols"]:
        topic = f"market-data-{symbol}"
        tasks.append(consume_and_process(topic))
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())

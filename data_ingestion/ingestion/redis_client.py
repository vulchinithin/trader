# data_ingestion/ingestion/redis_client.py
import redis
import json
import logging

logger = logging.getLogger("redis_client")

def get_redis_client(config):
    """Creates a Redis client from the configuration."""
    redis_cfg = config.get("redis", {})
    try:
        client = redis.Redis(
            host=redis_cfg.get("host", "localhost"),
            port=redis_cfg.get("port", 6379),
            db=redis_cfg.get("db", 0),
            decode_responses=True
        )
        client.ping() # Check the connection
        logger.info("Redis client connected successfully.")
        return client
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Failed to connect to Redis: {e}")
        return None

def publish_to_redis_stream(redis_client, stream_name, message):
    """Publishes a message to a Redis Stream."""
    if not redis_client:
        logger.error("Cannot publish to Redis: client is not connected.")
        return

    try:
        # Redis Streams expects a dictionary of field-value pairs.
        # We'll serialize the whole message as a JSON string under a 'data' field.
        payload = {"data": json.dumps(message)}
        redis_client.xadd(stream_name, payload)
        logger.info(f"Published to Redis stream {stream_name}")
    except Exception as e:
        logger.error(f"Failed to publish to Redis stream {stream_name}: {e}")

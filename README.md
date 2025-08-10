# Trader

This project is a high-frequency trading data ingestion and processing pipeline.

## Overview

The system ingests real-time market data from Binance (spot and futures) via WebSockets, publishes it to a Kafka message broker, and provides options for low-latency processing and storage.

## Running the System

1.  **Set up Environment Variables:**
    Create a `.env` file in the root directory or set environment variables for your Binance API keys:
    ```bash
    export BINANCE_API_KEY="your_api_key"
    export BINANCE_API_SECRET="your_api_secret"
    ```

2.  **Start Services with Docker Compose:**
    This will start Kafka, Zookeeper, and other configured services.
    ```bash
    docker-compose up -d
    ```

3.  **Run the Data Ingestion Client:**
    ```bash
    python -m data_ingestion.ingestion.websocket_client
    ```

## Consuming Market Data from Kafka

The ingested market data is published to Kafka topics in the format `market-data-{instrument_type}-{symbol}`.

A sample consumer is provided in `data_ingestion/ingestion/kafka_consumer_example.py`.

### Running the Example Consumer

To run the consumer and see the live data stream:
```bash
python -m data_ingestion.ingestion.kafka_consumer_example
```

### Parallel Processing

Kafka's consumer group mechanism allows you to easily parallelize the processing of data. To run multiple consumers in parallel:

1.  Open two or more terminal windows.
2.  In each terminal, run the same consumer command as above.

As long as the consumers share the same `group_id` (e.g., `"my-market-data-consumers"` in the example), Kafka will automatically distribute the topic partitions among them. This means each message will only be processed by one consumer instance in the group, allowing you to scale your processing power horizontally.

### Message Replayability

By default, a new consumer group will start reading messages from the end of a topic (only new messages). To replay and process all messages from the very beginning of a topic, you can configure the consumer's `auto_offset_reset` property.

In the example consumer, this is set to `'earliest'`:
```python
consumer = AIOKafkaConsumer(
    # ...,
    auto_offset_reset="earliest"
)
```
This configuration tells the consumer to start at the earliest available message in the topic if it doesn't have a previously committed offset. This is useful for back-filling data or re-processing historical events.

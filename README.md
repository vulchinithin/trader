# High-Frequency Trading & ML Pipeline

This project is a comprehensive, multi-service framework for high-frequency algorithmic trading, from data ingestion and feature engineering to model training, evaluation, and execution.

## Table of Contents
- [Architectural Overview](#architectural-overview)
- [Core Services](#core-services)
- [System Setup Instructions](#system-setup-instructions)
  - [1. Prerequisites](#1-prerequisites)
  - [2. Environment Variable Setup](#2-environment-variable-setup-api-keys)
  - [3. Building and Running the System](#3-building-and-running-the-entire-system)
- [Monitoring the Pipeline](#monitoring-the-pipeline)
  - [A. Kibana: Live Log Stream](#a-kibana-the-live-log-stream)
  - [B. QuestDB: Time-Series Data](#b-questdb-the-time-series-data)
  - [C. MLflow UI: ML Experiments](#c-mlflow-ui-the-machine-learning-lifecycle)
- [Development Guide](#development-guide)
  - [Frontend Setup](#frontend-setup)
  - [Running Individual Services](#running-individual-services)
  - [Running Tests](#running-tests)

## Architectural Overview

The system is built on a microservices architecture, with components communicating via a Kafka message broker and gRPC. It is designed to be scalable, extensible, and resilient.

- **Data Flow**: Data is ingested from multiple exchanges via WebSockets, published to Kafka, stored in QuestDB, processed for features, and then used for model training and backtesting.
- **Orchestration**: The system uses a real-time "Clock" service with gRPC for event-driven orchestration and Prefect for batch workflow management.
- **Technology Stack**: Python, Docker, Kafka, QuestDB, Redis, ELK Stack (Elasticsearch, Logstash, Kibana), FastAPI, React, Polars, MLflow, Ray, `vectorbt`, `ccxt`.

## Core Services
- `data_ingestion`: Connects to exchanges (Binance, Coinbase) and ingests market data.
- `feature_engineering`: Calculates technical indicators from streaming data.
- `ml_training`: Trains, tunes (with Ray), and tracks (with MLflow) various ML models.
- `backtesting`: Runs vectorized backtests of strategies using `vectorbt`.
- `model_selection`: Compares model performance via backtesting to find a "champion".
- `api_backend`: A FastAPI server providing a WebSocket stream of live data.
- `risk_management`: A microservice for pre-trade checks and position sizing.
- `execution_service`: Places paper trades on an exchange testnet, integrated with the risk service.
- `orchestration`: Contains a Prefect flow for batch jobs and a gRPC "Clock" for real-time triggers.
- `frontend`: A React-based skeleton for a user dashboard.

## System Setup Instructions

### 1. Prerequisites
- **Docker Desktop**: The most important requirement. Ensure it's installed and using the WSL 2 backend.
- **Node.js & npm**: Required for frontend development. Install the latest LTS version.
- **A modern terminal**: PowerShell or Windows Terminal is recommended.

### 2. Environment Variable Setup (API Keys)
The services require API keys to connect to exchanges.

1.  In the root directory of the project, create a new file named `.env`.
2.  Add the following lines, replacing the placeholders with your actual testnet API keys:
    ```.env
    # Binance Testnet API Keys
    BINANCE_TESTNET_API_KEY=your_binance_testnet_api_key
    BINANCE_TESTNET_API_SECRET=your_binance_testnet_api_secret

    # Coinbase Testnet (Sandbox) API Keys
    COINBASE_TESTNET_API_KEY=your_coinbase_testnet_api_key
    COINBASE_TESTNET_API_SECRET=your_coinbase_testnet_api_secret
    ```
### 3. Building and Running the Entire System
1.  Open your terminal.
2.  Navigate to the project root directory.
3.  Run the main Docker Compose command:
    ```bash
    docker-compose up --build
    ```
This will build all the service images and start all containers. The first run will take a considerable amount of time. You will see interleaved logs from all services in your terminal.

## Monitoring the Pipeline

Once the system is running, you can use the following UIs to monitor its activity.

### A. Kibana: The Live Log Stream
- **URL**: `http://localhost:5601`
- **Purpose**: View a centralized, real-time stream of logs from all services.
- **Setup**: On first visit, create an "index pattern". Go to **Management -> Stack Management -> Index Patterns**, click **Create index pattern**, and enter `logstash-*`. You can then view all logs in the **Discover** tab.

### B. QuestDB: The Time-Series Data
- **URL**: `http://localhost:9000`
- **Purpose**: Query the historical market and feature data.
- **Usage**: Run SQL queries like `SELECT * FROM market_data;` or `SELECT * FROM feature_data;` to inspect the persisted data.

### C. MLflow UI: The Machine Learning Lifecycle
- **URL**: `http://localhost:5000`
- **Purpose**: Track ML experiments, compare runs, and manage the model registry.
- **Setup**:
    1.  Open a **new, separate terminal**.
    2.  Navigate to the project root directory.
    3.  Run the command: `mlflow ui`
    4.  This starts the MLflow server. You can now access the UI at the URL above.

## Development Guide

### Frontend Setup
For development (e.g., to enable IDE intellisense), you need to install the frontend's local dependencies.
1.  In your terminal, navigate to the frontend directory:
    ```bash
    # Note: The path is currently incorrect due to an environment issue during creation.
    cd feature_store/frontend
    ```
2.  Install dependencies:
    ```bash
    npm install
    ```

### Running Individual Services
You can run a single service for focused development. For example, to run just the data ingestion client:
```bash
python -m data_ingestion.ingestion.websocket_client
```
*Note: This requires a running Kafka instance, so `docker-compose up kafka` might be needed.*

### Running Tests
To run the entire test suite, execute `pytest` from the root directory:
```bash
pytest -v
```
To run tests for a specific service:
```bash
pytest -v data_ingestion/tests/
```
*Note: The test runner was non-functional in the development environment where this was built, but the commands above describe the intended usage.*

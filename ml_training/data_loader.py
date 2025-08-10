import logging
import polars as pl
import psycopg2
from contextlib import contextmanager
import sys
import os

# --- Path Setup ---
if __package__ is None or __package__ == '':
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

from data_ingestion.config.config_loader import load_config

logger = logging.getLogger(__name__)

@contextmanager
def get_db_connection(config):
    """Provides a managed database connection."""
    db_cfg = config.get("db", {})
    conn_info = {
        "host": db_cfg.get("host", "localhost"),
        "port": db_cfg.get("port", 8812),
        "user": "admin",
        "password": "quest",
        "dbname": db_cfg.get("database", "qdb"),
    }
    connection = None
    try:
        connection = psycopg2.connect(**conn_info)
        logger.info("Database connection established.")
        yield connection
    except psycopg2.OperationalError as e:
        logger.error(f"Could not connect to the database: {e}")
        yield None
    finally:
        if connection:
            connection.close()
            logger.info("Database connection closed.")

def load_training_data(config, symbol: str, start_date: str, end_date: str) -> pl.DataFrame:
    """
    Loads and joins market data and feature data for a given symbol and date range.
    """
    query = f"""
    SELECT
        m.ts,
        m.price as close,
        m.volume,
        f.rsi,
        f.macd,
        f.macd_signal
    FROM market_data m
    ASOF JOIN feature_data f ON (ts, symbol)
    WHERE m.symbol = '{symbol}' AND m.ts BETWEEN to_timestamp('{start_date}', 'yyyy-MM-dd') AND to_timestamp('{end_date}', 'yyyy-MM-dd');
    """

    with get_db_connection(config) as conn:
        if conn:
            try:
                df = pl.read_database(query, conn)
                logger.info(f"Loaded {len(df)} rows of training data for {symbol}.")
                return df
            except Exception as e:
                logger.error(f"Failed to execute training data query: {e}")
                return pl.DataFrame()
        else:
            logger.error("No database connection available.")
            return pl.DataFrame()

if __name__ == '__main__':
    config = load_config()
    btc_training_data = load_training_data(config, "BTCUSDT", "2023-01-01", "2023-01-31")
    print(btc_training_data)

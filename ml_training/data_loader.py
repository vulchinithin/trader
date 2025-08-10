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
else:
    from ..data_ingestion.config.config_loader import load_config

logger = logging.getLogger(__name__)

@contextmanager
def get_db_connection(config):
    """Provides a managed database connection."""
    conn_info = {
        "host": config.get("questdb", {}).get("host", "localhost"),
        "port": config.get("questdb", {}).get("pg_port", 8812), # Assuming a pg_port config
        "user": config.get("questdb", {}).get("user", "admin"),
        "password": config.get("questdb", {}).get("password", "quest"),
        "dbname": config.get("questdb", {}).get("database", "qdb"),
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

def load_feature_data(config, symbol: str, start_date: str, end_date: str) -> pl.DataFrame:
    """
    Loads feature data for a given symbol and date range from QuestDB.

    Args:
        config: The main configuration dictionary.
        symbol: The asset symbol to load data for (e.g., 'BTCUSDT').
        start_date: The start of the date range (e.g., '2023-01-01').
        end_date: The end of the date range (e.g., '2023-01-31').

    Returns:
        A Polars DataFrame with the feature data.
    """
    query = f"""
    SELECT ts, rsi, macd, macd_signal
    FROM feature_data
    WHERE symbol = '{symbol}' AND ts BETWEEN to_timestamp('{start_date}', 'yyyy-MM-dd') AND to_timestamp('{end_date}', 'yyyy-MM-dd');
    """

    with get_db_connection(config) as conn:
        if conn:
            try:
                # Use polars' read_sql method for psycopg2 connections
                df = pl.read_database(query, conn)
                logger.info(f"Loaded {len(df)} rows of feature data for {symbol}.")
                return df
            except Exception as e:
                logger.error(f"Failed to execute query: {e}")
                return pl.DataFrame()
        else:
            logger.error("No database connection available.")
            return pl.DataFrame()

if __name__ == '__main__':
    # Example usage
    config = load_config()
    # Add questdb config if it's not in the main config files
    config.setdefault('questdb', {
        'host': 'localhost', 'pg_port': 8812, 'user': 'admin', 'password': 'quest', 'database': 'qdb'
    })

    # This will only work if you have data in your QuestDB instance
    btc_features = load_feature_data(config, "BTCUSDT", "2023-01-01", "2023-01-31")
    print(btc_features)

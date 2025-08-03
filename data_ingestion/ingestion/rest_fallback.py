# data_ingestion/ingestion/rest_fallback.py
import time
import logging
from binance.client import Client

logger = logging.getLogger("rest_fallback")

API_KEY = "your_api_key"
API_SECRET = "your_api_secret"
client = Client(API_KEY, API_SECRET)

def fetch_historical(symbol: str, interval: str, limit: int = 500):
    """
    Fetch REST historical klines if WebSocket is down.
    """
    try:
        logger.info(f"Fetching historical via REST: {symbol}, {interval}")
        klines = client.get_klines(symbol=symbol.upper(), interval=interval, limit=limit)
        return [
            {
                "open_time": k[0],
                "open": k[1],
                "high": k[2],
                "low": k[3],
                "close": k[4],
                "volume": k[5],
            }
            for k in klines
        ]
    except Exception as e:
        logger.error(f"REST fallback failed: {e}")
        time.sleep(1)
        return []

# data_ingestion/ingestion/rest_fallback.py
import time
import logging
from binance.client import Client
from binance.exceptions import BinanceAPIException

logger = logging.getLogger("rest_fallback")

def fetch_historical(config: dict, instrument_type: str, symbol: str, interval: str, limit: int = 500):
    """
    Fetch REST historical klines for a given instrument type if WebSocket is down.
    """
    api_key = config.get("binance", {}).get("api_key")
    api_secret = config.get("binance", {}).get("api_secret")

    # Public endpoints like get_klines do not require API keys, but futures might.
    # It's good practice to have them available.
    client = Client(api_key, api_secret)

    try:
        logger.info(f"Fetching historical via REST for {instrument_type}: {symbol}, {interval}")

        if instrument_type == 'spot':
            klines = client.get_klines(symbol=symbol.upper(), interval=interval, limit=limit)
        elif instrument_type == 'futures_usdm':
            klines = client.futures_klines(symbol=symbol.upper(), interval=interval, limit=limit)
        elif instrument_type == 'futures_coinm':
            klines = client.futures_coinm_klines(symbol=symbol.upper(), interval=interval, limit=limit)
        else:
            logger.error(f"Unsupported instrument type for REST fallback: {instrument_type}")
            return []

        # The kline format is consistent across these endpoints
        return [
            {
                "open_time": k[0],
                "open": k[1],
                "high": k[2],
                "low": k[3],
                "close": k[4],
                "volume": k[5],
                "close_time": k[6],
                "quote_asset_volume": k[7],
                "number_of_trades": k[8],
                "taker_buy_base_asset_volume": k[9],
                "taker_buy_quote_asset_volume": k[10],
            }
            for k in klines
        ]
    except BinanceAPIException as e:
        logger.error(f"REST fallback API error for {symbol} ({instrument_type}): {e}")
        return []
    except Exception as e:
        logger.error(f"Generic REST fallback failed for {symbol} ({instrument_type}): {e}")
        return []

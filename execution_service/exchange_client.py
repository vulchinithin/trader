import ccxt
import os
import logging

logger = logging.getLogger(__name__)

def get_exchange_client() -> ccxt.Exchange:
    """
    Initializes and returns a CCXT exchange client.

    Configures the client for the Binance testnet and uses API keys
    from environment variables.
    """
    try:
        exchange = ccxt.binance({
            'apiKey': os.getenv('BINANCE_TESTNET_API_KEY'),
            'secret': os.getenv('BINANCE_TESTNET_API_SECRET'),
            'options': {
                'defaultType': 'future',
            },
        })
        # Use the testnet
        exchange.set_sandbox_mode(True)
        logger.info("CCXT exchange client initialized for Binance testnet.")
        return exchange
    except Exception as e:
        logger.error(f"Failed to initialize CCXT exchange client: {e}", exc_info=True)
        raise

def create_market_buy_order(exchange: ccxt.Exchange, symbol: str, amount: float):
    """
    Places a market buy order.

    Args:
        exchange: The CCXT exchange client instance.
        symbol: The trading symbol (e.g., 'BTC/USDT').
        amount: The amount of the base currency to buy.

    Returns:
        The order result from the exchange.
    """
    try:
        logger.info(f"Placing market buy order for {amount} of {symbol}...")
        # CCXT requires the symbol in the format 'BASE/QUOTE'
        market_symbol = f"{symbol.upper()}/USDT"
        order = exchange.create_market_buy_order(market_symbol, amount)
        logger.info(f"Successfully placed market buy order: {order['id']}")
        return order
    except ccxt.NetworkError as e:
        logger.error(f"Network error placing order: {e}", exc_info=True)
        raise
    except ccxt.ExchangeError as e:
        logger.error(f"Exchange error placing order: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred placing order: {e}", exc_info=True)
        raise

if __name__ == '__main__':
    # Example usage (requires environment variables to be set)
    logging.basicConfig(level=logging.INFO)
    if not os.getenv('BINANCE_TESTNET_API_KEY') or not os.getenv('BINANCE_TESTNET_API_SECRET'):
        print("Please set BINANCE_TESTNET_API_KEY and BINANCE_TESTNET_API_SECRET environment variables.")
    else:
        client = get_exchange_client()
        # You can test fetching balance or other non-order-placing calls
        try:
            balance = client.fetch_balance()
            print("Successfully connected to exchange. Balance details:")
            print(balance['total'])
        except Exception as e:
            print(f"An error occurred: {e}")

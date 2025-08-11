import ccxt
import os
import logging

logger = logging.getLogger(__name__)

def get_exchange_client(exchange_name: str) -> ccxt.Exchange:
    """
    Initializes and returns a CCXT exchange client for a specific exchange.

    Configures the client for the testnet and uses API keys from environment variables.
    """
    logger.info(f"Initializing CCXT client for {exchange_name}...")
    try:
        api_key_env = f"{exchange_name.upper()}_TESTNET_API_KEY"
        secret_env = f"{exchange_name.upper()}_TESTNET_API_SECRET"

        api_key = os.getenv(api_key_env)
        secret = os.getenv(secret_env)

        if not api_key or not secret:
            raise ValueError(f"API keys ({api_key_env}, {secret_env}) are not set in environment variables.")

        exchange_class = getattr(ccxt, exchange_name)
        exchange = exchange_class({
            'apiKey': api_key,
            'secret': secret,
        })

        # Use the testnet
        exchange.set_sandbox_mode(True)
        logger.info(f"CCXT exchange client initialized for {exchange_name} testnet.")
        return exchange

    except AttributeError:
        logger.error(f"Exchange '{exchange_name}' is not supported by CCXT.")
        raise
    except Exception as e:
        logger.error(f"Failed to initialize CCXT exchange client for {exchange_name}: {e}", exc_info=True)
        raise

def format_symbol_for_exchange(symbol: str, exchange_name: str) -> str:
    """
    Formats a standard symbol into the format required by a specific exchange.
    CCXT generally expects 'BASE/QUOTE' format.
    Our internal format might be 'BASE-QUOTE' or 'BASEQUOTE'.
    """
    if exchange_name == 'coinbase':
        # e.g., BTC-USD -> BTC/USD
        return symbol.replace('-', '/')
    elif exchange_name == 'binance':
        # e.g., BTCUSDT -> BTC/USDT
        if 'USDT' in symbol:
            return f"{symbol.replace('USDT', '')}/USDT"

    # Default assumption if no specific rule
    return symbol


def create_market_buy_order(exchange: ccxt.Exchange, symbol: str, amount: float):
    """
    Places a market buy order.
    """
    try:
        market_symbol = format_symbol_for_exchange(symbol, exchange.id)
        logger.info(f"Placing market buy order for {amount} of {market_symbol} on {exchange.id}...")

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

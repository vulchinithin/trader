import os
import logging
import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Dict
import ccxt

from .exchange_client import get_exchange_client, create_market_buy_order

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Configuration ---
RISK_MANAGEMENT_URL = os.getenv('RISK_MANAGEMENT_URL', 'http://localhost:8001/pre-trade-check')

# --- Pydantic Models ---
class OrderRequest(BaseModel):
    exchange: str = Field(..., description="The exchange to execute on, e.g., 'binance' or 'coinbase'")
    symbol: str = Field(..., description="The asset symbol, e.g., BTCUSDT or BTC-USD")
    side: str = Field("buy", description="The side of the order ('buy' or 'sell')")

# --- FastAPI App ---
app = FastAPI(
    title="Execution Service",
    description="Handles order execution on multiple exchanges.",
    version="0.2.0",
)

# A simple in-memory cache for exchange clients
exchange_clients: Dict[str, ccxt.Exchange] = {}

def get_client_for_exchange(exchange_name: str) -> ccxt.Exchange:
    """
    Gets a cached exchange client or creates a new one.
    """
    if exchange_name not in exchange_clients:
        logger.info(f"No client for {exchange_name} in cache. Creating a new one.")
        exchange_clients[exchange_name] = get_exchange_client(exchange_name)
    return exchange_clients[exchange_name]


@app.get("/health", tags=["General"])
async def health_check():
    """Simple health check endpoint."""
    return {"status": "ok"}

@app.post("/execute-order", tags=["Execution"])
async def execute_order(order: OrderRequest):
    """
    Receives an order for a specific exchange, checks it, and executes it.
    """
    try:
        exchange_client = get_client_for_exchange(order.exchange)
    except Exception as e:
        logger.error(f"Failed to get exchange client for {order.exchange}: {e}")
        raise HTTPException(status_code=503, detail=f"Could not initialize client for exchange '{order.exchange}'.")

    # This is still a simplified integration for demonstration.
    dummy_risk_params = {
        "entry_price": 70000.0,
        "stop_loss_price": 69000.0,
        "account_balance": 100000.0,
        "risk_percentage": 0.01
    }

    # 1. Call Risk Management Service
    try:
        logger.info(f"Calling risk management service for order: {order.exchange}:{order.symbol}")
        response = requests.post(RISK_MANAGEMENT_URL, json=dummy_risk_params)
        response.raise_for_status()
        risk_decision = response.json()

        if not risk_decision.get("approved"):
            logger.warning(f"Trade for {order.symbol} was rejected by the risk management service.")
            raise HTTPException(status_code=400, detail="Trade rejected by risk management.")

        position_size = risk_decision.get("position_size")
        logger.info(f"Trade approved by risk manager with position size: {position_size}")

    except requests.exceptions.RequestException as e:
        logger.error(f"Could not connect to risk management service: {e}")
        raise HTTPException(status_code=503, detail="Risk management service is unavailable.")

    # 2. Execute Order via Exchange Client
    try:
        if order.side == "buy":
            exchange_order = create_market_buy_order(
                exchange=exchange_client,
                symbol=order.symbol,
                amount=position_size
            )
            return {"status": "success", "order_details": exchange_order}
        else:
            raise HTTPException(status_code=501, detail="Sell orders are not yet implemented.")

    except Exception as e:
        logger.error(f"Failed to execute order on {order.exchange}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to execute order on {order.exchange}.")

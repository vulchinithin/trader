import os
import logging
import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .exchange_client import get_exchange_client, create_market_buy_order

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Configuration ---
RISK_MANAGEMENT_URL = os.getenv('RISK_MANAGEMENT_URL', 'http://localhost:8001/pre-trade-check')

# --- Pydantic Models ---
class OrderRequest(BaseModel):
    symbol: str = Field(..., description="The asset symbol, e.g., BTCUSDT")
    side: str = Field("buy", description="The side of the order ('buy' or 'sell')")
    # In a real system, you'd have more params like order_type, etc.

# --- FastAPI App ---
app = FastAPI(
    title="Execution Service",
    description="Handles order execution and integration with the exchange.",
    version="0.1.0",
)

@app.on_event("startup")
async def startup_event():
    """Initialize the exchange client on startup."""
    try:
        app.state.exchange_client = get_exchange_client()
        logger.info("Exchange client successfully initialized.")
    except Exception as e:
        logger.critical(f"Could not initialize exchange client on startup: {e}. The service will not be able to place orders.")
        app.state.exchange_client = None

@app.get("/health", tags=["General"])
async def health_check():
    """Simple health check endpoint."""
    return {"status": "ok"}

@app.post("/execute-order", tags=["Execution"])
async def execute_order(order: OrderRequest):
    """
    Receives an order, checks it with the risk management service, and executes it.
    """
    if not app.state.exchange_client:
        raise HTTPException(status_code=503, detail="Exchange client is not available.")

    # For now, this is a simplified integration.
    # We would need more data (like account balance, entry/stop prices) to do a real risk check.
    # Let's assume we have some dummy data for the risk check call.
    dummy_risk_params = {
        "entry_price": 70000.0, # This would come from the signal or current market price
        "stop_loss_price": 69000.0, # This would come from the strategy
        "account_balance": 100000.0, # This would be fetched from the exchange
        "risk_percentage": 0.01
    }

    # 1. Call Risk Management Service
    try:
        logger.info(f"Calling risk management service for order: {order.symbol}")
        response = requests.post(RISK_MANAGEMENT_URL, json=dummy_risk_params)
        response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
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
                exchange=app.state.exchange_client,
                symbol=order.symbol,
                amount=position_size
            )
            return {"status": "success", "order_details": exchange_order}
        else:
            # Placeholder for sell logic
            raise HTTPException(status_code=501, detail="Sell orders are not yet implemented.")

    except Exception as e:
        logger.error(f"Failed to execute order on the exchange: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to execute order on the exchange.")

from fastapi import FastAPI, HTTPException
import logging
from .position_sizer import calculate_position_size, TradeParams

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create the FastAPI app
app = FastAPI(
    title="Risk Management Service",
    description="Provides pre-trade checks and position sizing.",
    version="0.1.0",
)

@app.get("/health", tags=["General"])
async def health_check():
    """Simple health check endpoint."""
    return {"status": "ok"}

@app.post("/pre-trade-check", tags=["Risk"])
async def pre_trade_check(params: TradeParams):
    """
    Performs a pre-trade risk check and calculates the position size.
    """
    try:
        position_size = calculate_position_size(params)

        if position_size <= 0:
            # This could be due to invalid inputs (e.g., stop loss above entry)
            # or a calculation resulting in zero.
            raise HTTPException(
                status_code=400,
                detail="Could not calculate a valid position size. Check your parameters (e.g., stop_loss_price must be less than entry_price)."
            )

        # The trade is approved with the calculated size.
        return {
            "approved": True,
            "position_size": position_size,
            "message": "Trade approved by risk manager."
        }

    except Exception as e:
        logger.error(f"An unexpected error occurred during pre-trade check: {e}", exc_info=True)
        # For any other unexpected errors, return a 500.
        raise HTTPException(
            status_code=500,
            detail="An internal error occurred in the risk management service."
        )

if __name__ == '__main__':
    # This allows running the app directly for development/testing.
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)

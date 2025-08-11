from pydantic import BaseModel, Field
import logging

logger = logging.getLogger(__name__)

class TradeParams(BaseModel):
    """
    Defines the parameters needed for a trade decision.
    This model will be used for API request validation.
    """
    entry_price: float = Field(..., gt=0, description="The entry price of the asset.")
    stop_loss_price: float = Field(..., gt=0, description="The price at which to exit for a loss.")
    account_balance: float = Field(..., gt=0, description="The total current account balance.")
    risk_percentage: float = Field(..., gt=0, lt=1, description="The percentage of the account to risk (e.g., 0.02 for 2%).")

def calculate_position_size(params: TradeParams) -> float:
    """
    Calculates the size of a position in the base asset.

    This function determines how many units of an asset to buy based on
    the classic percentage risk position sizing model.

    Args:
        params: An object containing the trade parameters.

    Returns:
        The number of units of the asset to purchase. Returns 0 if calculation is not possible.
    """
    if params.entry_price <= params.stop_loss_price:
        logger.warning("Stop loss must be below the entry price for a long position.")
        return 0.0

    # 1. Calculate the risk amount in currency
    risk_amount = params.account_balance * params.risk_percentage

    # 2. Calculate the risk per share (how much is lost if the stop loss is hit)
    risk_per_share = params.entry_price - params.stop_loss_price

    # 3. Calculate the position size
    if risk_per_share <= 0:
        return 0.0

    position_size = risk_amount / risk_per_share

    logger.info(f"Calculated position size: {position_size:.4f} units.")
    return position_size

if __name__ == '__main__':
    # Example usage:
    logging.basicConfig(level=logging.INFO)

    example_params = TradeParams(
        entry_price=100.0,
        stop_loss_price=98.0,
        account_balance=10000.0,
        risk_percentage=0.02 # Risk 2% of the account
    )

    size = calculate_position_size(example_params)

    # Verification:
    # Risk amount = 10000 * 0.02 = $200
    # Risk per share = 100 - 98 = $2
    # Position size = 200 / 2 = 100 shares
    print(f"Example position size: {size} units.")
    assert abs(size - 100.0) < 1e-9

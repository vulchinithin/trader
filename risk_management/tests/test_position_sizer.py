import pytest
from ..position_sizer import calculate_position_size, TradeParams

def test_calculate_position_size_valid():
    """
    Test position size calculation with a standard valid input.
    """
    params = TradeParams(
        entry_price=100.0,
        stop_loss_price=98.0,
        account_balance=10000.0,
        risk_percentage=0.02
    )
    # Expected: risk_amount = 200, risk_per_share = 2, size = 200 / 2 = 100
    assert abs(calculate_position_size(params) - 100.0) < 1e-9

def test_calculate_position_size_zero_risk_per_share():
    """
    Test case where stop loss is at the entry price, which should result in zero size.
    """
    params = TradeParams(
        entry_price=100.0,
        stop_loss_price=100.0,
        account_balance=10000.0,
        risk_percentage=0.02
    )
    assert calculate_position_size(params) == 0.0

def test_calculate_position_size_invalid_stop_loss():
    """
    Test case where stop loss is above the entry price, which is invalid.
    """
    params = TradeParams(
        entry_price=100.0,
        stop_loss_price=102.0,
        account_balance=10000.0,
        risk_percentage=0.02
    )
    assert calculate_position_size(params) == 0.0

@pytest.mark.parametrize("balance, risk_pct, expected_size", [
    (50000, 0.01, 250), # 500 / 2 = 250
    (1000, 0.05, 25),   # 50 / 2 = 25
    (10000, 0.1, 500)   # 1000 / 2 = 500
])
def test_calculate_position_size_parametrized(balance, risk_pct, expected_size):
    """
    Test with various account balances and risk percentages.
    """
    params = TradeParams(
        entry_price=100.0,
        stop_loss_price=98.0,
        account_balance=balance,
        risk_percentage=risk_pct
    )
    assert abs(calculate_position_size(params) - expected_size) < 1e-9

def test_trade_params_validation():
    """
    Test Pydantic model validation for negative or zero values.
    """
    with pytest.raises(ValueError):
        TradeParams(entry_price=-100, stop_loss_price=98, account_balance=10000, risk_percentage=0.02)

    with pytest.raises(ValueError):
        TradeParams(entry_price=100, stop_loss_price=98, account_balance=10000, risk_percentage=1.5) # gt=1

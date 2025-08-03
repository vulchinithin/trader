# data_ingestion/tests/test_asset_selector.py
import pytest
from selection.asset_selector import AssetSelector  # Assuming relative import works with your structure

mock_config = {
    'selection': {'mode': 'autonomous', 'symbols': ['BTCUSDT', 'ETHUSDT'], 'frequencies': ['1m', '5m'], 'instruments': ['spot']},
    'autonomous': {'parameters': {'window_size': 5, 'exploration_rate': 0.1, 'volatility_threshold': 0.05}},
    'redis': {'host': 'localhost', 'port': 6379, 'db': 0}
}

@pytest.fixture
def selector():
    return AssetSelector(mock_config)

def test_manual_selection(selector):
    selector.mode = 'manual'
    symbols, frequencies, instruments = selector.get_selected_assets()
    assert symbols == ['BTCUSDT', 'ETHUSDT']

def test_autonomous_selection(selector, monkeypatch):
    # Mock Redis lrange (accept self as first arg)
    def mock_lrange(self, key, start, end):
        if key == "prices:BTCUSDT":
            return [b'100', b'105', b'110', b'115', b'120']  # Low vol
        elif key == "prices:ETHUSDT":
            return [b'200', b'210', b'190', b'220', b'180']  # High vol
        return []

    monkeypatch.setattr("redis.Redis.lrange", mock_lrange)

    symbols, frequencies, instruments = selector.get_selected_assets()
    assert 'ETHUSDT' in symbols  # Selected for high vol

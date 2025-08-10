import pytest
from unittest.mock import MagicMock
import numpy as np
import random
import copy

from data_ingestion.selection.asset_selector import AssetSelector

MOCK_CONFIG = {
    'selection': {
        'mode': 'autonomous',
        'symbols': ['BTCUSDT', 'ETHUSDT', 'ADAUSDT', 'SOLUSDT'],
        'frequencies': ['1m', '5m'],
        'instruments': ['spot']
    },
    'autonomous': {
        'parameters': {
            'window_size': 5,
            'exploration_rate': 0.0,
            'volatility_threshold': 0.05,
            'max_selected_symbols': 2
        }
    },
    'redis': {'host': 'localhost', 'port': 6379, 'db': 0}
}

@pytest.fixture
def mock_redis():
    mock_client = MagicMock()
    mock_client.lrange.side_effect = lambda key, start, end: {
        b"prices:BTCUSDT": [b'100', b'101', b'102', b'103', b'104'],
        b"prices:ETHUSDT": [b'200', b'210', b'190', b'220', b'180'],
        b"prices:ADAUSDT": [b'1', b'1.5', b'1.2', b'1.8', b'1.3'],
        b"prices:SOLUSDT": [b'50', b'51', b'52', b'53', b'54'],
    }.get(key, []) # The key is already bytes
    return mock_client

@pytest.fixture
def selector(mock_redis):
    config = copy.deepcopy(MOCK_CONFIG)
    return AssetSelector(config, redis_client=mock_redis)

def test_manual_selection(mock_redis):
    config = copy.deepcopy(MOCK_CONFIG)
    config['selection']['mode'] = 'manual'
    manual_selector = AssetSelector(config, redis_client=mock_redis)
    symbols, frequencies, instruments = manual_selector.get_selected_assets()
    assert symbols == MOCK_CONFIG['selection']['symbols']
    assert frequencies == MOCK_CONFIG['selection']['frequencies']
    assert instruments == MOCK_CONFIG['selection']['instruments']

def test_autonomous_selects_top_n_by_volatility(selector):
    symbols, frequencies, _ = selector.get_selected_assets()
    assert len(symbols) == 2
    assert 'ADAUSDT' in symbols
    assert 'ETHUSDT' in symbols
    assert 'BTCUSDT' not in symbols

def test_autonomous_frequency_assignment(selector):
    symbols, frequencies, _ = selector.get_selected_assets()
    ada_index = symbols.index('ADAUSDT')
    eth_index = symbols.index('ETHUSDT')
    assert frequencies[ada_index] == '1m'
    assert frequencies[eth_index] == '1m'

    selector.config['autonomous']['parameters']['max_selected_symbols'] = 4
    selector.config['autonomous']['parameters']['volatility_threshold'] = 0.2
    symbols, frequencies, _ = selector.get_selected_assets()
    btc_index = symbols.index('BTCUSDT')
    sol_index = symbols.index('SOLUSDT')
    assert frequencies[btc_index] == '5m'
    assert frequencies[sol_index] == '5m'

def test_autonomous_exploration(selector, monkeypatch):
    selector.config['autonomous']['parameters']['exploration_rate'] = 1.0
    monkeypatch.setattr(random, 'choice', lambda x: 'BTCUSDT')
    symbols, _, _ = selector.get_selected_assets()
    assert len(symbols) == 3
    assert 'ADAUSDT' in symbols
    assert 'ETHUSDT' in symbols
    assert 'BTCUSDT' in symbols

def test_no_volatility_data_fallback(selector, mock_redis):
    mock_redis.lrange.side_effect = None # Clear the side effect
    mock_redis.lrange.return_value = []
    symbols, _, _ = selector.get_selected_assets()
    assert len(symbols) == 1
    assert symbols[0] in MOCK_CONFIG['selection']['symbols']

import redis
import numpy as np
import random

class AssetSelector:
    def __init__(self, config):
        self.config = config
        self.mode = config['selection']['mode']
        self.symbols = config['selection']['symbols']
        self.frequencies = config['selection']['frequencies']
        self.instruments = config['selection']['instruments']
        self.redis = redis.Redis(
            host=config['redis']['host'],
            port=config['redis']['port'],
            db=config['redis']['db']
        )
        if self.mode == 'autonomous':
            self.window_size = config['autonomous']['parameters']['window_size']
            self.exploration_rate = config['autonomous']['parameters']['exploration_rate']
            self.volatility_threshold = config['autonomous']['parameters']['volatility_threshold']

    def get_selected_assets(self):
        if self.mode == 'manual':
            return self.symbols, self.frequencies, self.instruments
        elif self.mode == 'autonomous':
            volatilities = {sym: self.calculate_volatility(sym) for sym in self.symbols}
            # Epsilon-greedy selection
            if random.random() < self.exploration_rate:
                selected_symbol = random.choice(self.symbols)
            else:
                selected_symbol = max(volatilities, key=volatilities.get)
            # Select frequency based on volatility
            selected_frequency = '1m' if volatilities[selected_symbol] > self.volatility_threshold else '5m'
            selected_instrument = random.choice(self.instruments)
            return [selected_symbol], [selected_frequency], [selected_instrument]

    def calculate_volatility(self, symbol):
        key = f"prices:{symbol}"
        prices = self.redis.lrange(key, -self.window_size, -1)
        if not prices:
            return 0.0
        prices = [float(p) for p in prices]
        returns = np.diff(prices) / prices[:-1]
        return np.std(returns) if len(returns) > 0 else 0.0

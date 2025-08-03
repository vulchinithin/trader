import logging
import numpy as np
import random

logger = logging.getLogger(__name__)

class AssetSelector:
    def __init__(self, config, redis_client=None):
        self.config = config
        self.mode = config['selection']['mode']
        self.symbols = config['selection']['symbols']
        self.frequencies = config['selection']['frequencies']
        self.instruments = config['selection']['instruments']
        # Allow injecting Redis client for testing/mocking
        if redis_client is None:
            import redis
            self.redis = redis.Redis(
                host=config['redis']['host'],
                port=config['redis']['port'],
                db=config['redis']['db']
            )
        else:
            self.redis = redis_client
        self.window_size = None
        self.exploration_rate = None
        self.volatility_threshold = None
        self._set_autonomous_params()

    def _set_autonomous_params(self):
        if self.mode == 'autonomous':
            try:
                params = self.config['autonomous']['parameters']
                self.window_size = params['window_size']
                self.exploration_rate = params['exploration_rate']
                self.volatility_threshold = params['volatility_threshold']
            except KeyError as e:
                logger.error(f"Missing autonomous parameter: {e}")
                raise ValueError(f"Invalid config for autonomous mode: {e}")

    def get_selected_assets(self):
        if self.mode == 'manual':
            logger.info("Using manual selection mode")
            return self.symbols, self.frequencies, self.instruments
        elif self.mode == 'autonomous':
            self._set_autonomous_params()  # Re-set in case mode changed
            if self.window_size is None:
                raise ValueError("Autonomous parameters not set")
            logger.info("Using autonomous selection mode")
            volatilities = {}
            for sym in self.symbols:
                try:
                    vol = self.calculate_volatility(sym)
                    volatilities[sym] = vol
                except Exception as e:
                    logger.warning(f"Volatility calculation failed for {sym}: {e}")
                    volatilities[sym] = 0.0
            if not volatilities:
                logger.warning("No volatility data; falling back to random")
                selected_symbol = random.choice(self.symbols)
            else:
                if random.random() < self.exploration_rate:
                    selected_symbol = random.choice(self.symbols)
                else:
                    selected_symbol = max(volatilities, key=volatilities.get)
            selected_frequency = '1m' if volatilities.get(selected_symbol, 0) > self.volatility_threshold else '5m'
            selected_instrument = random.choice(self.instruments)  # Could enhance based on vol
            logger.info(f"Selected: symbol={selected_symbol}, freq={selected_frequency}, instr={selected_instrument}, vol={volatilities.get(selected_symbol)}")
            return [selected_symbol], [selected_frequency], [selected_instrument]
        else:
            raise ValueError(f"Unknown mode: {self.mode}")

    def calculate_volatility(self, symbol):
        key = f"prices:{symbol}"
        prices = self.redis.lrange(key, -self.window_size, -1)
        if len(prices) < 2:
            logger.warning(f"Insufficient data for {symbol}: only {len(prices)} prices")
            return 0.0
        try:
            prices = [float(p) for p in prices]
            returns = np.diff(prices) / prices[:-1]
            vol = np.std(returns)
            if np.isnan(vol):
                return 0.0
            return vol
        except ValueError as e:
            logger.error(f"Invalid price data for {symbol}: {e}")
            return 0.0

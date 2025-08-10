import logging
import numpy as np
import random
import redis # Import redis at the top level

logger = logging.getLogger(__name__)

class AssetSelector:
    def __init__(self, config, redis_client=None):
        self.config = config
        self.mode = config['selection']['mode']
        self.symbols = config['selection']['symbols']
        self.frequencies = config['selection']['frequencies']
        self.instruments = config['selection']['instruments']

        if redis_client is None:
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
        self.max_selected_symbols = None
        self._set_autonomous_params()

    def _set_autonomous_params(self):
        if self.mode == 'autonomous':
            try:
                params = self.config['autonomous']['parameters']
                self.window_size = params['window_size']
                self.exploration_rate = params['exploration_rate']
                self.volatility_threshold = params['volatility_threshold']
                self.max_selected_symbols = params.get('max_selected_symbols', 5)
            except KeyError as e:
                logger.error(f"Missing autonomous parameter: {e}")
                raise ValueError(f"Invalid config for autonomous mode: {e}")

    def get_selected_assets(self):
        if self.mode == 'manual':
            logger.info("Using manual selection mode")
            return self.symbols, self.frequencies, self.instruments

        elif self.mode == 'autonomous':
            self._set_autonomous_params()
            if self.window_size is None:
                raise ValueError("Autonomous parameters not set")

            logger.info("Using autonomous selection mode")

            volatilities = {sym: self.calculate_volatility(sym) for sym in self.symbols}

            if all(v == 0.0 for v in volatilities.values()):
                logger.warning("No volatility data for any symbol; falling back to random selection.")
                selected_symbols = {random.choice(self.symbols)}
            else:
                sorted_symbols = sorted(volatilities, key=volatilities.get, reverse=True)
                selected_symbols = set(sorted_symbols[:self.max_selected_symbols])

                if random.random() < self.exploration_rate:
                    random_symbol = random.choice(self.symbols)
                    selected_symbols.add(random_symbol)
                    logger.info(f"Exploring with randomly added symbol: {random_symbol}")

            if not selected_symbols:
                logger.warning("No symbols were selected, falling back to one random symbol.")
                selected_symbols = {random.choice(self.symbols)}

            final_symbols = list(selected_symbols)
            selected_frequencies = [
                '1m' if volatilities.get(s, 0) > self.volatility_threshold else '5m'
                for s in final_symbols
            ]

            logger.info(f"Autonomous selection complete. Selected symbols: {final_symbols}")
            return final_symbols, selected_frequencies, self.instruments

        else:
            raise ValueError(f"Unknown mode: {self.mode}")

    def calculate_volatility(self, symbol):
        key = f"prices:{symbol}"
        try:
            prices_bytes = self.redis.lrange(key.encode(), -self.window_size, -1)
            if len(prices_bytes) < 2:
                return 0.0

            prices = [float(p.decode('utf-8')) for p in prices_bytes]
            returns = np.diff(prices) / prices[:-1]
            vol = np.std(returns)

            return 0.0 if np.isnan(vol) else vol

        except redis.exceptions.RedisError as e:
            logger.error(f"Redis error for {symbol}: {e}")
            return 0.0
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid price data for {symbol}: {e}")
            return 0.0

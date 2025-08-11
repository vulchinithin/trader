import logging
import numpy as np
import random
import redis
from typing import List, Dict, Any

# It's better to run from the project root so that imports work.
from data_ingestion.ingestion.redis_client import get_redis_client

logger = logging.getLogger(__name__)

class AssetSelector:
    def __init__(self, config: Dict[str, Any], redis_client=None):
        self.config = config
        self.mode = config['selection']['mode']

        if self.mode == 'manual':
            self.portfolios = config['selection'].get('portfolios', [])
        else: # autonomous
            # In autonomous mode, we consider all symbols from all manual portfolios as the universe.
            self.portfolios = config['selection'].get('portfolios', [])
            self.all_symbols = list(set(s for p in self.portfolios for s in p['symbols']))

        if redis_client is None:
            self.redis = get_redis_client(config)
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

    def get_selected_assets(self) -> List[Dict[str, Any]]:
        """
        Returns a list of portfolio configurations to subscribe to.
        Each portfolio is a dictionary containing exchange, instruments, symbols, and frequencies.
        """
        if self.redis is None and self.mode == 'autonomous':
            logger.error("Autonomous mode requires a Redis connection, but it's not available.")
            logger.warning("Falling back to manual selection mode.")
            self.mode = 'manual'

        if self.mode == 'manual':
            logger.info("Using manual selection mode")
            if not self.portfolios:
                logger.warning("No portfolios defined in manual mode.")
            return self.portfolios

        elif self.mode == 'autonomous':
            self._set_autonomous_params()
            if self.window_size is None:
                raise ValueError("Autonomous parameters not set")

            logger.info("Using autonomous selection mode")

            # Calculate volatility for all possible symbols
            volatilities = {sym: self.calculate_volatility(sym) for sym in self.all_symbols}

            # Select top N symbols based on volatility
            if all(v == 0.0 for v in volatilities.values()):
                logger.warning("No volatility data for any symbol; falling back to random selection.")
                selected_symbols_set = {random.choice(self.all_symbols)} if self.all_symbols else set()
            else:
                sorted_symbols = sorted(volatilities, key=volatilities.get, reverse=True)
                selected_symbols_set = set(sorted_symbols[:self.max_selected_symbols])

                # Exploration: add a random symbol
                if self.all_symbols and random.random() < self.exploration_rate:
                    random_symbol = random.choice(self.all_symbols)
                    selected_symbols_set.add(random_symbol)
                    logger.info(f"Exploring with randomly added symbol: {random_symbol}")

            if not selected_symbols_set:
                logger.warning("No symbols were selected, falling back to one random symbol.")
                selected_symbols_set = {random.choice(self.all_symbols)} if self.all_symbols else set()

            # Now, we need to construct portfolios from the selected symbols
            autonomous_portfolios = []
            for symbol in selected_symbols_set:
                # Find which original portfolio this symbol belongs to to get its exchange and instruments
                for p in self.portfolios:
                    if symbol in p['symbols']:
                        # Assign frequency based on volatility
                        frequency = '1m' if volatilities.get(symbol, 0) > self.volatility_threshold else '5m'

                        # Check if we already have a portfolio for this exchange
                        existing_portfolio = next((ap for ap in autonomous_portfolios if ap['exchange'] == p['exchange']), None)
                        if existing_portfolio:
                            existing_portfolio['symbols'].append(symbol)
                            existing_portfolio['frequencies'].append(frequency)
                        else:
                            autonomous_portfolios.append({
                                'exchange': p['exchange'],
                                'instruments': p['instruments'],
                                'symbols': [symbol],
                                'frequencies': [frequency]
                            })
                        break # Move to the next selected symbol

            logger.info(f"Autonomous selection complete. Selected portfolios: {autonomous_portfolios}")
            return autonomous_portfolios

        else:
            raise ValueError(f"Unknown mode: {self.mode}")

    def calculate_volatility(self, symbol: str) -> float:
        if self.redis is None:
            return 0.0

        key = f"prices:{symbol}"
        try:
            prices_bytes = self.redis.lrange(key, 0, self.window_size - 1)
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

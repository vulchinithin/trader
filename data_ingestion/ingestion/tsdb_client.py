# data_ingestion/ingestion/tsdb_client.py
import asyncio
import asyncpg
import yaml
import os

# Load DB config
def load_db_config():
    cfg_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'db_config.yaml')
    with open(cfg_path) as f:
        return yaml.safe_load(f)

db_cfg = load_db_config()

async def init_pool():
    return await asyncpg.create_pool(
        user='postgres',
        password=db_cfg['password'],
        database=db_cfg['database'],
        host=db_cfg['host'],
        port=db_cfg['port'],
        min_size=5,
        max_size=20
    )

async def insert_market_record(pool, record):
    async with pool.acquire() as conn:
        await conn.execute(
            '''
            INSERT INTO market_data(time, symbol, interval, open, high, low, close, volume)
            VALUES($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT DO NOTHING
            ''',
            record['time'], record['symbol'], record['interval'],
            record['open'], record['high'], record['low'],
            record['close'], record['volume']
        )

async def main():
    pool = await init_pool()
    # Example insert
    await insert_market_record(pool, {
        'time': '2025-08-03T12:00:00Z',
        'symbol': 'BTCUSDT',
        'interval': '1m',
        'open': 50000.0,
        'high': 50050.0,
        'low': 49950.0,
        'close': 50025.0,
        'volume': 12.34
    })
    await pool.close()

if __name__ == '__main__':
    asyncio.run(main())

-- This script creates the main table for storing market data in QuestDB.

CREATE TABLE IF NOT EXISTS market_data (
    ts TIMESTAMP,
    symbol SYMBOL,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume DOUBLE,
    instrument_type SYMBOL
) timestamp(ts) PARTITION BY DAY;

-- The `SYMBOL` type is an indexed string type in QuestDB, suitable for repeating values.
-- `timestamp(ts)` designates 'ts' as the primary timestamp column for time-series operations.
-- `PARTITION BY DAY` creates a hypertable, partitioning the data on disk for efficient queries.
-- We use IF NOT EXISTS to make the script idempotent.

-- 2.1 Create raw table
CREATE TABLE market_data (
  time        TIMESTAMPTZ NOT NULL,
  symbol      TEXT        NOT NULL,
  interval    TEXT        NOT NULL,
  open        DOUBLE PRECISION,
  high        DOUBLE PRECISION,
  low         DOUBLE PRECISION,
  close       DOUBLE PRECISION,
  volume      DOUBLE PRECISION,
  PRIMARY KEY (symbol, time)
);

-- 2.2 Convert to hypertable partitioned by time and symbol
SELECT create_hypertable(
  'market_data',
  'time',
  'symbol',
  4,                -- number of space partitions (parallelism)
  create_default_indexes => FALSE
);

-- 2.3 Create indexes for time and symbol
CREATE INDEX ON market_data (symbol, time DESC);

-- 2.4 Enable native compression on chunks older than 7 days
ALTER TABLE market_data
  SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'time DESC'
  );
SELECT add_compression_policy('market_data', INTERVAL '7 days');

-- Enable compression on the hypertable
ALTER TABLE public.market_data SET (timescaledb.compress = true);

-- (Optional) specify which columns to compress (defaults to all non-primary key columns)
-- For example, to compress only certain columns:
-- ALTER TABLE public.market_data SET (timescaledb.compress_segmentby = 'symbol');

-- Add a compression policy for chunks older than 7 days
SELECT add_compression_policy('public.market_data', INTERVAL '7 days');

-- Insert 100 rows spanning 2 hours to force chunk creation
INSERT INTO market_data(time, symbol, interval, open, high, low, close, volume)
SELECT
  now() - ((100 - i) * interval '1 minute') AS time,
  'BTCUSDT'   AS symbol,
  '1m'        AS interval,
  random()*100 + 10000 AS open,
  random()*100 + 10000 AS high,
  random()*100 + 10000 AS low,
  random()*100 + 10000 AS close,
  random()*10         AS volume
FROM generate_series(1,100) AS s(i);

SELECT show_chunks('market_data');

SELECT *
  FROM timescaledb_information.chunks
 WHERE hypertable_name = 'market_data';

-- Before compression
SELECT *
  FROM chunk_compression_stats('public.market_data'::regclass);

-- Trigger compression policy immediately (for testing)
SELECT compress_chunk(show_chunks('market_data')::regclass);

-- Then view stats again
SELECT *
  FROM chunk_compression_stats('public.market_data'::regclass);

SELECT *
  FROM timescaledb_information.jobs
 WHERE proc_name = 'policy_compression';

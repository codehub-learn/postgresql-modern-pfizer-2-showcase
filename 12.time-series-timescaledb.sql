-- =====================================================
-- INSTALL TimescaleDB to PostgreSQL 18 Bookworm Edition
-- apt-get update && apt-get install -y --no-install-recommends gnupg curl ca-certificates \
--  && curl -fsSL https://packagecloud.io/timescale/timescaledb/gpgkey | gpg --dearmor -o /usr/share/keyrings/timescale.gpg \
--  && echo "deb [signed-by=/usr/share/keyrings/timescale.gpg] https://packagecloud.io/timescale/timescaledb/debian/ bookworm main" > /etc/apt/sources.list.d/timescaledb.list \
--  && apt-get update && apt-get install -y --no-install-recommends timescaledb-2-postgresql-18 \
--  && apt-get clean && rm -rf /var/lib/apt/lists/*
--
-- INSTALL TimescaleDB Tools
-- apt-get update && apt-get install -y --no-install-recommends timescaledb-2-postgresql-18 timescaledb-tools
--
-- RUN
-- timescaledb-tune --quiet --yes
--
-- Alternatively, use the following image: timescale/timescaledb-ha:pg18
-- WARNING: Its size is more than 1GB as it is containing TimescaleDB and Patroni for High Availability
-- =====================================================

-- =====================================================
-- CREATE EXTENSION
-- =====================================================
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- =====================================================
-- SENSOR METRICS Reset and introduce partitioning
-- =====================================================
DROP TABLE IF EXISTS sensor_metrics_ht;

-- =====================================================
-- SENSOR METRICS TABLE (RAW TIME-SERIES HEAP TABLE)
-- Demonstrates: Unoptimized append-only time-series storage
-- NO indexes, NO partitioning
-- =====================================================
CREATE TABLE sensor_metrics_ht
(
    sensor_id   INT REFERENCES sensors (sensor_id),
    ts          TIMESTAMP,
    temperature DOUBLE PRECISION,
    cpu_usage   DOUBLE PRECISION,
    status      TEXT
);


-- =====================================================
-- CREATE HYPERTABLE
-- Demonstrates: Converting a regular PostgreSQL table
-- into a TimescaleDB hypertable (automatic partitioning)
-- =====================================================

SELECT create_hypertable(
               'sensor_metrics_ht',
               'ts',
               chunk_time_interval => INTERVAL '7 days',
               create_default_indexes => TRUE
       );

-- =====================================================
-- CREATE INDEX FOR SENSOR + TIME LOOKUPS
-- Demonstrates: Efficient filtering and ordering inside hypertable chunks
--
-- Before (raw PostgreSQL)
-- BRIN needed for large tables
-- manual partitions reduce size
--
-- After (TimescaleDB)
-- chunks already small
-- B-tree becomes efficient again
-- system handles data locality
-- =====================================================
CREATE INDEX idx_sensor_metrics_ht_sensor_ts
    ON sensor_metrics_ht (sensor_id, ts DESC);

-- =====================================================
-- DATA GENERATION (VARIABLE EVENT STREAM)
-- Demonstrates: Irregular time intervals (1ms - 1000ms)
-- Adjustable dataset size via parameter n
-- =====================================================
WITH RECURSIVE params AS (SELECT 10000000 AS n),
               base AS (SELECT i,
                               (random() * 1000)::int           AS ms,
                               (1 + floor(random() * 200))::int AS sensor_id
                        FROM generate_series(1, (SELECT n FROM params)) i),
               timeline AS (SELECT i,
                                   sensor_id,
                                   ms,
                                   SUM(ms) OVER (ORDER BY i) AS cum_ms
                            FROM base),
               final AS (SELECT sensor_id,
                                (TIMESTAMP '2026-01-01 00:00:00'
                                    + (cum_ms * INTERVAL '1 millisecond')) AS ts,
                                20 + random() * 15                         AS temperature,
                                random() * 100                             AS cpu_usage,
                                CASE
                                    WHEN random() > 0.97 THEN 'FAIL'
                                    ELSE 'OK'
                                    END                                    AS status
                         FROM timeline)
INSERT
INTO sensor_metrics_ht (sensor_id, ts, temperature, cpu_usage, status)
SELECT sensor_id, ts, temperature, cpu_usage, status
FROM final;

-- =====================================================
-- TIME RANGE QUERY (CHUNK PRUNING)
-- Demonstrates: Only relevant chunks are scanned
-- =====================================================

SELECT *
FROM sensor_metrics_ht
WHERE ts >= '2026-02-01'
  AND ts < '2026-02-08';


-- =====================================================
-- SENSOR + TIME FILTER
-- Demonstrates: Chunk pruning + index usage
-- =====================================================

SELECT *
FROM sensor_metrics_ht
WHERE sensor_id = 42
  AND ts >= '2026-02-01'
  AND ts < '2026-02-15';


-- =====================================================
-- LATEST EVENTS (TOP-N QUERY)
-- Demonstrates: Efficient ORDER BY using index
-- across chunks
-- =====================================================

SELECT *
FROM sensor_metrics_ht
ORDER BY ts DESC
LIMIT 100;


-- =====================================================
-- RECENT FAILURES
-- Demonstrates: Time pruning + selective filtering
-- =====================================================

SELECT *
FROM sensor_metrics_ht
WHERE status = 'FAIL'
  AND ts >= now() - interval '2 months 20 day';


-- =====================================================
-- WEEKLY AGGREGATION
-- Demonstrates: Aggregation across time chunks
-- =====================================================

SELECT date_trunc('week', ts) AS week_bucket,
       avg(cpu_usage)
FROM sensor_metrics_ht
GROUP BY week_bucket
ORDER BY week_bucket;


-- =====================================================
-- SENSOR ACTIVITY SUMMARY
-- Demonstrates: Aggregation with pre-filtering
-- =====================================================

SELECT sensor_id,
       count(*)       AS events,
       avg(cpu_usage) AS avg_cpu
FROM sensor_metrics_ht
WHERE ts >= '2026-02-01'
  AND ts < '2026-03-01'
GROUP BY sensor_id
ORDER BY events DESC;

-- =====================================================
-- TIME-BASED AGGREGATION (SCALABLE GROUPING)
-- Demonstrates: Aggregation over time buckets with
-- reduced data scanning via chunk pruning
-- =====================================================

SELECT date_trunc('hour', ts) AS hour_bucket,
       avg(cpu_usage)         AS avg_cpu
FROM sensor_metrics_ht
WHERE ts >= now() - interval '2 months 20 days'
GROUP BY hour_bucket
ORDER BY hour_bucket;


-- =====================================================
-- HIGH-LOAD SENSOR DETECTION
-- Demonstrates: Efficient aggregation with filtering
-- inside limited chunks
-- =====================================================

SELECT sensor_id,
       count(*)       AS events,
       avg(cpu_usage) AS avg_cpu
FROM sensor_metrics_ht
WHERE ts >= now() - interval '2 months 20 days'
GROUP BY sensor_id
ORDER BY avg_cpu DESC
LIMIT 10;


-- =====================================================
-- FAILURE ANALYSIS (TIME + STATUS FILTER)
-- Demonstrates: Combined filtering benefits from
-- chunk pruning and index usage
-- =====================================================

SELECT *
FROM sensor_metrics_ht
WHERE status = 'FAIL'
  AND ts >= now() - interval '2 months 20 days';


-- =====================================================
-- MULTI-DIMENSION ANALYTICS (CITY + TIME)
-- Demonstrates: Joining dimension table with hypertable
-- while still benefiting from chunk pruning
-- =====================================================

SELECT s.city,
       count(*)         AS total_events,
       avg(m.cpu_usage) AS avg_cpu
FROM sensor_metrics_ht m
         JOIN sensors s ON s.sensor_id = m.sensor_id
WHERE m.ts >= now() - interval '2 months'
GROUP BY s.city
ORDER BY total_events DESC;


-- =====================================================
-- SLIDING WINDOW ANALYSIS
-- Demonstrates: Window functions over recent data
-- with limited chunk scanning
-- =====================================================

SELECT sensor_id,
       ts,
       avg(cpu_usage) OVER (
           PARTITION BY sensor_id
           ORDER BY ts
           RANGE BETWEEN INTERVAL '5 minutes' PRECEDING AND CURRENT ROW
           ) AS moving_avg_cpu
FROM sensor_metrics_ht
WHERE ts >= now() - interval '1 day';

--------------------------------------------------------
-- TIMESCALE DB Specific Functions
--------------------------------------------------------
-- =====================================================
-- TIME_BUCKET (Hourly Average Temperature)
-- Demonstrates: Basic time aggregation
-- =====================================================

SELECT time_bucket('1 hour', ts) AS bucket,
       AVG(temperature)          AS avg_temp
FROM sensor_metrics
GROUP BY bucket
ORDER BY bucket;

-- =====================================================
-- TIME_BUCKET PER SENSOR
-- Demonstrates: Grouping by time + dimension
-- =====================================================

SELECT s.sensor_name,
       time_bucket('1 hour', m.ts) AS bucket,
       AVG(m.temperature)          AS avg_temp
FROM sensor_metrics m
         JOIN sensors s ON s.sensor_id = m.sensor_id
GROUP BY s.sensor_name, bucket
ORDER BY s.sensor_name, bucket;


-- =====================================================
-- TIME_BUCKET (Daily Aggregation)
-- Demonstrates: Changing granularity
-- =====================================================

SELECT time_bucket('1 day', ts) AS day,
       MIN(temperature)         AS min_temp,
       MAX(temperature)         AS max_temp
FROM sensor_metrics
GROUP BY day
ORDER BY day;


-- =====================================================
-- TIME_BUCKET_GAPFILL (Fill Missing Intervals)
-- Demonstrates: Handling gaps in time-series
-- =====================================================

SELECT time_bucket_gapfill(
               '1 hour',
               ts,
               NOW() - INTERVAL '3 months', -- start
               NOW() -- finish
       )                AS bucket,
       AVG(temperature) AS avg_temp
FROM sensor_metrics
WHERE ts >= NOW() - INTERVAL '3 months'
GROUP BY bucket
ORDER BY bucket;


-- =====================================================
-- GAPFILL + COALESCE
-- Demonstrates: Replace missing values with defaults
-- =====================================================

SELECT time_bucket_gapfill(
               '1 hour',
               ts,
               NOW() - INTERVAL '3 months', -- start
               NOW() -- finish
       )                             AS bucket,
       COALESCE(AVG(temperature), 0) AS avg_temp
FROM sensor_metrics
WHERE ts >= NOW() - INTERVAL '1 day'
GROUP BY bucket
ORDER BY bucket;


-- =====================================================
-- GAPFILL PER SENSOR
-- Demonstrates: Time-series + dimension + gap filling
-- =====================================================

SELECT s.sensor_name,
       time_bucket_gapfill(
               '1 hour',
               ts,
               NOW() - INTERVAL '3 months', -- start
               NOW() -- finish
       )                               AS bucket,
       COALESCE(AVG(m.temperature), 0) AS avg_temp
FROM sensor_metrics m
         JOIN sensors s ON s.sensor_id = m.sensor_id
WHERE m.ts >= NOW() - INTERVAL '3 months'
GROUP BY s.sensor_name, bucket
ORDER BY s.sensor_name, bucket;

select show_chunks('sensor_metrics_ht')

-- =====================================================
-- CONTINUOUS AGGREGATE (Hourly Avg Temperature)
-- Demonstrates: Materialized time-series view
-- =====================================================

CREATE MATERIALIZED VIEW sensor_metrics_hourly_avg
    WITH (timescaledb.continuous) AS
SELECT sensor_id,
       time_bucket('1 hour', ts) AS bucket,
       AVG(temperature)          AS avg_temp,
       AVG(cpu_usage)            AS avg_cpu_usage
FROM sensor_metrics_ht
GROUP BY sensor_id, bucket;


-- =====================================================
-- Query Continuous Aggregate
-- Demonstrates: Fast query (pre-aggregated data)
-- =====================================================

SELECT *
FROM sensor_metrics_hourly_avg
ORDER BY bucket DESC
LIMIT 20;


-- =====================================================
-- REFRESH POLICY
-- Demonstrates: Automatic background refresh
-- start_offset: how far back to refresh
-- end_offset: ignore most recent (still changing)
-- =====================================================

SELECT add_continuous_aggregate_policy(
               'sensor_metrics_hourly_avg',
               start_offset => INTERVAL '15 days',
               end_offset => INTERVAL '1 hour',
               schedule_interval => INTERVAL '5 minutes'
       );


-- =====================================================
-- MANUAL REFRESH (for demo/testing)
-- =====================================================

CALL refresh_continuous_aggregate(
        'sensor_metrics_hourly_avg',
        (NOW() AT TIME ZONE 'UTC')::timestamp - INTERVAL '1 day',
        (NOW() AT TIME ZONE 'UTC')::timestamp
     );


-- =====================================================
-- DAILY AGGREGATE ON TOP OF HOURLY
-- Demonstrates: Hierarchical aggregation (very powerful)
-- =====================================================

CREATE MATERIALIZED VIEW sensor_daily_avg
    WITH (timescaledb.continuous) AS
SELECT sensor_id,
       time_bucket('1 day', bucket) AS day,
       AVG(avg_temp)                AS avg_temp
FROM sensor_metrics_hourly_avg
GROUP BY sensor_id, day;

SELECT add_continuous_aggregate_policy(
               'sensor_daily_avg',
               start_offset => INTERVAL '15 days',
               end_offset => INTERVAL '1 hour',
               schedule_interval => INTERVAL '5 minutes'
       );
-- =====================================================
-- Query Daily Aggregate
-- =====================================================

SELECT *
FROM sensor_daily_avg
ORDER BY day DESC
LIMIT 20;


-- =====================================================
-- RETENTION POLICY (Raw Data)
-- Demonstrates: Automatically delete old data
-- =====================================================

SELECT add_retention_policy(
               'sensor_metrics_ht',
               INTERVAL '6 months'
       );


-- =====================================================
-- RETENTION POLICY FOR AGGREGATES
-- Demonstrates: Keep aggregates longer than raw data
-- =====================================================

SELECT add_retention_policy(
               'sensor_metrics_hourly_avg',
               INTERVAL '6 months'
       );
SELECT add_retention_policy(
               'sensor_daily_avg',
               INTERVAL '6 months'
       );


-- =====================================================
-- COMPRESSION POLICY (Optional but Powerful)
-- Demonstrates: Reduce storage for older data
-- =====================================================

ALTER TABLE sensor_metrics_ht
    SET (timescaledb.compress);

SELECT add_compression_policy(
               'sensor_metrics_ht',
               INTERVAL '3 months'
       );


-- =====================================================
-- INSPECT JOBS (Background Workers)
-- Demonstrates: View scheduled policies
-- =====================================================

SELECT *
FROM timescaledb_information.jobs;

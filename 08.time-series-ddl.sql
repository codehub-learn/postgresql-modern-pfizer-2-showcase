-- =====================================================
-- CLEAN START (RESET ENVIRONMENT)
-- Demonstrates: Fresh baseline for time-series lab
-- =====================================================

DROP TABLE IF EXISTS sensor_metrics;
DROP TABLE IF EXISTS sensors;

-- =====================================================
-- SENSOR DIMENSION TABLE
-- Demonstrates: Simple dimension table with city metadata
-- =====================================================
CREATE TABLE sensors
(
    sensor_id   INT PRIMARY KEY,
    city        TEXT,
    sensor_name TEXT
);
INSERT INTO sensors (sensor_id, city, sensor_name)
SELECT i                                                AS sensor_id,
       city_name                                        AS city,
       city_name || '-sensor-' || lpad(i::text, 3, '0') AS sensor_name
FROM generate_series(1, 500) i,
     LATERAL (
         SELECT (ARRAY ['Athens','Thessaloniki','Patras','Heraklion','Larisa', 'Ioannina', 'Volos'])[
                    1 + floor(random() * (i * 0 + 7))::int -- i * 0 forces dependency on i
                    ] AS city_name
         ) c;

-- =====================================================
-- SENSOR METRICS TABLE (RAW TIME-SERIES HEAP TABLE)
-- Demonstrates: Unoptimized append-only time-series storage
-- NO indexes, NO partitioning
-- =====================================================
CREATE TABLE sensor_metrics
(
    sensor_id   INT REFERENCES sensors (sensor_id),
    ts          TIMESTAMP,
    temperature DOUBLE PRECISION,
    cpu_usage   DOUBLE PRECISION,
    status      TEXT
);

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
INTO sensor_metrics (sensor_id, ts, temperature, cpu_usage, status)
SELECT sensor_id, ts, temperature, cpu_usage, status
FROM final;

-- =====================================================
-- INSTALL PG Cron extension (https://access.crunchydata.com/documentation/pg_cron/latest/, https://github.com/citusdata/pg_cron)
--
-- RUN
-- apt-get update && apt-get install -y postgresql-18-cron
-- Update postgresql.conf:
-- - Add setting cron.database_name = 'postgres'
-- - Update setting shared_preload_libraries and add 'pg_cron'
-- Restart postgresql
-- =====================================================

-- =====================================================
-- CREATE EXTENSION
-- =====================================================

CREATE EXTENSION IF NOT EXISTS pg_cron;

-- =====================================================
-- Cron configuration
-- =====================================================

SELECT *
FROM pg_settings
WHERE name LIKE 'cron%';

-- =====================================================
-- JOB SCHEDULING
-- Demonstrates: Create a new job to run every minute
-- =====================================================

SELECT cron.schedule('my-job', '* * * * *', 'NOTIFY heartbeat, ''ping''');

-- =====================================================
-- JOB SHECULING
-- Demonstrates: Create a job combined with NOTIFY
-- =====================================================

SELECT cron.schedule(
               'mq_worker_notify',
               '*/1 * * * *',
               $$
    WITH batch AS (
        SELECT id, event_type
        FROM mq.queue
        WHERE processed = FALSE
        ORDER BY created_at ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 5
    ),
    updated AS (
        UPDATE mq.queue
        SET processed = TRUE
        WHERE id IN (SELECT id FROM batch)
        RETURNING id, event_type
    )
    SELECT pg_notify(
        'mq_channel',
        json_build_object('processed', json_agg(updated.*))::text
    ) FROM updated;
    $$
       );

-- =====================================================
-- JOB LISTING
-- Demonstrates: Show scheduled jobs
-- =====================================================

SELECT *
FROM cron.job;

-- =====================================================
-- JOB LISTING
-- Demonstrates: Show scheduled jobs run history
-- =====================================================

SELECT *
FROM cron.job_run_details
ORDER BY start_time DESC
LIMIT 100;

-- =====================================================
-- MODIFY JOB SCHEDULE
-- Demonstrates: Change existing job's schedule
-- =====================================================

SELECT cron.alter_job(4, '*/2 * * * *');

-- =====================================================
-- JOB REMOVAL
-- Demonstrates: Remove a job from schedule
-- =====================================================

SELECT cron.unschedule('my-job');
SELECT cron.unschedule('mq_worker_notify');

-- =====================================================
-- Simple Bounded Counter (Very Light)
-- Demonstrates: Basic recursion structure
-- Resource impact: Minimal
-- =====================================================

WITH RECURSIVE counter AS (
    -- Anchor
    SELECT 1 AS n

    UNION ALL

    -- Recursive step (strictly bounded)
    SELECT n + 1
    FROM counter
    WHERE n < 10)
SELECT *
FROM counter;


-- =====================================================
-- Generate Small Date Range
-- Demonstrates: Controlled time series
-- Resource impact: Small, bounded output
-- =====================================================

WITH RECURSIVE dates AS (SELECT DATE '2026-01-01' AS d

                         UNION ALL

                         SELECT (d + INTERVAL '1 day')::date
                         FROM dates
                         WHERE d < DATE '2026-01-07')
SELECT *
FROM dates;


-- =====================================================
-- Simple Region → Nation Traversal (Small Hierarchy)
-- Demonstrates: Hierarchical recursion
-- Resource impact: Very low (dataset small)
-- =====================================================

WITH RECURSIVE geo AS (
    -- Anchor: Regions
    SELECT r_regionkey AS id,
           r_name      AS name,
           1           AS level
    FROM region

    UNION ALL

    -- Recursive: Nations under each region
    SELECT n.n_nationkey,
           n.n_name,
           geo.level + 1
    FROM nation n
             JOIN geo
                  ON n.n_regionkey = geo.id
    WHERE geo.level = 1)
SELECT *
FROM geo
ORDER BY level, id;


-- =====================================================
-- Depth-Limited Recursion (Best Practice Pattern)
-- Demonstrates: Safety control using depth limit
-- Resource impact: Predictable and safe
-- =====================================================

WITH RECURSIVE numbered AS (SELECT 1 AS id, 1 AS depth

                            UNION ALL

                            SELECT id + 1, depth + 1
                            FROM numbered
                            WHERE depth < 3)
SELECT *
FROM numbered;


-- =====================================================
-- Controlled Hierarchy Count
-- Demonstrates: Measuring structure size
-- Resource impact: Very light
-- =====================================================

WITH RECURSIVE geo AS (SELECT r_regionkey AS id,
                              r_name      AS name,
                              1           AS level
                       FROM region

                       UNION ALL

                       SELECT n.n_nationkey,
                              n.n_name,
                              geo.level + 1
                       FROM nation n
                                JOIN geo
                                     ON n.n_regionkey = geo.id
                       WHERE geo.level = 1)
SELECT COUNT(*)
FROM geo;

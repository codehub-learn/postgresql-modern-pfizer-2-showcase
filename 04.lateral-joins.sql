-- =====================================================
-- Latest Order Per Customer
-- Demonstrates: Correlated subquery using LATERAL
-- Very common production pattern
-- =====================================================

SELECT c.c_custkey,
       c.c_name,
       o.*
FROM customer c
         CROSS JOIN LATERAL (
    SELECT *
    FROM orders o
    WHERE o.o_custkey = c.c_custkey
    ORDER BY o.o_orderdate DESC
    LIMIT 1
    ) o;


-- =====================================================
-- Top 3 Orders Per Customer
-- Demonstrates: LIMIT inside LATERAL
-- Efficient alternative to complex window filtering
-- =====================================================

SELECT c.c_custkey,
       c.c_name,
       o.*
FROM customer c
         CROSS JOIN LATERAL (
    SELECT *
    FROM orders o
    WHERE o.o_custkey = c.c_custkey
    ORDER BY o.o_totalprice DESC
    LIMIT 3
    ) o;


-- =====================================================
-- Customer With Their Total + Latest Order
-- Demonstrates: Combining aggregation + LATERAL
-- =====================================================

SELECT c.c_custkey,
       c.c_name,
       totals.total_spent,
       latest.o_orderdate
FROM customer c
         JOIN LATERAL (
    SELECT SUM(o_totalprice) AS total_spent
    FROM orders o
    WHERE o.o_custkey = c.c_custkey
    ) totals ON TRUE
         LEFT JOIN LATERAL (
    SELECT o_orderdate
    FROM orders o
    WHERE o.o_custkey = c.c_custkey
    ORDER BY o_orderdate DESC
    LIMIT 1
    ) latest ON TRUE;


-- =====================================================
-- Exploding Data with LATERAL (Row Expansion Pattern)
-- Demonstrates: Using LATERAL with set-returning logic
-- =====================================================

SELECT c.c_custkey,
       o.*
FROM customer c
         LEFT JOIN LATERAL (
    SELECT *
    FROM orders o
    WHERE o.o_custkey = c.c_custkey
    ) o ON TRUE;


-- =====================================================
-- LATERAL for Conditional Filtering
-- Demonstrates: Compute first, then filter
-- =====================================================

SELECT c.c_custkey,
       stats.order_count
FROM customer c
         JOIN LATERAL (
    SELECT COUNT(*) AS order_count
    FROM orders o
    WHERE o.o_custkey = c.c_custkey
    ) stats ON stats.order_count > 5;


-- =====================================================
-- LATERAL with Aggregation
-- Demonstrates: Per-row computation pattern
-- =====================================================

SELECT c.c_custkey,
       summary.total_spent
FROM customer c
         JOIN LATERAL (
    SELECT SUM(o_totalprice) AS total_spent
    FROM orders o
    WHERE o.o_custkey = c.c_custkey
    ) summary ON TRUE
ORDER BY summary.total_spent DESC;


-- =====================================================
-- LATERAL with Top-N + Join
-- Demonstrates: Real-world analytical pattern
-- =====================================================

SELECT r.r_name,
       top_customers.*
FROM region r
         JOIN nation n ON n.n_regionkey = r.r_regionkey
         JOIN customer c ON c.c_nationkey = n.n_nationkey
         JOIN LATERAL (
    SELECT c.c_custkey,
           SUM(o.o_totalprice) AS total_spent
    FROM orders o
    WHERE o.o_custkey = c.c_custkey
    GROUP BY c.c_custkey
    ) top_customers ON TRUE
ORDER BY r.r_name, total_spent DESC NULLS LAST;

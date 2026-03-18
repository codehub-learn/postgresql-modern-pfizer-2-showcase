-- =====================================================
-- Join JSON Event Data with Customer Table
-- Demonstrates: Extract JSON field to join relational data
-- =====================================================

SELECT c.c_custkey,
       c.c_name,
       e.event_data ->> 'event_type' AS event_type,
       e.created_at
FROM customer_events e
         JOIN customer c
              ON c.c_custkey = e.customer_id
LIMIT 20;



-- =====================================================
-- Find Customers Who Made Purchases (JSON Filter + Join)
-- Demonstrates: Filtering JSON before joining
-- =====================================================

SELECT c.c_custkey,
       c.c_name,
       e.event_data ->> 'event_type' AS event_type,
       e.event_data ->> 'amount'     AS purchase_amount
FROM customer_events e
         JOIN customer c
              ON c.c_custkey = e.customer_id
WHERE e.event_data ->> 'event_type' = 'purchase'
LIMIT 20;



-- =====================================================
-- Join Orders with JSON Events
-- Demonstrates: Combining structured transactions with event logs
-- =====================================================

SELECT o.o_orderkey,
       o.o_totalprice,
       e.event_data ->> 'event_type' AS event_type,
       e.created_at
FROM orders o
         JOIN customer_events e
              ON e.customer_id = o.o_custkey
WHERE e.event_data ->> 'event_type' = 'purchase'
LIMIT 20;



-- =====================================================
-- Aggregate JSON Events per Customer
-- Demonstrates: Relational GROUP BY with JSON extraction
-- =====================================================

SELECT c.c_name,
       e.event_data ->> 'event_type' AS event_type,
       COUNT(*)                      AS event_count
FROM customer_events e
         JOIN customer c
              ON c.c_custkey = e.customer_id
GROUP BY c.c_name, event_type
ORDER BY event_count DESC
LIMIT 20;



-- =====================================================
-- Join Nested JSON Fields
-- Demonstrates: Extract nested JSON metadata
-- =====================================================

SELECT c.c_name,
       e.event_data -> 'metadata' ->> 'browser' AS browser,
       COUNT(*)                                 AS events
FROM customer_events e
         JOIN customer c
              ON c.c_custkey = e.customer_id
GROUP BY c.c_name, browser
ORDER BY events DESC
LIMIT 20;



-- =====================================================
-- Join JSON Events with LATERAL for Analysis
-- Demonstrates: LATERAL join for per-customer event statistics
-- =====================================================

SELECT c.c_name,
       stats.total_events,
       stats.total_purchases
FROM customer c
         JOIN LATERAL (
    SELECT COUNT(*) AS total_events,
           COUNT(*) FILTER (
               WHERE event_data ->> 'event_type' = 'purchase'
               )    AS total_purchases
    FROM customer_events e
    WHERE e.customer_id = c.c_custkey
    ) stats ON TRUE
LIMIT 20;



-- =====================================================
-- Combine JSON Path Filtering with Relational Join
-- Demonstrates: Advanced JSON filtering in joins
-- =====================================================

SELECT c.c_name,
       e.created_at,
       e.event_data
FROM customer_events e
         JOIN customer c
              ON c.c_custkey = e.customer_id
WHERE jsonb_path_exists(
              e.event_data,
              '$.metadata ? (@.browser == "chrome")'
      )
LIMIT 20;



-- =====================================================
-- Customers with High-Value Purchases
-- Demonstrates: JSON numeric extraction with relational joins
-- =====================================================

SELECT c.c_name,
       (e.event_data ->> 'amount')::INT AS purchase_amount,
       e.created_at
FROM customer_events e
         JOIN customer c
              ON c.c_custkey = e.customer_id
WHERE e.event_data ->> 'event_type' = 'purchase'
  AND (e.event_data ->> 'amount')::INT > 500
ORDER BY purchase_amount DESC
LIMIT 20;

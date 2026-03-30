-- =====================================================
-- CREATE EXTENSION (ONLY if used the Docker image or compiled it manually)
-- =====================================================
CREATE EXTENSION IF NOT EXISTS pgmq;


-- =====================================================
-- PGMQ EXTENSION SETUP
-- Demonstrates: Installing and enabling the extension
-- =====================================================

CREATE EXTENSION IF NOT EXISTS pgmq;

-- =====================================================
-- QUEUE MANAGEMENT
-- Demonstrates: Creating, listing, and dropping queues
-- =====================================================

-- Create a standard queue
SELECT pgmq.create('my_queue');

-- Create a partitioned queue (requires pg_partman)
SELECT pgmq.create_partitioned('my_partitioned_queue');

-- Create an unlogged queue (skips WAL, faster writes, not crash-safe, does not replicate)
SELECT pgmq.create_unlogged('my_unlogged_queue');

-- List all queues
SELECT *
FROM pgmq.list_queues();

-- Drop a queue (and all its messages, archived messages, partitions, etc)
SELECT pgmq.drop_queue('my_unlogged_queue');


-- =====================================================
-- SENDING MESSAGES
-- Demonstrates: Enqueuing single and batch messages
-- =====================================================

-- Send a single message
SELECT *
FROM pgmq.send(
        queue_name => 'my_queue',
        msg => '{"event": "user_signup", "user_id": 42}'
     );

-- Send a message with a delay (visible after 10 seconds)
SELECT *
FROM pgmq.send(
        queue_name => 'my_queue',
        msg => '{"event": "send_email", "to": "user@example.com"}',
        delay => 10
     );

-- Send a batch of messages at once
SELECT *
FROM pgmq.send_batch(
        queue_name => 'my_queue',
        msgs => ARRAY [
            '{
              "event": "job_1",
              "payload": "a"
            }'::jsonb,
            '{
              "event": "job_2",
              "payload": "b"
            }'::jsonb,
            '{
              "event": "job_3",
              "payload": "c"
            }'::jsonb
            ]
     );

-- Send a batch with delay
SELECT *
FROM pgmq.send_batch(
        queue_name => 'my_queue',
        msgs => ARRAY [
            '{
              "event": "scheduled_1"
            }'::jsonb,
            '{
              "event": "scheduled_2"
            }'::jsonb
            ],
        delay => 30
     );


-- =====================================================
-- BASIC QUEUE CONSUMPTION
-- Demonstrates: Pull-based message retrieval
-- =====================================================

-- Read 1 message (visibility timeout = 30s)
SELECT *
FROM pgmq.read(
        queue_name => 'my_queue',
        vt => 30,
        qty => 1
     );

-- Read up to 5 messages (visibility timeout = 60s)
SELECT *
FROM pgmq.read(
        queue_name => 'my_queue',
        vt => 60,
        qty => 5
     );

-- Read with polling (waits up to 5s for a message to arrive)
SELECT *
FROM pgmq.read_with_poll(
        queue_name => 'my_queue',
        vt => 30,
        qty => 1,
        max_poll_seconds => 5
     );

-- Pop: read + delete atomically (no need to delete manually)
SELECT *
FROM pgmq.pop('my_queue');


-- =====================================================
-- MESSAGE LIFECYCLE
-- Demonstrates: Deleting and archiving consumed messages
-- =====================================================

-- Delete a single message after processing
SELECT pgmq.delete('my_queue', 1);

-- Delete multiple messages by ID
SELECT pgmq.delete('my_queue', ARRAY [2, 3, 4]);

-- Archive a message (moves to pgmq.a_my_queue for audit trail)
SELECT pgmq.archive(
               queue_name => 'my_queue',
               msg_id => 5
       );

-- Archive multiple messages
SELECT pgmq.archive(
               queue_name => 'my_queue',
               msg_ids => ARRAY [6, 7, 8]
       );

-- View archived messages
SELECT *
FROM pgmq.a_my_queue;


-- =====================================================
-- VISIBILITY TIMEOUT MANAGEMENT
-- Demonstrates: Extending or resetting message locks
-- =====================================================

-- Extend visibility timeout for a message being processed
-- (resets the clock by another 60 seconds)
SELECT pgmq.set_vt(
               queue_name => 'my_queue',
               msg_id => 9,
               vt => 60
       );


-- =====================================================
-- QUEUE INSPECTION
-- Demonstrates: Monitoring queue depth and message state
-- =====================================================

-- Get queue metrics (depth, hidden count, oldest message age)
SELECT *
FROM pgmq.metrics('my_queue');

-- Get metrics for all queues at once
SELECT *
FROM pgmq.metrics_all();

-- Peek at the next N messages without locking them
SELECT *
FROM pgmq.read(
        queue_name => 'my_queue',
        vt => 0, -- vt=0 means immediately visible again
        qty => 10
     );

-- View all messages currently in the queue table directly
SELECT *
FROM pgmq.q_my_queue;


-- =====================================================
-- PGMQ DOCKER DEMO
-- =====================================================

-- =====================================================
-- STEP 1: CREATE QUEUES
-- Demonstrates: Different queue types
-- =====================================================

-- Standard durable queue
SELECT pgmq.create('orders');

-- Unlogged queue (faster, not crash-safe)
SELECT pgmq.create_unlogged('notifications');

-- Confirm both queues exist
SELECT queue_name, is_unlogged, created_at
FROM pgmq.list_queues();


-- =====================================================
-- STEP 3: SEND MESSAGES
-- Demonstrates: Enqueuing single, delayed, batch messages
-- =====================================================

-- Send a single order event
SELECT *
FROM pgmq.send(
        queue_name => 'orders',
        msg => '{"order_id": 1001, "item": "keyboard", "qty": 1}',
        headers => '{"trace_id": "D123456789"}'
     );

-- Send another
SELECT *
FROM pgmq.send(
        queue_name => 'orders',
        msg => '{"order_id": 1002, "item": "monitor", "qty": 2}'
     );

-- Send a delayed message (visible after 15 seconds)
SELECT *
FROM pgmq.send(
        queue_name => 'orders',
        msg => '{"order_id": 1003, "item": "mouse", "qty": 1}',
        delay => 15
     );

-- Send a batch of notifications
SELECT *
FROM pgmq.send_batch(
        queue_name => 'notifications',
        msgs => ARRAY [
            '{
              "user_id": 1,
              "type": "email",
              "body": "Your order shipped!"
            }'::jsonb,
            '{
              "user_id": 2,
              "type": "sms",
              "body": "Flash sale starts now!"
            }'::jsonb,
            '{
              "user_id": 3,
              "type": "push",
              "body": "You have a new message."
            }'::jsonb
            ]
     );

-- Check queue depth after sending
SELECT *
FROM pgmq.metrics('orders');
SELECT *
FROM pgmq.metrics('notifications');


-- =====================================================
-- STEP 4: CONSUME MESSAGES
-- Demonstrates: Reading with visibility timeout
-- =====================================================

-- Read 1 order (locked for 30 seconds)
SELECT *
FROM pgmq.read(
        queue_name => 'orders',
        vt => 30,
        qty => 1
     );

-- Read up to 3 notifications
SELECT *
FROM pgmq.read(
        queue_name => 'notifications',
        vt => 30,
        qty => 3
     );

-- Read with polling — waits up to 5s for a message
SELECT *
FROM pgmq.read_with_poll(
        queue_name => 'orders',
        vt => 30,
        qty => 1,
        max_poll_seconds => 5
     );


-- =====================================================
-- STEP 5: ACK / DELETE MESSAGES
-- Demonstrates: Acknowledging processed messages
-- =====================================================

-- Delete message with msg_id = 1 (replace with actual id from step 4)
SELECT pgmq.delete('orders', 1);

-- Delete multiple messages at once
SELECT pgmq.delete('notifications', ARRAY [1, 2, 3]);

-- Pop: read + delete in one atomic call (no manual delete needed)
SELECT *
FROM pgmq.pop('orders');


-- =====================================================
-- STEP 6: ARCHIVE MESSAGES
-- Demonstrates: Keeping a processed message audit trail
-- =====================================================

-- Send a fresh message to archive
SELECT *
FROM pgmq.send(
        queue_name => 'orders',
        msg => '{"order_id": 1004, "item": "desk", "qty": 1}'
     );

-- Read it to get the msg_id
SELECT *
FROM pgmq.read(
        queue_name => 'orders',
        vt => 30,
        qty => 1
     );

-- Archive it instead of deleting (replace 4 with actual msg_id)
SELECT pgmq.archive(queue_name => 'orders', msg_id => 4);

-- View the archive
SELECT *
FROM pgmq.a_orders;


-- =====================================================
-- STEP 7: VISIBILITY TIMEOUT EXTENSION
-- Demonstrates: Extending lock on a slow-processing message
-- =====================================================

-- Send a message
SELECT *
FROM pgmq.send(
        queue_name => 'orders',
        msg => '{"order_id": 1005, "item": "chair", "qty": 3}'
     );

-- Read it (locked for 10 seconds)
SELECT *
FROM pgmq.read(
        queue_name => 'orders',
        vt => 10,
        qty => 1
     );

-- Extend by another 60 seconds before it expires (replace 5 with actual msg_id)
SELECT pgmq.set_vt(
               queue_name => 'orders',
               msg_id => 5,
               vt => 60
       );


-- =====================================================
-- STEP 8: INSPECT & MONITOR
-- Demonstrates: Metrics and raw table inspection
-- =====================================================

-- Metrics for a specific queue
SELECT *
FROM pgmq.metrics('orders');

-- Metrics for all queues
SELECT *
FROM pgmq.metrics_all();

-- Peek directly at the raw queue table
SELECT *
FROM pgmq.q_orders;
SELECT *
FROM pgmq.q_notifications;


-- =====================================================
-- STEP 9: CLEANUP
-- Demonstrates: Dropping queues
-- =====================================================

-- Drop queue and archive
SELECT pgmq.drop_queue('orders', true);
SELECT pgmq.drop_queue('notifications', true);

-- Confirm all gone
SELECT queue_name
FROM pgmq.list_queues();

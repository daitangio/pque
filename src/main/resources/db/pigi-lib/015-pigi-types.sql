
-- liquibase formatted sql
-- changeset GG:1 runOnChange:false
-- comment: Pigi types

-- This type has the shape of a message in a queue, and is often returned by
-- pgmq functions that return messages
CREATE TYPE pigi_message_record AS (
    msg_id BIGINT,
    read_ct INTEGER,
    enqueued_at TIMESTAMP WITH TIME ZONE,
    vt TIMESTAMP WITH TIME ZONE,
    message JSONB
);

CREATE TYPE pigi_queue_record AS (
    queue_name VARCHAR,
    is_partitioned BOOLEAN,
    is_unlogged BOOLEAN,
    created_at TIMESTAMP WITH TIME ZONE
);

-- returned by pigi_metrics() and pigi_metrics_all
CREATE TYPE pigi_metrics_result AS (
    queue_name text,
    queue_length bigint,
    newest_msg_age_sec int,
    oldest_msg_age_sec int,
    total_messages bigint,
    scrape_time timestamp with time zone
);

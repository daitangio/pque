-- liquibase formatted sql
-- changeset GG:1 runOnChange:true splitStatements:false
-- comment: Pque stored procedures

------------------------------------------------------------
-- Schema, tables, records, privileges, indexes, etc
-- Ported from https://github.com/tembo-io/pgmq version 1.4.2 August 2024
------------------------------------------------------------
-- We don't need to create the `pgmq` schema because it is automatically
-- created by postgres due to being declared in extension control file


------------------------------------------------------------
-- Functions
------------------------------------------------------------

-- a helper to format table names and check for invalid characters
CREATE OR REPLACE FUNCTION pque_format_table_name(queue_name text, prefix text)
RETURNS TEXT AS $$
BEGIN
    IF queue_name ~ '\$|;|--|'''
    THEN
        RAISE EXCEPTION 'queue name contains invalid characters: $, ;, --, or \''';
    END IF;
    RETURN lower(prefix || '_' || queue_name);
END;
$$ LANGUAGE plpgsql;

-- read
-- reads a number of messages from a queue, setting a visibility timeout on them
CREATE OR REPLACE FUNCTION pque_read(
    queue_name TEXT,
    vt INTEGER,
    qty INTEGER
)
RETURNS SETOF pque_message_record AS $$
DECLARE
    sql TEXT;
    qtable TEXT := pque_format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $QUERY$
        WITH cte AS
        (
            SELECT msg_id
            FROM pque_%I
            WHERE vt <= clock_timestamp()
            ORDER BY msg_id ASC
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE pque_%I m
        SET
            vt = clock_timestamp() + %L,
            read_ct = read_ct + 1
        FROM cte
        WHERE m.msg_id = cte.msg_id
        RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.vt, m.message;
        $QUERY$,
        qtable, qtable, make_interval(secs => vt)
    );
    RETURN QUERY EXECUTE sql USING qty;
END;
$$ LANGUAGE plpgsql;

---- read_with_poll
---- reads a number of messages from a queue, setting a visibility timeout on them
CREATE OR REPLACE FUNCTION pque_read_with_poll(
    queue_name TEXT,
    vt INTEGER,
    qty INTEGER,
    max_poll_seconds INTEGER DEFAULT 5,
    poll_interval_ms INTEGER DEFAULT 100
)
RETURNS SETOF pque_message_record AS $$
DECLARE
    r pque_message_record;
    stop_at TIMESTAMP;
    sql TEXT;
    qtable TEXT := pque_format_table_name(queue_name, 'q');
BEGIN
    stop_at := clock_timestamp() + make_interval(secs => max_poll_seconds);
    LOOP
      IF (SELECT clock_timestamp() >= stop_at) THEN
        RETURN;
      END IF;

      sql := FORMAT(
          $QUERY$
          WITH cte AS
          (
              SELECT msg_id
              FROM pque_%I
              WHERE vt <= clock_timestamp()
              ORDER BY msg_id ASC
              LIMIT $1
              FOR UPDATE SKIP LOCKED
          )
          UPDATE pque_%I m
          SET
              vt = clock_timestamp() + %L,
              read_ct = read_ct + 1
          FROM cte
          WHERE m.msg_id = cte.msg_id
          RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.vt, m.message;
          $QUERY$,
          qtable, qtable, make_interval(secs => vt)
      );

      FOR r IN
        EXECUTE sql USING qty
      LOOP
        RETURN NEXT r;
      END LOOP;
      IF FOUND THEN
        RETURN;
      ELSE
        PERFORM pg_sleep(poll_interval_ms / 1000);
      END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

---- archive
---- removes a message from the queue, and sends it to the archive, where its
---- saved permanently.
CREATE OR REPLACE FUNCTION pque_archive(
    queue_name TEXT,
    msg_id BIGINT
)
RETURNS BOOLEAN AS $$
DECLARE
    sql TEXT;
    result BIGINT;
    qtable TEXT := pque_format_table_name(queue_name, 'q');
    atable TEXT := pque_format_table_name(queue_name, 'a');
BEGIN
    sql := FORMAT(
        $QUERY$
        WITH archived AS (
            DELETE FROM pque_%I
            WHERE msg_id = $1
            RETURNING msg_id, vt, read_ct, enqueued_at, message
        )
        INSERT INTO pque_%I (msg_id, vt, read_ct, enqueued_at, message)
        SELECT msg_id, vt, read_ct, enqueued_at, message
        FROM archived
        RETURNING msg_id;
        $QUERY$,
        qtable, atable
    );
    EXECUTE sql USING msg_id INTO result;
    RETURN NOT (result IS NULL);
END;
$$ LANGUAGE plpgsql;

---- archive
---- removes an array of message ids from the queue, and sends it to the archive,
---- where these messages will be saved permanently.
CREATE OR REPLACE FUNCTION pque_archive(
    queue_name TEXT,
    msg_ids BIGINT[]
)
RETURNS SETOF BIGINT AS $$
DECLARE
    sql TEXT;
    qtable TEXT := pque_format_table_name(queue_name, 'q');
    atable TEXT := pque_format_table_name(queue_name, 'a');
BEGIN
    sql := FORMAT(
        $QUERY$
        WITH archived AS (
            DELETE FROM pque_%I
            WHERE msg_id = ANY($1)
            RETURNING msg_id, vt, read_ct, enqueued_at, message
        )
        INSERT INTO pque_%I (msg_id, vt, read_ct, enqueued_at, message)
        SELECT msg_id, vt, read_ct, enqueued_at, message
        FROM archived
        RETURNING msg_id;
        $QUERY$,
        qtable, atable
    );
    RETURN QUERY EXECUTE sql USING msg_ids;
END;
$$ LANGUAGE plpgsql;

---- delete
---- deletes a message id from the queue permanently
CREATE OR REPLACE FUNCTION pque_delete(
    queue_name TEXT,
    msg_id BIGINT
)
RETURNS BOOLEAN AS $$
DECLARE
    sql TEXT;
    result BIGINT;
    qtable TEXT := pque_format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $QUERY$
        DELETE FROM pque_%I
        WHERE msg_id = $1
        RETURNING msg_id
        $QUERY$,
        qtable
    );
    EXECUTE sql USING msg_id INTO result;
    RETURN NOT (result IS NULL);
END;
$$ LANGUAGE plpgsql;

---- delete
---- deletes an array of message ids from the queue permanently
CREATE OR REPLACE FUNCTION pque_delete(
    queue_name TEXT,
    msg_ids BIGINT[]
)
RETURNS SETOF BIGINT AS $$
DECLARE
    sql TEXT;
    qtable TEXT := pque_format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $QUERY$
        DELETE FROM pque_%I
        WHERE msg_id = ANY($1)
        RETURNING msg_id
        $QUERY$,
        qtable
    );
    RETURN QUERY EXECUTE sql USING msg_ids;
END;
$$ LANGUAGE plpgsql;

-- send
-- sends a message to a queue, optionally with a delay
CREATE OR REPLACE FUNCTION pque_send(
    queue_name TEXT,
    msg JSONB,
    delay INTEGER DEFAULT 0
) RETURNS SETOF BIGINT AS $$
DECLARE
    sql TEXT;
    qtable TEXT := pque_format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $QUERY$
        INSERT INTO pque_%I (vt, message)
        VALUES ((clock_timestamp() + %L), $1)
        RETURNING msg_id;
        $QUERY$,
        qtable, make_interval(secs => delay)
    );
    RETURN QUERY EXECUTE sql USING msg;
END;
$$ LANGUAGE plpgsql;

-- send_batch
-- sends an array of list of messages to a queue, optionally with a delay
CREATE OR REPLACE FUNCTION pque_send_batch(
    queue_name TEXT,
    msgs JSONB[],
    delay INTEGER DEFAULT 0
) RETURNS SETOF BIGINT AS $$
DECLARE
    sql TEXT;
    qtable TEXT := pque_format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $QUERY$
        INSERT INTO pque_%I (vt, message)
        SELECT clock_timestamp() + %L, unnest($1)
        RETURNING msg_id;
        $QUERY$,
        qtable, make_interval(secs => delay)
    );
    RETURN QUERY EXECUTE sql USING msgs;
END;
$$ LANGUAGE plpgsql;


-- get metrics for a single queue
CREATE OR REPLACE FUNCTION pque_metrics(queue_name TEXT)
RETURNS pque_metrics_result AS $$
DECLARE
    result_row pque_metrics_result;
    query TEXT;
    qtable TEXT := pque_format_table_name(queue_name, 'q');
BEGIN
    query := FORMAT(
        $QUERY$
        WITH q_summary AS (
            SELECT
                count(*) as queue_length,
                EXTRACT(epoch FROM (NOW() - max(enqueued_at)))::int as newest_msg_age_sec,
                EXTRACT(epoch FROM (NOW() - min(enqueued_at)))::int as oldest_msg_age_sec,
                NOW() as scrape_time
            FROM pque_%I
        ),
        all_metrics AS (
            SELECT CASE
                WHEN is_called THEN last_value ELSE 0
                END as total_messages
            FROM pque_%I
        )
        SELECT
            %L as queue_name,
            q_summary.queue_length,
            q_summary.newest_msg_age_sec,
            q_summary.oldest_msg_age_sec,
            all_metrics.total_messages,
            q_summary.scrape_time
        FROM q_summary, all_metrics
        $QUERY$,
        qtable, qtable || '_msg_id_seq', queue_name
    );
    EXECUTE query INTO result_row;
    RETURN result_row;
END;
$$ LANGUAGE plpgsql;

-- get metrics for all queues
CREATE OR REPLACE FUNCTION pque_metrics_all()
RETURNS SETOF pque_metrics_result AS $$
DECLARE
    row_name RECORD;
    result_row pque_metrics_result;
BEGIN
    FOR row_name IN SELECT queue_name FROM t_pque_meta LOOP
        result_row := pque_metrics(row_name.queue_name);
        RETURN NEXT result_row;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- list queues
CREATE OR REPLACE FUNCTION pque_list_queues()
RETURNS SETOF pque_queue_record AS $$
BEGIN
  RETURN QUERY SELECT * FROM t_pque_meta;
END
$$ LANGUAGE plpgsql;

-- purge queue, deleting all entries in it.
CREATE OR REPLACE FUNCTION pque_purge_queue(queue_name TEXT)
RETURNS BIGINT AS $$
DECLARE
  deleted_count INTEGER;
  qtable TEXT := pque_format_table_name(queue_name, 'q');
BEGIN
  EXECUTE format('DELETE FROM pque_%I', qtable);
  GET DIAGNOSTICS deleted_count = ROW_COUNT;
  RETURN deleted_count;
END
$$ LANGUAGE plpgsql;

-- unassign archive, so it can be kept when a queue is deleted
CREATE OR REPLACE FUNCTION pque_detach_archive(queue_name TEXT)
RETURNS VOID AS $$
DECLARE
  atable TEXT := pque_format_table_name(queue_name, 'a');
BEGIN
  EXECUTE format('DROP TABLE pque_%I', atable);
END
$$ LANGUAGE plpgsql;

-- pop a single message
CREATE OR REPLACE FUNCTION pque_pop(queue_name TEXT)
RETURNS SETOF pque_message_record AS $$
DECLARE
    sql TEXT;
    result pque_message_record;
    qtable TEXT := pque_format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $QUERY$
        WITH cte AS
            (
                SELECT msg_id
                FROM pque_%I
                WHERE vt <= now()
                ORDER BY msg_id ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
        DELETE from pque_%I
        WHERE msg_id = (select msg_id from cte)
        RETURNING *;
        $QUERY$,
        qtable, qtable
    );
    RETURN QUERY EXECUTE sql;
END;
$$ LANGUAGE plpgsql;

-- Sets vt of a message, returns it
CREATE OR REPLACE FUNCTION pque_set_vt(queue_name TEXT, msg_id BIGINT, vt INTEGER)
RETURNS SETOF pque_message_record AS $$
DECLARE
    sql TEXT;
    result pque_message_record;
    qtable TEXT := pque_format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $QUERY$
        UPDATE pque_%I
        SET vt = (now() + %L)
        WHERE msg_id = %L
        RETURNING *;
        $QUERY$,
        qtable, make_interval(secs => vt), msg_id
    );
    RETURN QUERY EXECUTE sql;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pque_drop_queue(queue_name TEXT, partitioned BOOLEAN DEFAULT FALSE)
RETURNS BOOLEAN AS $$
DECLARE
    qtable TEXT := pque_format_table_name(queue_name, 'q');
    fq_qtable TEXT := 'pque_' || qtable;
    atable TEXT := pque_format_table_name(queue_name, 'a');
    fq_atable TEXT := 'pque_' || atable;
BEGIN

    EXECUTE FORMAT(
        $QUERY$
        DROP TABLE IF EXISTS pque_%I
        $QUERY$,
        qtable
    );

    EXECUTE FORMAT(
        $QUERY$
        DROP TABLE IF EXISTS pque_%I
        $QUERY$,
        atable
    );

     IF EXISTS (
          SELECT 1
          FROM information_schema.tables
          WHERE table_name = 't_pque_meta'
     ) THEN
        EXECUTE FORMAT(
            $QUERY$
            DELETE FROM t_pque_meta WHERE queue_name = %L
            $QUERY$,
            queue_name
        );
     END IF;

     IF partitioned THEN
        EXECUTE FORMAT(
          $QUERY$
          DELETE FROM public.part_config where parent_table in (%L, %L)
          $QUERY$,
          fq_qtable, fq_atable
        );
     END IF;

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pque_validate_queue_name(queue_name TEXT)
RETURNS void AS $$
BEGIN
  IF length(queue_name) >= 48 THEN
    RAISE EXCEPTION 'queue name is too long, maximum length is 48 characters';
  END IF;
END;
$$ LANGUAGE plpgsql;

-- FIXME: Useless, removed belongs_to_pgmq

CREATE OR REPLACE FUNCTION pque_create_non_partitioned(queue_name TEXT)
RETURNS void AS $$
DECLARE
  qtable TEXT := pque_format_table_name(queue_name, 'q');
  atable TEXT := pque_format_table_name(queue_name, 'a');
BEGIN
  PERFORM pque_validate_queue_name(queue_name);

  EXECUTE FORMAT(
    $QUERY$
    CREATE TABLE IF NOT EXISTS pque_%I (
        msg_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        read_ct INT DEFAULT 0 NOT NULL,
        enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        vt TIMESTAMP WITH TIME ZONE NOT NULL,
        message JSONB
    )
    $QUERY$,
    qtable
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE TABLE IF NOT EXISTS pque_%I (
      msg_id BIGINT PRIMARY KEY,
      read_ct INT DEFAULT 0 NOT NULL,
      enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      archived_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      vt TIMESTAMP WITH TIME ZONE NOT NULL,
      message JSONB
    );
    $QUERY$,
    atable
  );

  -- GG Removed ALTER EXTENSION pgmq ADD TABLE pgmq.%I

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pque_%I (vt ASC);
    $QUERY$,
    qtable || '_vt_idx', qtable
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pque_%I (archived_at);
    $QUERY$,
    'archived_at_idx_' || queue_name, atable
  );

  EXECUTE FORMAT(
    $QUERY$
    INSERT INTO t_pque_meta (queue_name, is_partitioned, is_unlogged)
    VALUES (%L, false, false)
    ON CONFLICT
    DO NOTHING;
    $QUERY$,
    queue_name
  );
END;
$$ LANGUAGE plpgsql;








CREATE OR REPLACE FUNCTION pque_create(queue_name TEXT)
RETURNS void AS $$
BEGIN
    PERFORM pque_create_non_partitioned(queue_name);
END;
$$ LANGUAGE plpgsql;



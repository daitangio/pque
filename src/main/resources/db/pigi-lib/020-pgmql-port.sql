-- liquibase formatted sql
-- changeset GG:1 runOnChange:true splitStatements:false
-- comment: Pigi stored procedures

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
CREATE OR REPLACE FUNCTION pigi_format_table_name(queue_name text, prefix text)
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
CREATE OR REPLACE FUNCTION pigi_read(
    queue_name TEXT,
    vt INTEGER,
    qty INTEGER
)
RETURNS SETOF pigi_message_record AS $$
DECLARE
    sql TEXT;
    qtable TEXT := pigi_format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $QUERY$
        WITH cte AS
        (
            SELECT msg_id
            FROM pigi_%I
            WHERE vt <= clock_timestamp()
            ORDER BY msg_id ASC
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE pigi_%I m
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
CREATE OR REPLACE FUNCTION pigi_read_with_poll(
    queue_name TEXT,
    vt INTEGER,
    qty INTEGER,
    max_poll_seconds INTEGER DEFAULT 5,
    poll_interval_ms INTEGER DEFAULT 100
)
RETURNS SETOF pigi_message_record AS $$
DECLARE
    r pigi_message_record;
    stop_at TIMESTAMP;
    sql TEXT;
    qtable TEXT := pigi_format_table_name(queue_name, 'q');
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
              FROM pigi_%I
              WHERE vt <= clock_timestamp()
              ORDER BY msg_id ASC
              LIMIT $1
              FOR UPDATE SKIP LOCKED
          )
          UPDATE pigi_%I m
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
CREATE OR REPLACE FUNCTION pigi_archive(
    queue_name TEXT,
    msg_id BIGINT
)
RETURNS BOOLEAN AS $$
DECLARE
    sql TEXT;
    result BIGINT;
    qtable TEXT := pigi_format_table_name(queue_name, 'q');
    atable TEXT := pigi_format_table_name(queue_name, 'a');
BEGIN
    sql := FORMAT(
        $QUERY$
        WITH archived AS (
            DELETE FROM pigi_%I
            WHERE msg_id = $1
            RETURNING msg_id, vt, read_ct, enqueued_at, message
        )
        INSERT INTO pigi_%I (msg_id, vt, read_ct, enqueued_at, message)
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
CREATE OR REPLACE FUNCTION pigi_archive(
    queue_name TEXT,
    msg_ids BIGINT[]
)
RETURNS SETOF BIGINT AS $$
DECLARE
    sql TEXT;
    qtable TEXT := pigi_format_table_name(queue_name, 'q');
    atable TEXT := pigi_format_table_name(queue_name, 'a');
BEGIN
    sql := FORMAT(
        $QUERY$
        WITH archived AS (
            DELETE FROM pigi_%I
            WHERE msg_id = ANY($1)
            RETURNING msg_id, vt, read_ct, enqueued_at, message
        )
        INSERT INTO pigi_%I (msg_id, vt, read_ct, enqueued_at, message)
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
CREATE OR REPLACE FUNCTION pigi_delete(
    queue_name TEXT,
    msg_id BIGINT
)
RETURNS BOOLEAN AS $$
DECLARE
    sql TEXT;
    result BIGINT;
    qtable TEXT := pigi_format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $QUERY$
        DELETE FROM pigi_%I
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
CREATE OR REPLACE FUNCTION pigi_delete(
    queue_name TEXT,
    msg_ids BIGINT[]
)
RETURNS SETOF BIGINT AS $$
DECLARE
    sql TEXT;
    qtable TEXT := pigi_format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $QUERY$
        DELETE FROM pigi_%I
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
CREATE OR REPLACE FUNCTION pigi_send(
    queue_name TEXT,
    msg JSONB,
    delay INTEGER DEFAULT 0
) RETURNS SETOF BIGINT AS $$
DECLARE
    sql TEXT;
    qtable TEXT := pigi_format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $QUERY$
        INSERT INTO pigi_%I (vt, message)
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
CREATE OR REPLACE FUNCTION pigi_send_batch(
    queue_name TEXT,
    msgs JSONB[],
    delay INTEGER DEFAULT 0
) RETURNS SETOF BIGINT AS $$
DECLARE
    sql TEXT;
    qtable TEXT := pigi_format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $QUERY$
        INSERT INTO pigi_%I (vt, message)
        SELECT clock_timestamp() + %L, unnest($1)
        RETURNING msg_id;
        $QUERY$,
        qtable, make_interval(secs => delay)
    );
    RETURN QUERY EXECUTE sql USING msgs;
END;
$$ LANGUAGE plpgsql;


-- get metrics for a single queue
CREATE OR REPLACE FUNCTION pigi_metrics(queue_name TEXT)
RETURNS pigi_metrics_result AS $$
DECLARE
    result_row pigi_metrics_result;
    query TEXT;
    qtable TEXT := pigi_format_table_name(queue_name, 'q');
BEGIN
    query := FORMAT(
        $QUERY$
        WITH q_summary AS (
            SELECT
                count(*) as queue_length,
                EXTRACT(epoch FROM (NOW() - max(enqueued_at)))::int as newest_msg_age_sec,
                EXTRACT(epoch FROM (NOW() - min(enqueued_at)))::int as oldest_msg_age_sec,
                NOW() as scrape_time
            FROM pigi_%I
        ),
        all_metrics AS (
            SELECT CASE
                WHEN is_called THEN last_value ELSE 0
                END as total_messages
            FROM pigi_%I
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
CREATE OR REPLACE FUNCTION pigi_metrics_all()
RETURNS SETOF pigi_metrics_result AS $$
DECLARE
    row_name RECORD;
    result_row pigi_metrics_result;
BEGIN
    FOR row_name IN SELECT queue_name FROM t_pigi_meta LOOP
        result_row := pigi_metrics(row_name.queue_name);
        RETURN NEXT result_row;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- list queues
CREATE OR REPLACE FUNCTION pigi_list_queues()
RETURNS SETOF pigi_queue_record AS $$
BEGIN
  RETURN QUERY SELECT * FROM t_pigi_meta;
END
$$ LANGUAGE plpgsql;

-- purge queue, deleting all entries in it.
CREATE OR REPLACE FUNCTION pigi_purge_queue(queue_name TEXT)
RETURNS BIGINT AS $$
DECLARE
  deleted_count INTEGER;
  qtable TEXT := pigi_format_table_name(queue_name, 'q');
BEGIN
  EXECUTE format('DELETE FROM pigi_%I', qtable);
  GET DIAGNOSTICS deleted_count = ROW_COUNT;
  RETURN deleted_count;
END
$$ LANGUAGE plpgsql;

-- unassign archive, so it can be kept when a queue is deleted
CREATE OR REPLACE FUNCTION pigi_detach_archive(queue_name TEXT)
RETURNS VOID AS $$
DECLARE
  atable TEXT := pigi_format_table_name(queue_name, 'a');
BEGIN
  EXECUTE format('ALTER EXTENSION pgmq DROP TABLE pigi_%I', atable);
END
$$ LANGUAGE plpgsql;

-- pop a single message
CREATE OR REPLACE FUNCTION pigi_pop(queue_name TEXT)
RETURNS SETOF pigi_message_record AS $$
DECLARE
    sql TEXT;
    result pigi_message_record;
    qtable TEXT := pigi_format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $QUERY$
        WITH cte AS
            (
                SELECT msg_id
                FROM pigi_%I
                WHERE vt <= now()
                ORDER BY msg_id ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
        DELETE from pigi_%I
        WHERE msg_id = (select msg_id from cte)
        RETURNING *;
        $QUERY$,
        qtable, qtable
    );
    RETURN QUERY EXECUTE sql;
END;
$$ LANGUAGE plpgsql;

-- Sets vt of a message, returns it
CREATE OR REPLACE FUNCTION pigi_set_vt(queue_name TEXT, msg_id BIGINT, vt INTEGER)
RETURNS SETOF pigi_message_record AS $$
DECLARE
    sql TEXT;
    result pigi_message_record;
    qtable TEXT := pigi_format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $QUERY$
        UPDATE pigi_%I
        SET vt = (now() + %L)
        WHERE msg_id = %L
        RETURNING *;
        $QUERY$,
        qtable, make_interval(secs => vt), msg_id
    );
    RETURN QUERY EXECUTE sql;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pigi_drop_queue(queue_name TEXT, partitioned BOOLEAN DEFAULT FALSE)
RETURNS BOOLEAN AS $$
DECLARE
    qtable TEXT := pigi_format_table_name(queue_name, 'q');
    fq_qtable TEXT := 'pigi_' || qtable;
    atable TEXT := pigi_format_table_name(queue_name, 'a');
    fq_atable TEXT := 'pigi_' || atable;
BEGIN
    EXECUTE FORMAT(
        $QUERY$
        ALTER EXTENSION pgmq DROP TABLE pigi_%I
        $QUERY$,
        qtable
    );

    EXECUTE FORMAT(
        $QUERY$
        ALTER EXTENSION pgmq DROP TABLE pigi_%I
        $QUERY$,
        atable
    );

    EXECUTE FORMAT(
        $QUERY$
        DROP TABLE IF EXISTS pigi_%I
        $QUERY$,
        qtable
    );

    EXECUTE FORMAT(
        $QUERY$
        DROP TABLE IF EXISTS pigi_%I
        $QUERY$,
        atable
    );

     IF EXISTS (
          SELECT 1
          FROM information_schema.tables
          WHERE table_name = 'meta' and table_schema = 'pgmq'
     ) THEN
        EXECUTE FORMAT(
            $QUERY$
            DELETE FROM t_pigi_meta WHERE queue_name = %L
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

CREATE OR REPLACE FUNCTION pigi_validate_queue_name(queue_name TEXT)
RETURNS void AS $$
BEGIN
  IF length(queue_name) >= 48 THEN
    RAISE EXCEPTION 'queue name is too long, maximum length is 48 characters';
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pigi__belongs_to_pgmq(table_name TEXT)
RETURNS BOOLEAN AS $$
DECLARE
    sql TEXT;
    result BOOLEAN;
BEGIN
  SELECT EXISTS (
    SELECT 1
    FROM pg_depend
    WHERE refobjid = (SELECT oid FROM pg_extension WHERE extname = 'pgmq')
    AND objid = (
        SELECT oid
        FROM pg_class
        WHERE relname = table_name
    )
  ) INTO result;
  RETURN result;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pigi_create_non_partitioned(queue_name TEXT)
RETURNS void AS $$
DECLARE
  qtable TEXT := pigi_format_table_name(queue_name, 'q');
  atable TEXT := pigi_format_table_name(queue_name, 'a');
BEGIN
  PERFORM pigi_validate_queue_name(queue_name);

  EXECUTE FORMAT(
    $QUERY$
    CREATE TABLE IF NOT EXISTS pigi_%I (
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
    CREATE TABLE IF NOT EXISTS pigi_%I (
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
    CREATE INDEX IF NOT EXISTS %I ON pigi_%I (vt ASC);
    $QUERY$,
    qtable || '_vt_idx', qtable
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pigi_%I (archived_at);
    $QUERY$,
    'archived_at_idx_' || queue_name, atable
  );

  EXECUTE FORMAT(
    $QUERY$
    INSERT INTO t_pigi_meta (queue_name, is_partitioned, is_unlogged)
    VALUES (%L, false, false)
    ON CONFLICT
    DO NOTHING;
    $QUERY$,
    queue_name
  );
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pigi_create_unlogged(queue_name TEXT)
RETURNS void AS $$
DECLARE
  qtable TEXT := pigi_format_table_name(queue_name, 'q');
  atable TEXT := pigi_format_table_name(queue_name, 'a');
BEGIN
  PERFORM pigi_validate_queue_name(queue_name);
  EXECUTE FORMAT(
    $QUERY$
    CREATE UNLOGGED TABLE IF NOT EXISTS pigi_%I (
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
    CREATE TABLE IF NOT EXISTS pigi_%I (
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

  IF NOT pigi__belongs_to_pgmq(qtable) THEN
      EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pigi_%I', qtable);
  END IF;

  IF NOT pigi__belongs_to_pgmq(atable) THEN
      EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pigi_%I', atable);
  END IF;

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pigi_%I (vt ASC);
    $QUERY$,
    qtable || '_vt_idx', qtable
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pigi_%I (archived_at);
    $QUERY$,
    'archived_at_idx_' || queue_name, atable
  );

  EXECUTE FORMAT(
    $QUERY$
    INSERT INTO t_pigi_meta (queue_name, is_partitioned, is_unlogged)
    VALUES (%L, false, true)
    ON CONFLICT
    DO NOTHING;
    $QUERY$,
    queue_name
  );
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pigi__get_partition_col(partition_interval TEXT)
RETURNS TEXT AS $$
DECLARE
  num INTEGER;
BEGIN
    BEGIN
        num := partition_interval::INTEGER;
        RETURN 'msg_id';
    EXCEPTION
        WHEN others THEN
            RETURN 'enqueued_at';
    END;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pigi__ensure_pg_partman_installed()
RETURNS void AS $$
DECLARE
  extension_exists BOOLEAN;
BEGIN
  SELECT EXISTS (
    SELECT 1
    FROM pg_extension
    WHERE extname = 'pg_partman'
  ) INTO extension_exists;

  IF NOT extension_exists THEN
    RAISE EXCEPTION 'pg_partman is required for partitioned queues';
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pigi_create_partitioned(
  queue_name TEXT,
  partition_interval TEXT DEFAULT '10000',
  retention_interval TEXT DEFAULT '100000'
)
RETURNS void AS $$
DECLARE
  partition_col TEXT;
  a_partition_col TEXT;
  qtable TEXT := pigi_format_table_name(queue_name, 'q');
  atable TEXT := pigi_format_table_name(queue_name, 'a');
  fq_qtable TEXT := 'pigi_' || qtable;
  fq_atable TEXT := 'pigi_' || atable;
BEGIN
  PERFORM pigi_validate_queue_name(queue_name);
  PERFORM pigi__ensure_pg_partman_installed();
  SELECT pigi__get_partition_col(partition_interval) INTO partition_col;

  EXECUTE FORMAT(
    $QUERY$
    CREATE TABLE IF NOT EXISTS pigi_%I (
        msg_id BIGINT GENERATED ALWAYS AS IDENTITY,
        read_ct INT DEFAULT 0 NOT NULL,
        enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        vt TIMESTAMP WITH TIME ZONE NOT NULL,
        message JSONB
    ) PARTITION BY RANGE (%I)
    $QUERY$,
    qtable, partition_col
  );

  IF NOT pigi__belongs_to_pgmq(qtable) THEN
      EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pigi_%I', qtable);
  END IF;

  -- https://github.com/pgpartman/pg_partman/blob/master/doc/pg_partman.md
  -- p_parent_table - the existing parent table. MUST be schema qualified, even if in public schema.
  PERFORM public.create_parent(
    fq_qtable,
    partition_col, 'native', partition_interval
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pigi_%I (%I);
    $QUERY$,
    qtable || '_part_idx', qtable, partition_col
  );

  EXECUTE FORMAT(
    $QUERY$
    UPDATE public.part_config
    SET
        retention = %L,
        retention_keep_table = false,
        retention_keep_index = true,
        automatic_maintenance = 'on'
    WHERE parent_table = %L;
    $QUERY$,
    retention_interval, 'pigi_' || qtable
  );

  EXECUTE FORMAT(
    $QUERY$
    INSERT INTO t_pigi_meta (queue_name, is_partitioned, is_unlogged)
    VALUES (%L, true, false)
    ON CONFLICT
    DO NOTHING;
    $QUERY$,
    queue_name
  );

  IF partition_col = 'enqueued_at' THEN
    a_partition_col := 'archived_at';
  ELSE
    a_partition_col := partition_col;
  END IF;

  EXECUTE FORMAT(
    $QUERY$
    CREATE TABLE IF NOT EXISTS pigi_%I (
      msg_id BIGINT,
      read_ct INT DEFAULT 0 NOT NULL,
      enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      archived_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      vt TIMESTAMP WITH TIME ZONE NOT NULL,
      message JSONB
    ) PARTITION BY RANGE (%I);
    $QUERY$,
    atable, a_partition_col
  );

  IF NOT pigi__belongs_to_pgmq(atable) THEN
      EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pigi_%I', atable);
  END IF;

  -- https://github.com/pgpartman/pg_partman/blob/master/doc/pg_partman.md
  -- p_parent_table - the existing parent table. MUST be schema qualified, even if in public schema.
  PERFORM public.create_parent(
    fq_atable,
    a_partition_col, 'native', partition_interval
  );

  EXECUTE FORMAT(
    $QUERY$
    UPDATE public.part_config
    SET
        retention = %L,
        retention_keep_table = false,
        retention_keep_index = true,
        automatic_maintenance = 'on'
    WHERE parent_table = %L;
    $QUERY$,
    retention_interval, 'pigi_' || atable
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pigi_%I (archived_at);
    $QUERY$,
    'archived_at_idx_' || queue_name, atable
  );

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION pigi_create(queue_name TEXT)
RETURNS void AS $$
BEGIN
    PERFORM pigi_create_non_partitioned(queue_name);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pigi_convert_archive_partitioned(table_name TEXT,
                                                 partition_interval TEXT DEFAULT '10000',
                                                 retention_interval TEXT DEFAULT '100000',
                                                 leading_partition INT DEFAULT 10)
RETURNS void AS $$
DECLARE
a_table_name TEXT := pigi_format_table_name(table_name, 'a');
a_table_name_old TEXT := pigi_format_table_name(table_name, 'a') || '_old';
qualified_a_table_name TEXT := format('pigi_%I', a_table_name);
BEGIN

  PERFORM c.relkind
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = a_table_name
    AND c.relkind = 'p';

  IF FOUND THEN
    RAISE NOTICE 'Table %s is already partitioned', a_table_name;
    RETURN;
  END IF;

  PERFORM c.relkind
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = a_table_name
    AND c.relkind = 'r';

  IF NOT FOUND THEN
    RAISE NOTICE 'Table %s does not exists', a_table_name;
    RETURN;
  END IF;

  EXECUTE 'ALTER TABLE ' || qualified_a_table_name || ' RENAME TO ' || a_table_name_old;

  EXECUTE format( 'CREATE TABLE pigi_%I (LIKE pigi_%I including all) PARTITION BY RANGE (msg_id)', a_table_name, a_table_name_old );

  EXECUTE 'ALTER INDEX pigi_archived_at_idx_' || table_name || ' RENAME TO archived_at_idx_' || table_name || '_old';
  EXECUTE 'CREATE INDEX archived_at_idx_'|| table_name || ' ON ' || qualified_a_table_name ||'(archived_at)';

  -- https://github.com/pgpartman/pg_partman/blob/master/doc/pg_partman.md
  -- p_parent_table - the existing parent table. MUST be schema qualified, even if in public schema.
  PERFORM create_parent(qualified_a_table_name, 'msg_id', 'native',  partition_interval,
                         p_premake := leading_partition);

  UPDATE part_config
    SET retention = retention_interval,
    retention_keep_table = false,
    retention_keep_index = false,
    infinite_time_partitions = true
    WHERE parent_table = qualified_a_table_name;
END;
$$ LANGUAGE plpgsql;

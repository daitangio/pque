-- GG: Data written to unlogged tables is not written to the write-ahead log.
-- They are faster but not replicated and not crash safe. see https://www.postgresql.org/docs/current/sql-createtable.html
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

  -- GG Removed ALTER EXTENSION pgmq ADD TABLE


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
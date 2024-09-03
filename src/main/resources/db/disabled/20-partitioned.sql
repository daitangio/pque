CREATE OR REPLACE FUNCTION pque__ensure_pg_partman_installed()
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


CREATE OR REPLACE FUNCTION pque_create_partitioned(
  queue_name TEXT,
  partition_interval TEXT DEFAULT '10000',
  retention_interval TEXT DEFAULT '100000'
)
RETURNS void AS $$
DECLARE
  partition_col TEXT;
  a_partition_col TEXT;
  qtable TEXT := pque_format_table_name(queue_name, 'q');
  atable TEXT := pque_format_table_name(queue_name, 'a');
  fq_qtable TEXT := 'pque_' || qtable;
  fq_atable TEXT := 'pque_' || atable;
BEGIN
  PERFORM pque_validate_queue_name(queue_name);
  PERFORM pque__ensure_pg_partman_installed();
  SELECT pque__get_partition_col(partition_interval) INTO partition_col;

  EXECUTE FORMAT(
    $QUERY$
    CREATE TABLE IF NOT EXISTS pque_%I (
        msg_id BIGINT GENERATED ALWAYS AS IDENTITY,
        read_ct INT DEFAULT 0 NOT NULL,
        enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        vt TIMESTAMP WITH TIME ZONE NOT NULL,
        message JSONB
    ) PARTITION BY RANGE (%I)
    $QUERY$,
    qtable, partition_col
  );

-- GG Removed ALTER EXTENSION

  -- https://github.com/pgpartman/pg_partman/blob/master/doc/pg_partman.md
  -- p_parent_table - the existing parent table. MUST be schema qualified, even if in public schema.
  PERFORM public.create_parent(
    fq_qtable,
    partition_col, 'native', partition_interval
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pque_%I (%I);
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
    retention_interval, 'pque_' || qtable
  );

  EXECUTE FORMAT(
    $QUERY$
    INSERT INTO t_pque_meta (queue_name, is_partitioned, is_unlogged)
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
    CREATE TABLE IF NOT EXISTS pque_%I (
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

  -- GG Removed ALTER EXTENSION


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
    retention_interval, 'pque_' || atable
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pque_%I (archived_at);
    $QUERY$,
    'archived_at_idx_' || queue_name, atable
  );

END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pque_convert_archive_partitioned(table_name TEXT,
                                                 partition_interval TEXT DEFAULT '10000',
                                                 retention_interval TEXT DEFAULT '100000',
                                                 leading_partition INT DEFAULT 10)
RETURNS void AS $$
DECLARE
a_table_name TEXT := pque_format_table_name(table_name, 'a');
a_table_name_old TEXT := pque_format_table_name(table_name, 'a') || '_old';
qualified_a_table_name TEXT := format('pque_%I', a_table_name);
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

  EXECUTE format( 'CREATE TABLE pque_%I (LIKE pque_%I including all) PARTITION BY RANGE (msg_id)', a_table_name, a_table_name_old );

  EXECUTE 'ALTER INDEX pque_archived_at_idx_' || table_name || ' RENAME TO archived_at_idx_' || table_name || '_old';
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


CREATE OR REPLACE FUNCTION pque__get_partition_col(partition_interval TEXT)
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
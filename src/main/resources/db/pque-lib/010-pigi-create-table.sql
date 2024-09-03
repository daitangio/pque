-- liquibase formatted sql
-- changeset GG:1 runOnChange:false
-- comment: Pigi master meta table

------------------------------------------------------------
-- Ported from https://github.com/tembo-io/pgmq version 1.4.2 August 2024
------------------------------------------------------------


-- Table where queues and metadata about them is stored
CREATE TABLE if not exists t_pque_meta (
    queue_name VARCHAR UNIQUE NOT NULL,
    is_partitioned BOOLEAN NOT NULL,
    is_unlogged BOOLEAN NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL
);

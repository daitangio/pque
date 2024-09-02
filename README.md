# pigi
PostgreSQL super LiGht queue System, pigi for friends

Do you have a PostgresQL 13 with a Java 11 application, and do you need a quick and dirty queue implementation? This is the answer.

Pigi born

Ported from pgmql, offer a lighter solution, and also include the Spring Boot Interface, backported and extended from
https://github.com/adamalexandru4/pgmq-spring

PostgreSQL Target: 13.15+

- [pigi](#pigi)
- [Simple checks](#simple-checks)
- [PSQL Interface](#psql-interface)
  - [SQL Examples](#sql-examples)
    - [Creating a queue and interacting with it](#creating-a-queue-and-interacting-with-it)
  - [PGML Examples](#pgml-examples)
    - [Creating a queue](#creating-a-queue)
    - [Send two messages](#send-two-messages)
    - [Read messages](#read-messages)
    - [Pop a message](#pop-a-message)
    - [Archive a message](#archive-a-message)
    - [Delete a message](#delete-a-message)
    - [Drop a queue](#drop-a-queue)
- [About the port](#about-the-port)


# Simple checks
    ./mvnw spring-boot:run
    curl http://localhost:8080/v1/info


# PSQL Interface
## SQL Examples

### Creating a queue and interacting with it
Connect to postgresql via psql.
Then try:
```sql
-- creates the queue
SELECT pigi_create('mail_queue');

-- Send something everyone knows
SELECT * from pigi_send(
  queue_name  => 'mail_queue',
  msg         => '{"xml": "sucks"}'
);

-- Read it back
SELECT * FROM pigi_pop('mail_queue');
```

## PGML Examples

Below some examples taken from the original pgmql project, adapted to work with pigi


### Creating a queue

Every queue is its own table in the `pgmq` schema. The table name is the queue name prefixed with `q_`.
For example, `pigi_q_my_queue` is the table for the queue `my_queue`.

```sql
-- creates the queue
SELECT pigi_create('my_queue');
```

```text
 create
-------------

(1 row)
```

### Send two messages

```sql
-- messages are sent as JSON
SELECT * from pigi_send(
  queue_name  => 'my_queue',
  msg         => '{"foo": "bar1"}'
);
```

The message id is returned from the send function.

```text
 send
-----------
         1
(1 row)
```

```sql
-- Optionally provide a delay
-- this message will be on the queue but unable to be consumed for 5 seconds
SELECT * from pigi_send(
  queue_name => 'my_queue',
  msg        => '{"foo": "bar2"}',
  delay      => 5
);
```

```text
 send
-----------
         2
(1 row)
```

### Read messages

Read `2` message from the queue. Make them invisible for `30` seconds.
If the messages are not deleted or archived within 30 seconds, they will become visible again
and can be read by another consumer.

```sql
SELECT * FROM pigi_read(
  queue_name => 'my_queue',
  vt         => 30,
  qty        => 2
);
```

```text
 msg_id | read_ct |          enqueued_at          |              vt               |     message
--------+---------+-------------------------------+-------------------------------+-----------------
      1 |       1 | 2023-08-16 08:37:54.567283-05 | 2023-08-16 08:38:29.989841-05 | {"foo": "bar1"}
      2 |       1 | 2023-08-16 08:37:54.572933-05 | 2023-08-16 08:38:29.989841-05 | {"foo": "bar2"}
```

If the queue is empty, or if all messages are currently invisible, no rows will be returned.

```sql
SELECT * FROM pigi_read(
  queue_name => 'my_queue',
  vt         => 30,
  qty        => 1
);
```

```text
 msg_id | read_ct | enqueued_at | vt | message
--------+---------+-------------+----+---------
```

### Pop a message

```sql
-- Read a message and immediately delete it from the queue. Returns an empty record if the queue is empty or all messages are invisible.
SELECT * FROM pigi_pop('my_queue');
```

```text
 msg_id | read_ct |          enqueued_at          |              vt               |     message
--------+---------+-------------------------------+-------------------------------+-----------------
      1 |       1 | 2023-08-16 08:37:54.567283-05 | 2023-08-16 08:38:29.989841-05 | {"foo": "bar1"}
```

### Archive a message

Archiving a message removes it from the queue and inserts it to the archive table.

```sql
-- Archive message with msg_id=2.
SELECT pigi_archive(
  queue_name => 'my_queue',
  msg_id     => 2
);
```

```text
 archive
--------------
 t
(1 row)
```

Or archive several messages in one operation using `msg_ids` (plural) parameter:

First, send a batch of messages

```sql
SELECT pigi_send_batch(
  queue_name => 'my_queue',
  msgs       => ARRAY['{"foo": "bar3"}','{"foo": "bar4"}','{"foo": "bar5"}']::jsonb[]
);
```

```text
 send_batch 
------------
          3
          4
          5
(3 rows)
```

Then archive them by using the msg_ids (plural) parameter.

```sql
SELECT pigi_archive(
  queue_name => 'my_queue',
  msg_ids    => ARRAY[3, 4, 5]
);
```

```text
 archive 
---------
       3
       4
       5
(3 rows)
```

Archive tables can be inspected directly with SQL.
 Archive tables have the prefix `a_` in the `pgmq` schema.

```sql
SELECT * FROM pigi_a_my_queue;
```

```text
 msg_id | read_ct |          enqueued_at          |          archived_at          |              vt               |     message     
--------+---------+-------------------------------+-------------------------------+-------------------------------+-----------------
      2 |       0 | 2024-08-06 16:03:41.531556+00 | 2024-08-06 16:03:52.811063+00 | 2024-08-06 16:03:46.532246+00 | {"foo": "bar2"}
      3 |       0 | 2024-08-06 16:03:58.586444+00 | 2024-08-06 16:04:02.85799+00  | 2024-08-06 16:03:58.587272+00 | {"foo": "bar3"}
      4 |       0 | 2024-08-06 16:03:58.586444+00 | 2024-08-06 16:04:02.85799+00  | 2024-08-06 16:03:58.587508+00 | {"foo": "bar4"}
      5 |       0 | 2024-08-06 16:03:58.586444+00 | 2024-08-06 16:04:02.85799+00  | 2024-08-06 16:03:58.587543+00 | {"foo": "bar5"}
```

### Delete a message

Send another message, so that we can delete it.

```sql
SELECT pigi_send('my_queue', '{"foo": "bar6"}');
```

```text
 send
-----------
        6
(1 row)
```

Delete the message with id `6` from the queue named `my_queue`.

```sql
SELECT pigi_delete('my_queue', 6);
```

```text
 delete
-------------
 t
(1 row)
```

### Drop a queue

Delete the queue `my_queue`.

```sql
SELECT pigi_drop_queue('my_queue');
```

```text
 drop_queue
-----------------
 t
(1 row)
```

# About the port

The port was done removing the extension name space, and renaming ' pgmq. ' into ' pigi_ '
The table was renamed t_pigi_*
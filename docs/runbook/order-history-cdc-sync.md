# Runbook: Order History CDC Sync

## Purpose

This runbook follows an order from the PostgreSQL `order_history` table into its ClickHouse read replica on a local
stack, and explains how to diagnose a sync that stopped. The design behind it is described in
[ADR 0003](../adr/0003-clickhouse-read-replica-for-order-history.md).

## Prerequisites

1. Build the application and start the stack from the `infra` folder:
   ```bash
   ./gradlew clean bootJar
   cd infra
   docker compose --profile app up --build -d
   ```
2. If the stack ran before, remove its volumes first with `docker compose --profile app down -v`, so that both
   databases start empty with the current schema.
3. Wait until the application is healthy: `curl -s http://localhost:8080/actuator/health` returns `"status":"UP"`.
4. Open the SQL clients used below:
   * **PostgreSQL:** `docker compose exec postgres psql -U order_user -d order_db`
   * **ClickHouse:** open the client in the browser `http://localhost:8123/play`.

## Health Checks

Run these checks first when the history endpoint returns missing or outdated data.

1. **Both connectors are `RUNNING`.**
   ```bash
   curl -s http://localhost:8083/connectors/postgres-outbox-connector/status
   curl -s http://localhost:8083/connectors/postgres-order-history-connector/status
   ```
   A `FAILED` task shows its stack trace in the `trace` field. Restart it with
   `curl -X POST http://localhost:8083/connectors/postgres-order-history-connector/restart?includeTasks=true`.
2. **The replication slot is active.** Run in PostgreSQL:
   ```sql
   SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag
   FROM pg_replication_slots;
   ```
   `order_history_slot` must be `active`. A growing `lag` means Debezium does not confirm changes, and PostgreSQL keeps
   the WAL until it does.
3. **The CDC topic receives messages.** In Kafka UI (`http://localhost:8082`), open the topic
   `order_history_cdc.public.order_history`. Each message is one changed row with the fields `__lsn` and `__deleted`.
4. **ClickHouse consumes the topic.** Run in ClickHouse:
   ```sql
   SELECT database, table, num_messages_read, last_poll_time, exceptions.text
   FROM system.kafka_consumers
   WHERE table = 'order_history_sync';
   ```
   `num_messages_read` must grow after a change. Parsing errors appear in `exceptions.text`.

## Following an Order

1. **Create an order and change it.**
   ```bash
   curl -s -X POST http://localhost:8080/orders -H 'Content-Type: application/json' \
     -d '{"customerId": "customer-carol", "totalAmount": 12.50}'
   ```
   Copy the `id` from the response; the following steps call it `<order-id>`. Then update the status and repair the
   amount:
   ```bash
   curl -s -X PUT http://localhost:8080/orders/<order-id>/status -H 'Content-Type: application/json' \
     -d '{"status": "PROCESSING"}'
   curl -s -X PUT http://localhost:8080/orders/<order-id>/total-amount -H 'Content-Type: application/json' \
     -d '{"totalAmount": 9.90}'
   ```
2. **Check PostgreSQL, the primary store.** You should see two rows, `CREATED` and `PROCESSING`, both with amount
   `9.90` and `version = 1` (the amount repair updated each row once):
   ```sql
   SELECT * FROM order_history WHERE order_id = '<order-id>' ORDER BY updated_at;
   ```
3. **Check the raw changes in ClickHouse.** You should see four rows: the two inserts and, for each row `id`, the update
   with a higher `lsn`. Until a background merge runs, the old and the new amount exist side by side:
   ```sql
   SELECT id, status, total_amount, updated_at, version, lsn, is_deleted
   FROM order_history WHERE order_id = '<order-id>' ORDER BY id, lsn;
   ```
4. **Check the current rows in ClickHouse.** With `FINAL`, you should see the same two rows as in PostgreSQL:
   ```sql
   SELECT id, status, total_amount, updated_at, version
   FROM order_history FINAL WHERE order_id = '<order-id>' ORDER BY updated_at;
   ```
5. **Repair the history and check the deletes.**
   ```bash
   curl -s -X POST http://localhost:8080/orders/<order-id>/history-repair
   ```
   PostgreSQL now holds two new rows with new `id` values and `version = 0`. In the raw ClickHouse rows, each old `id`
   has an additional change with `is_deleted = 1`, and the query with `FINAL` returns only the two new rows.
6. **Query a snapshot through the API.** Use a `targetTime` between the creation and the status update; the response
   shows `CREATED` with the repaired amount `9.90`. `targetTime` is an ISO-8601 instant in UTC. Take the `updated_at`
   of the `CREATED` row from step 4 and add a second. For example, if the order was created at
   `2026-11-03 08:30:15.123`:
   ```bash
   curl -s "http://localhost:8080/orders-history?orderId=<order-id>&targetTime=2026-11-03T08:30:16Z"
   ```
   Without `targetTime`, the API uses the current time and shows `PROCESSING`.

## Common Problems

| Symptom                                                   | Cause and fix                                                                                                             |
|:----------------------------------------------------------|:--------------------------------------------------------------------------------------------------------------------------|
| PostgreSQL has the rows, ClickHouse has none              | Check the order history connector (health check 1) and the ClickHouse consumer (health check 4).                          |
| Flyway fails at startup with a checksum mismatch          | A migration changed after it ran locally. Recreate the stack with `docker compose --profile app down -v`.                 |
| Deleted states still appear in a raw ClickHouse query     | Expected: deletes are stored as changes with `is_deleted = 1`. Query with `FINAL`, as the application does.               |
| Old amounts appear next to new ones in a raw query        | Expected until a background merge runs. `FINAL` returns only the latest change per row.                                   |
| `connector-init` exits with `TIMEOUT` or `CRITICAL`       | Inspect `docker compose logs debezium`; a common cause is PostgreSQL started without `wal_level=logical`.                 |

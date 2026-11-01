# Runbook: ClickHouse Order History Scenarios

## Purpose

This runbook walks through the five order history scenarios of
[ADR 0003](../adr/0003-postgres-primary-for-order-history.md) on a local stack. Each scenario lists the commands to run and
the result to expect. The design note [ClickHouse Order History: Scenarios Explained](../design/clickhouse-order-history-scenarios.md)
explains why the results look the way they do.

## Prerequisites

1. Build the application and start the stack from the `infra` folder:
   ```bash
   ./gradlew clean bootJar
   cd infra
   docker compose --profile app up --build -d
   ```
2. If the stack ran before, remove its volumes first with `docker compose --profile app down -v`, so that the scenarios
   start from an empty database with the current schema.
3. Wait until the application is healthy: `curl -s http://localhost:8080/actuator/health` returns `"status":"UP"`.

## Scenario 1: Order Creation

**Goal:** A new order appears in the order history as its first version.

1. Create an order:
   ```bash
   curl -s -X POST http://localhost:8080/orders -H 'Content-Type: application/json' \
     -d '{"customerId": "customer-carol", "totalAmount": 12.50}'
   ```
   Copy the `id` from the response; the following steps call it `<order-id>`.
2. Run the [pipeline checks](#pipeline-checks) for `<order-id>`:
   * Check the `orders` table. You should see one row with customer `customer-carol`, total amount `12.50`, status
     `CREATED` and `version = 0`.
   * Check the `order_status_changes` table. You should see one row with status `CREATED` and the same `created_at` as
     the order.
   * Check the `outbox` table. You should see one `ORDER_CREATED` row with key `customer-carol`.
   * Check Kafka UI. You should see one message with key `customer-carol` and the outbox payload as value.
   * Check the `order_history` table. You should see one `CREATED` row with `created_at` equal to `updated_at` and a
     `version` equal to the Kafka offset.

## Scenario 2: Order Status Update

**Goal:** Every status change is appended as a new row, and earlier states remain readable.

1. Create an order:
   ```bash
   curl -s -X POST http://localhost:8080/orders -H 'Content-Type: application/json' \
     -d '{"customerId": "customer-dave", "totalAmount": 30.00}'
   ```
   Copy the `id` from the response; the following steps call it `<order-id>`.
2. Update the order status to `PROCESSING`:
   ```bash
   curl -s -X PUT http://localhost:8080/orders/<order-id>/status -H 'Content-Type: application/json' \
     -d '{"status": "PROCESSING"}'
   ```
3. Run the [pipeline checks](#pipeline-checks) for `<order-id>`:
   * Check the `orders` table. You should see one row with status `PROCESSING` and `version = 1`; the table holds only
     the current state.
   * Check the `order_status_changes` table. You should see two rows, `CREATED` and `PROCESSING`, the second with a
     later `created_at`.
   * Check the `outbox` table. You should see an `ORDER_CREATED` and an `ORDER_STATUS_UPDATED` row; the second payload
     holds `"newStatus":"PROCESSING"`.
   * Check Kafka UI. You should see two messages with key `customer-dave` in the same partition, the status update with
     the higher offset.
   * Check the `order_history` table. You should see two rows with the same `created_at`; the `PROCESSING` row has a
     later `updated_at`, and each `version` equals the offset of its event.

## Scenario 3: Retroactive Total Amount Correction

**Goal:** A wrong amount is corrected in every past snapshot, while the status timeline stays unchanged.

1. Create an order with the wrong amount `99.00`:
   ```bash
   curl -s -X POST http://localhost:8080/orders -H 'Content-Type: application/json' \
     -d '{"customerId": "customer-erin", "totalAmount": 99.00}'
   ```
   Copy the `id` from the response; the following steps call it `<order-id>`.
2. Update the order status to `PROCESSING`:
   ```bash
   curl -s -X PUT http://localhost:8080/orders/<order-id>/status -H 'Content-Type: application/json' \
     -d '{"status": "PROCESSING"}'
   ```
3. Correct the total amount to `9.90`:
   ```bash
   curl -s -X PUT http://localhost:8080/orders/<order-id>/total-amount -H 'Content-Type: application/json' \
     -d '{"totalAmount": 9.90}'
   ```
4. Run the [pipeline checks](#pipeline-checks) for `<order-id>`:
   * Check the `orders` table. You should see one row with total amount `9.90`, status `PROCESSING` and `version = 2`.
   * Check the `order_status_changes` table. You should see two rows, `CREATED` and `PROCESSING`; the amount
     correction adds no status change.
   * Check the `outbox` table. You should see an `ORDER_CREATED`, an `ORDER_STATUS_UPDATED` and an
     `ORDER_TOTAL_AMOUNT_REPAIR` row; the last payload holds the new amount `9.90`.
   * Check Kafka UI. You should see three messages with key `customer-erin` in the same partition, the amount correction
     with the highest offset.
   * Check the `order_history` table. You should still see two rows, `CREATED` and `PROCESSING`, both with
     `total_amount = 9.90`. Their versions and timestamps are unchanged; the correction rewrote the amount in every
     past version instead of appending a new row.

## Scenario 4: Historical Point-in-Time Reconstruction (Time Travel)

**Goal:** A snapshot returns every order exactly as it was at the target time, one row per order.

1. Run steps 1 and 2 of [Scenario 2](#scenario-2-order-status-update) to create an order and move it to `PROCESSING`.
2. Read the current state of the order:
   ```bash
   curl -s "http://localhost:8080/orders-history?orderId=<order-id>"
   ```
   You should see one entry with status `PROCESSING`; the target time defaults to now. Note its `createdAt` (when the
   order was created) and `updatedAt` (when it moved to `PROCESSING`).
3. Read the order at specific points in time, using those values as `targetTime`:
   ```bash
   curl -s "http://localhost:8080/orders-history?orderId=<order-id>&targetTime=<target-time>"
   ```
   `<target-time>` is an ISO-8601 instant in UTC with the `Z` suffix, for example `2026-10-31T05:39:47.874Z`;
   fractional seconds are optional. The `createdAt` and `updatedAt` values from step 2 already use this format, so you
   can copy them as they are.
   * Shortly before `createdAt`: you should see `[]`, because the order did not exist yet.
   * At `createdAt`: you should see one entry with status `CREATED`.
   * At `updatedAt`: you should see one entry with status `PROCESSING`.

## Scenario 5: History Rebuild (Discrepancy Troubleshooting)

**Goal:** Repair an order whose history no longer matches PostgreSQL, including rows that never happened.

1. Run steps 1 and 2 of [Scenario 2](#scenario-2-order-status-update) to create an order and move it to `PROCESSING`.
2. Simulate drift: run the following statement in ClickHouse. It stores a `COMPLETED` state that never happened in
   PostgreSQL, one second after the newest version:
   ```sql
   INSERT INTO order_history
   SELECT order_id, customer_id, total_amount, 'COMPLETED', created_at, updated_at + INTERVAL 1 SECOND, version
   FROM order_history WHERE order_id = '<order-id>' ORDER BY version DESC, updated_at DESC LIMIT 1;
   ```
3. Confirm the discrepancy. The history reports `COMPLETED`:
   ```bash
   curl -s "http://localhost:8080/orders-history?orderId=<order-id>"
   ```
   PostgreSQL still reports `PROCESSING`:
   ```sql
   SELECT * FROM orders WHERE id = '<order-id>';
   ```
4. Trigger the history rebuild. It returns `202 Accepted`; the rebuild runs once the repair event is consumed:
   ```bash
   curl -s -i -X POST http://localhost:8080/orders/<order-id>/history-repair
   ```
5. Run the [pipeline checks](#pipeline-checks) for `<order-id>`:
   * Check the `orders` table. You should see the unchanged row with status `PROCESSING` and `version = 1`.
   * Check the `order_status_changes` table. You should see the unchanged rows `CREATED` and `PROCESSING`.
   * Check the `outbox` table. You should see an additional `ORDER_REPAIR` row; its payload lists the `CREATED` and
     `PROCESSING` steps.
   * Check Kafka UI. You should see a third message, the `ORDER_REPAIR` event, with the highest offset.
   * Check the `order_history` table. You should see exactly two rows, `CREATED` and `PROCESSING`, both with the offset
     of the repair event as `version`. The `COMPLETED` row is gone.

**If the rows are not repaired:** Check the application logs (`docker logs outbox-spring-app`) for the repair message
of the order, and the order events topic in Kafka UI (`http://localhost:8082`) for an `ORDER_REPAIR` message. If the
consumer failed, follow [Kafka Poison Pill Inspection and Re-drive](kafka-poison-pill-handling.md).

## Reset

Remove all containers and volumes to start again with an empty database:

```bash
docker compose --profile app down -v
```

## Pipeline Checks

These checks follow an order through every hop of the outbox pipeline. Replace `<order-id>` with the ID of the order;
each scenario lists the results to expect.

1. **Check the `orders` table.** Run the following query in PostgreSQL:
   ```sql
   SELECT * FROM orders WHERE id = '<order-id>';
   ```
2. **Check the `order_status_changes` table.** Run the following query in PostgreSQL:
   ```sql
   SELECT * FROM order_status_changes WHERE order_id = '<order-id>';
   ```
3. **Check the `outbox` table.** Run the following query in PostgreSQL. Every row targets the topic `order-events`
   and uses the customer ID as key:
   ```sql
   SELECT * FROM outbox WHERE aggregateid = '<order-id>';
   ```
4. **Check the events in Kafka UI.** Open `http://localhost:8082`, select the topic `order-events`, open the
   **Messages** tab and search for `<order-id>`. Note the offset of each message.
5. **Check the `order_history` table.** Run the following query in ClickHouse. The `version` of each row equals the
   Kafka offset of its event:
   ```sql
   SELECT * FROM order_history WHERE order_id = '<order-id>';
   ```

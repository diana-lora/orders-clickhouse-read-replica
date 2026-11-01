# ADR: PostgreSQL as Primary Store for the Order History, ClickHouse as Read Replica

## Status

Accepted (phase 1 implemented; phases 2 and 3 pending)

## Context

PostgreSQL stores the orders and their history, one row per state (creation and each status change), which enables
time travel. Single-row writes are fast and transactional in PostgreSQL, but time travel snapshots scan the history of
many orders and reduce it to the latest state per order. As the history grows, these analytical queries become slow and
compete with the transactional load.

**ClickHouse**, a columnar OLAP store, answers such queries quickly, but handles single-row changes poorly: no
transactions across statements, patch-based updates and deletes, and only eventual deduplication. We therefore need
every modification to stay transactional in PostgreSQL while time travel reads are served from ClickHouse, without the
application writing to both databases.

## Decision

PostgreSQL becomes the **primary store** of `order_history` and is used with regular CRUD operations; ClickHouse
becomes a **read-only replica** that is kept in sync by change data capture. The migration runs in three phases:

1. **PostgreSQL write path (implemented).** All modifications go through `OrderHistoryMutationRepository`, implemented
   directly by the Spring Data JDBC repository `PostgresOrderHistoryRepository`. Reads go through
   `OrderHistoryReadRepository` (ClickHouse).
2. **CDC sync (pending).** Debezium captures `order_history` changes in PostgreSQL and ClickHouse ingests them
   directly from Kafka.
3. **Read path (pending).** The order history endpoint reads only from ClickHouse.

### Table design

| Column / constraint         | Purpose                                                                                |
|:----------------------------|:---------------------------------------------------------------------------------------|
| `id` (primary key)          | Surrogate key of one state of an order                                                 |
| `UNIQUE (order_id, updated_at)` | Looks up the timeline of an order; rejects the same state change stored twice      |
| `updated_at`                | Time of the state change; one row per state keeps time travel possible                 |
| `version`                   | Optimistic locking (`@Version`): increased on every update of the row, checked on save |

### Write rules

| Event                         | Effect in PostgreSQL                                                                                   |
|:------------------------------|:-------------------------------------------------------------------------------------------------------|
| `ORDER_CREATED`               | Inserts the first row; skipped when the order already has a history (redelivery)                       |
| `ORDER_STATUS_UPDATED`        | Inserts a new row; skipped when its `updatedAt` is not later than the latest row (stale or redelivery) |
| `ORDER_TOTAL_AMOUNT_REPAIR`   | Updates the amount in every row of the order; `status`, `updated_at` stay untouched                    |
| `ORDER_REPAIR`                | Deletes every row of the order and inserts the repaired timeline, in one transaction                   |

The amount correction event was renamed from `ORDER_TOTAL_AMOUNT_UPDATED` to `ORDER_TOTAL_AMOUNT_REPAIR`, because it
retroactively modifies stored data instead of recording a new state.

A missing history (`OrderHistoryNotFoundException`) and a concurrent change of the same row
(`OptimisticLockingFailureException`) are rethrown, so the Kafka retry and dead letter handling of ADR 0002 applies.

## Consequences

### Positive Impact

* **Atomic repairs:** delete and insert of a history repair share one PostgreSQL transaction.
* **Immediate idempotency:** redelivered events are detected by the write rules at write time.
* **Safe corrections:** optimistic locking prevents lost updates when the same row is changed concurrently.
* **Fast analytical reads:** time travel snapshots run on ClickHouse and do not load the transactional database.
* **Separation of write and read models:** the application writes only to PostgreSQL; ClickHouse only serves reads
  and receives its data through CDC, never through mutations issued by the application.

### Negative Impact

* **Additional infrastructure:** the CDC pipeline (phase 2) must be operated and monitored.
* **Replication lag:** reads from ClickHouse are eventually consistent with PostgreSQL.
* **Gap between phases:** until phase 2 is in place, ClickHouse receives no new history data. The integration tests
  that depend on the sync are disabled until then.
* **Open topics for phase 2:** the ClickHouse deduplication key must follow PostgreSQL (`id` or
  `(order_id, updated_at)`, with `version` as the `ReplacingMergeTree` version), and the deletes of a history repair
  must reach ClickHouse (`is_deleted` column, `REPLICA IDENTITY FULL`).

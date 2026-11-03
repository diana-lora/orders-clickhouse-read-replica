-- Read replica of the PostgreSQL order_history table, filled only by CDC (Debezium -> Kafka -> Kafka engine)

-- Target table: one row per order state. Every change of a PostgreSQL row arrives as a new row with a higher lsn;
-- ReplacingMergeTree keeps the latest one per sorting key and drops it entirely when is_deleted = 1
CREATE TABLE order_history
(
    id           UUID,
    order_id     UUID,
    customer_id  String,
    total_amount Decimal(18, 4),
    status       String,
    created_at   DateTime64(3),
    updated_at   DateTime64(3),
    version      Int64,
    -- PostgreSQL WAL position of the change: increases with every change in the database
    lsn          UInt64,
    is_deleted   UInt8
)
ENGINE = ReplacingMergeTree(lsn, is_deleted)
PARTITION BY toYYYYMM(created_at)
-- Only columns that never change for one id, so every change of a row replaces the same row in the same partition
ORDER BY (customer_id, order_id, updated_at, id);

-- Kafka consumer of the Debezium topic (flattened by ExtractNewRecordState); it stores nothing itself
CREATE TABLE IF NOT EXISTS order_history_sync
(
    id           String,
    order_id     String,
    customer_id  String,
    total_amount String,
    status       String,
    created_at   String,
    updated_at   String,
    version      Int64,
    __lsn        UInt64,
    __deleted    String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = '${kafka_broker_list}',
         kafka_topic_list = 'order_history_cdc.public.order_history',
         kafka_group_name = 'clickhouse_order_history',
         kafka_format = 'JSONEachRow',
         input_format_skip_unknown_fields = 1;

-- Moves every consumed message into the target table, converting the Debezium JSON types
CREATE MATERIALIZED VIEW IF NOT EXISTS order_history_mv TO order_history AS
SELECT toUUID(id)                                         AS id,
       toUUID(order_id)                                   AS order_id,
       customer_id,
       toDecimal64(total_amount, 4)                       AS total_amount,
       status,
       parseDateTime64BestEffort(created_at, 3, 'UTC')    AS created_at,
       parseDateTime64BestEffort(updated_at, 3, 'UTC')    AS updated_at,
       version,
       __lsn                                              AS lsn,
       toUInt8(__deleted = 'true')                        AS is_deleted
FROM order_history_sync;

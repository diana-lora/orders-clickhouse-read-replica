-- 4. Order history, primary store of every state of an order; ClickHouse is synchronised from it (read replica)
CREATE TABLE order_history
(
    id           UUID PRIMARY KEY,
    order_id     UUID                     NOT NULL,
    customer_id  VARCHAR(255)             NOT NULL,
    total_amount NUMERIC(18, 4)           NOT NULL,
    status       VARCHAR(50)              NOT NULL,
    created_at   TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at   TIMESTAMP WITH TIME ZONE NOT NULL,
    version      BIGINT                   NOT NULL,
    CONSTRAINT uq_order_history_order_updated UNIQUE (order_id, updated_at)
);

-- CDC to ClickHouse: update and delete events carry the whole row, so a deleted state can be marked in the replica
ALTER TABLE order_history REPLICA IDENTITY FULL;

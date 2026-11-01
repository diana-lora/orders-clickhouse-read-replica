package com.example.clickreadrep.history.infra.db

import com.example.clickreadrep.commons.OrderStatus
import com.example.clickreadrep.commons.toUtcInstant
import com.example.clickreadrep.history.application.queries.GetOrderHistoryQuery
import com.example.clickreadrep.history.domain.OrderHistory
import com.example.clickreadrep.history.domain.OrderHistoryReadRepository
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Repository
import java.sql.ResultSet
import java.time.LocalDateTime
import javax.sql.DataSource

// Read replica of the order history; it is filled by the CDC sync from PostgreSQL, never by the application
@Repository
open class ClickhouseOrderHistoryRepository(
    @Qualifier("clickhouseDataSource") private val datasource: DataSource,
) : OrderHistoryReadRepository {
    protected val log = KotlinLogging.logger {}

    /**
     * Reconstructs the state of every order exactly as it was at [targetTime] (time travel).
     *
     * **How the latest version is calculated:** each status change is appended as a new row, so one order owns
     * several rows that differ only by `version` (the Kafka offset of the event). Instead of the expensive `FINAL`
     * modifier, the rows are collapsed in memory with `GROUP BY order_id`: `argMax(column, (version, updated_at))`
     * returns the value of the row with the highest version, i.e. the most recent state. A history repair stores
     * the whole timeline with one shared version, so `updated_at` breaks the tie and the latest repaired step wins.
     * `created_at` is the same on every row of an order, so `any(created_at)` is enough.
     *
     * **How the search is restricted to partitions:** the table is partitioned by `toYYYYMM(created_at)`. An order
     * created after [targetTime] cannot be part of the snapshot, so the `created_at <= ?` filter lets ClickHouse
     * skip all monthly partitions newer than [targetTime] without reading them. Inside the remaining partitions,
     * `updated_at <= ?` drops the versions that happened after [targetTime] before the aggregation runs.
     */
    override fun findBy(query: GetOrderHistoryQuery): List<OrderHistory> {
        val customerFilter = if (query.customerId != null) "AND customer_id = ?" else ""
        val orderFilter = if (query.orderId != null) "AND order_id = ?" else ""

        val sql =
            """
            SELECT
                order_id,
                argMax(customer_id, (version, updated_at)) AS latest_customer_id,
                argMax(total_amount, (version, updated_at)) AS latest_total_amount,
                argMax(status, (version, updated_at)) AS latest_status,
                any(created_at) AS latest_created_at,
                argMax(updated_at, (version, updated_at)) AS latest_updated_at,
                max(version) AS latest_version
            FROM order_history
            WHERE created_at <= ? AND updated_at <= ?
                $customerFilter
                $orderFilter
            GROUP BY order_id
            """.trimIndent()

        val records = mutableListOf<OrderHistory>()

        datasource.connection.use { connection ->
            connection.prepareStatement(sql).use { stmt ->
                stmt.setObject(1, query.targetTime)
                stmt.setObject(2, query.targetTime)
                // Optional filters take the next free placeholder, in the same order as in the SQL text
                var parameterIndex = 3
                query.customerId?.let { stmt.setString(parameterIndex++, it) }
                query.orderId?.let { stmt.setObject(parameterIndex, it) }

                stmt.executeQuery().use { resultSet ->
                    while (resultSet.next()) {
                        records.add(resultSet.toOrderHistory())
                    }
                }
            }
        }
        log.info {
            "Time travel execution completed for timestamp: ${query.targetTime} " +
                "(customerId=${query.customerId}, orderId=${query.orderId}). Found ${records.size} historical states."
        }
        return records
    }

    protected fun ResultSet.toOrderHistory(): OrderHistory =
        OrderHistory(
            orderId = getObject("order_id", java.util.UUID::class.java),
            customerId = getString("latest_customer_id"),
            totalAmount = getBigDecimal("latest_total_amount"),
            orderStatus = OrderStatus.valueOf(getString("latest_status")),
            createdAt = getObject("latest_created_at", LocalDateTime::class.java).toUtcInstant(),
            updatedAt = getObject("latest_updated_at", LocalDateTime::class.java).toUtcInstant(),
            version = getLong("latest_version"),
        )
}

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
     * Returns every order as it was at [GetOrderHistoryQuery.targetTime] (time travel).
     *
     * - **`FINAL`**: the table receives every PostgreSQL change as a new row. `FINAL` keeps only the latest change per
     *   row `id` (highest `lsn`) and drops deleted rows, which background merges would otherwise do at an unknown time.
     * - **`argMax(..., updated_at)`**: an order still has one row per state, so `GROUP BY order_id` keeps the most
     *   recent state up to the target time. `version` cannot order states, because it only counts the updates of
     *   a single row.
     *
     * All filters use sorting or partition key columns, so ClickHouse applies them before `FINAL` and skips the
     * monthly partitions newer than the target time.
     */
    override fun findBy(query: GetOrderHistoryQuery): List<OrderHistory> {
        val customerFilter = if (query.customerId != null) "AND customer_id = ?" else ""
        val orderFilter = if (query.orderId != null) "AND order_id = ?" else ""

        val sql =
            """
            SELECT
                order_id,
                argMax(customer_id, updated_at) AS latest_customer_id,
                argMax(total_amount, updated_at) AS latest_total_amount,
                argMax(status, updated_at) AS latest_status,
                any(created_at) AS latest_created_at,
                max(updated_at) AS latest_updated_at,
                argMax(version, updated_at) AS latest_version
            FROM order_history FINAL
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

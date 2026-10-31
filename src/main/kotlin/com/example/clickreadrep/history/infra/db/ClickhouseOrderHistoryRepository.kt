package com.example.clickreadrep.history.infra.db

import com.example.clickreadrep.commons.OrderStatus
import com.example.clickreadrep.commons.monthPartitionRange
import com.example.clickreadrep.commons.toUtcInstant
import com.example.clickreadrep.history.application.queries.GetOrderHistoryQuery
import com.example.clickreadrep.history.domain.OrderHistory
import com.example.clickreadrep.history.domain.OrderHistoryRepository
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Repository
import java.math.BigDecimal
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.time.Instant
import java.time.LocalDateTime
import java.util.UUID
import javax.sql.DataSource

@Repository
open class ClickhouseOrderHistoryRepository(
    @Qualifier("clickhouseDataSource") private val datasource: DataSource,
) : OrderHistoryRepository {
    protected val log = KotlinLogging.logger {}

    override fun save(history: OrderHistory): OrderHistory {
        datasource.connection.use { connection ->
            connection.prepareStatement(INSERT_SQL).use { stmt ->
                stmt.setOrderHistory(history)
                stmt.executeUpdate()
            }
        }
        log.info {
            "Successfully appended domain record to ClickHouse for Order ID: ${history.orderId} with version offset: ${history.version}"
        }
        return history
    }

    override fun saveAll(histories: List<OrderHistory>) {
        datasource.connection.use { connection ->
            connection.prepareStatement(INSERT_SQL).use { stmt ->
                histories.forEach { history ->
                    stmt.setOrderHistory(history)
                    stmt.addBatch()
                }
                // The driver sends the whole batch as one INSERT, which ClickHouse writes as a single block
                stmt.executeBatch()
            }
        }
        log.info { "Successfully appended ${histories.size} domain records to ClickHouse in one batch" }
    }

    /**
     * Removes every version of an order older than [version] (lightweight delete), after a history repair.
     *
     * **Why the delete runs after the repair insert:** the repaired rows already win every `argMax` because of
     * their higher version, so readers never see the order without rows. Redelivering the repair is idempotent:
     * the inserted rows deduplicate (same sorting key and version) and the second delete matches nothing.
     *
     * **Why there is no `customer_id` or partition filter:** the delete must also remove orphan rows, i.e. rows
     * without a repaired counterpart, which may hold a wrong `customer_id` or a wrong `created_at` and therefore
     * live in another partition. The lightweight delete only marks the matching rows as deleted; background merges
     * remove them physically.
     */
    override fun deleteVersionsBefore(
        orderId: UUID,
        version: Long,
    ) {
        val sql = "DELETE FROM order_history WHERE order_id = ? AND version < ?"

        datasource.connection.use { connection ->
            connection.prepareStatement(sql).use { stmt ->
                stmt.setObject(1, orderId)
                stmt.setLong(2, version)
                stmt.executeUpdate()
            }
        }
        log.info { "Successfully deleted versions older than $version in ClickHouse for Order ID: $orderId" }
    }

    private fun PreparedStatement.setOrderHistory(history: OrderHistory) {
        setObject(1, history.orderId)
        setString(2, history.customerId)
        setBigDecimal(3, history.totalAmount)
        setString(4, history.orderStatus.name)
        // Instants are bound directly: the driver converts a LocalDateTime from the JVM time zone
        setObject(5, history.createdAt)
        setObject(6, history.updatedAt)
        setLong(7, history.version)
    }

    /**
     * Retroactively corrects the total amount of an order in **all** its versions (lightweight update).
     *
     * **Why an update instead of an append:** every event carries the full order state, so a wrong amount is copied
     * into every version. An appended row would only replace one version in `argMax`, so earlier snapshots would keep
     * the wrong value. The lightweight `UPDATE` writes a small patch part that every read applies, so all snapshots
     * return the corrected amount while `status`, `updated_at` and `version` stay untouched.
     *
     * **How the patch is restricted to one partition:** all versions of an order share the same `created_at`, so the
     * table partition `toYYYYMM(created_at)` is computed from [createdAt]. The `created_at` range from the start of
     * that month (UTC) to the start of the next month lets ClickHouse skip every other partition, while
     * `customer_id` and `order_id` match the prefix of the sorting key.
     */
    override fun updateTotalAmount(
        orderId: UUID,
        customerId: String,
        createdAt: Instant,
        newTotalAmount: BigDecimal,
    ) {
        val sql =
            """
            UPDATE order_history
            SET total_amount = ?
            WHERE customer_id = ? AND order_id = ?
              AND created_at >= ? AND created_at < ?
            """.trimIndent()

        val (partitionStart, partitionEnd) = monthPartitionRange(createdAt)

        datasource.connection.use { connection ->
            connection.prepareStatement(sql).use { stmt ->
                stmt.setBigDecimal(1, newTotalAmount)
                stmt.setString(2, customerId)
                stmt.setObject(3, orderId)
                stmt.setObject(4, partitionStart)
                stmt.setObject(5, partitionEnd)
                stmt.executeUpdate()
            }
        }
        log.info { "Successfully patched total amount in ClickHouse for Order ID: $orderId to $newTotalAmount" }
    }

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

    private companion object {
        val INSERT_SQL =
            """
            INSERT INTO order_history (order_id, customer_id, total_amount, status, created_at, updated_at, version)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """.trimIndent()
    }
}

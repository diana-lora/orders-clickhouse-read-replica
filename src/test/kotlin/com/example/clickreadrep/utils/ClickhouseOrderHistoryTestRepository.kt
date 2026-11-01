package com.example.clickreadrep.utils

import com.example.clickreadrep.history.domain.OrderHistory
import com.example.clickreadrep.history.infra.db.ClickhouseOrderHistoryRepository
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Repository
import java.sql.PreparedStatement
import java.util.UUID
import javax.sql.DataSource

@Repository
class ClickhouseOrderHistoryTestRepository(
    @Qualifier("clickhouseDataSource") private val datasource: DataSource,
) : ClickhouseOrderHistoryRepository(datasource) {
    // Seeds ClickHouse directly: the application itself only reads from it
    fun save(history: OrderHistory): OrderHistory {
        val sql =
            """
            INSERT INTO order_history (order_id, customer_id, total_amount, status, created_at, updated_at, version)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """.trimIndent()
        datasource.connection.use { connection ->
            connection.prepareStatement(sql).use { stmt ->
                stmt.setObject(1, history.orderId)
                stmt.setString(2, history.customerId)
                stmt.setBigDecimal(3, history.totalAmount)
                stmt.setString(4, history.orderStatus.name)
                // Instants are bound directly: the driver converts a LocalDateTime from the JVM time zone
                stmt.setObject(5, history.createdAt)
                stmt.setObject(6, history.updatedAt)
                stmt.setLong(7, history.version ?: 0)
                stmt.executeUpdate()
            }
        }
        return history
    }

    fun findAll(): List<OrderHistory> = findLatestStates(whereClause = "")

    // Returns the latest state of the order (one consolidated row), not every stored version
    fun findByOrderId(orderId: UUID): List<OrderHistory> =
        findLatestStates(whereClause = "WHERE order_id = ?") { stmt -> stmt.setObject(1, orderId) }

    // Counts the stored versions of the order (no aggregation); rows marked by a lightweight delete are excluded
    fun countRows(orderId: UUID): Long =
        datasource.connection.use { connection ->
            connection.prepareStatement("SELECT count() FROM order_history WHERE order_id = ?").use { stmt ->
                stmt.setObject(1, orderId)
                stmt.executeQuery().use { resultSet ->
                    resultSet.next()
                    resultSet.getLong(1)
                }
            }
        }

    /** Forces ClickHouse to merge all parts immediately, as a background merge would do eventually. */
    fun optimizeFinal() {
        datasource.connection.use { connection ->
            connection.createStatement().use { stmt -> stmt.execute("OPTIMIZE TABLE order_history FINAL") }
        }
    }

    private fun findLatestStates(
        whereClause: String,
        bind: (PreparedStatement) -> Unit = {},
    ): List<OrderHistory> {
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
            $whereClause
            GROUP BY order_id
            """.trimIndent()
        val records = mutableListOf<OrderHistory>()

        datasource.connection.use { connection ->
            connection.prepareStatement(sql).use { stmt ->
                bind(stmt)
                stmt.executeQuery().use { resultSet ->
                    while (resultSet.next()) {
                        records.add(resultSet.toOrderHistory())
                    }
                }
            }
        }
        log.info { "Successfully fetched ${records.size} consolidated records from ClickHouse using argMax aggregation" }
        return records
    }
}

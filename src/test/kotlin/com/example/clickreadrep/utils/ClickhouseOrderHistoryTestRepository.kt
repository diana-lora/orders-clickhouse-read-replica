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
    // Returns the latest state of the order (one consolidated row), not every stored state
    fun findByOrderId(orderId: UUID): List<OrderHistory> =
        findLatestStates(whereClause = "WHERE order_id = ?") { stmt -> stmt.setObject(1, orderId) }

    // Counts the current states of the order: FINAL hides replaced changes and deleted rows
    fun countRows(orderId: UUID): Long =
        datasource.connection.use { connection ->
            connection.prepareStatement("SELECT count() FROM order_history FINAL WHERE order_id = ?").use { stmt ->
                stmt.setObject(1, orderId)
                stmt.executeQuery().use { resultSet ->
                    resultSet.next()
                    resultSet.getLong(1)
                }
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
                argMax(customer_id, updated_at) AS latest_customer_id,
                argMax(total_amount, updated_at) AS latest_total_amount,
                argMax(status, updated_at) AS latest_status,
                any(created_at) AS latest_created_at,
                max(updated_at) AS latest_updated_at,
                argMax(version, updated_at) AS latest_version
            FROM order_history FINAL
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
        return records
    }
}

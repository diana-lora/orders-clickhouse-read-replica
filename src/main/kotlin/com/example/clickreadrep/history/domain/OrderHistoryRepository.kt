package com.example.clickreadrep.history.domain

import com.example.clickreadrep.history.application.queries.GetOrderHistoryQuery
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

interface OrderHistoryRepository {
    fun save(history: OrderHistory): OrderHistory

    // Appends all versions in one INSERT block, so ClickHouse applies them atomically
    fun saveAll(histories: List<OrderHistory>)

    // Lightweight DELETE of every version of the order older than version, in every partition
    fun deleteVersionsBefore(
        orderId: UUID,
        version: Long,
    )

    // Overwrites the amount in every version of the order, leaving status, updatedAt and version untouched
    fun updateTotalAmount(
        orderId: UUID,
        customerId: String,
        createdAt: Instant,
        newTotalAmount: BigDecimal,
    )

    // Latest state of each order as it was at targetTime; customerId and orderId narrow the result when present
    fun findBy(query: GetOrderHistoryQuery): List<OrderHistory>
}

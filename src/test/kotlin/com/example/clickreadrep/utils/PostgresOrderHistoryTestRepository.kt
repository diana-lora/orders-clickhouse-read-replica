package com.example.clickreadrep.utils

import com.example.clickreadrep.history.domain.OrderHistory
import com.example.clickreadrep.history.infra.db.PostgresOrderHistoryRepository
import java.util.UUID

// Test-only queries on top of the production repository
interface PostgresOrderHistoryTestRepository : PostgresOrderHistoryRepository {
    // Every stored state of the order
    fun findAllByOrderId(orderId: UUID): List<OrderHistory>
}

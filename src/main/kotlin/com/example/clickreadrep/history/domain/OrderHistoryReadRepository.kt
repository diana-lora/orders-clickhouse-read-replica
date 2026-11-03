package com.example.clickreadrep.history.domain

import com.example.clickreadrep.history.application.queries.GetOrderHistoryQuery

// Read side of the order history, backed by the read replica (ClickHouse)
interface OrderHistoryReadRepository {
    // Latest state of each order as it was at targetTime; customerId and orderId narrow the result when present
    fun findBy(query: GetOrderHistoryQuery): List<OrderHistory>
}

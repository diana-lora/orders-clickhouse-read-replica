package com.example.clickreadrep.history.application.queries

import java.time.Instant
import java.util.UUID

data class GetOrderHistoryQuery(
    val targetTime: Instant,
    val customerId: String? = null,
    val orderId: UUID? = null,
)

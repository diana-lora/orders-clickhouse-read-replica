package com.example.clickreadrep.history.domain

import com.example.clickreadrep.commons.OrderStatus
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

data class OrderHistory(
    val orderId: UUID,
    val customerId: String,
    val totalAmount: BigDecimal,
    val orderStatus: OrderStatus,
    val createdAt: Instant,
    val updatedAt: Instant,
    val version: Long = 0L,
)

package com.example.clickreadrep.order.infra.web.dtos

import com.example.clickreadrep.commons.OrderStatus
import com.example.clickreadrep.order.domain.Order
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

data class CreateOrderRequest(
    val customerId: String,
    val totalAmount: BigDecimal,
)

data class OrderResponse(
    val id: UUID,
    val customerId: String,
    val totalAmount: BigDecimal,
    val status: OrderStatus,
    val createdAt: Instant,
) {
    companion object {
        fun fromDomain(order: Order): OrderResponse =
            OrderResponse(
                id = order.id,
                customerId = order.customerId,
                totalAmount = order.totalAmount,
                status = order.status,
                createdAt = order.createdAt,
            )
    }
}

data class UpdateOrderStatusRequest(
    val status: OrderStatus,
)

data class UpdateOrderTotalAmountRequest(
    val totalAmount: BigDecimal,
)

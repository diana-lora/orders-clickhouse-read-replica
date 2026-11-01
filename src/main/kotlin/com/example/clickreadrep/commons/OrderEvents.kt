package com.example.clickreadrep.commons

import com.example.clickreadrep.commons.OrderEventType.ORDER_CREATED
import com.example.clickreadrep.commons.OrderEventType.ORDER_REPAIR
import com.example.clickreadrep.commons.OrderEventType.ORDER_STATUS_UPDATED
import com.example.clickreadrep.commons.OrderEventType.ORDER_TOTAL_AMOUNT_REPAIR
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import java.math.BigDecimal
import java.time.Instant

object OrderEventType {
    const val ORDER_CREATED = "ORDER_CREATED"
    const val ORDER_STATUS_UPDATED = "ORDER_STATUS_UPDATED"
    const val ORDER_TOTAL_AMOUNT_REPAIR = "ORDER_TOTAL_AMOUNT_REPAIR"
    const val ORDER_REPAIR = "ORDER_REPAIR"
}

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type",
)
@JsonSubTypes(
    JsonSubTypes.Type(value = OrderCreatedEvent::class, name = ORDER_CREATED),
    JsonSubTypes.Type(value = OrderStatusUpdatedEvent::class, name = ORDER_STATUS_UPDATED),
    JsonSubTypes.Type(value = OrderTotalAmountRepairEvent::class, name = ORDER_TOTAL_AMOUNT_REPAIR),
    JsonSubTypes.Type(value = OrderRepairEvent::class, name = ORDER_REPAIR),
)
sealed interface OrderEvent

data class OrderCreatedEvent(
    val orderId: String,
    val customerId: String,
    val totalAmount: BigDecimal,
    val status: OrderStatus,
    val createdAt: Instant,
    val updatedAt: Instant,
) : OrderEvent

data class OrderStatusUpdatedEvent(
    val orderId: String,
    val customerId: String,
    val totalAmount: BigDecimal,
    val newStatus: OrderStatus,
    val createdAt: Instant,
    val updatedAt: Instant,
) : OrderEvent

// Retroactive repair of the amount: patches every stored state of the order, so it carries no status or updatedAt
data class OrderTotalAmountRepairEvent(
    val orderId: String,
    val customerId: String,
    val newTotalAmount: BigDecimal,
    val createdAt: Instant,
) : OrderEvent

// History rebuild: carries the whole status timeline, so every version is replaced at its original updatedAt
data class OrderRepairEvent(
    val orderId: String,
    val customerId: String,
    // Current amount: corrections are retroactive, so it applies to every step of the timeline
    val totalAmount: BigDecimal,
    val createdAt: Instant,
    // Chronological order, the first step is the order creation
    val steps: List<OrderRepairStep>,
) : OrderEvent

data class OrderRepairStep(
    val status: OrderStatus,
    val updatedAt: Instant,
)

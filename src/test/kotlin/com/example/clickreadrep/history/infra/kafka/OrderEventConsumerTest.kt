package com.example.clickreadrep.history.infra.kafka

import com.example.clickreadrep.commons.OrderCreatedEvent
import com.example.clickreadrep.commons.OrderEvent
import com.example.clickreadrep.commons.OrderRepairEvent
import com.example.clickreadrep.commons.OrderRepairStep
import com.example.clickreadrep.commons.OrderStatus
import com.example.clickreadrep.commons.OrderStatusUpdatedEvent
import com.example.clickreadrep.commons.OrderTotalAmountRepairEvent
import com.example.clickreadrep.config.KafkaConfig
import com.example.clickreadrep.history.application.ProcessOrderHistoryService
import com.fasterxml.jackson.core.JsonProcessingException
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.math.BigDecimal
import java.time.Instant

class OrderEventConsumerTest {
    private val processOrderHistoryService = mockk<ProcessOrderHistoryService>(relaxed = true)
    private val objectMapper = KafkaConfig("unused").customObjectMapper()
    private val consumer = OrderEventConsumer(processOrderHistoryService, objectMapper)

    private val createdAt = Instant.parse("2026-01-15T10:00:00Z")

    private fun toJson(event: OrderEvent): String = objectMapper.writerFor(OrderEvent::class.java).writeValueAsString(event)

    @Test
    fun `should log the order creation`() {
        val event = OrderCreatedEvent("order-1", "customer-1", BigDecimal("49.90"), OrderStatus.CREATED, createdAt, createdAt)

        consumer.consumeOrderCreatedEvent(toJson(event))

        verify { processOrderHistoryService.logOrderCreation(event) }
    }

    @Test
    fun `should log the status update`() {
        val updatedAt = Instant.parse("2026-01-15T10:05:00Z")
        val event =
            OrderStatusUpdatedEvent("order-1", "customer-1", BigDecimal("49.90"), OrderStatus.PROCESSING, createdAt, updatedAt)

        consumer.consumeOrderCreatedEvent(toJson(event))

        verify { processOrderHistoryService.logOrderStatusUpdate(event) }
    }

    @Test
    fun `should repair the total amount`() {
        val event = OrderTotalAmountRepairEvent("order-1", "customer-1", BigDecimal("9.90"), createdAt)

        consumer.consumeOrderCreatedEvent(toJson(event))

        verify { processOrderHistoryService.repairOrderTotalAmount(event) }
    }

    @Test
    fun `should repair the order history`() {
        val event =
            OrderRepairEvent(
                "order-1",
                "customer-1",
                BigDecimal("49.90"),
                createdAt,
                listOf(OrderRepairStep(OrderStatus.CREATED, createdAt)),
            )

        consumer.consumeOrderCreatedEvent(toJson(event))

        verify { processOrderHistoryService.repairOrderHistory(event) }
    }

    @Test
    fun `should reject an unknown event type`() {
        val payload = """{"type": "ORDER_DELETED", "orderId": "order-1"}"""

        assertThrows<JsonProcessingException> { consumer.consumeOrderCreatedEvent(payload) }
        verify(exactly = 0) { processOrderHistoryService.logOrderCreation(any()) }
    }

    @Test
    fun `should reject a malformed payload`() {
        assertThrows<JsonProcessingException> { consumer.consumeOrderCreatedEvent("not-json") }
    }
}

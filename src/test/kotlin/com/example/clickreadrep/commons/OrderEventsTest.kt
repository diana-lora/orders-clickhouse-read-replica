package com.example.clickreadrep.commons

import com.example.clickreadrep.commons.OrderEventType.ORDER_CREATED
import com.example.clickreadrep.commons.OrderEventType.ORDER_REPAIR
import com.example.clickreadrep.commons.OrderEventType.ORDER_STATUS_UPDATED
import com.example.clickreadrep.commons.OrderEventType.ORDER_TOTAL_AMOUNT_UPDATED
import com.example.clickreadrep.config.KafkaConfig
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.time.Instant

class OrderEventsTest {
    private val objectMapper = KafkaConfig("unused").customObjectMapper()
    private val createdAt = Instant.parse("2026-01-15T10:00:00Z")
    private val updatedAt = Instant.parse("2026-01-15T10:05:00Z")

    private val eventsByType =
        mapOf(
            ORDER_CREATED to
                OrderCreatedEvent("order-1", "customer-1", BigDecimal("49.90"), OrderStatus.CREATED, createdAt, createdAt),
            ORDER_STATUS_UPDATED to
                OrderStatusUpdatedEvent("order-1", "customer-1", BigDecimal("49.90"), OrderStatus.PROCESSING, createdAt, updatedAt),
            ORDER_TOTAL_AMOUNT_UPDATED to
                OrderTotalAmountUpdateEvent("order-1", "customer-1", BigDecimal("9.90"), createdAt),
            ORDER_REPAIR to
                OrderRepairEvent(
                    "order-1",
                    "customer-1",
                    BigDecimal("9.90"),
                    createdAt,
                    listOf(OrderRepairStep(OrderStatus.CREATED, createdAt), OrderRepairStep(OrderStatus.PROCESSING, updatedAt)),
                ),
        )

    @Test
    fun `should write the event type as discriminator`() {
        eventsByType.forEach { (type, event) ->
            val json = objectMapper.writerFor(OrderEvent::class.java).writeValueAsString(event)

            assertThat(objectMapper.readTree(json)["type"].asText()).isEqualTo(type)
        }
    }

    @Test
    fun `should read every event back into its own type`() {
        eventsByType.values.forEach { event ->
            val json = objectMapper.writerFor(OrderEvent::class.java).writeValueAsString(event)

            assertThat(objectMapper.readValue(json, OrderEvent::class.java)).isEqualTo(event)
        }
    }
}

package com.example.clickreadrep.order.application

import com.example.clickreadrep.commons.OrderEvent
import com.example.clickreadrep.commons.OrderEventType.ORDER_REPAIR
import com.example.clickreadrep.commons.OrderRepairEvent
import com.example.clickreadrep.commons.OrderRepairStep
import com.example.clickreadrep.commons.OrderStatus
import com.example.clickreadrep.config.KafkaConfig
import com.example.clickreadrep.config.KafkaConfig.Companion.ORDER_EVENTS_TOPIC
import com.example.clickreadrep.order.application.commands.RepairOrderHistoryCommand
import com.example.clickreadrep.order.domain.Order
import com.example.clickreadrep.order.domain.OrderRepository
import com.example.clickreadrep.order.domain.OrderStatusChange
import com.example.clickreadrep.order.domain.OrderStatusChangeRepository
import com.example.clickreadrep.order.domain.exceptions.OrderNotFoundException
import com.example.clickreadrep.order.domain.outbox.OutboxEvent
import com.example.clickreadrep.order.domain.outbox.OutboxRepository
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

class RepairOrderHistoryServiceTest {
    private val orderRepository = mockk<OrderRepository>()
    private val orderStatusChangeRepository = mockk<OrderStatusChangeRepository>()
    private val outboxRepository = mockk<OutboxRepository>()
    private val objectMapper = KafkaConfig("unused").customObjectMapper()
    private val service =
        RepairOrderHistoryService(orderRepository, orderStatusChangeRepository, OutboxEventService(outboxRepository, objectMapper))

    private val orderCreatedAt = Instant.parse("2026-01-15T10:00:00Z")
    private val processingAt = Instant.parse("2026-01-15T10:05:00Z")
    private val completedAt = Instant.parse("2026-01-15T10:10:00Z")
    private val order =
        Order(
            customerId = "customer-1",
            totalAmount = BigDecimal("9.90"),
            status = OrderStatus.COMPLETED,
            createdAt = orderCreatedAt,
            version = 2,
        )

    @Test
    fun `should save an ORDER_REPAIR outbox event with the whole status timeline`() {
        every { orderRepository.findLockedOrderById(order.id) } returns order
        every { orderStatusChangeRepository.findAllByOrderIdOrderByCreatedAtAsc(order.id) } returns
            listOf(
                OrderStatusChange(orderId = order.id, status = OrderStatus.CREATED, createdAt = orderCreatedAt),
                OrderStatusChange(orderId = order.id, status = OrderStatus.PROCESSING, createdAt = processingAt),
                OrderStatusChange(orderId = order.id, status = OrderStatus.COMPLETED, createdAt = completedAt),
            )
        val outboxEvent = slot<OutboxEvent>()
        every { outboxRepository.save(capture(outboxEvent)) } answers { firstArg() }

        service.execute(RepairOrderHistoryCommand(order.id))

        with(outboxEvent.captured) {
            assertThat(eventType).isEqualTo(ORDER_REPAIR)
            assertThat(aggregateId).isEqualTo(order.id.toString())
            // Same partition as every earlier event of the order, so the repair offset is higher than all of them
            assertThat(aggregateKey).isEqualTo("customer-1")
            assertThat(targetTopic).isEqualTo(ORDER_EVENTS_TOPIC)
            assertThat(objectMapper.readValue(payload, OrderEvent::class.java)).isEqualTo(
                OrderRepairEvent(
                    orderId = order.id.toString(),
                    customerId = "customer-1",
                    // The current amount applies to every step
                    totalAmount = BigDecimal("9.90"),
                    createdAt = orderCreatedAt,
                    steps =
                        listOf(
                            OrderRepairStep(OrderStatus.CREATED, orderCreatedAt),
                            OrderRepairStep(OrderStatus.PROCESSING, processingAt),
                            OrderRepairStep(OrderStatus.COMPLETED, completedAt),
                        ),
                ),
            )
        }
    }

    @Test
    fun `should throw and save nothing when the order does not exist`() {
        val unknownOrderId = UUID.randomUUID()
        every { orderRepository.findLockedOrderById(unknownOrderId) } returns null

        assertThrows<OrderNotFoundException> { service.execute(RepairOrderHistoryCommand(unknownOrderId)) }

        verify(exactly = 0) { outboxRepository.save(any()) }
    }
}

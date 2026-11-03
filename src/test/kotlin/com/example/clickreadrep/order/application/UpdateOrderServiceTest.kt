package com.example.clickreadrep.order.application

import com.example.clickreadrep.commons.OrderEvent
import com.example.clickreadrep.commons.OrderEventType.ORDER_STATUS_UPDATED
import com.example.clickreadrep.commons.OrderEventType.ORDER_TOTAL_AMOUNT_REPAIR
import com.example.clickreadrep.commons.OrderStatus
import com.example.clickreadrep.commons.OrderStatusUpdatedEvent
import com.example.clickreadrep.commons.OrderTotalAmountRepairEvent
import com.example.clickreadrep.config.KafkaConfig
import com.example.clickreadrep.order.application.commands.UpdateOrderStatusCommand
import com.example.clickreadrep.order.application.commands.UpdateOrderTotalAmountCommand
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

class UpdateOrderServiceTest {
    private val orderRepository = mockk<OrderRepository>()
    private val orderStatusChangeRepository = mockk<OrderStatusChangeRepository>()
    private val outboxRepository = mockk<OutboxRepository>()
    private val objectMapper = KafkaConfig("unused").customObjectMapper()
    private val service =
        UpdateOrderService(orderRepository, orderStatusChangeRepository, OutboxEventService(outboxRepository, objectMapper))

    private val order =
        Order(
            customerId = "customer-1",
            totalAmount = BigDecimal("99.00"),
            createdAt = Instant.parse("2026-01-15T10:00:00Z"),
            version = 0,
        )
    private val outboxEvent = slot<OutboxEvent>()
    private val statusChange = slot<OrderStatusChange>()

    private fun givenExistingOrder() {
        every { orderRepository.findOrderById(order.id) } returns order
        every { orderRepository.save(any()) } answers { firstArg() }
        every { orderStatusChangeRepository.save(capture(statusChange)) } answers { firstArg() }
        every { outboxRepository.save(capture(outboxEvent)) } answers { firstArg() }
    }

    @Test
    fun `should update the status and save the status change and an ORDER_STATUS_UPDATED outbox event`() {
        givenExistingOrder()

        val updated = service.execute(UpdateOrderStatusCommand(order.id, OrderStatus.PROCESSING))

        assertThat(updated.status).isEqualTo(OrderStatus.PROCESSING)
        assertThat(outboxEvent.captured.eventType).isEqualTo(ORDER_STATUS_UPDATED)
        assertThat(outboxEvent.captured.aggregateKey).isEqualTo("customer-1")
        val event = objectMapper.readValue(outboxEvent.captured.payload, OrderEvent::class.java) as OrderStatusUpdatedEvent
        assertThat(event.orderId).isEqualTo(order.id.toString())
        assertThat(event.newStatus).isEqualTo(OrderStatus.PROCESSING)
        assertThat(event.totalAmount).isEqualByComparingTo("99.00")
        assertThat(event.createdAt).isEqualTo(order.createdAt)
        assertThat(event.updatedAt).isAfter(order.createdAt)
        // Millisecond precision, so PostgreSQL and ClickHouse DateTime64(3) store the same value
        assertThat(event.updatedAt.nano % 1_000_000).isZero()

        // A history repair reproduces this version only if the timeline holds the same timestamp as the event
        with(statusChange.captured) {
            assertThat(orderId).isEqualTo(order.id)
            assertThat(status).isEqualTo(OrderStatus.PROCESSING)
            assertThat(createdAt).isEqualTo(event.updatedAt)
        }
    }

    @Test
    fun `should update the total amount and save an ORDER_TOTAL_AMOUNT_REPAIR outbox event`() {
        givenExistingOrder()

        val updated = service.execute(UpdateOrderTotalAmountCommand(order.id, BigDecimal("9.90")))

        assertThat(updated.totalAmount).isEqualByComparingTo("9.90")
        assertThat(updated.status).isEqualTo(OrderStatus.CREATED)
        // A correction changes no state on the timeline
        verify(exactly = 0) { orderStatusChangeRepository.save(any()) }
        assertThat(outboxEvent.captured.eventType).isEqualTo(ORDER_TOTAL_AMOUNT_REPAIR)
        assertThat(objectMapper.readValue(outboxEvent.captured.payload, OrderEvent::class.java))
            .isEqualTo(
                OrderTotalAmountRepairEvent(
                    orderId = order.id.toString(),
                    customerId = "customer-1",
                    newTotalAmount = BigDecimal("9.90"),
                    // Locates the history partition of the order
                    createdAt = order.createdAt,
                ),
            )
    }

    @Test
    fun `should throw and save nothing when the order does not exist`() {
        val unknownOrderId = UUID.randomUUID()
        every { orderRepository.findOrderById(unknownOrderId) } returns null

        assertThrows<OrderNotFoundException> {
            service.execute(UpdateOrderStatusCommand(unknownOrderId, OrderStatus.PROCESSING))
        }
        assertThrows<OrderNotFoundException> {
            service.execute(UpdateOrderTotalAmountCommand(unknownOrderId, BigDecimal("9.90")))
        }

        verify(exactly = 0) { orderRepository.save(any()) }
        verify(exactly = 0) { outboxRepository.save(any()) }
    }
}

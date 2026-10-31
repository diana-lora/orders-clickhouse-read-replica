package com.example.clickreadrep.history.application

import com.example.clickreadrep.commons.OrderCreatedEvent
import com.example.clickreadrep.commons.OrderRepairEvent
import com.example.clickreadrep.commons.OrderRepairStep
import com.example.clickreadrep.commons.OrderStatus
import com.example.clickreadrep.commons.OrderStatusUpdatedEvent
import com.example.clickreadrep.commons.OrderTotalAmountUpdateEvent
import com.example.clickreadrep.history.domain.OrderHistory
import com.example.clickreadrep.history.domain.OrderHistoryRepository
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.slot
import io.mockk.verify
import io.mockk.verifyOrder
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

class ProcessOrderHistoryServiceTest {
    private val orderHistoryRepository = mockk<OrderHistoryRepository>()
    private val service = ProcessOrderHistoryService(orderHistoryRepository)

    private val orderId = UUID.randomUUID()
    private val createdAt = Instant.parse("2026-01-15T10:00:00Z")
    private val savedHistory = slot<OrderHistory>()

    @Test
    fun `should store the created order as a history version using the Kafka offset`() {
        every { orderHistoryRepository.save(capture(savedHistory)) } answers { firstArg() }

        service.logOrderCreation(
            OrderCreatedEvent(orderId.toString(), "customer-1", BigDecimal("49.90"), OrderStatus.CREATED, createdAt, createdAt),
            version = 101,
        )

        assertThat(savedHistory.captured).isEqualTo(
            OrderHistory(orderId, "customer-1", BigDecimal("49.90"), OrderStatus.CREATED, createdAt, createdAt, version = 101),
        )
    }

    @Test
    fun `should store the status update as a new history version using the Kafka offset`() {
        every { orderHistoryRepository.save(capture(savedHistory)) } answers { firstArg() }
        val updatedAt = Instant.parse("2026-01-15T10:05:00Z")

        service.logOrderStatusUpdate(
            OrderStatusUpdatedEvent(orderId.toString(), "customer-1", BigDecimal("49.90"), OrderStatus.PROCESSING, createdAt, updatedAt),
            version = 102,
        )

        assertThat(savedHistory.captured).isEqualTo(
            OrderHistory(orderId, "customer-1", BigDecimal("49.90"), OrderStatus.PROCESSING, createdAt, updatedAt, version = 102),
        )
    }

    @Test
    fun `should insert the repaired timeline with the Kafka offset and then delete the older versions`() {
        every { orderHistoryRepository.saveAll(any()) } just runs
        every { orderHistoryRepository.deleteVersionsBefore(any(), any()) } just runs
        val processingAt = Instant.parse("2026-01-15T10:05:00Z")

        service.repairOrderHistory(
            OrderRepairEvent(
                orderId.toString(),
                "customer-1",
                BigDecimal("9.90"),
                createdAt,
                listOf(OrderRepairStep(OrderStatus.CREATED, createdAt), OrderRepairStep(OrderStatus.PROCESSING, processingAt)),
            ),
            version = 110,
        )

        // Insert first, so the order never disappears between the two statements
        verifyOrder {
            orderHistoryRepository.saveAll(
                listOf(
                    OrderHistory(orderId, "customer-1", BigDecimal("9.90"), OrderStatus.CREATED, createdAt, createdAt, version = 110),
                    OrderHistory(orderId, "customer-1", BigDecimal("9.90"), OrderStatus.PROCESSING, createdAt, processingAt, version = 110),
                ),
            )
            orderHistoryRepository.deleteVersionsBefore(orderId, 110)
        }
    }

    @Test
    fun `should patch the total amount of every version instead of storing a new one`() {
        every { orderHistoryRepository.updateTotalAmount(any(), any(), any(), any()) } just runs

        service.correctOrderTotalAmount(
            OrderTotalAmountUpdateEvent(orderId.toString(), "customer-1", BigDecimal("9.90"), createdAt),
        )

        verify { orderHistoryRepository.updateTotalAmount(orderId, "customer-1", createdAt, BigDecimal("9.90")) }
        verify(exactly = 0) { orderHistoryRepository.save(any()) }
    }
}

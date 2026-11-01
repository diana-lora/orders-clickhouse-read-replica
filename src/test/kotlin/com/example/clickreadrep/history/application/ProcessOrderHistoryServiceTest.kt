package com.example.clickreadrep.history.application

import com.example.clickreadrep.commons.OrderCreatedEvent
import com.example.clickreadrep.commons.OrderRepairEvent
import com.example.clickreadrep.commons.OrderRepairStep
import com.example.clickreadrep.commons.OrderStatus
import com.example.clickreadrep.commons.OrderStatusUpdatedEvent
import com.example.clickreadrep.commons.OrderTotalAmountRepairEvent
import com.example.clickreadrep.history.domain.OrderHistory
import com.example.clickreadrep.history.domain.OrderHistoryMutationRepository
import com.example.clickreadrep.history.domain.exceptions.OrderHistoryNotFoundException
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.slot
import io.mockk.verify
import io.mockk.verifyOrder
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

class ProcessOrderHistoryServiceTest {
    private val orderHistoryRepository = mockk<OrderHistoryMutationRepository>()
    private val service = ProcessOrderHistoryService(orderHistoryRepository)

    private val orderId = UUID.randomUUID()
    private val createdAt = Instant.parse("2026-01-15T10:00:00Z")
    private val processingAt = Instant.parse("2026-01-15T10:05:00Z")
    private val savedHistory = slot<OrderHistory>()

    private val createdRow =
        OrderHistory(orderId, "customer-1", BigDecimal("49.90"), OrderStatus.CREATED, createdAt, createdAt, version = 0)
    private val processingRow =
        OrderHistory(orderId, "customer-1", BigDecimal("49.90"), OrderStatus.PROCESSING, createdAt, processingAt, version = 0)

    @Test
    fun `should insert the created order as the first row of its history`() {
        every { orderHistoryRepository.findLatestOrderState(orderId) } returns null
        every { orderHistoryRepository.save(capture(savedHistory)) } answers { firstArg() }

        service.logOrderCreation(
            OrderCreatedEvent(orderId.toString(), "customer-1", BigDecimal("49.90"), OrderStatus.CREATED, createdAt, createdAt),
        )

        assertThat(savedHistory.captured)
            .usingRecursiveComparison()
            .ignoringFields("id")
            .isEqualTo(createdRow.copy(version = null))
    }

    @Test
    fun `should skip a redelivered creation event`() {
        every { orderHistoryRepository.findLatestOrderState(orderId) } returns createdRow

        service.logOrderCreation(
            OrderCreatedEvent(orderId.toString(), "customer-1", BigDecimal("49.90"), OrderStatus.CREATED, createdAt, createdAt),
        )

        verify(exactly = 0) { orderHistoryRepository.save(any()) }
    }

    @Test
    fun `should insert the status update as a new row and keep the earlier state`() {
        every { orderHistoryRepository.findLatestOrderState(orderId) } returns createdRow
        every { orderHistoryRepository.save(capture(savedHistory)) } answers { firstArg() }

        service.logOrderStatusUpdate(
            OrderStatusUpdatedEvent(orderId.toString(), "customer-1", BigDecimal("49.90"), OrderStatus.PROCESSING, createdAt, processingAt),
        )

        assertThat(savedHistory.captured)
            .usingRecursiveComparison()
            .ignoringFields("id")
            .isEqualTo(processingRow.copy(version = null))
        assertThat(savedHistory.captured.id).isNotEqualTo(createdRow.id)
    }

    @Test
    fun `should skip a status update that is not newer than the latest state`() {
        every { orderHistoryRepository.findLatestOrderState(orderId) } returns processingRow

        service.logOrderStatusUpdate(
            OrderStatusUpdatedEvent(orderId.toString(), "customer-1", BigDecimal("49.90"), OrderStatus.PROCESSING, createdAt, processingAt),
        )

        verify(exactly = 0) { orderHistoryRepository.save(any()) }
    }

    @Test
    fun `should fail a status update of an order without history`() {
        every { orderHistoryRepository.findLatestOrderState(orderId) } returns null

        assertThrows<OrderHistoryNotFoundException> {
            service.logOrderStatusUpdate(
                OrderStatusUpdatedEvent(
                    orderId.toString(),
                    "customer-1",
                    BigDecimal("49.90"),
                    OrderStatus.PROCESSING,
                    createdAt,
                    processingAt,
                ),
            )
        }
    }

    @Test
    fun `should repair the total amount of every row with one update`() {
        every { orderHistoryRepository.updateTotalAmount(orderId, BigDecimal("9.90")) } returns 2

        service.repairOrderTotalAmount(OrderTotalAmountRepairEvent(orderId.toString(), "customer-1", BigDecimal("9.90"), createdAt))

        verify { orderHistoryRepository.updateTotalAmount(orderId, BigDecimal("9.90")) }
        verify(exactly = 0) { orderHistoryRepository.saveAll(any<List<OrderHistory>>()) }
    }

    @Test
    fun `should replace the whole timeline with the repaired steps`() {
        every { orderHistoryRepository.deleteAllByOrderId(orderId) } just runs
        val inserted = slot<List<OrderHistory>>()
        every { orderHistoryRepository.saveAll(capture(inserted)) } answers { firstArg() }

        service.repairOrderHistory(
            OrderRepairEvent(
                orderId.toString(),
                "customer-1",
                BigDecimal("9.90"),
                createdAt,
                listOf(OrderRepairStep(OrderStatus.CREATED, createdAt), OrderRepairStep(OrderStatus.PROCESSING, processingAt)),
            ),
        )

        // Delete first: the repaired steps reuse the (order_id, updated_at) of the rows they replace
        verifyOrder {
            orderHistoryRepository.deleteAllByOrderId(orderId)
            orderHistoryRepository.saveAll(any<List<OrderHistory>>())
        }
        assertThat(inserted.captured)
            .usingRecursiveFieldByFieldElementComparatorIgnoringFields("id")
            .containsExactly(
                createdRow.copy(totalAmount = BigDecimal("9.90"), version = null),
                processingRow.copy(totalAmount = BigDecimal("9.90"), version = null),
            )
    }
}

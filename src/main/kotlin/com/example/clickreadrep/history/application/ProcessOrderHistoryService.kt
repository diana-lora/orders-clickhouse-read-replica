package com.example.clickreadrep.history.application

import com.example.clickreadrep.commons.OrderCreatedEvent
import com.example.clickreadrep.commons.OrderRepairEvent
import com.example.clickreadrep.commons.OrderStatusUpdatedEvent
import com.example.clickreadrep.commons.OrderTotalAmountRepairEvent
import com.example.clickreadrep.history.domain.OrderHistory
import com.example.clickreadrep.history.domain.OrderHistoryMutationRepository
import com.example.clickreadrep.history.domain.exceptions.OrderHistoryNotFoundException
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.util.UUID

@Service
class ProcessOrderHistoryService(
    @Qualifier("postgresOrderHistoryRepository") private val orderHistoryRepository: OrderHistoryMutationRepository,
) {
    private val log = KotlinLogging.logger {}

    @Transactional
    fun logOrderCreation(event: OrderCreatedEvent) {
        val orderId = UUID.fromString(event.orderId)
        if (orderHistoryRepository.findLatestOrderState(orderId) != null) {
            log.info { "Skipping redelivered creation of Order ID: $orderId, its history already exists" }
            return
        }

        orderHistoryRepository.save(
            OrderHistory(
                orderId = orderId,
                customerId = event.customerId,
                totalAmount = event.totalAmount,
                orderStatus = event.status,
                createdAt = event.createdAt,
                updatedAt = event.updatedAt,
            ),
        )
    }

    @Transactional
    fun logOrderStatusUpdate(event: OrderStatusUpdatedEvent) {
        val orderId = UUID.fromString(event.orderId)
        val latest =
            orderHistoryRepository.findLatestOrderState(orderId)
                ?: throw OrderHistoryNotFoundException("Order history not found (ID=$orderId)")

        if (!event.updatedAt.isAfter(latest.updatedAt)) {
            log.info { "Skipping stale status update of Order ID: $orderId at ${event.updatedAt} (latest: ${latest.updatedAt})" }
            return
        }

        orderHistoryRepository.save(
            OrderHistory(
                orderId = orderId,
                customerId = event.customerId,
                totalAmount = event.totalAmount,
                orderStatus = event.newStatus,
                createdAt = event.createdAt,
                updatedAt = event.updatedAt,
            ),
        )
    }

    @Transactional
    fun repairOrderTotalAmount(event: OrderTotalAmountRepairEvent) {
        val orderId = UUID.fromString(event.orderId)
        val updatedRows = orderHistoryRepository.updateTotalAmount(orderId, event.newTotalAmount)
        if (updatedRows == 0) log.warn { "Order history not found (ID=$orderId)" }
    }

    @Transactional
    fun repairOrderHistory(event: OrderRepairEvent) {
        val orderId = UUID.fromString(event.orderId)
        val histories =
            event.steps.map { step ->
                OrderHistory(
                    orderId = orderId,
                    customerId = event.customerId,
                    totalAmount = event.totalAmount,
                    orderStatus = step.status,
                    createdAt = event.createdAt,
                    updatedAt = step.updatedAt,
                )
            }

        orderHistoryRepository.deleteAllByOrderId(orderId)
        orderHistoryRepository.saveAll(histories)
    }
}

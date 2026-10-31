package com.example.clickreadrep.history.application

import com.example.clickreadrep.commons.OrderCreatedEvent
import com.example.clickreadrep.commons.OrderRepairEvent
import com.example.clickreadrep.commons.OrderStatusUpdatedEvent
import com.example.clickreadrep.commons.OrderTotalAmountUpdateEvent
import com.example.clickreadrep.history.domain.OrderHistory
import com.example.clickreadrep.history.domain.OrderHistoryRepository
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Service
import java.util.UUID

@Service
class ProcessOrderHistoryService(
    @Qualifier("clickhouseOrderHistoryRepository") private val orderHistoryRepository: OrderHistoryRepository,
) {
    fun logOrderCreation(
        event: OrderCreatedEvent,
        version: Long,
    ) {
        val history =
            OrderHistory(
                orderId = UUID.fromString(event.orderId),
                customerId = event.customerId,
                totalAmount = event.totalAmount,
                orderStatus = event.status,
                createdAt = event.createdAt,
                updatedAt = event.updatedAt,
                version = version,
            )

        orderHistoryRepository.save(history)
    }

    fun logOrderStatusUpdate(
        event: OrderStatusUpdatedEvent,
        version: Long,
    ) {
        val history =
            OrderHistory(
                orderId = UUID.fromString(event.orderId),
                customerId = event.customerId,
                totalAmount = event.totalAmount,
                orderStatus = event.newStatus,
                createdAt = event.createdAt,
                updatedAt = event.updatedAt,
                version = version,
            )

        orderHistoryRepository.save(history)
    }

    fun repairOrderHistory(
        event: OrderRepairEvent,
        version: Long,
    ) {
        val orderId = UUID.fromString(event.orderId)
        // Every step shares the offset of the repair event; findBy breaks the tie by updatedAt
        val histories =
            event.steps.map { step ->
                OrderHistory(
                    orderId = orderId,
                    customerId = event.customerId,
                    totalAmount = event.totalAmount,
                    orderStatus = step.status,
                    createdAt = event.createdAt,
                    updatedAt = step.updatedAt,
                    version = version,
                )
            }

        orderHistoryRepository.saveAll(histories)
        orderHistoryRepository.deleteVersionsBefore(orderId = orderId, version = version)
    }

    fun correctOrderTotalAmount(event: OrderTotalAmountUpdateEvent) {
        orderHistoryRepository.updateTotalAmount(
            orderId = UUID.fromString(event.orderId),
            customerId = event.customerId,
            createdAt = event.createdAt,
            newTotalAmount = event.newTotalAmount,
        )
    }
}

package com.example.clickreadrep.order.application

import com.example.clickreadrep.commons.OrderRepairEvent
import com.example.clickreadrep.commons.OrderRepairStep
import com.example.clickreadrep.order.application.commands.RepairOrderHistoryCommand
import com.example.clickreadrep.order.domain.OrderRepository
import com.example.clickreadrep.order.domain.OrderStatusChangeRepository
import com.example.clickreadrep.order.domain.exceptions.OrderNotFoundException
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class RepairOrderHistoryService(
    private val orderRepository: OrderRepository,
    private val orderStatusChangeRepository: OrderStatusChangeRepository,
    private val outboxEventService: OutboxEventService,
) {
    @Transactional
    fun execute(command: RepairOrderHistoryCommand) {
        val order =
            orderRepository.findLockedOrderById(command.orderId)
                ?: throw OrderNotFoundException("Order not found (ID=${command.orderId})")

        val steps =
            orderStatusChangeRepository
                .findAllByOrderIdOrderByCreatedAtAsc(order.id)
                .map { OrderRepairStep(status = it.status, updatedAt = it.createdAt) }

        val repairEvent =
            OrderRepairEvent(
                orderId = order.id.toString(),
                customerId = order.customerId,
                totalAmount = order.totalAmount,
                createdAt = order.createdAt,
                steps = steps,
            )
        outboxEventService.save(order, repairEvent)
    }
}

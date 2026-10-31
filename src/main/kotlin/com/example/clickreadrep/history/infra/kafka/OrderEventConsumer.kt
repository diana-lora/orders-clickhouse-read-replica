package com.example.clickreadrep.history.infra.kafka

import com.example.clickreadrep.commons.OrderCreatedEvent
import com.example.clickreadrep.commons.OrderEvent
import com.example.clickreadrep.commons.OrderRepairEvent
import com.example.clickreadrep.commons.OrderStatusUpdatedEvent
import com.example.clickreadrep.commons.OrderTotalAmountUpdateEvent
import com.example.clickreadrep.config.KafkaConfig
import com.example.clickreadrep.history.application.ProcessOrderHistoryService
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.stereotype.Component

@Component
class OrderEventConsumer(
    private val processOrderHistoryService: ProcessOrderHistoryService,
    private val objectMapper: ObjectMapper,
) {
    private val log = LoggerFactory.getLogger(OrderEventConsumer::class.java)

    @KafkaListener(
        topics = [KafkaConfig.ORDER_EVENTS_TOPIC],
        groupId = "\${spring.kafka.consumer.group-id}",
    )
    fun consumeOrderCreatedEvent(
        message: String,
        @Header(KafkaHeaders.OFFSET) offset: Long,
    ) {
        val event = objectMapper.readValue(message, OrderEvent::class.java)

        try {
            when (event) {
                is OrderCreatedEvent -> {
                    log.info("Creating event for Order ID: ${event.orderId}")
                    processOrderHistoryService.logOrderCreation(event = event, version = offset)
                }

                is OrderStatusUpdatedEvent -> {
                    log.info("Updating event's status for Order ID: ${event.orderId} to ${event.newStatus}")
                    processOrderHistoryService.logOrderStatusUpdate(event = event, version = offset)
                }

                is OrderTotalAmountUpdateEvent -> {
                    log.info("Correcting total amount for Order ID: ${event.orderId} to ${event.newTotalAmount}")
                    processOrderHistoryService.correctOrderTotalAmount(event = event)
                }

                is OrderRepairEvent -> {
                    log.info("Repairing history of Order ID: ${event.orderId} with ${event.steps.size} steps")
                    processOrderHistoryService.repairOrderHistory(event = event, version = offset)
                }
            }
        } catch (ex: Exception) {
            log.error("Failed to process order history recording from typed payload contract", ex)
            throw ex
        }
    }
}

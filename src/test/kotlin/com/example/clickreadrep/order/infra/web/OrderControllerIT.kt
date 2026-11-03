package com.example.clickreadrep.order.infra.web

import com.example.clickreadrep.commons.OrderCreatedEvent
import com.example.clickreadrep.commons.OrderEvent
import com.example.clickreadrep.commons.OrderEventType.ORDER_CREATED
import com.example.clickreadrep.commons.OrderEventType.ORDER_REPAIR
import com.example.clickreadrep.commons.OrderEventType.ORDER_STATUS_UPDATED
import com.example.clickreadrep.commons.OrderEventType.ORDER_TOTAL_AMOUNT_REPAIR
import com.example.clickreadrep.commons.OrderRepairEvent
import com.example.clickreadrep.commons.OrderRepairStep
import com.example.clickreadrep.commons.OrderStatus
import com.example.clickreadrep.commons.OrderStatusUpdatedEvent
import com.example.clickreadrep.commons.OrderTotalAmountRepairEvent
import com.example.clickreadrep.config.KafkaConfig.Companion.ORDER_EVENTS_TOPIC
import com.example.clickreadrep.order.domain.Order
import com.example.clickreadrep.order.domain.outbox.OutboxEvent
import com.example.clickreadrep.order.infra.db.PostgresOrderRepository
import com.example.clickreadrep.order.infra.db.PostgresOutboxRepository
import com.example.clickreadrep.order.infra.web.dtos.CreateOrderRequest
import com.example.clickreadrep.order.infra.web.dtos.OrderResponse
import com.example.clickreadrep.order.infra.web.dtos.UpdateOrderStatusRequest
import com.example.clickreadrep.order.infra.web.dtos.UpdateOrderTotalAmountRequest
import com.example.clickreadrep.utils.IntegrationTestContainers
import com.fasterxml.jackson.module.kotlin.readValue
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.get
import org.springframework.test.web.servlet.post
import org.springframework.test.web.servlet.put
import java.math.BigDecimal
import java.util.UUID

/** Order endpoints in, `orders` and `outbox` tables out. The history side is covered by the history tests (OrderEventConsumerIT). */
class OrderControllerIT : IntegrationTestContainers() {
    @Autowired
    private lateinit var orderRepository: PostgresOrderRepository

    @Autowired
    private lateinit var outboxRepository: PostgresOutboxRepository

    private val customerId = "customer-order-${UUID.randomUUID()}"

    @Test
    fun `should store a new order and its ORDER_CREATED outbox event`() {
        val response = createOrder(BigDecimal("49.90"))

        assertThat(response.customerId).isEqualTo(customerId)
        assertThat(response.totalAmount).isEqualByComparingTo("49.90")
        assertThat(response.status).isEqualTo(OrderStatus.CREATED)

        val order = findOrder(response.id)
        assertThat(order.customerId).isEqualTo(customerId)
        assertThat(order.totalAmount).isEqualByComparingTo("49.90")
        assertThat(order.status).isEqualTo(OrderStatus.CREATED)
        assertThat(order.version).isEqualTo(0)

        val outboxEvent = findOutboxEvents(response.id).single()
        assertThat(outboxEvent.eventType).isEqualTo(ORDER_CREATED)
        assertThat(outboxEvent.aggregateType).isEqualTo("Order")
        assertThat(outboxEvent.aggregateKey).isEqualTo(customerId)
        assertThat(outboxEvent.targetTopic).isEqualTo(ORDER_EVENTS_TOPIC)
        val event = readEvent<OrderCreatedEvent>(outboxEvent)
        assertThat(event.orderId).isEqualTo(response.id.toString())
        assertThat(event.totalAmount).isEqualByComparingTo("49.90")
        assertThat(event.status).isEqualTo(OrderStatus.CREATED)
        assertThat(event.updatedAt).isEqualTo(event.createdAt)
    }

    @Test
    fun `should update an order's status and add an ORDER_STATUS_UPDATED outbox event`() {
        val orderId = createOrder(BigDecimal("49.90")).id

        updateOrderStatus(orderId, OrderStatus.PROCESSING)

        val order = findOrder(orderId)
        assertThat(order.status).isEqualTo(OrderStatus.PROCESSING)
        // Optimistic locking increments the version on every update
        assertThat(order.version).isEqualTo(1)

        val outboxEvents = findOutboxEvents(orderId)
        assertThat(outboxEvents.map { it.eventType }).containsExactlyInAnyOrder(ORDER_CREATED, ORDER_STATUS_UPDATED)
        val event = readEvent<OrderStatusUpdatedEvent>(outboxEvents.single { it.eventType == ORDER_STATUS_UPDATED })
        assertThat(event.newStatus).isEqualTo(OrderStatus.PROCESSING)
        assertThat(event.totalAmount).isEqualByComparingTo("49.90")
        assertThat(event.createdAt).isEqualTo(order.createdAt)
        assertThat(event.updatedAt).isAfter(event.createdAt)
    }

    @Test
    fun `should correct the total amount and add an ORDER_TOTAL_AMOUNT_REPAIR outbox event`() {
        val orderId = createOrder(BigDecimal("99.00")).id

        updateOrderTotalAmount(orderId, BigDecimal("9.90"))

        val order = findOrder(orderId)
        assertThat(order.totalAmount).isEqualByComparingTo("9.90")
        assertThat(order.status).isEqualTo(OrderStatus.CREATED)

        val outboxEvents = findOutboxEvents(orderId)
        assertThat(outboxEvents.map { it.eventType }).containsExactlyInAnyOrder(ORDER_CREATED, ORDER_TOTAL_AMOUNT_REPAIR)
        val event = readEvent<OrderTotalAmountRepairEvent>(outboxEvents.single { it.eventType == ORDER_TOTAL_AMOUNT_REPAIR })
        assertThat(event.customerId).isEqualTo(customerId)
        assertThat(event.newTotalAmount).isEqualByComparingTo("9.90")
        assertThat(event.createdAt).isEqualTo(order.createdAt)
    }

    @Test
    fun `should add an ORDER_REPAIR outbox event with the whole status timeline`() {
        val orderId = createOrder(BigDecimal("49.90")).id
        updateOrderStatus(orderId, OrderStatus.PROCESSING)
        updateOrderStatus(orderId, OrderStatus.COMPLETED)

        mockMvc
            .post("/orders/$orderId/history-repair")
            .andExpect { status { isAccepted() } }

        val outboxEvents = findOutboxEvents(orderId)
        val statusEvents =
            outboxEvents
                .filter { it.eventType == ORDER_STATUS_UPDATED }
                .map { readEvent<OrderStatusUpdatedEvent>(it) }
                .sortedBy { it.updatedAt }
        val repair = readEvent<OrderRepairEvent>(outboxEvents.single { it.eventType == ORDER_REPAIR })
        assertThat(repair.customerId).isEqualTo(customerId)
        assertThat(repair.totalAmount).isEqualByComparingTo("49.90")
        assertThat(repair.createdAt).isEqualTo(findOrder(orderId).createdAt)
        // Every step holds exactly the updatedAt of the event it repairs
        assertThat(repair.steps).containsExactly(
            OrderRepairStep(OrderStatus.CREATED, repair.createdAt),
            OrderRepairStep(OrderStatus.PROCESSING, statusEvents[0].updatedAt),
            OrderRepairStep(OrderStatus.COMPLETED, statusEvents[1].updatedAt),
        )
    }

    @Test
    fun `should list the created order`() {
        val orderId = createOrder(BigDecimal("15.00")).id

        val response =
            mockMvc
                .get("/orders")
                .andExpect { status { isOk() } }
                .andReturn()
                .response.contentAsString

        assertThat(objectMapper.readValue<List<OrderResponse>>(response).map { it.id }).contains(orderId)
    }

    private fun createOrder(totalAmount: BigDecimal): OrderResponse {
        val response =
            mockMvc
                .post("/orders") {
                    contentType = MediaType.APPLICATION_JSON
                    content = objectMapper.writeValueAsString(CreateOrderRequest(customerId, totalAmount))
                }.andExpect { status { isCreated() } }
                .andReturn()
                .response.contentAsString
        return objectMapper.readValue(response)
    }

    private fun findOrder(orderId: UUID): Order = checkNotNull(orderRepository.findOrderById(orderId))

    private fun findOutboxEvents(orderId: UUID): List<OutboxEvent> =
        outboxRepository.findAll().filter { it.aggregateId == orderId.toString() }

    private fun updateOrderStatus(
        orderId: UUID,
        newStatus: OrderStatus,
    ) {
        mockMvc
            .put("/orders/$orderId/status") {
                contentType = MediaType.APPLICATION_JSON
                content = objectMapper.writeValueAsString(UpdateOrderStatusRequest(newStatus))
            }.andExpect { status { isOk() } }
    }

    private fun updateOrderTotalAmount(
        orderId: UUID,
        newTotalAmount: BigDecimal,
    ) {
        mockMvc
            .put("/orders/$orderId/total-amount") {
                contentType = MediaType.APPLICATION_JSON
                content = objectMapper.writeValueAsString(UpdateOrderTotalAmountRequest(newTotalAmount))
            }.andExpect { status { isOk() } }
    }

    private inline fun <reified T : OrderEvent> readEvent(outboxEvent: OutboxEvent): T =
        objectMapper.readValue(outboxEvent.payload, OrderEvent::class.java) as T
}

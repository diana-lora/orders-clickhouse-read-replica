package com.example.clickreadrep.history

import com.example.clickreadrep.commons.OrderStatus
import com.example.clickreadrep.history.domain.OrderHistory
import com.example.clickreadrep.history.infra.web.dtos.OrderHistoryResponse
import com.example.clickreadrep.order.infra.web.dtos.CreateOrderRequest
import com.example.clickreadrep.order.infra.web.dtos.OrderResponse
import com.example.clickreadrep.order.infra.web.dtos.UpdateOrderStatusRequest
import com.example.clickreadrep.order.infra.web.dtos.UpdateOrderTotalAmountRequest
import com.example.clickreadrep.utils.ClickhouseOrderHistoryTestRepository
import com.example.clickreadrep.utils.IntegrationTestContainers
import com.example.clickreadrep.utils.PostgresOrderHistoryTestRepository
import com.fasterxml.jackson.module.kotlin.readValue
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.await
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.get
import org.springframework.test.web.servlet.post
import org.springframework.test.web.servlet.put
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant
import java.util.UUID

class OrderHistoryTimeTravelIT : IntegrationTestContainers() {
    @Autowired
    private lateinit var historyRepository: ClickhouseOrderHistoryTestRepository

    @Autowired
    private lateinit var postgresHistoryRepository: PostgresOrderHistoryTestRepository

    @Test
    fun `order status update returns the status valid at each point in time`() {
        val customerId = "customer-status-${UUID.randomUUID()}"
        val orderId = createOrder(customerId, BigDecimal("49.90"))
        val createdTime = awaitLatestState(orderId) { assertThat(it.status).isEqualTo(OrderStatus.CREATED) }.updatedAt

        // Some time later the order moves on
        Thread.sleep(50)
        updateStatus(orderId, OrderStatus.PROCESSING)
        val processingTime = awaitLatestState(orderId) { assertThat(it.status).isEqualTo(OrderStatus.PROCESSING) }.updatedAt

        assertThat(processingTime).isAfter(createdTime)
        assertThat(getOrderHistory(orderId, createdTime.minusMillis(1))).isEmpty()
        assertThat(getSnapshot(orderId, createdTime).status).isEqualTo(OrderStatus.CREATED)
        assertThat(getSnapshot(orderId, processingTime).status).isEqualTo(OrderStatus.PROCESSING)
        assertThat(getOrderHistory(orderId).single().status).isEqualTo(OrderStatus.PROCESSING)
    }

    @Test
    fun `retroactive total amount correction applies to every past snapshot`() {
        val customerId = "customer-amount-${UUID.randomUUID()}"
        val wrongAmount = BigDecimal("99.00")
        val correctAmount = BigDecimal("9.90")
        val orderId = createOrder(customerId, wrongAmount)
        val createdTime = awaitLatestState(orderId) { assertThat(it.status).isEqualTo(OrderStatus.CREATED) }.updatedAt

        Thread.sleep(50)
        updateStatus(orderId, OrderStatus.PROCESSING)
        val processingTime = awaitLatestState(orderId) { assertThat(it.status).isEqualTo(OrderStatus.PROCESSING) }.updatedAt

        // The typo was copied into every state of the order
        assertThat(getSnapshot(orderId, createdTime).totalAmount).isEqualByComparingTo(wrongAmount)
        assertThat(getSnapshot(orderId, processingTime).totalAmount).isEqualByComparingTo(wrongAmount)

        updateTotalAmount(orderId, correctAmount)
        awaitLatestState(orderId) { assertThat(it.totalAmount).isEqualByComparingTo(correctAmount) }

        val createdSnapshot = getSnapshot(orderId, createdTime)
        assertThat(createdSnapshot.status).isEqualTo(OrderStatus.CREATED)
        assertThat(createdSnapshot.totalAmount).isEqualByComparingTo(correctAmount)
        assertThat(createdSnapshot.updatedAt).isEqualTo(createdTime)

        val processingSnapshot = getSnapshot(orderId, processingTime)
        assertThat(processingSnapshot.status).isEqualTo(OrderStatus.PROCESSING)
        assertThat(processingSnapshot.totalAmount).isEqualByComparingTo(correctAmount)
        assertThat(processingSnapshot.updatedAt).isEqualTo(processingTime)
    }

    @Test
    fun `history rebuild replaces the corrupted timeline and the deletes reach ClickHouse`() {
        val customerId = "customer-rebuild-${UUID.randomUUID()}"
        val orderId = createOrder(customerId, BigDecimal("49.90"))
        val createdTime = awaitLatestState(orderId) { assertThat(it.status).isEqualTo(OrderStatus.CREATED) }.updatedAt

        Thread.sleep(50)
        updateStatus(orderId, OrderStatus.PROCESSING)
        val processingTime = awaitLatestState(orderId) { assertThat(it.status).isEqualTo(OrderStatus.PROCESSING) }.updatedAt

        // Drift: a COMPLETED state that never happened in the order domain, written into the history store
        val corruptedTime = processingTime.plusSeconds(1)
        postgresHistoryRepository.save(
            OrderHistory(orderId, customerId, BigDecimal("49.90"), OrderStatus.COMPLETED, createdTime, corruptedTime),
        )
        await.atMost(SYNC_TIMEOUT).pollInterval(POLL_INTERVAL).untilAsserted {
            assertThat(getSnapshot(orderId, corruptedTime).status).isEqualTo(OrderStatus.COMPLETED)
        }

        repairHistory(orderId)
        await.atMost(SYNC_TIMEOUT).pollInterval(POLL_INTERVAL).untilAsserted {
            // The repair deleted all three rows in PostgreSQL and inserted the two real states; FINAL hides the deletes
            assertThat(historyRepository.countRows(orderId)).isEqualTo(2)
            assertThat(getSnapshot(orderId, corruptedTime).status).isEqualTo(OrderStatus.PROCESSING)
        }

        assertThat(getOrderHistory(orderId, createdTime.minusMillis(1))).isEmpty()
        assertThat(getSnapshot(orderId, createdTime).status).isEqualTo(OrderStatus.CREATED)
        val repairedSnapshot = getSnapshot(orderId, processingTime)
        assertThat(repairedSnapshot.status).isEqualTo(OrderStatus.PROCESSING)
        assertThat(repairedSnapshot.updatedAt).isEqualTo(processingTime)
    }

    private fun createOrder(
        customerId: String,
        totalAmount: BigDecimal,
    ): UUID {
        val response =
            mockMvc
                .post("/orders") {
                    contentType = MediaType.APPLICATION_JSON
                    content = objectMapper.writeValueAsString(CreateOrderRequest(customerId, totalAmount))
                }.andExpect { status { isCreated() } }
                .andReturn()
                .response.contentAsString
        return objectMapper.readValue<OrderResponse>(response).id
    }

    private fun updateStatus(
        orderId: UUID,
        status: OrderStatus,
    ) {
        mockMvc
            .put("/orders/$orderId/status") {
                contentType = MediaType.APPLICATION_JSON
                content = objectMapper.writeValueAsString(UpdateOrderStatusRequest(status))
            }.andExpect { status { isOk() } }
    }

    private fun updateTotalAmount(
        orderId: UUID,
        totalAmount: BigDecimal,
    ) {
        mockMvc
            .put("/orders/$orderId/total-amount") {
                contentType = MediaType.APPLICATION_JSON
                content = objectMapper.writeValueAsString(UpdateOrderTotalAmountRequest(totalAmount))
            }.andExpect { status { isOk() } }
    }

    private fun repairHistory(orderId: UUID) {
        mockMvc
            .post("/orders/$orderId/history-repair")
            .andExpect { status { isAccepted() } }
    }

    // Without targetTime the endpoint uses its default (now)
    private fun getOrderHistory(
        orderId: UUID,
        targetTime: Instant? = null,
    ): List<OrderHistoryResponse> {
        val response =
            mockMvc
                .get("/orders-history") {
                    param("orderId", orderId.toString())
                    targetTime?.let { param("targetTime", it.toString()) }
                }.andExpect { status { isOk() } }
                .andReturn()
                .response.contentAsString
        return objectMapper.readValue(response)
    }

    private fun getSnapshot(
        orderId: UUID,
        targetTime: Instant,
    ): OrderHistoryResponse = getOrderHistory(orderId, targetTime).single()

    // The history reaches ClickHouse asynchronously (Kafka consumer, then CDC), so the latest state is polled until it matches
    private fun awaitLatestState(
        orderId: UUID,
        assertion: (OrderHistoryResponse) -> Unit,
    ): OrderHistoryResponse {
        lateinit var latest: OrderHistoryResponse
        await.atMost(SYNC_TIMEOUT).pollInterval(POLL_INTERVAL).untilAsserted {
            val snapshots = getOrderHistory(orderId)
            assertThat(snapshots).hasSize(1)
            latest = snapshots.first()
            assertion(latest)
        }
        return latest
    }

    private companion object {
        val SYNC_TIMEOUT: Duration = Duration.ofSeconds(30)
        val POLL_INTERVAL: Duration = Duration.ofMillis(250)
    }
}

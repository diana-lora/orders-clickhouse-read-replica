package com.example.clickreadrep.history.infra.web

import com.example.clickreadrep.commons.OrderStatus
import com.example.clickreadrep.history.domain.OrderHistory
import com.example.clickreadrep.history.infra.web.dtos.OrderHistoryResponse
import com.example.clickreadrep.utils.ClickhouseOrderHistoryTestRepository
import com.example.clickreadrep.utils.IntegrationTestContainers
import com.fasterxml.jackson.module.kotlin.readValue
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.web.servlet.get
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

class OrderHistoryControllerIT : IntegrationTestContainers() {
    @Autowired
    private lateinit var historyRepository: ClickhouseOrderHistoryTestRepository

    private val customerA = "customer-a-${UUID.randomUUID()}"
    private val customerB = "customer-b-${UUID.randomUUID()}"
    private val orderA1 = UUID.randomUUID()
    private val orderA2 = UUID.randomUUID()
    private val orderB1 = UUID.randomUUID()

    @BeforeEach
    fun seedHistory() {
        seed(orderA1, customerA)
        seed(orderA2, customerA)
        seed(orderB1, customerB)
    }

    @Test
    fun `should return every order of the customer`() {
        val response = getOrderHistory("customerId" to customerA)

        assertThat(response.map { it.orderId }).containsExactlyInAnyOrder(orderA1, orderA2)
        assertThat(response).allSatisfy { assertThat(it.customerId).isEqualTo(customerA) }
    }

    @Test
    fun `should return only the requested order of the customer`() {
        val response = getOrderHistory("customerId" to customerA, "orderId" to orderA1.toString())

        assertThat(response.map { it.orderId }).containsExactly(orderA1)
    }

    @Test
    fun `should return nothing when the order belongs to another customer`() {
        val response = getOrderHistory("customerId" to customerA, "orderId" to orderB1.toString())

        assertThat(response).isEmpty()
    }

    @Test
    fun `should return the orders of all customers without filters`() {
        val response = getOrderHistory()

        // The database is shared with other tests, so only the seeded orders are checked
        assertThat(response.map { it.orderId }).contains(orderA1, orderA2, orderB1)
    }

    private fun seed(
        orderId: UUID,
        customerId: String,
    ) {
        val createdAt = Instant.parse("2026-02-01T10:00:00Z")
        historyRepository.save(
            OrderHistory(orderId, customerId, BigDecimal("20.00"), OrderStatus.CREATED, createdAt, createdAt, version = 1),
        )
    }

    private fun getOrderHistory(vararg params: Pair<String, String>): List<OrderHistoryResponse> {
        val response =
            mockMvc
                .get("/orders-history") { params.forEach { (name, value) -> param(name, value) } }
                .andExpect { status { isOk() } }
                .andReturn()
                .response.contentAsString
        return objectMapper.readValue(response)
    }
}

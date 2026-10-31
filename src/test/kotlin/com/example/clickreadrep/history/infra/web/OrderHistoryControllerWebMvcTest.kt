package com.example.clickreadrep.history.infra.web

import com.example.clickreadrep.commons.OrderStatus
import com.example.clickreadrep.history.application.GetOrderHistoryService
import com.example.clickreadrep.history.application.queries.GetOrderHistoryQuery
import com.example.clickreadrep.history.domain.OrderHistory
import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
import io.mockk.slot
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.get
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

@WebMvcTest(OrderHistoryController::class)
class OrderHistoryControllerWebMvcTest {
    @Autowired
    private lateinit var mockMvc: MockMvc

    @MockkBean
    private lateinit var getOrderHistoryService: GetOrderHistoryService

    private val query = slot<GetOrderHistoryQuery>()

    @Test
    fun `should default targetTime to the time of the request`() {
        every { getOrderHistoryService.execute(capture(query)) } returns emptyList()
        val before = Instant.now()

        mockMvc.get("/orders-history").andExpect { status { isOk() } }

        assertThat(query.captured.targetTime).isBetween(before, Instant.now())
        assertThat(query.captured.customerId).isNull()
        assertThat(query.captured.orderId).isNull()
    }

    @Test
    fun `should pass all request parameters into the query`() {
        every { getOrderHistoryService.execute(capture(query)) } returns emptyList()
        val orderId = UUID.randomUUID()

        mockMvc
            .get("/orders-history") {
                param("targetTime", "2026-01-15T10:02:00Z")
                param("customerId", "customer-1")
                param("orderId", orderId.toString())
            }.andExpect { status { isOk() } }

        assertThat(query.captured).isEqualTo(
            GetOrderHistoryQuery(Instant.parse("2026-01-15T10:02:00Z"), "customer-1", orderId),
        )
    }

    @Test
    fun `should not expose the internal version`() {
        val orderId = UUID.randomUUID()
        val createdAt = Instant.parse("2026-01-15T10:00:00Z")
        every { getOrderHistoryService.execute(any()) } returns
            listOf(OrderHistory(orderId, "customer-1", BigDecimal("49.90"), OrderStatus.CREATED, createdAt, createdAt, version = 101))

        mockMvc.get("/orders-history").andExpect {
            status { isOk() }
            jsonPath("$[0].orderId") { value(orderId.toString()) }
            jsonPath("$[0].status") { value("CREATED") }
            jsonPath("$[0].version") { doesNotExist() }
        }
    }

    @Test
    fun `should return 400 for an invalid targetTime`() {
        mockMvc
            .get("/orders-history") { param("targetTime", "not-a-date") }
            .andExpect { status { isBadRequest() } }

        verify(exactly = 0) { getOrderHistoryService.execute(any()) }
    }

    @Test
    fun `should return 400 for an invalid orderId`() {
        mockMvc
            .get("/orders-history") { param("orderId", "abc") }
            .andExpect { status { isBadRequest() } }

        verify(exactly = 0) { getOrderHistoryService.execute(any()) }
    }
}

package com.example.clickreadrep.order.infra.web

import com.example.clickreadrep.order.application.CreateOrderService
import com.example.clickreadrep.order.application.GetOrdersService
import com.example.clickreadrep.order.application.RepairOrderHistoryService
import com.example.clickreadrep.order.application.UpdateOrderService
import com.example.clickreadrep.order.application.commands.UpdateOrderStatusCommand
import com.example.clickreadrep.order.application.commands.UpdateOrderTotalAmountCommand
import com.example.clickreadrep.order.domain.exceptions.OrderNotFoundException
import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
import io.mockk.just
import io.mockk.runs
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.post
import org.springframework.test.web.servlet.put
import java.util.UUID

@WebMvcTest(OrderController::class)
class OrderControllerWebMvcTest {
    @Autowired
    private lateinit var mockMvc: MockMvc

    @MockkBean
    private lateinit var createOrderService: CreateOrderService

    @MockkBean
    private lateinit var updateOrderService: UpdateOrderService

    @MockkBean
    private lateinit var getOrdersService: GetOrdersService

    @MockkBean
    private lateinit var repairOrderHistoryService: RepairOrderHistoryService

    private val unknownOrderId = UUID.randomUUID()

    @Test
    fun `should return 202 when repairing the history of an order`() {
        val orderId = UUID.randomUUID()
        every { repairOrderHistoryService.execute(any()) } just runs

        mockMvc
            .post("/orders/$orderId/history-repair")
            .andExpect { status { isAccepted() } }

        verify { repairOrderHistoryService.execute(match { it.orderId == orderId }) }
    }

    @Test
    fun `should return 404 when repairing the history of an unknown order`() {
        every { repairOrderHistoryService.execute(any()) } throws OrderNotFoundException("Order not found (ID=$unknownOrderId)")

        mockMvc
            .post("/orders/$unknownOrderId/history-repair")
            .andExpect {
                status { isNotFound() }
                jsonPath("$.detail") { value("Order not found (ID=$unknownOrderId)") }
            }
    }

    @Test
    fun `should return 404 when updating the status of an unknown order`() {
        every { updateOrderService.execute(any<UpdateOrderStatusCommand>()) } throws
            OrderNotFoundException("Order not found (ID=$unknownOrderId)")

        mockMvc
            .put("/orders/$unknownOrderId/status") {
                contentType = MediaType.APPLICATION_JSON
                content = """{"status": "PROCESSING"}"""
            }.andExpect {
                status { isNotFound() }
                jsonPath("$.detail") { value("Order not found (ID=$unknownOrderId)") }
            }
    }

    @Test
    fun `should return 404 when correcting the total amount of an unknown order`() {
        every { updateOrderService.execute(any<UpdateOrderTotalAmountCommand>()) } throws
            OrderNotFoundException("Order not found (ID=$unknownOrderId)")

        mockMvc
            .put("/orders/$unknownOrderId/total-amount") {
                contentType = MediaType.APPLICATION_JSON
                content = """{"totalAmount": 9.90}"""
            }.andExpect {
                status { isNotFound() }
                jsonPath("$.detail") { value("Order not found (ID=$unknownOrderId)") }
            }
    }

    @Test
    fun `should return 400 for a malformed request body`() {
        mockMvc
            .post("/orders") {
                contentType = MediaType.APPLICATION_JSON
                content = """{"customerId": "customer-1", "totalAmount": "not-a-number"}"""
            }.andExpect { status { isBadRequest() } }
    }
}

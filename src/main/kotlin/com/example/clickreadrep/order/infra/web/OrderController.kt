package com.example.clickreadrep.order.infra.web

import com.example.clickreadrep.order.application.CreateOrderService
import com.example.clickreadrep.order.application.GetOrdersService
import com.example.clickreadrep.order.application.RepairOrderHistoryService
import com.example.clickreadrep.order.application.UpdateOrderService
import com.example.clickreadrep.order.application.commands.PlaceOrderCommand
import com.example.clickreadrep.order.application.commands.RepairOrderHistoryCommand
import com.example.clickreadrep.order.application.commands.UpdateOrderStatusCommand
import com.example.clickreadrep.order.application.commands.UpdateOrderTotalAmountCommand
import com.example.clickreadrep.order.infra.web.dtos.CreateOrderRequest
import com.example.clickreadrep.order.infra.web.dtos.OrderResponse
import com.example.clickreadrep.order.infra.web.dtos.UpdateOrderStatusRequest
import com.example.clickreadrep.order.infra.web.dtos.UpdateOrderTotalAmountRequest
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.util.UUID

@RestController
@RequestMapping("/orders")
class OrderController(
    private val createOrderService: CreateOrderService,
    private val updateOrderService: UpdateOrderService,
    private val getOrdersService: GetOrdersService,
    private val repairOrderHistoryService: RepairOrderHistoryService,
) {
    @PostMapping
    fun createOrder(
        @RequestBody request: CreateOrderRequest,
    ): ResponseEntity<OrderResponse> {
        val command =
            PlaceOrderCommand(
                customerId = request.customerId,
                totalAmount = request.totalAmount,
            )

        val savedOrder = createOrderService.execute(command)

        return ResponseEntity
            .status(HttpStatus.CREATED)
            .body(OrderResponse.fromDomain(savedOrder))
    }

    @PutMapping("/{orderId}/status")
    fun updateOrderStatus(
        @PathVariable orderId: UUID,
        @RequestBody request: UpdateOrderStatusRequest,
    ) {
        val command =
            UpdateOrderStatusCommand(
                orderId = orderId,
                newStatus = request.status,
            )

        updateOrderService.execute(command)
    }

    @PutMapping("/{orderId}/total-amount")
    fun updateOrderTotalAmount(
        @PathVariable orderId: UUID,
        @RequestBody request: UpdateOrderTotalAmountRequest,
    ) {
        val command =
            UpdateOrderTotalAmountCommand(
                orderId = orderId,
                newTotalAmount = request.totalAmount,
            )

        updateOrderService.execute(command)
    }

    @PostMapping("/{orderId}/history-repair")
    fun repairOrderHistory(
        @PathVariable orderId: UUID,
    ): ResponseEntity<Unit> {
        repairOrderHistoryService.execute(RepairOrderHistoryCommand(orderId = orderId))

        return ResponseEntity.accepted().build()
    }

    @GetMapping
    fun getAllOrders(): ResponseEntity<List<OrderResponse>> {
        val ordersList = getOrdersService.execute()

        val responseList = ordersList.map { OrderResponse.fromDomain(it) }
        return ResponseEntity.ok(responseList)
    }
}

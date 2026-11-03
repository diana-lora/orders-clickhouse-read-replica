package com.example.clickreadrep.history.infra.web

import com.example.clickreadrep.history.application.GetOrderHistoryService
import com.example.clickreadrep.history.application.queries.GetOrderHistoryQuery
import com.example.clickreadrep.history.infra.web.dtos.OrderHistoryResponse
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.time.Instant
import java.util.UUID

@RestController
@RequestMapping("/orders-history")
class OrderHistoryController(
    private val getOrderHistoryService: GetOrderHistoryService,
) {
    @GetMapping
    fun getOrderHistory(
        @RequestParam(defaultValue = "#{T(java.time.Instant).now()}") targetTime: Instant,
        @RequestParam(required = false) customerId: String?,
        @RequestParam(required = false) orderId: UUID?,
    ): ResponseEntity<List<OrderHistoryResponse>> {
        val query =
            GetOrderHistoryQuery(
                targetTime = targetTime,
                customerId = customerId,
                orderId = orderId,
            )

        val historyList = getOrderHistoryService.execute(query)

        val responseList = historyList.map { OrderHistoryResponse.fromDomain(it) }
        return ResponseEntity.ok(responseList)
    }
}

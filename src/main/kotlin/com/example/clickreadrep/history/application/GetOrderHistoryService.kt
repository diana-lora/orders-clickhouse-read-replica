package com.example.clickreadrep.history.application

import com.example.clickreadrep.history.application.queries.GetOrderHistoryQuery
import com.example.clickreadrep.history.domain.OrderHistory
import com.example.clickreadrep.history.domain.OrderHistoryRepository
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Service

@Service
class GetOrderHistoryService(
    @Qualifier("clickhouseOrderHistoryRepository") private val orderHistoryRepository: OrderHistoryRepository,
) {
    fun execute(query: GetOrderHistoryQuery): List<OrderHistory> = orderHistoryRepository.findBy(query)
}

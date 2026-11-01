package com.example.clickreadrep.history.application

import com.example.clickreadrep.history.application.queries.GetOrderHistoryQuery
import com.example.clickreadrep.history.domain.OrderHistory
import com.example.clickreadrep.history.domain.OrderHistoryReadRepository
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Service

@Service
class GetOrderHistoryService(
    // Qualified because the test subclass ClickhouseOrderHistoryTestRepository is a second candidate
    @Qualifier("clickhouseOrderHistoryRepository") private val orderHistoryRepository: OrderHistoryReadRepository,
) {
    fun execute(query: GetOrderHistoryQuery): List<OrderHistory> = orderHistoryRepository.findBy(query)
}

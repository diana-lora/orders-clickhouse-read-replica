package com.example.clickreadrep.order.application

import com.example.clickreadrep.order.domain.Order
import com.example.clickreadrep.order.domain.OrderRepository
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class GetOrdersService(
    private val orderRepository: OrderRepository,
) {
    @Transactional(readOnly = true)
    fun execute(): List<Order> = orderRepository.findAll()
}

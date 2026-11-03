package com.example.clickreadrep.order.domain

import java.util.UUID

interface OrderRepository {
    fun findOrderById(orderId: UUID): Order?

    // Row lock (SELECT ... FOR UPDATE) held until the transaction ends; blocks concurrent writers of the order
    fun findLockedOrderById(orderId: UUID): Order?

    fun save(order: Order): Order

    fun findAll(): List<Order>
}

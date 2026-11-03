package com.example.clickreadrep.order.infra.db

import com.example.clickreadrep.order.domain.Order
import com.example.clickreadrep.order.domain.OrderRepository
import org.springframework.data.relational.core.sql.LockMode
import org.springframework.data.relational.repository.Lock
import org.springframework.data.repository.CrudRepository
import java.util.UUID

interface PostgresOrderRepository :
    CrudRepository<Order, UUID>,
    OrderRepository {
    @Lock(LockMode.PESSIMISTIC_WRITE)
    override fun findLockedOrderById(orderId: UUID): Order?
}

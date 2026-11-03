package com.example.clickreadrep.order.infra.db

import com.example.clickreadrep.order.domain.OrderStatusChange
import com.example.clickreadrep.order.domain.OrderStatusChangeRepository
import org.springframework.data.repository.CrudRepository
import java.util.UUID

interface PostgresOrderStatusChangeRepository :
    CrudRepository<OrderStatusChange, UUID>,
    OrderStatusChangeRepository

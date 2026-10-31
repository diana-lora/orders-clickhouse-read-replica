package com.example.clickreadrep.order.domain

import com.example.clickreadrep.commons.OrderStatus
import com.example.clickreadrep.commons.nowInMillis
import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Version
import org.springframework.data.relational.core.mapping.Table
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

@Table("orders")
class Order(
    @Id
    val id: UUID = UUID.randomUUID(),
    val customerId: String,
    var totalAmount: BigDecimal,
    var status: OrderStatus = OrderStatus.CREATED,
    val createdAt: Instant = nowInMillis(),
    // Null for new orders: Spring Data JDBC INSERTs, otherwise UPDATEs with optimistic locking
    @Version
    var version: Long? = null,
)

package com.example.clickreadrep.history.domain

import com.example.clickreadrep.commons.OrderStatus
import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Version
import org.springframework.data.relational.core.mapping.Column
import org.springframework.data.relational.core.mapping.Table
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

// One state of an order: a new row per creation and per status change, which makes time travel possible
@Table("order_history")
data class OrderHistory(
    val orderId: UUID,
    val customerId: String,
    val totalAmount: BigDecimal,
    @Column("status")
    val orderStatus: OrderStatus,
    val createdAt: Instant,
    val updatedAt: Instant,
    // Null for new rows: Spring Data JDBC INSERTs, otherwise UPDATEs with optimistic locking
    @Version
    val version: Long? = null,
    @Id
    val id: UUID = UUID.randomUUID(),
)

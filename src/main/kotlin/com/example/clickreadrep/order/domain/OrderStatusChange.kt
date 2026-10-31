package com.example.clickreadrep.order.domain

import com.example.clickreadrep.commons.OrderStatus
import com.example.clickreadrep.commons.nowInMillis
import org.springframework.data.annotation.Id
import org.springframework.data.domain.Persistable
import org.springframework.data.relational.core.mapping.Table
import java.time.Instant
import java.util.UUID

// One step of the status timeline of an order, used to rebuild its history
@Table("order_status_changes")
data class OrderStatusChange(
    @Id
    private val id: UUID = UUID.randomUUID(),
    val orderId: UUID,
    val status: OrderStatus,
    // Same value as the updatedAt of the published event, so a repair replaces exactly that history version
    val createdAt: Instant = nowInMillis(),
) : Persistable<UUID> {
    override fun getId(): UUID = id

    override fun isNew(): Boolean = true
}

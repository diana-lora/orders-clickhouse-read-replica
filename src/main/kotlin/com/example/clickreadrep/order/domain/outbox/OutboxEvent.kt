package com.example.clickreadrep.order.domain.outbox

import com.example.clickreadrep.commons.nowInMillis
import org.springframework.data.annotation.Id
import org.springframework.data.domain.Persistable
import org.springframework.data.relational.core.mapping.Column
import org.springframework.data.relational.core.mapping.Table
import java.time.Instant
import java.util.UUID

@Table("outbox")
data class OutboxEvent(
    @Id
    private val id: UUID = UUID.randomUUID(),
    // Name of the transported entity
    @Column("aggregatetype") val aggregateType: String,
    // Unique identifier of the payload
    @Column("aggregateid") val aggregateId: String,
    // Determines to which kafka partition of a topic is sent. Important for concurrency control.
    val aggregateKey: String,
    @Column("type") val eventType: String,
    val payload: String,
    // Name of the kafka topic to which the event is sent
    val targetTopic: String,
    val createdAt: Instant = nowInMillis(),
) : Persistable<UUID> {
    override fun getId(): UUID = id

    override fun isNew(): Boolean = true
}

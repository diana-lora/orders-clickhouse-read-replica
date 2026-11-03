package com.example.clickreadrep.order.domain.outbox

interface OutboxRepository {
    fun save(event: OutboxEvent): OutboxEvent
}

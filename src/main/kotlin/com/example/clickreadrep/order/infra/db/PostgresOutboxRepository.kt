package com.example.clickreadrep.order.infra.db

import com.example.clickreadrep.order.domain.outbox.OutboxEvent
import com.example.clickreadrep.order.domain.outbox.OutboxRepository
import org.springframework.data.repository.CrudRepository
import java.util.UUID

interface PostgresOutboxRepository :
    CrudRepository<OutboxEvent, UUID>,
    OutboxRepository

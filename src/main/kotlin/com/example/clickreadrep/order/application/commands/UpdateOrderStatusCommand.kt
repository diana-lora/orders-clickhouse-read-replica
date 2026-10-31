package com.example.clickreadrep.order.application.commands

import com.example.clickreadrep.commons.OrderStatus
import java.util.UUID

class UpdateOrderStatusCommand(
    val orderId: UUID,
    val newStatus: OrderStatus,
)

package com.example.clickreadrep.order.application.commands

import java.math.BigDecimal
import java.util.UUID

class UpdateOrderTotalAmountCommand(
    val orderId: UUID,
    val newTotalAmount: BigDecimal,
)

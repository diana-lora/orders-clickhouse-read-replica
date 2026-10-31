package com.example.clickreadrep.order.application.commands

import java.math.BigDecimal

data class PlaceOrderCommand(
    val customerId: String,
    val totalAmount: BigDecimal,
)

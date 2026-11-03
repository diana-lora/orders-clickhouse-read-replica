package com.example.clickreadrep.history.domain

import java.math.BigDecimal
import java.util.UUID

interface OrderHistoryMutationRepository {
    // Latest state of the order, or null when the order has no history yet
    fun findLatestOrderState(orderId: UUID): OrderHistory?

    fun save(history: OrderHistory): OrderHistory

    // Same generic signature as CrudRepository.saveAll, so the Spring Data repository implements it directly
    fun <S : OrderHistory> saveAll(histories: Iterable<S>): Iterable<S>

    fun deleteAllByOrderId(orderId: UUID)

    // Sets the amount in every stored state of the order and returns the number of updated rows
    fun updateTotalAmount(
        orderId: UUID,
        totalAmount: BigDecimal,
    ): Int
}

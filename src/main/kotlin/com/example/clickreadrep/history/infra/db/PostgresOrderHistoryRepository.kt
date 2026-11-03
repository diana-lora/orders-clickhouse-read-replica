package com.example.clickreadrep.history.infra.db

import com.example.clickreadrep.history.domain.OrderHistory
import com.example.clickreadrep.history.domain.OrderHistoryMutationRepository
import org.springframework.data.jdbc.repository.query.Modifying
import org.springframework.data.jdbc.repository.query.Query
import org.springframework.data.repository.CrudRepository
import org.springframework.data.repository.query.Param
import java.math.BigDecimal
import java.util.UUID

interface PostgresOrderHistoryRepository :
    CrudRepository<OrderHistory, UUID>,
    OrderHistoryMutationRepository {
    @Query("SELECT * FROM order_history WHERE order_id = :orderId ORDER BY updated_at DESC LIMIT 1")
    override fun findLatestOrderState(
        @Param("orderId") orderId: UUID,
    ): OrderHistory?

    @Modifying
    @Query("UPDATE order_history SET total_amount = :totalAmount, version = version + 1 WHERE order_id = :orderId")
    override fun updateTotalAmount(
        @Param("orderId") orderId: UUID,
        @Param("totalAmount") totalAmount: BigDecimal,
    ): Int

    @Modifying
    @Query("DELETE FROM order_history WHERE order_id = :orderId")
    override fun deleteAllByOrderId(
        @Param("orderId") orderId: UUID,
    )
}

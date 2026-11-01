package com.example.clickreadrep.history.infra.db

import com.example.clickreadrep.commons.OrderStatus
import com.example.clickreadrep.history.domain.OrderHistory
import com.example.clickreadrep.utils.PostgresOrderHistoryTestRepository
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.data.jdbc.test.autoconfigure.DataJdbcTest
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.dao.DuplicateKeyException
import org.springframework.dao.OptimisticLockingFailureException
import org.springframework.test.context.TestPropertySource
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.postgresql.PostgreSQLContainer
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

// Slice test: only Spring Data JDBC, Flyway (PostgreSQL migrations) and the repositories are loaded
@DataJdbcTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@TestPropertySource(properties = ["spring.flyway.enabled=true", "spring.flyway.locations=classpath:db/migration/postgres"])
@Testcontainers
class PostgresOrderHistoryRepositoryTest {
    companion object {
        @Container
        @ServiceConnection
        @JvmStatic
        val postgres: PostgreSQLContainer = PostgreSQLContainer("postgres:16-alpine")
    }

    @Autowired
    private lateinit var repository: PostgresOrderHistoryTestRepository

    private val orderId = UUID.randomUUID()
    private val createdAt = Instant.parse("2026-01-15T10:00:00Z")
    private val processingAt = Instant.parse("2026-01-15T10:05:00Z")

    @Test
    fun `should insert a new row with version 0 and increment the version on update`() {
        val inserted = repository.save(history(OrderStatus.CREATED, createdAt))
        assertThat(inserted.version).isEqualTo(0)

        val updated = repository.save(inserted.copy(totalAmount = BigDecimal("9.90")))

        assertThat(updated.version).isEqualTo(1)
        assertThat(repository.findAllByOrderId(orderId).single().totalAmount).isEqualByComparingTo("9.90")
    }

    @Test
    fun `should reject the update of a stale copy`() {
        val inserted = repository.save(history(OrderStatus.CREATED, createdAt))
        repository.save(inserted.copy(totalAmount = BigDecimal("9.90")))

        assertThatThrownBy { repository.save(inserted.copy(totalAmount = BigDecimal("1.00"))) }
            .isInstanceOf(OptimisticLockingFailureException::class.java)
    }

    @Test
    fun `should reject the same state change stored twice`() {
        repository.save(history(OrderStatus.CREATED, createdAt))

        assertThatThrownBy { repository.save(history(OrderStatus.CREATED, createdAt)) }
            .isInstanceOf(DuplicateKeyException::class.java)
    }

    @Test
    fun `should return the latest state of the order`() {
        repository.saveAll(listOf(history(OrderStatus.PROCESSING, processingAt), history(OrderStatus.CREATED, createdAt)))

        val latest = repository.findLatestOrderState(orderId)

        assertThat(latest?.orderStatus).isEqualTo(OrderStatus.PROCESSING)
        assertThat(repository.findLatestOrderState(UUID.randomUUID())).isNull()
    }

    @Test
    fun `should update the total amount of every row of the order and of no other order`() {
        val otherOrderId = UUID.randomUUID()
        repository.saveAll(
            listOf(
                history(OrderStatus.CREATED, createdAt),
                history(OrderStatus.PROCESSING, processingAt),
                history(OrderStatus.CREATED, createdAt, otherOrderId),
            ),
        )

        val updatedRows = repository.updateTotalAmount(orderId, BigDecimal("9.90"))

        assertThat(updatedRows).isEqualTo(2)
        assertThat(repository.findAllByOrderId(orderId)).allSatisfy {
            assertThat(it.totalAmount).isEqualByComparingTo("9.90")
            assertThat(it.version).isEqualTo(1)
        }
        assertThat(repository.findAllByOrderId(otherOrderId).single().totalAmount).isEqualByComparingTo("49.90")
    }

    @Test
    fun `should reject the save of a copy loaded before the total amount update`() {
        val inserted = repository.save(history(OrderStatus.CREATED, createdAt))
        repository.updateTotalAmount(orderId, BigDecimal("9.90"))

        assertThatThrownBy { repository.save(inserted.copy(totalAmount = BigDecimal("1.00"))) }
            .isInstanceOf(OptimisticLockingFailureException::class.java)
    }

    @Test
    fun `should delete every row of the order and of no other order`() {
        val otherOrderId = UUID.randomUUID()
        repository.saveAll(
            listOf(
                history(OrderStatus.CREATED, createdAt),
                history(OrderStatus.PROCESSING, processingAt),
                history(OrderStatus.CREATED, createdAt, otherOrderId),
            ),
        )

        repository.deleteAllByOrderId(orderId)

        assertThat(repository.findAllByOrderId(orderId)).isEmpty()
        assertThat(repository.findAllByOrderId(otherOrderId)).hasSize(1)
    }

    private fun history(
        status: OrderStatus,
        updatedAt: Instant,
        id: UUID = orderId,
    ) = OrderHistory(id, "customer-1", BigDecimal("49.90"), status, createdAt, updatedAt)
}

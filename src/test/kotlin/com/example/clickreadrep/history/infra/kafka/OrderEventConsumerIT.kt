package com.example.clickreadrep.history.infra.kafka

import com.example.clickreadrep.commons.OrderCreatedEvent
import com.example.clickreadrep.commons.OrderEvent
import com.example.clickreadrep.commons.OrderRepairEvent
import com.example.clickreadrep.commons.OrderRepairStep
import com.example.clickreadrep.commons.OrderStatus
import com.example.clickreadrep.commons.OrderStatusUpdatedEvent
import com.example.clickreadrep.commons.OrderTotalAmountUpdateEvent
import com.example.clickreadrep.config.KafkaConfig
import com.example.clickreadrep.history.application.queries.GetOrderHistoryQuery
import com.example.clickreadrep.history.domain.OrderHistory
import com.example.clickreadrep.utils.ClickhouseOrderHistoryTestRepository
import com.example.clickreadrep.utils.IntegrationTestContainers
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.await
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant
import java.util.UUID

/**
 * Kafka events in, ClickHouse rows out. Events are published as plain JSON strings without type headers, exactly as
 * Debezium routes them from the outbox. Fixed timestamps make the time travel assertions deterministic.
 */
class OrderEventConsumerIT : IntegrationTestContainers() {
    @Value($$"${spring.kafka.bootstrap-servers}")
    private lateinit var bootstrapServers: String

    @Autowired
    private lateinit var historyRepository: ClickhouseOrderHistoryTestRepository

    private val orderId = UUID.randomUUID()
    private val customerId = "customer-consumer-${UUID.randomUUID()}"
    private val createdAt = Instant.parse("2026-01-15T10:00:00Z")
    private val processingAt = Instant.parse("2026-01-15T10:05:00Z")
    private val beforeProcessing = Instant.parse("2026-01-15T10:02:00Z")

    private val created =
        OrderCreatedEvent(orderId.toString(), customerId, BigDecimal("99.00"), OrderStatus.CREATED, createdAt, createdAt)
    private val processing =
        OrderStatusUpdatedEvent(orderId.toString(), customerId, BigDecimal("99.00"), OrderStatus.PROCESSING, createdAt, processingAt)

    @Test
    fun `should store an ORDER_CREATED event as the first version of the order`() {
        publish(created)

        val record = awaitLatestState { assertThat(it.orderStatus).isEqualTo(OrderStatus.CREATED) }
        assertThat(record.customerId).isEqualTo(customerId)
        assertThat(record.totalAmount).isEqualByComparingTo("99.00")
        assertThat(record.createdAt).isEqualTo(createdAt)
        assertThat(record.updatedAt).isEqualTo(createdAt)

        val storedEvent = findSnapshot(createdAt)
        assertThat(storedEvent.customerId).isEqualTo(customerId)
        assertThat(storedEvent.totalAmount).isEqualByComparingTo("99.00")
        assertThat(storedEvent.createdAt).isEqualTo(createdAt)
        assertThat(storedEvent.updatedAt).isEqualTo(createdAt)
    }

    @Test
    fun `should append an ORDER_STATUS_UPDATED event as a new version and keep the earlier state`() {
        publish(created)
        publish(processing)

        val latest = awaitLatestState { assertThat(it.orderStatus).isEqualTo(OrderStatus.PROCESSING) }
        assertThat(latest.updatedAt).isEqualTo(processingAt)

        val snapshotBeforeProcessing = findSnapshot(beforeProcessing)
        assertThat(snapshotBeforeProcessing.orderStatus).isEqualTo(OrderStatus.CREATED)
        assertThat(latest.version).isGreaterThan(snapshotBeforeProcessing.version)

        // Background merges must not remove earlier states, otherwise time travel loses them
        historyRepository.optimizeFinal()
        assertThat(findSnapshot(beforeProcessing).orderStatus).isEqualTo(OrderStatus.CREATED)
    }

    @Test
    fun `should patch an ORDER_TOTAL_AMOUNT_UPDATED event into every version without a new one`() {
        publish(created)
        publish(processing)
        val processingVersion = awaitLatestState { assertThat(it.orderStatus).isEqualTo(OrderStatus.PROCESSING) }.version

        publish(OrderTotalAmountUpdateEvent(orderId.toString(), customerId, BigDecimal("9.90"), createdAt))

        val latest = awaitLatestState { assertThat(it.totalAmount).isEqualByComparingTo("9.90") }
        assertThat(latest.orderStatus).isEqualTo(OrderStatus.PROCESSING)
        assertThat(latest.updatedAt).isEqualTo(processingAt)
        assertThat(latest.version).isEqualTo(processingVersion)

        val snapshotBeforeProcessing = findSnapshot(beforeProcessing)
        assertThat(snapshotBeforeProcessing.orderStatus).isEqualTo(OrderStatus.CREATED)
        assertThat(snapshotBeforeProcessing.totalAmount).isEqualByComparingTo("9.90")
        assertThat(snapshotBeforeProcessing.updatedAt).isEqualTo(createdAt)
    }

    @Test
    fun `should replace every version with the ORDER_REPAIR timeline and delete the older ones`() {
        publish(created)
        publish(processing)
        val processingVersion = awaitLatestState { assertThat(it.orderStatus).isEqualTo(OrderStatus.PROCESSING) }.version
        // Orphan: a state that never happened, at a timestamp no repaired row shares
        val orphanAt = Instant.parse("2026-01-15T10:07:00Z")
        historyRepository.save(
            OrderHistory(orderId, customerId, BigDecimal("99.00"), OrderStatus.COMPLETED, createdAt, orphanAt, processingVersion),
        )

        val repair =
            OrderRepairEvent(
                orderId.toString(),
                customerId,
                BigDecimal("9.90"),
                createdAt,
                listOf(OrderRepairStep(OrderStatus.CREATED, createdAt), OrderRepairStep(OrderStatus.PROCESSING, processingAt)),
            )
        publish(repair)

        val repaired = awaitRepair(previousVersion = processingVersion)
        assertThat(repaired.orderStatus).isEqualTo(OrderStatus.PROCESSING)
        assertThat(repaired.totalAmount).isEqualByComparingTo("9.90")
        assertThat(repaired.updatedAt).isEqualTo(processingAt)

        // Both repaired rows share one version, so updated_at decides which one each snapshot returns
        assertThat(findSnapshot(beforeProcessing).orderStatus).isEqualTo(OrderStatus.CREATED)
        assertThat(findSnapshot(orphanAt).orderStatus).isEqualTo(OrderStatus.PROCESSING)

        // A second repair replaces the first one and leaves the same timeline
        publish(repair)
        val repairedAgain = awaitRepair(previousVersion = repaired.version)
        assertThat(repairedAgain.orderStatus).isEqualTo(OrderStatus.PROCESSING)
        assertThat(findSnapshot(beforeProcessing).orderStatus).isEqualTo(OrderStatus.CREATED)

        historyRepository.optimizeFinal()
        assertThat(historyRepository.countRows(orderId)).isEqualTo(2)
    }

    @Test
    fun `should route a poison pill payload to the DLT topic`() {
        val poisonPayload = "invalid-non-json-byte-payload".toByteArray(Charsets.UTF_8)
        val testKey = "poison-key-${UUID.randomUUID()}"

        val producerProps =
            mapOf(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java.name,
            )

        val dltConsumerProps =
            mapOf(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG to "test-dlt-verifier-group-${UUID.randomUUID()}",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java.name,
            )

        KafkaConsumer<String, ByteArray>(dltConsumerProps).use { dltConsumer ->
            dltConsumer.subscribe(listOf(KafkaConfig.ORDER_EVENTS_DLT_TOPIC))

            // Initial poll to join group / partition assignment
            dltConsumer.poll(Duration.ofMillis(500))

            KafkaProducer<String, ByteArray>(producerProps).use { producer ->
                producer.send(ProducerRecord(KafkaConfig.ORDER_EVENTS_TOPIC, testKey, poisonPayload)).get()
            }

            val received = mutableListOf<ConsumerRecord<String, ByteArray>>()
            await.atMost(Duration.ofSeconds(15)).pollInSameThread().untilAsserted {
                received += dltConsumer.poll(Duration.ofMillis(500)).filter { it.key() == testKey }
                assertThat(received).isNotEmpty()
            }
            assertThat(received.first().value()).isEqualTo(poisonPayload)
        }
    }

    // Keyed by customer like the outbox events, so all events of the order land in one partition and keep their order
    private fun publish(event: OrderEvent) {
        val producerProps =
            mapOf(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
            )
        val payload = objectMapper.writerFor(OrderEvent::class.java).writeValueAsString(event)

        KafkaProducer<String, String>(producerProps).use { producer ->
            producer.send(ProducerRecord(KafkaConfig.ORDER_EVENTS_TOPIC, customerId, payload)).get()
        }
    }

    private fun awaitLatestState(assertion: (OrderHistory) -> Unit): OrderHistory {
        lateinit var latest: OrderHistory
        await.atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(250)).untilAsserted {
            val records = historyRepository.findByOrderId(orderId)
            assertThat(records).hasSize(1)
            latest = records.first()
            assertion(latest)
        }
        return latest
    }

    // Waits until the repair rows are stored and every older version (orphans included) is deleted
    private fun awaitRepair(previousVersion: Long): OrderHistory {
        lateinit var latest: OrderHistory
        await.atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(250)).untilAsserted {
            latest = historyRepository.findByOrderId(orderId).single()
            assertThat(latest.version).isGreaterThan(previousVersion)
            assertThat(historyRepository.countRows(orderId)).isEqualTo(2)
        }
        return latest
    }

    private fun findSnapshot(targetTime: Instant): OrderHistory =
        historyRepository.findBy(GetOrderHistoryQuery(targetTime = targetTime, orderId = orderId)).single()
}

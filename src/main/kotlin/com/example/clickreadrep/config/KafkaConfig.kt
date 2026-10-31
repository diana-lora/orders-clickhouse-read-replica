package com.example.clickreadrep.config

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.kotlinModule
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.context.event.EventListener
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.util.backoff.FixedBackOff

@EnableKafka
@Configuration
class KafkaConfig(
    @Value($$"${spring.kafka.bootstrap-servers}")
    private val bootstrapServers: String,
) {
    private val logger = KotlinLogging.logger {}

    @Bean
    @Primary
    fun customObjectMapper(): ObjectMapper =
        ObjectMapper().apply {
            registerModule(kotlinModule())
            registerModule(JavaTimeModule())
            configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
        }

    @Bean
    fun kafkaAdmin(): KafkaAdmin = KafkaAdmin(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers))

    @Bean
    fun customProducerFactory(): ProducerFactory<String, String> {
        val configProps =
            mapOf(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
            )
        return DefaultKafkaProducerFactory(configProps)
    }

    @Bean
    fun customKafkaTemplate(producerFactory: ProducerFactory<String, String>): KafkaTemplate<String, String> =
        KafkaTemplate(producerFactory)

    @Bean
    fun errorHandler(template: KafkaTemplate<String, String>): DefaultErrorHandler {
        // 1. If an error occurs, send the poison pill message to the Dead Letter Topic (topic-name.DLT) [source]
        val recoverer =
            DeadLetterPublishingRecoverer(template) { cr, _ ->
                TopicPartition(ORDER_EVENTS_DLT_TOPIC, cr.partition())
            }

        // 2. Retry policy: Retry 3 times with a 2-second interval before giving up [source]
        val backOff = FixedBackOff(2000L, 3L)

        val errorHandler = DefaultErrorHandler(recoverer, backOff)

        // 3. Do not retry if the error is purely due to fatal Deserialization issues [source]
        errorHandler.addNotRetryableExceptions(org.apache.kafka.common.errors.SerializationException::class.java)
        errorHandler.addNotRetryableExceptions(org.springframework.kafka.support.serializer.DeserializationException::class.java)
        // Payloads are parsed in the listener, so malformed JSON surfaces as a Jackson exception
        errorHandler.addNotRetryableExceptions(com.fasterxml.jackson.core.JsonProcessingException::class.java)

        return errorHandler
    }

    @Bean
    fun customConsumerFactory(): ConsumerFactory<String, String> {
        val configProps =
            mapOf<String, Any>(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            )

        return DefaultKafkaConsumerFactory(
            configProps,
            StringDeserializer(),
            StringDeserializer(),
        )
    }

    @Bean
    fun kafkaListenerContainerFactory(
        consumerFactory: ConsumerFactory<String, String>,
        errorHandler: DefaultErrorHandler,
    ): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.setConsumerFactory(consumerFactory)
        factory.setCommonErrorHandler(errorHandler)
        return factory
    }

    @EventListener(ApplicationReadyEvent::class)
    fun createTopicsOnStartup() {
        AdminClient.create(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers)).use { admin ->
            val topics =
                listOf(
                    NewTopic(ORDER_EVENTS_TOPIC, NUMBER_PARTITIONS, NUMBER_REPLICAS),
                    NewTopic(ORDER_EVENTS_DLT_TOPIC, NUMBER_PARTITIONS, NUMBER_REPLICAS),
                )

            val existing = admin.listTopics().names().get()
            val toCreate = topics.filter { !existing.contains(it.name()) }

            if (toCreate.isNotEmpty()) {
                try {
                    admin.createTopics(toCreate).all().get()
                    logger.info { "Kafka topics created successfully on startup." }
                } catch (e: java.util.concurrent.ExecutionException) {
                    if (e.cause !is TopicExistsException) throw e
                }
            }
        }
    }

    companion object {
        const val NUMBER_PARTITIONS: Int = 3
        const val NUMBER_REPLICAS: Short = 1
        const val ORDER_EVENTS_TOPIC: String = "order-events"
        const val ORDER_EVENTS_DLT_TOPIC: String = "order-events.DLT"
    }
}

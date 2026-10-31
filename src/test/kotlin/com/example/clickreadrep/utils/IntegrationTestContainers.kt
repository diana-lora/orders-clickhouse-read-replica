package com.example.clickreadrep.utils

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.test.context.TestPropertySource
import org.springframework.test.web.servlet.MockMvc
import org.testcontainers.clickhouse.ClickHouseContainer
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.postgresql.PostgreSQLContainer
import java.io.File
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean

// Every IT uses the same configuration, so they all share one cached Spring context and one Kafka consumer
@SpringBootTest
@AutoConfigureMockMvc
@TestPropertySource(properties = ["spring.kafka.consumer.group-id=order-history-it"])
abstract class IntegrationTestContainers {
    @Autowired
    protected lateinit var mockMvc: MockMvc

    @Autowired
    protected lateinit var objectMapper: ObjectMapper

    companion object {
        private const val CONNECT_PORT = 8083
        private const val KAFKA_INTERNAL_LISTENER = "kafka:19092"

        private val network: Network = Network.newNetwork()

        val postgres: PostgreSQLContainer =
            PostgreSQLContainer("postgres:16-alpine")
                .withNetwork(network)
                .withNetworkAliases("postgres")
                .withDatabaseName("outbox_db")
                .withUsername("outbox_user")
                .withPassword("outbox_password")
                .withCommand("postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=4", "-c", "max_wal_senders=4")

        val clickhouse: ClickHouseContainer =
            ClickHouseContainer("clickhouse/clickhouse-server:26.8-alpine")
                .withNetwork(network)
                .withDatabaseName("orders_analytics")
                .withUsername("ch_user")
                .withPassword("ch_password")

        val kafka: KafkaContainer =
            KafkaContainer("apache/kafka:3.8.0")
                .withNetwork(network)
                .withNetworkAliases("kafka")
                .withListener(KAFKA_INTERNAL_LISTENER)

        val debezium: GenericContainer<*> =
            GenericContainer("quay.io/debezium/connect:2.7")
                .withNetwork(network)
                .withExposedPorts(CONNECT_PORT)
                .withEnv("BOOTSTRAP_SERVERS", KAFKA_INTERNAL_LISTENER)
                .withEnv("GROUP_ID", "clickreadrep-it-connect")
                .withEnv("CONFIG_STORAGE_TOPIC", "clickreadrep_it_connect_configs")
                .withEnv("OFFSET_STORAGE_TOPIC", "clickreadrep_it_connect_offsets")
                .withEnv("STATUS_STORAGE_TOPIC", "clickreadrep_it_connect_statuses")
                .dependsOn(kafka, postgres)
                .waitingFor(Wait.forHttp("/connectors").forPort(CONNECT_PORT).forStatusCode(200))
                .withStartupTimeout(Duration.ofMinutes(3))

        private val connectorRegistered = AtomicBoolean(false)
        private val http: HttpClient = HttpClient.newHttpClient()
        private val mapper = ObjectMapper()

        init {
            postgres.start()
            clickhouse.start()
            kafka.start()
            debezium.start()
        }

        @JvmStatic
        @DynamicPropertySource
        fun containerProperties(registry: DynamicPropertyRegistry) {
            registry.add("spring.datasource.url") { postgres.jdbcUrl }
            registry.add("spring.datasource.username") { postgres.username }
            registry.add("spring.datasource.password") { postgres.password }
            registry.add("clickhouse.datasource.url") { clickhouse.jdbcUrl }
            registry.add("clickhouse.datasource.username") { clickhouse.username }
            registry.add("clickhouse.datasource.password") { clickhouse.password }
            registry.add("spring.kafka.bootstrap-servers") { kafka.bootstrapServers }
        }

        private fun connectUrl(path: String) = URI.create("http://${debezium.host}:${debezium.getMappedPort(CONNECT_PORT)}$path")

        @Suppress("UNCHECKED_CAST")
        private fun registerOutboxConnector() {
            val config =
                mapper
                    .readValue(File("infra/debezium-outbox-connector.json"), Map::class.java)
                    .toMutableMap() as MutableMap<String, Any>
            val name = config["name"] as String
            config["database.hostname"] = "postgres"
            config["database.port"] = "5432"
            config["database.dbname"] = postgres.databaseName
            config["database.user"] = postgres.username
            config["database.password"] = postgres.password

            val put =
                HttpRequest
                    .newBuilder(connectUrl("/connectors/$name/config"))
                    .header("Content-Type", "application/json")
                    .PUT(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(config)))
                    .build()
            val response = http.send(put, HttpResponse.BodyHandlers.ofString())
            check(response.statusCode() in 200..201) { "Connector registration failed: ${response.body()}" }

            val deadline = System.currentTimeMillis() + 60_000
            while (System.currentTimeMillis() < deadline) {
                val status =
                    http.send(
                        HttpRequest.newBuilder(connectUrl("/connectors/$name/status")).GET().build(),
                        HttpResponse.BodyHandlers.ofString(),
                    )
                if (status.statusCode() == 200) {
                    val tree = mapper.readTree(status.body())
                    val tasks = tree.path("tasks")
                    if (tree.path("connector").path("state").asText() == "RUNNING" &&
                        tasks.size() > 0 && tasks.all { it.path("state").asText() == "RUNNING" }
                    ) {
                        return
                    }
                }
                Thread.sleep(500)
            }
            error("Connector $name did not reach RUNNING state")
        }
    }

    /** Runs after the Spring context has started (and Flyway created the outbox table). */
    @BeforeEach
    fun ensureOutboxConnector() {
        if (connectorRegistered.compareAndSet(false, true)) {
            registerOutboxConnector()
        }
    }
}

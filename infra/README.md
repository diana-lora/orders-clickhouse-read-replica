## Deployment Guide via Docker Compose (Container Mode)

This section describes how to spin up the complete ecosystem (Database, Kafka, Debezium, and the Spring Boot
application) fully automated inside Docker containers. This is the ideal setup for continuous integration (CI)
environments or quick demonstrations.

### 1. Prerequisites

Before starting, ensure you compile the application executable binary (`.jar`) so Docker can build the image correctly:

```bash
./gradlew clean bootJar
```

### 2. Standard Stack Initialization

Run the following command in your terminal to build the local images and spin up all services in the background:

```bash
docker compose --profile app up --build -d
```

The `app` service belongs to the `app` profile, so it only starts when the profile is active. The
[order history CDC sync runbook](../docs/runbook/order-history-cdc-sync.md) explains how to create orders
and follow them from PostgreSQL into ClickHouse. Run `docker compose --profile app down -v` to start again from a clean
database.

### 3. Service Access Ports

Once the ecosystem is fully initialized, you can verify the status of the components through the following local
addresses:

* **Spring Boot Application (REST API):** `http://localhost:8080`
* **Health Check (Actuator):** `http://localhost:8080/actuator/health`
* **Kafka Visual Interface (Kafka UI):** `http://localhost:8082`
* **Kafka Connect REST API (Debezium):** `http://localhost:8083/connectors`

---

## Configuration & Workflow for Local Development with IntelliJ IDEA

If you want to modify source code, debug features, or run interactive tests in real-time, you should run the Spring Boot
application natively within **IntelliJ IDEA** and delegate only the heavy infrastructure to Docker Compose.

No changes to `docker-compose.yml` are required. The `app` service belongs to the `app` profile, and no other service
depends on it, so starting the stack without the profile runs everything except the application.

**Step 1: Start the infrastructure**

```bash
docker compose up -d
```

This starts PostgreSQL, ClickHouse, Kafka, Kafka UI, Debezium, and `connector-init`, which registers the connector.

**Step 2: Start the application in IntelliJ IDEA**
Run or debug the main application class. The app connects to the containers via `localhost` and Flyway creates the
schema (e.g., `orders`, `outbox`). Debezium creates its publication for all tables (`publication.autocreate.mode`
defaults to `all_tables`), so it captures the `outbox` table as soon as Flyway creates it.

Run `docker compose down` to stop the infrastructure.

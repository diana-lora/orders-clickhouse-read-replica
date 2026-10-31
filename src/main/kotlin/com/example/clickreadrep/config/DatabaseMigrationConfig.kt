package com.example.clickreadrep.config

import jakarta.annotation.PostConstruct
import org.flywaydb.core.Flyway
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Configuration
import javax.sql.DataSource

@Configuration
class DatabaseMigrationConfig(
    @Qualifier("dataSource") private val postgresDataSource: DataSource,
    @Qualifier("clickhouseDataSource") private val clickhouseDataSource: DataSource,
) {
    @PostConstruct
    fun migrateDatabases() {
        Flyway
            .configure()
            .dataSource(postgresDataSource)
            .locations("classpath:db/migration/postgres")
            .baselineOnMigrate(true)
            .load()
            .migrate()

        Flyway
            .configure()
            .dataSource(clickhouseDataSource)
            .locations("classpath:db/migration/clickhouse")
            .baselineOnMigrate(true)
            .load()
            .migrate()
    }
}

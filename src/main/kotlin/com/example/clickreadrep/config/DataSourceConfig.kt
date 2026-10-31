package com.example.clickreadrep.config

import com.clickhouse.jdbc.ClickHouseDataSource
import com.zaxxer.hikari.HikariDataSource
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.jdbc.autoconfigure.DataSourceProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import java.util.Properties
import javax.sql.DataSource

@Configuration
@EnableConfigurationProperties(ClickhouseDataSourceProperties::class)
class DataSourceConfig {
    @Bean(name = ["dataSource"])
    @Primary
    fun dataSource(properties: DataSourceProperties): DataSource =
        properties.initializeDataSourceBuilder().type(HikariDataSource::class.java).build()

    @Bean(name = ["clickhouseDataSource"])
    fun clickhouseDataSource(properties: ClickhouseDataSourceProperties): DataSource {
        val clickhouseProperties =
            Properties().apply {
                setProperty("user", properties.username)
                setProperty("password", properties.password)
            }
        return ClickHouseDataSource(properties.url, clickhouseProperties)
    }
}

@ConfigurationProperties("clickhouse.datasource")
data class ClickhouseDataSourceProperties(
    val url: String,
    val username: String,
    val password: String,
)

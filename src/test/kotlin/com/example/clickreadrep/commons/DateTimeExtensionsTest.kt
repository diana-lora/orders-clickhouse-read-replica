package com.example.clickreadrep.commons

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.LocalDateTime

class DateTimeExtensionsTest {
    @Test
    fun `should convert instant to UTC local date time`() {
        val instant = Instant.parse("2026-01-15T10:00:00.123Z")

        assertThat(instant.toUtcLocalDateTime()).isEqualTo(LocalDateTime.of(2026, 1, 15, 10, 0, 0, 123_000_000))
    }

    @Test
    fun `should convert UTC local date time to instant`() {
        val localDateTime = LocalDateTime.of(2026, 1, 15, 10, 0, 0, 123_000_000)

        assertThat(localDateTime.toUtcInstant()).isEqualTo(Instant.parse("2026-01-15T10:00:00.123Z"))
    }

    @Test
    fun `should keep the same instant after a round trip`() {
        val instant = Instant.parse("2026-10-26T08:59:20.026639Z")

        assertThat(instant.toUtcLocalDateTime().toUtcInstant()).isEqualTo(instant)
    }

    @Test
    fun `should truncate an instant to milliseconds`() {
        val instant = Instant.parse("2026-10-26T08:59:20.026639123Z")

        assertThat(instant.truncatedToMillis()).isEqualTo(Instant.parse("2026-10-26T08:59:20.026Z"))
    }

    @Test
    fun `should return the current instant with millisecond precision`() {
        assertThat(nowInMillis().nano % 1_000_000).isZero()
    }

    @Test
    fun `should ignore daylight saving time changes of the default time zone`() {
        // 01:30 UTC falls into the hour that is skipped in Central Europe when summer time starts
        val instant = Instant.parse("2026-03-29T01:30:00Z")

        assertThat(instant.toUtcLocalDateTime()).isEqualTo(LocalDateTime.of(2026, 3, 29, 1, 30))
        assertThat(instant.toUtcLocalDateTime().toUtcInstant()).isEqualTo(instant)
    }
}

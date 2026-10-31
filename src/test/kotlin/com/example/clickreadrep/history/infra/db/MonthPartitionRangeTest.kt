package com.example.clickreadrep.history.infra.db

import com.example.clickreadrep.commons.monthPartitionRange
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant

class MonthPartitionRangeTest {
    @Test
    fun `should cover the whole UTC month of a mid-month instant`() {
        val range = monthPartitionRange(Instant.parse("2026-01-15T10:00:00.123Z"))

        assertThat(range).isEqualTo(Instant.parse("2026-01-01T00:00:00Z") to Instant.parse("2026-02-01T00:00:00Z"))
    }

    @Test
    fun `should include the first instant of the month`() {
        val range = monthPartitionRange(Instant.parse("2026-03-01T00:00:00Z"))

        assertThat(range.first).isEqualTo(Instant.parse("2026-03-01T00:00:00Z"))
    }

    @Test
    fun `should keep the last millisecond of a month in that month`() {
        val range = monthPartitionRange(Instant.parse("2026-04-30T23:59:59.999Z"))

        assertThat(range).isEqualTo(Instant.parse("2026-04-01T00:00:00Z") to Instant.parse("2026-05-01T00:00:00Z"))
    }

    @Test
    fun `should roll over into the next year for December`() {
        val range = monthPartitionRange(Instant.parse("2026-12-31T12:00:00Z"))

        assertThat(range).isEqualTo(Instant.parse("2026-12-01T00:00:00Z") to Instant.parse("2027-01-01T00:00:00Z"))
    }
}

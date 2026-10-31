package com.example.clickreadrep.commons

import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit

/** Truncates this instant to milliseconds, the precision of the ClickHouse `DateTime64(3)` columns. */
fun Instant.truncatedToMillis(): Instant = truncatedTo(ChronoUnit.MILLIS)

/** Current instant truncated to milliseconds, so that PostgreSQL and ClickHouse store exactly the same value. */
fun nowInMillis(): Instant = Instant.now().truncatedToMillis()

/** Converts this instant to a [LocalDateTime] on the UTC time-line, independent of the JVM default time zone. */
fun Instant.toUtcLocalDateTime(): LocalDateTime = LocalDateTime.ofInstant(this, ZoneOffset.UTC)

/** Interprets this [LocalDateTime] as UTC and converts it to an [Instant], independent of the JVM default time zone. */
fun LocalDateTime.toUtcInstant(): Instant = toInstant(ZoneOffset.UTC)

/** Start (inclusive) and end (exclusive) of the UTC month holding [createdAt], i.e. its `toYYYYMM(created_at)` partition. */
internal fun monthPartitionRange(createdAt: Instant): Pair<Instant, Instant> {
    val partitionStart =
        createdAt
            .atZone(ZoneOffset.UTC)
            .toLocalDate()
            .withDayOfMonth(1)
            .atStartOfDay(ZoneOffset.UTC)
    return partitionStart.toInstant() to partitionStart.plusMonths(1).toInstant()
}

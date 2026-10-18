package org.sensorhub.impl.datastore.postgis.archive;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * Explicit UTC conversion for JDBC timestamp columns.
 *
 * <p>OSH observation tables use {@code timestamp without time zone} as a UTC
 * wall clock, while the archive catalog uses {@code timestamptz}.  The JDBC
 * no-calendar overloads apply the host default timezone to the former and can
 * shift archive windows, rows, and comparisons.  Supplying a UTC calendar is
 * correct for both PostgreSQL column types and DuckDB's timezone-free Parquet
 * timestamps.</p>
 */
public final class UtcJdbcTime {
    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    private UtcJdbcTime() {
    }

    public static void setInstant(PreparedStatement statement, int index, Instant instant) throws SQLException {
        statement.setTimestamp(index, Timestamp.from(instant), calendar());
    }

    /**
     * DuckDB's Calendar overload converts through {@code Timestamp#getTime()},
     * which discards sub-millisecond precision. Bind a UTC local timestamp
     * directly so Parquet retains PostgreSQL's full microsecond value.
     */
    public static void setDuckDbInstant(PreparedStatement statement, int index, Instant instant) throws SQLException {
        statement.setObject(index, LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
    }

    /**
     * PostgreSQL's default timestamp precision is six fractional digits. It
     * pgjdbc rounds discarded nanoseconds to the nearest microsecond, with a
     * 500-nanosecond tie rounded upward. Conflict verification must compare
     * the exact value sent by the driver rather than the caller's nanoseconds.
     */
    public static Instant toDatabasePrecision(Instant instant) {
        if (instant == null)
            return null;
        int microseconds = instant.getNano() / 1_000;
        int discardedNanos = instant.getNano() % 1_000;
        if (discardedNanos >= 500)
            microseconds++;
        if (microseconds == 1_000_000)
            return Instant.ofEpochSecond(Math.addExact(instant.getEpochSecond(), 1L));
        return Instant.ofEpochSecond(instant.getEpochSecond(), microseconds * 1_000L);
    }

    public static Instant getInstant(ResultSet result, int index) throws SQLException {
        Timestamp timestamp = result.getTimestamp(index, calendar());
        return timestamp == null ? null : timestamp.toInstant();
    }

    public static Instant getInstant(ResultSet result, String label) throws SQLException {
        Timestamp timestamp = result.getTimestamp(label, calendar());
        return timestamp == null ? null : timestamp.toInstant();
    }

    /** Reads a timezone-free DuckDB/Parquet timestamp as a UTC instant. */
    public static Instant getDuckDbInstant(ResultSet result, int index) throws SQLException {
        LocalDateTime timestamp = result.getObject(index, LocalDateTime.class);
        return timestamp == null ? null : timestamp.toInstant(ZoneOffset.UTC);
    }

    private static Calendar calendar() {
        return Calendar.getInstance(UTC);
    }
}

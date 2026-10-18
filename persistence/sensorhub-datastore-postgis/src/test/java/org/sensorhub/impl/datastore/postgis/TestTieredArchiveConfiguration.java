package org.sensorhub.impl.datastore.postgis;

import org.junit.Test;
import org.sensorhub.impl.datastore.postgis.archive.TieredArchiveConfig;
import org.sensorhub.impl.datastore.postgis.archive.ArchiveExporter;
import org.sensorhub.impl.datastore.postgis.archive.UtcJdbcTime;
import org.sensorhub.impl.datastore.postgis.builder.QueryBuilderDataStreamStore;
import org.sensorhub.impl.datastore.postgis.builder.QueryBuilderObsStore;
import org.sensorhub.impl.datastore.postgis.database.OscarTieredObsSystemDatabase;
import org.sensorhub.impl.datastore.postgis.store.obs.OscarTieredObsStore;
import org.sensorhub.api.datastore.obs.IObsStore;

import java.lang.reflect.Proxy;
import java.time.Instant;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TestTieredArchiveConfiguration {
    @Test
    public void hotDeletionIsFailSafeByDefault() {
        TieredArchiveConfig config = new TieredArchiveConfig();
        assertTrue(config.enabled);
        assertFalse(config.deleteHotAfterArchive);
        assertTrue(config.requireParityReceipt);
        assertTrue(config.maximumGenerationsPerCycle > 1);
        assertTrue(config.maximumCycleMinutes > 0);
        assertTrue(config.maximumUncompressedBytesPerGeneration > 0);
        assertTrue(config.outputNames.contains("gammaCounts"));
        assertFalse(config.outputNames.contains("occupancy"));
    }

    @Test
    public void parityReceiptCannotBeDisabled() {
        TieredArchiveConfig config = new TieredArchiveConfig();
        config.requireParityReceipt = false;
        assertThrows(IllegalArgumentException.class, config::validate);
    }

    @Test
    public void archivePayloadByteCapCannotBeDisabled() {
        TieredArchiveConfig config = new TieredArchiveConfig();
        config.maximumUncompressedBytesPerGeneration = 0;
        assertThrows(IllegalArgumentException.class, config::validate);
    }

    @Test
    public void archiveWindowsAreAnchoredToUtcHours() {
        assertEquals(Instant.parse("2026-01-01T12:00:00Z"),
                OscarTieredObsSystemDatabase.utcHourWindowStart(
                        Instant.parse("2026-01-01T12:34:56.789Z")));
    }

    @Test
    public void archiveWindowBisectionHalvesAndClampsAtOneSecond() {
        Instant start = Instant.parse("2026-01-01T12:00:00Z");
        assertEquals(start.plusSeconds(30), OscarTieredObsStore.bisectArchiveWindowEnd(
                start, start.plusSeconds(60)));
        assertEquals(start.plusSeconds(1), OscarTieredObsStore.bisectArchiveWindowEnd(
                start, start.plusMillis(1_500)));
        assertThrows(IllegalArgumentException.class, () ->
                OscarTieredObsStore.bisectArchiveWindowEnd(start, start.plusSeconds(1)));
    }

    @Test
    public void archiveByteEstimateCountsUtf8RatherThanJavaCharacters() {
        String resultJson = "{\"label\":\"gamma-γ-🚀\"}";
        assertEquals(ArchiveExporter.ESTIMATED_FIXED_BYTES_PER_ROW +
                        resultJson.getBytes(StandardCharsets.UTF_8).length,
                ArchiveExporter.estimateUncompressedRowBytes(resultJson));
        assertEquals(ArchiveExporter.ESTIMATED_FIXED_BYTES_PER_ROW,
                ArchiveExporter.estimateUncompressedRowBytes(null));
    }

    @Test
    public void postgresTimestampPrecisionMatchesPgjdbcHalfUpMicroseconds() {
        assertEquals(Instant.parse("2026-01-01T00:00:00.123457Z"),
                UtcJdbcTime.toDatabasePrecision(Instant.parse("2026-01-01T00:00:00.123456500Z")));
        assertEquals(Instant.parse("2026-01-01T00:00:00.123456Z"),
                UtcJdbcTime.toDatabasePrecision(Instant.parse("2026-01-01T00:00:00.123455500Z")));
        assertEquals(Instant.parse("2026-01-01T00:00:01Z"),
                UtcJdbcTime.toDatabasePrecision(Instant.parse("2026-01-01T00:00:00.999999999Z")));
    }

    @Test
    public void datastreamRangeTriggerAggregatesTransitionTable() {
        QueryBuilderDataStreamStore dataStreams = new QueryBuilderDataStreamStore("obs_datastreams");
        IObsStore observations = (IObsStore) Proxy.newProxyInstance(
                IObsStore.class.getClassLoader(), new Class<?>[]{IObsStore.class},
                (proxy, method, args) -> "getDatastoreName".equals(method.getName()) ? "obs" : null);
        dataStreams.linkTo(observations);
        String sql = dataStreams.createMinMaxTimeTriggerQuery();
        assertTrue(sql.contains("REFERENCING NEW TABLE AS inserted_observations"));
        assertTrue(sql.contains("FOR EACH STATEMENT"));
        assertFalse(sql.contains("FOR EACH ROW"));
        assertTrue(sql.contains("GROUP BY dataStreamID"));
    }

    @Test
    public void archiveMarkerMigrationIsIdempotentAndIndexesOnlyUnarchivedRows() {
        QueryBuilderObsStore observations = new QueryBuilderObsStore("obs");
        assertTrue(observations.addArchiveGenerationColumnQuery()
                .contains("ADD COLUMN IF NOT EXISTS archivegenerationid uuid"));
        String index = observations.createUnarchivedTimeIndexQuery();
        assertTrue(index.contains("CREATE INDEX IF NOT EXISTS"));
        assertTrue(index.contains("(phenomenontime, id)"));
        assertTrue(index.contains("WHERE archivegenerationid IS NULL"));
        String generationIndex = observations.createArchiveGenerationIndexQuery();
        assertTrue(generationIndex.contains("(archivegenerationid, id)"));
        assertTrue(generationIndex.contains("WHERE archivegenerationid IS NOT NULL"));
    }
}

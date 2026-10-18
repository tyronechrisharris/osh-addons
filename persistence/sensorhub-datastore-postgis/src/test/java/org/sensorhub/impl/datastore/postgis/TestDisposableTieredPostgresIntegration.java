package org.sensorhub.impl.datastore.postgis;

import com.google.common.collect.Range;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import net.opengis.swe.v20.DataBlock;
import org.junit.Test;
import org.sensorhub.api.common.BigId;
import org.sensorhub.api.data.ObsData;
import org.sensorhub.api.datastore.ICancellableDataStore;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.impl.datastore.postgis.archive.ArchiveCatalog;
import org.sensorhub.impl.datastore.postgis.archive.ArchiveExporter;
import org.sensorhub.impl.datastore.postgis.archive.ArchiveGeneration;
import org.sensorhub.impl.datastore.postgis.archive.ArchivedObservation;
import org.sensorhub.impl.datastore.postgis.archive.ParquetArchiveReader;
import org.sensorhub.impl.datastore.postgis.archive.TieredArchiveConfig;
import org.sensorhub.impl.datastore.postgis.archive.UtcJdbcTime;
import org.sensorhub.impl.datastore.postgis.database.OscarTieredObsSystemDatabase;
import org.sensorhub.impl.datastore.postgis.connection.ConnectionManager;
import org.sensorhub.impl.datastore.postgis.store.obs.OscarTieredObsStore;
import org.sensorhub.impl.datastore.postgis.store.obs.PostgisDataStreamStoreImpl;
import org.sensorhub.impl.datastore.postgis.store.obs.PostgisObsStoreImpl;
import org.sensorhub.impl.datastore.postgis.utils.SerializerUtils;
import org.vast.data.DataBlockDouble;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** Disposable, explicitly selected real-PostgreSQL integration probe. */
public class TestDisposableTieredPostgresIntegration extends TestPostgisObsStore {
    private static final String TABLE = "codex_storage_it_obs";
    private static final int ACTIVATION_LOCK_CLASS = 0x4f534341;
    private static final int ACTIVATION_LOCK_OBJECT = 0x41524348;
    private TieredArchiveConfig archiveConfig;

    @Override
    protected PostgisObsStoreImpl initStore() throws Exception {
        String archiveDir = System.getenv("OSCAR_IT_ARCHIVE_DIR");
        org.junit.Assume.assumeTrue("OSCAR_IT_ARCHIVE_DIR enables the disposable PostgreSQL probe",
                archiveDir != null && !archiveDir.isBlank());
        Path root = Path.of(archiveDir).toAbsolutePath().normalize();
        Files.createDirectories(root);

        archiveConfig = new TieredArchiveConfig();
        archiveConfig.rootDirectory = root.toString();
        archiveConfig.outputNames = List.of("gammaCounts");
        archiveConfig.deleteBatchSize = 2;
        archiveConfig.rollbackGraceDays = 1;
        archiveConfig.deleteHotAfterArchive = false;
        this.postgisObsStore = new TestableTieredStore(url, DB_NAME, login, password, TABLE,
                DATABASE_NUM, IdProviderType.SEQUENTIAL, archiveConfig);
        return this.postgisObsStore;
    }

    @Test
    public void scopedAbortCancelsRealPostgresWorkWithoutTouchingAnotherConnection() throws Exception {
        ConnectionManager manager = new ConnectionManager(url, DB_NAME, login, password);
        String mutationTable = "codex_cancel_scope_" + UUID.randomUUID().toString().replace("-", "");
        AtomicReference<ICancellableDataStore.CancellableOperation> operation = new AtomicReference<>();
        AtomicReference<Throwable> workerFailure = new AtomicReference<>();
        AtomicInteger backendPid = new AtomicInteger();
        AtomicBoolean completedNormally = new AtomicBoolean();
        CountDownLatch statementEntered = new CountDownLatch(1);
        CountDownLatch workerExited = new CountDownLatch(1);
        Thread worker = null;

        try (Connection unrelated = manager.getConnection();
             Statement setup = unrelated.createStatement()) {
            setup.execute("CREATE TABLE " + mutationTable + " (value integer NOT NULL)");

            worker = new Thread(() -> {
                try (ICancellableDataStore.CancellableOperation scope =
                             ConnectionManager.beginCancellableOperation();
                     Connection scoped = manager.getConnection()) {
                    operation.set(scope);
                    scoped.setAutoCommit(false);
                    try (Statement statement = scoped.createStatement()) {
                        statement.executeUpdate("INSERT INTO " + mutationTable + " VALUES (1)");
                        try (ResultSet result = statement.executeQuery("SELECT pg_backend_pid()")) {
                            assertTrue(result.next());
                            backendPid.set(result.getInt(1));
                        }
                        statementEntered.countDown();
                        statement.execute("SELECT pg_sleep(30)");
                    }
                    scoped.commit();
                    completedNormally.set(true);
                } catch (Throwable error) {
                    workerFailure.set(error);
                } finally {
                    workerExited.countDown();
                }
            }, "real-postgres-cancellable-operation");
            worker.setDaemon(true);
            worker.start();

            assertTrue("The worker must reach the blocking PostgreSQL statement",
                    statementEntered.await(5, TimeUnit.SECONDS));
            waitForActivePgSleep(unrelated, backendPid.get());
            assertEquals("An unrelated pooled connection must remain usable while the worker blocks",
                    41L, queryLong(unrelated, "SELECT 41"));

            long cancelStarted = System.nanoTime();
            operation.get().cancel();
            operation.get().cancel(); // Explicitly verify idempotency against the real driver.
            assertTrue("Connection.abort must release the blocked JDBC worker promptly",
                    workerExited.await(5, TimeUnit.SECONDS));
            waitForBackendExit(unrelated, backendPid.get());
            long cancelMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - cancelStarted);
            worker.join(1_000L);

            assertFalse("The cancelled transaction must not reach commit", completedNormally.get());
            assertNotNull("The blocked statement must observe connection cancellation", workerFailure.get());
            assertTrue("Cancellation must leave headroom inside the core 10 second stop budget: "
                    + cancelMillis + " ms", cancelMillis < 5_000L);
            assertEquals("The unrelated connection must survive scoped cancellation",
                    42L, queryLong(unrelated, "SELECT 42"));
            assertEquals("Aborting the scoped transaction must roll back its mutation",
                    0L, queryLong(unrelated, "SELECT count(*) FROM " + mutationTable));
            Thread.sleep(100L);
            assertEquals("No cancelled worker may mutate after termination",
                    0L, queryLong(unrelated, "SELECT count(*) FROM " + mutationTable));

            try (Connection replacement = manager.getConnection()) {
                assertEquals("The Hikari pool must remain healthy after evicting the aborted connection",
                        43L, queryLong(replacement, "SELECT 43"));
            }
        } finally {
            ICancellableDataStore.CancellableOperation active = operation.get();
            if (worker != null && worker.isAlive() && active != null)
                active.cancel();
            if (worker != null)
                worker.join(5_000L);
            try (Connection cleanup = DriverManager.getConnection(
                    "jdbc:postgresql://" + url + "/" + DB_NAME, login, password);
                 Statement statement = cleanup.createStatement()) {
                statement.execute("DROP TABLE IF EXISTS " + mutationTable);
            } finally {
                manager.close();
            }
        }
    }

    @Override
    protected void forceReadBackFromStorage() {
        try {
            postgisObsStore.commit();
            postgisObsStore.getDataStreams().commit();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    @org.junit.After
    public void cleanup() throws Exception {
        if (postgisObsStore != null) {
            postgisObsStore.drop();
            if (postgisObsStore.getDataStreams() != null) {
                ((PostgisDataStreamStoreImpl) postgisObsStore.getDataStreams()).drop();
                ((PostgisDataStreamStoreImpl) postgisObsStore.getDataStreams()).clearCache();
            }
        }
        // This probe is opt-in and must point at a uniquely named disposable
        // PostgreSQL instance. Keep its global archive catalog isolated
        // between test methods without touching any OSCAR deployment.
        if (System.getenv("OSCAR_IT_ARCHIVE_DIR") != null) {
            try (Connection connection = open(); Statement statement = connection.createStatement()) {
                statement.execute("TRUNCATE oscar_archive_job CASCADE");
                statement.execute("TRUNCATE oscar_retention_hold, oscar_deletion_plan, " +
                        "oscar_media_manifest, oscar_backup_epoch CASCADE");
            }
            Path root = Path.of(System.getenv("OSCAR_IT_ARCHIVE_DIR")).toAbsolutePath().normalize();
            if (Files.isDirectory(root)) {
                try (var paths = Files.walk(root)) {
                    for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                        if (!path.equals(root))
                            Files.deleteIfExists(path);
                    }
                }
            }
        }
    }

    @Test
    public void completeHotColdLifecycleAgainstRealPostgres() throws Exception {
        String expectedZone = System.getenv("OSCAR_IT_EXPECTED_ZONE");
        if (expectedZone != null)
            assertEquals(expectedZone, ZoneId.systemDefault().getId());
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        ArchiveCatalog catalog = tiered.getArchiveCatalog();
        Path root = Path.of(archiveConfig.rootDirectory);
        Instant requestedStart = Instant.parse("2026-01-01T00:00:00.123456789Z");
        Instant start = UtcJdbcTime.toDatabasePrecision(requestedStart);

        BigId stream = addSimpleDataStream(bigId(10), "gammaCounts");
        // Real OSCAR datastreams use a human-facing name and a separate
        // machine-facing outputName. Archive eligibility must use the latter.
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "UPDATE " + TABLE + "_datastreams SET data=jsonb_set(data, '{name}', " +
                        "to_jsonb(?::text), true) WHERE id=?")) {
            statement.setString(1, "Lane06 - RPM - Gamma Counts");
            statement.setLong(2, stream.getIdAsLong());
            assertEquals(1, statement.executeUpdate());
        }
        Map<BigId, org.sensorhub.api.data.IObsData> inserted =
                addSimpleObsWithoutResultTime(stream, BigId.NONE, requestedStart, 6, 60_000);
        forceReadBackFromStorage();

        long firstId = inserted.keySet().stream().mapToLong(BigId::getIdAsLong).min().orElseThrow();
        BigId firstKey = BigId.fromLong(DATABASE_NUM, firstId);
        assertEquals(start, tiered.get(firstKey).getPhenomenonTime());
        DataBlock hotReplacement = new DataBlockDouble(5);
        for (int i = 0; i < 5; i++) hotReplacement.setDoubleValue(i, 500 + i);
        tiered.put(firstKey, new ObsData.Builder()
                .withDataStream(stream)
                .withPhenomenonTime(requestedStart)
                .withResultTime(requestedStart.plusSeconds(30))
                .withResult(hotReplacement)
                .build());
        assertEquals(start, tiered.get(firstKey).getPhenomenonTime());
        assertEquals(start.plusSeconds(30), tiered.get(firstKey).getResultTime());
        assertEquals(500.0, tiered.get(firstKey).getResult().getDoubleValue(0), 0.0);
        var range = tiered.getDataStreamPhenomenonTimeRanges(List.of(stream.getIdAsLong())).get(stream.getIdAsLong());
        assertEquals(start, range.begin());
        assertEquals(start.plus(5, ChronoUnit.MINUTES), range.end());
        var resultRange = tiered.getDataStreamResultTimeRanges(List.of(stream.getIdAsLong())).get(stream.getIdAsLong());
        assertEquals(start.plusSeconds(30), resultRange.begin());
        // ObsData defaults an omitted result time to its phenomenon time.
        assertEquals(start.plus(5, ChronoUnit.MINUTES), resultRange.end());

        assertEquals(6, tiered.getNumRecords());
        assertEquals(6, selectAll(tiered));
        assertStatementTriggerRange(stream, start, start.plus(5, ChronoUnit.MINUTES));
        assertIdempotentInsertContract(inserted.keySet().iterator().next().getIdAsLong());
        assertEquals(start, tiered.nextArchiveWindowStart(start.plus(1, ChronoUnit.HOURS),
                "RAW_GATED").orElseThrow());
        assertEquals(6, tiered.countArchiveCandidates(start, start.plus(1, ChronoUnit.HOURS), 100));

        ArchiveGeneration generation = tiered.archiveWindow(start, start.plus(1, ChronoUnit.HOURS),
                "RAW_GATED").orElseThrow();
        assertEquals(6, generation.rowCount());
        assertTrue(Files.isRegularFile(root.resolve(generation.objectPath())));
        try (var paths = Files.walk(root)) {
            assertEquals(0, paths.filter(path -> path.getFileName().toString().endsWith(".partial")).count());
        }
        assertCatalogV4AndActivation(generation);

        // Grace overlap must be deduplicated by both the full query and exact cardinality paths.
        assertEquals(6, selectAll(tiered));
        assertEquals(6, tiered.getNumRecords());
        assertGlobalOrderingAndLatest(tiered, start);
        try (var early = tiered.selectEntries(new ObsFilter.Builder().build())) {
            assertTrue(early.findFirst().isPresent());
            assertEquals(1, tiered.getOpenArchiveCursorCount());
        }
        assertEquals(0, tiered.getOpenArchiveCursorCount());
        try (var limited = tiered.selectEntries(new ObsFilter.Builder().withLimit(1).build())) {
            assertEquals(1, limited.count());
        }
        assertEquals(0, tiered.getOpenArchiveCursorCount());

        // A sparse overlay can change a row's sort key while its original hot
        // copy is still present during the rollback grace period. It must be
        // emitted once from the independently sorted mutation stream, not
        // substituted in place into the SQL-ordered hot stream.
        var schema = tiered.getDataStreams().get(new DataStreamKey(stream));
        String immutableHotBefore = hotResultJson(firstId);
        DataBlock graceReplacement = new DataBlockDouble(5);
        for (int i = 0; i < 5; i++) graceReplacement.setDoubleValue(i, 700 + i);
        tiered.put(firstKey, new ObsData.Builder()
                .withDataStream(stream)
                .withPhenomenonTime(start.plus(10, ChronoUnit.MINUTES))
                .withResult(graceReplacement)
                .build());
        assertEquals(700.0, tiered.get(firstKey).getResult().getDoubleValue(0), 0.0);
        assertEquals(immutableHotBefore, hotResultJson(firstId));
        try (var rows = tiered.selectEntries(new ObsFilter.Builder().build())) {
            assertEquals(List.of(start.plus(1, ChronoUnit.MINUTES), start.plus(2, ChronoUnit.MINUTES),
                            start.plus(3, ChronoUnit.MINUTES), start.plus(4, ChronoUnit.MINUTES),
                            start.plus(5, ChronoUnit.MINUTES), start.plus(10, ChronoUnit.MINUTES)),
                    rows.map(entry -> entry.getValue().getPhenomenonTime()).toList());
        }

        long removedDuringGrace = inserted.keySet().stream().mapToLong(BigId::getIdAsLong)
                .sorted().skip(1).findFirst().orElseThrow();
        assertTrue(tiered.remove(BigId.fromLong(DATABASE_NUM, removedDuringGrace)) != null);
        assertEquals(null, tiered.get(BigId.fromLong(DATABASE_NUM, removedDuringGrace)));
        assertTrue(hotRowExists(removedDuringGrace));

        // Sparse logical mutations must not alter the immutable hot parity
        // source during grace. Verification therefore still succeeds.
        tiered.maintainArchiveLifecycle();
        assertTrue(oneGeneration(catalog).parityVerified());
        try (Connection connection = open()) {
            try (PreparedStatement statement = connection.prepareStatement(
                    "DELETE FROM oscar_obs_overlay WHERE store_key=? AND id_scope=? AND observation_id=?")) {
                statement.setString(1, TABLE);
                statement.setInt(2, DATABASE_NUM);
                statement.setLong(3, firstId);
                assertEquals(1, statement.executeUpdate());
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "DELETE FROM oscar_tombstone WHERE store_key=? AND id_scope=? " +
                            "AND subject_type='OBSERVATION' AND subject_id=?")) {
                statement.setString(1, TABLE);
                statement.setInt(2, DATABASE_NUM);
                statement.setString(3, Long.toString(removedDuringGrace));
                assertEquals(1, statement.executeUpdate());
            }
        }
        assertEquals(6, selectAll(tiered));

        tiered.maintainArchiveLifecycle();
        ArchiveGeneration verified = oneGeneration(catalog);
        assertTrue(verified.parityVerified());

        long heldId = inserted.keySet().stream().mapToLong(BigId::getIdAsLong).sorted().skip(2).findFirst().orElseThrow();
        UUID holdId = UUID.randomUUID();
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO oscar_retention_hold(id, store_key, id_scope, subject_type, subject_id, reason, created_by) " +
                        "VALUES (?, ?, ?, 'OBSERVATION', ?, 'integration hold', 'codex-integration')")) {
            statement.setObject(1, holdId);
            statement.setString(2, TABLE);
            statement.setInt(3, DATABASE_NUM);
            statement.setString(4, Long.toString(heldId));
            statement.executeUpdate();
        }
        ageGeneration(generation.id());
        archiveConfig.deleteHotAfterArchive = true;
        tiered.maintainArchiveLifecycle();
        assertEquals(1, hotRows());
        assertEquals(6, tiered.getNumRecords());
        assertEquals(6, selectAll(tiered));
        assertEquals("ACTIVE", oneGeneration(catalog).state());

        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "UPDATE oscar_retention_hold SET released_at=now(), released_by='codex-integration' WHERE id=?")) {
            statement.setObject(1, holdId);
            assertEquals(1, statement.executeUpdate());
        }
        tiered.maintainArchiveLifecycle();
        assertEquals(0, hotRows());
        assertEquals("SEALED", oneGeneration(catalog).state());
        assertEquals(6, tiered.getNumRecords());
        assertEquals(6, selectAll(tiered));

        // Recreate the catalog and reader to model a process restart after hot deletion.
        ArchiveCatalog restartedCatalog = new ArchiveCatalog(() -> {
            try {
                return open();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }, TABLE, DATABASE_NUM);
        restartedCatalog.migrate();
        ArchiveGeneration restartedGeneration = oneGeneration(restartedCatalog);
        ParquetArchiveReader restartedReader = new ParquetArchiveReader(root);
        assertEquals(6, restartedReader.read(List.of(restartedGeneration), new ObsFilter.Builder().build(),
                null, null, Long.MAX_VALUE).size());

        // Sparse mutation records remain queryable through the normal OSH store contract.
        long overlayId = inserted.keySet().stream().mapToLong(BigId::getIdAsLong).min().orElseThrow();
        DataBlock replacement = new DataBlockDouble(5);
        for (int i = 0; i < 5; i++) replacement.setDoubleValue(i, 999 + i);
        String replacementJson = SerializerUtils.writeDataBlockToJson(
                schema.getRecordStructure(), schema.getRecordEncoding(), replacement);
        String overlaySchemaJson = SerializerUtils.writeIDataStreamInfoToJson(schema);
        restartedCatalog.upsertOverlay(generation.id(), new ArchivedObservation(DATABASE_NUM, overlayId, DATABASE_NUM,
                stream.getIdAsLong(), null, null, start, null, replacementJson, "RAW_GATED"),
                overlaySchemaJson);
        long overlayRowsBefore = catalog.getOverlayRowsRead();
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO oscar_obs_overlay(store_key, id_scope, observation_id, generation_id, datastream_id, foi_id, " +
                "phenomenon_time, result_time, result_json, schema_json) " +
                        "SELECT ?, ?, ? + value, ?, ?, NULL, ?, NULL, ?::jsonb, ?::jsonb " +
                        "FROM generate_series(1, 10000) value")) {
            statement.setString(1, TABLE);
            statement.setInt(2, DATABASE_NUM);
            statement.setLong(3, overlayId + 10_000_000L);
            statement.setObject(4, generation.id());
            statement.setLong(5, stream.getIdAsLong());
            UtcJdbcTime.setInstant(statement, 6, start.plus(2, ChronoUnit.HOURS));
            statement.setString(7, replacementJson);
            statement.setString(8, overlaySchemaJson);
            assertEquals(10_000, statement.executeUpdate());
        }

        // Both cold bases and overlays must be self-describing. Removing the
        // mutable live metadata after publication must not change or break
        // decoding of historical payloads.
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "DELETE FROM " + TABLE + "_datastreams WHERE id=?")) {
            statement.setLong(1, stream.getIdAsLong());
            assertEquals(1, statement.executeUpdate());
        }
        ((PostgisDataStreamStoreImpl) tiered.getDataStreams()).clearCache();

        org.sensorhub.api.data.IObsData overlaid;
        try (var rows = tiered.selectEntries(new ObsFilter.Builder()
                .withInternalIDs(BigId.fromLong(DATABASE_NUM, overlayId)).build())) {
            overlaid = rows.findFirst().orElseThrow().getValue();
            assertEquals(1, catalog.getOpenOverlayCursorCount());
        }
        assertEquals(999.0, overlaid.getResult().getDoubleValue(0), 0.0);
        assertEquals(1, catalog.getOverlayRowsRead() - overlayRowsBefore);
        assertEquals(0, catalog.getOpenOverlayCursorCount());
        assertEquals(0, tiered.getOpenArchiveCursorCount());
        try (var currentPhenomenon = tiered.selectEntries(new ObsFilter.Builder()
                .withPhenomenonTime().withCurrentTime().done().build())) {
            assertEquals(0, currentPhenomenon.count());
        }
        try (var currentResult = tiered.selectEntries(new ObsFilter.Builder()
                .withResultTime(new TemporalFilter.Builder().withCurrentTime().build()).build())) {
            assertEquals(0, currentResult.count());
        }
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                    "DELETE FROM oscar_obs_overlay WHERE store_key=? AND id_scope=? AND observation_id > ?")) {
            statement.setString(1, TABLE);
            statement.setInt(2, DATABASE_NUM);
            statement.setLong(3, overlayId + 10_000_000L);
            assertEquals(10_000, statement.executeUpdate());
        }

        long deletedId = inserted.keySet().stream().mapToLong(BigId::getIdAsLong).max().orElseThrow();
        restartedCatalog.tombstone(deletedId, generation.id(), "integration deletion", "TEST");
        assertEquals(5, tiered.getNumRecords());
        assertEquals(5, selectAll(tiered));

        // A new reader must reject an object that no longer matches its catalog checksum.
        Path parquet = root.resolve(generation.objectPath());
        byte[] original = Files.readAllBytes(parquet);
        byte[] damaged = original.clone();
        damaged[damaged.length - 1] ^= 1;
        Files.write(parquet, damaged);
        try {
            assertThrows(IllegalStateException.class, () -> new ParquetArchiveReader(root).read(
                    List.of(restartedGeneration), new ObsFilter.Builder().build(), null, null, Long.MAX_VALUE));
        } finally {
            Files.write(parquet, original);
        }
        assertEquals(6, new ParquetArchiveReader(root).read(List.of(restartedGeneration),
                new ObsFilter.Builder().build(), null, null, Long.MAX_VALUE).size());

        // PURGED generations are no longer query-visible. Sparse mutation
        // records may remain for audit, but must never resurrect their rows in
        // get/select/count after a restart or lifecycle transition.
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "UPDATE oscar_archive_generation SET state='PURGED' WHERE id=?")) {
            statement.setObject(1, generation.id());
            assertEquals(1, statement.executeUpdate());
        }

        // A different live generation can have an overlapping coarse ID
        // block when eligible and ineligible streams interleave. It must not
        // inherit mutations owned by the purged generation.
        UUID overlappingJob = UUID.randomUUID();
        UUID overlappingGeneration = UUID.randomUUID();
        try (Connection connection = open()) {
            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO oscar_archive_job(id, store_key, id_scope, profile, window_start, window_end, revision, state) " +
                            "VALUES (?, ?, ?, 'OVERLAP_TEST', ?, ?, 1, 'ACTIVE')")) {
                statement.setObject(1, overlappingJob);
                statement.setString(2, TABLE);
                statement.setInt(3, DATABASE_NUM);
                UtcJdbcTime.setInstant(statement, 4, start);
                UtcJdbcTime.setInstant(statement, 5, start.plus(1, ChronoUnit.HOURS));
                statement.executeUpdate();
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO oscar_archive_generation(id, job_id, store_key, id_scope, profile, window_start, window_end, state, " +
                            "object_path, manifest_path, sha256, row_count, min_id, max_id) " +
                            "VALUES (?, ?, ?, ?, 'OVERLAP_TEST', ?, ?, 'ACTIVE', 'never-opened.parquet', " +
                            "'never-opened.manifest.json', 'none', 0, ?, ?)")) {
                statement.setObject(1, overlappingGeneration);
                statement.setObject(2, overlappingJob);
                statement.setString(3, TABLE);
                statement.setInt(4, DATABASE_NUM);
                UtcJdbcTime.setInstant(statement, 5, start);
                UtcJdbcTime.setInstant(statement, 6, start.plus(1, ChronoUnit.HOURS));
                statement.setLong(7, overlayId - 1);
                statement.setLong(8, overlayId + 1);
                statement.executeUpdate();
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO oscar_archive_id_block(generation_id, min_id, max_id) VALUES (?, ?, ?)")) {
                statement.setObject(1, overlappingGeneration);
                statement.setLong(2, overlayId - 1);
                statement.setLong(3, overlayId + 1);
                statement.executeUpdate();
            }
        }
        assertTrue(catalog.overlay(overlayId, DATABASE_NUM, "OVERLAY").isEmpty());
        assertFalse(catalog.mutatedObservationIds(List.of(overlayId)).contains(overlayId));
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "UPDATE oscar_archive_generation SET state='PURGED' WHERE id=?")) {
            statement.setObject(1, overlappingGeneration);
            assertEquals(1, statement.executeUpdate());
        }
        assertEquals(null, tiered.get(BigId.fromLong(DATABASE_NUM, overlayId)));
        assertEquals(0, selectAll(tiered));
        assertEquals(0, tiered.getNumRecords());
        assertTrue(catalog.overlay(overlayId, DATABASE_NUM, "OVERLAY").isEmpty());
        assertFalse(catalog.isTombstoned(deletedId));
    }

    @Test
    public void tieredRemoveRevalidatesHalfOpenPredicateAgainstRealPostgres() throws Exception {
        TestableTieredStore tiered = (TestableTieredStore) postgisObsStore;
        Instant start = Instant.parse("2026-02-01T00:00:00Z");
        Instant endExclusive = start.plus(10, ChronoUnit.MINUTES);
        BigId stream = addSimpleDataStream(bigId(60), "gammaCounts");
        Map<BigId, org.sensorhub.api.data.IObsData> inserted =
                addSimpleObsWithoutResultTime(stream, BigId.NONE, start, 3, 60_000);
        forceReadBackFromStorage();

        long movedId = inserted.keySet().stream()
                .mapToLong(BigId::getIdAsLong)
                .min()
                .orElseThrow();
        tiered.beforeNextRemoveBatch(() -> {
            try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                    "UPDATE " + TABLE + " SET phenomenontime=? WHERE id=?")) {
                UtcJdbcTime.setInstant(statement, 1, endExclusive);
                statement.setLong(2, movedId);
                assertEquals(1, statement.executeUpdate());
            } catch (Exception error) {
                throw new IllegalStateException(error);
            }
        });

        ObsFilter halfOpen = new ObsFilter.Builder()
                .withPhenomenonTime(new TemporalFilter.Builder()
                        .withRange(Range.closedOpen(start, endExclusive))
                        .build())
                .build();

        // The hook moves one already-selected row exactly onto the open upper
        // endpoint. The final locked predicate check must spare it.
        assertEquals(2, tiered.removeEntries(halfOpen));
        assertEquals(1, tiered.getNumRecords());
        assertEquals(endExclusive,
                tiered.get(BigId.fromLong(DATABASE_NUM, movedId)).getPhenomenonTime());
        assertEquals(0, tiered.removeEntries(halfOpen));
        try (var endpoint = tiered.selectEntries(new ObsFilter.Builder()
                .withPhenomenonTime(new TemporalFilter.Builder()
                        .withSingleValue(endExclusive)
                        .build())
                .build())) {
            assertEquals(1, endpoint.count());
        }
    }

    @Test
    public void tieredRemoveTombstonesArchivedRowsExactlyOnce() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Instant start = Instant.parse("2026-02-02T00:00:00Z");
        BigId stream = addSimpleDataStream(bigId(61), "gammaCounts");
        addSimpleObsWithoutResultTime(stream, BigId.NONE, start, 2, 60_000);
        forceReadBackFromStorage();
        tiered.archiveWindow(start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED").orElseThrow();

        ObsFilter filter = new ObsFilter.Builder()
                .withPhenomenonTime(new TemporalFilter.Builder()
                        .withRange(Range.closedOpen(start, start.plus(1, ChronoUnit.HOURS)))
                        .build())
                .build();
        assertEquals(2, tiered.removeEntries(filter));
        assertEquals(0, tiered.getNumRecords());
        assertEquals(0, selectAll(tiered));
        assertEquals(2, hotRows());
        assertEquals(0, tiered.removeEntries(filter));

        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT count(*) FROM oscar_tombstone WHERE store_key=? AND id_scope=? " +
                        "AND subject_type='OBSERVATION'")) {
            statement.setString(1, TABLE);
            statement.setInt(2, DATABASE_NUM);
            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                assertEquals(2, result.getLong(1));
            }
        }
    }

    @Test
    public void activationSerializesDirectPutIntoAnOverlay() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Instant start = Instant.parse("2026-02-03T00:00:00Z");
        BigId stream = addSimpleDataStream(bigId(62), "gammaCounts");
        BigId key = addSimpleObsWithoutResultTime(
                stream, BigId.NONE, start, 1, 0).keySet().iterator().next();
        forceReadBackFromStorage();
        String immutableBase = hotResultJson(key.getIdAsLong());
        ObsData correction = new ObsData.Builder()
                .withDataStream(stream)
                .withPhenomenonTime(start)
                .withResult(values(501))
                .build();

        installActivationBarrier();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try (Connection barrier = open(); Statement lock = barrier.createStatement()) {
            lock.execute("SELECT pg_advisory_lock(" + ACTIVATION_LOCK_CLASS + "," +
                    ACTIVATION_LOCK_OBJECT + ")");
            Future<java.util.Optional<ArchiveGeneration>> activation = executor.submit(() ->
                    tiered.archiveWindow(start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED"));
            waitForBlockedActivation();
            Future<org.sensorhub.api.data.IObsData> update = executor.submit(() ->
                    tiered.put(key, correction));
            // The activation barrier owns the exclusive store lock; direct put
            // must be a second advisory waiter, not a timing-based race.
            waitForAdvisoryWaiters(2);
            lock.execute("SELECT pg_advisory_unlock(" + ACTIVATION_LOCK_CLASS + "," +
                    ACTIVATION_LOCK_OBJECT + ")");

            ArchiveGeneration generation = activation.get(30, TimeUnit.SECONDS).orElseThrow();
            update.get(30, TimeUnit.SECONDS);
            assertEquals(immutableBase, hotResultJson(key.getIdAsLong()));
            assertEquals(501.0, tiered.get(key).getResult().getDoubleValue(0), 0.0);
            assertEquals(generation.id(), tiered.getArchiveCatalog().overlayWithGeneration(
                    key.getIdAsLong(), DATABASE_NUM, "OVERLAY").orElseThrow().generationId());
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(30, TimeUnit.SECONDS);
            dropActivationBarrier();
        }
    }

    @Test
    public void activationSerializesDirectRemoveIntoATombstone() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Instant start = Instant.parse("2026-02-04T00:00:00Z");
        BigId stream = addSimpleDataStream(bigId(63), "gammaCounts");
        BigId key = addSimpleObsWithoutResultTime(
                stream, BigId.NONE, start, 1, 0).keySet().iterator().next();
        forceReadBackFromStorage();

        installActivationBarrier();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try (Connection barrier = open(); Statement lock = barrier.createStatement()) {
            lock.execute("SELECT pg_advisory_lock(" + ACTIVATION_LOCK_CLASS + "," +
                    ACTIVATION_LOCK_OBJECT + ")");
            Future<java.util.Optional<ArchiveGeneration>> activation = executor.submit(() ->
                    tiered.archiveWindow(start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED"));
            waitForBlockedActivation();
            Future<org.sensorhub.api.data.IObsData> removal = executor.submit(() -> tiered.remove(key));
            waitForAdvisoryWaiters(2);
            lock.execute("SELECT pg_advisory_unlock(" + ACTIVATION_LOCK_CLASS + "," +
                    ACTIVATION_LOCK_OBJECT + ")");

            ArchiveGeneration generation = activation.get(30, TimeUnit.SECONDS).orElseThrow();
            assertTrue(removal.get(30, TimeUnit.SECONDS) != null);
            assertTrue(hotRowExists(key.getIdAsLong()));
            assertTrue(tiered.getArchiveCatalog().isTombstoned(key.getIdAsLong()));
            assertEquals(generation.id(), tombstoneGeneration(key.getIdAsLong()));
            assertTrue(tiered.get(key) == null);
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(30, TimeUnit.SECONDS);
            dropActivationBarrier();
        }
    }

    @Test
    public void filteredColdRemoveSerializesAnOverlappingOverlayPut() throws Exception {
        TestableTieredStore tiered = (TestableTieredStore) postgisObsStore;
        Instant start = Instant.parse("2026-02-05T00:00:00Z");
        BigId stream = addSimpleDataStream(bigId(64), "gammaCounts");
        BigId key = addSimpleObsWithoutResultTime(
                stream, BigId.NONE, start, 1, 0).keySet().iterator().next();
        forceReadBackFromStorage();
        ArchiveGeneration generation = tiered.archiveWindow(
                start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED").orElseThrow();
        tiered.maintainArchiveLifecycle();
        ageGeneration(generation.id());
        archiveConfig.deleteHotAfterArchive = true;
        tiered.maintainArchiveLifecycle();
        assertFalse(hotRowExists(key.getIdAsLong()));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicReference<Future<org.sensorhub.api.data.IObsData>> update = new AtomicReference<>();
        tiered.afterNextRemoveBatchLocked(() -> {
            update.set(executor.submit(() -> tiered.put(key, new ObsData.Builder()
                    .withDataStream(stream)
                    .withPhenomenonTime(start)
                    .withResult(values(601))
                    .build())));
            try {
                waitForAdvisoryWaiters(1);
            } catch (Exception error) {
                throw new IllegalStateException(error);
            }
        });
        try {
            assertEquals(1, tiered.removeEntries(new ObsFilter.Builder()
                    .withInternalIDs(key)
                    .build()));
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> update.get().get(30, TimeUnit.SECONDS));
            assertTrue(failure.getCause() instanceof UnsupportedOperationException);
            assertTrue(tiered.getArchiveCatalog().isTombstoned(key.getIdAsLong()));
            assertTrue(tiered.getArchiveCatalog().overlayWithGeneration(
                    key.getIdAsLong(), DATABASE_NUM, "OVERLAY").isEmpty());
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    @Test
    public void coldFilteredRemoveCompletesWithOneObservationConnection() throws Exception {
        TestableTieredStore tiered = (TestableTieredStore) postgisObsStore;
        Instant start = Instant.parse("2026-02-06T00:00:00Z");
        BigId stream = addSimpleDataStream(bigId(65), "gammaCounts");
        BigId key = addSimpleObsWithoutResultTime(
                stream, BigId.NONE, start, 1, 0).keySet().iterator().next();
        forceReadBackFromStorage();
        ArchiveGeneration generation = tiered.archiveWindow(
                start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED").orElseThrow();
        tiered.maintainArchiveLifecycle();
        ageGeneration(generation.id());
        archiveConfig.deleteHotAfterArchive = true;
        tiered.maintainArchiveLifecycle();
        assertFalse(hotRowExists(key.getIdAsLong()));

        // Force the initial candidate query through all three sources. Before
        // overlay keyset paging, the eager overlay cursor retained the pool's
        // sole connection while base mutation filtering tried to borrow it.
        tiered.put(key, new ObsData.Builder()
                .withDataStream(stream)
                .withPhenomenonTime(start)
                .withResult(values(651))
                .build());
        assertTrue(tiered.getArchiveCatalog().overlayWithGeneration(
                key.getIdAsLong(), DATABASE_NUM, "OVERLAY").isPresent());
        tiered.limitObservationPool(1);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Long> removal = executor.submit(() -> tiered.removeEntries(
                    new ObsFilter.Builder()
                            .withInternalIDs(key)
                            .withDataStreams(stream)
                            .build()));
            assertEquals(Long.valueOf(1), removal.get(5, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    @Test
    public void restartAllocatesAboveFullyColdCatalogMaximum() throws Exception {
        TestableTieredStore tiered = (TestableTieredStore) postgisObsStore;
        Instant start = Instant.parse("2026-02-07T00:00:00Z");
        BigId stream = addSimpleDataStream(bigId(66), "gammaCounts");
        Map<BigId, org.sensorhub.api.data.IObsData> inserted =
                addSimpleObsWithoutResultTime(stream, BigId.NONE, start, 2, 60_000);
        forceReadBackFromStorage();
        long archivedMaximum = inserted.keySet().stream()
                .mapToLong(BigId::getIdAsLong).max().orElseThrow();
        ArchiveGeneration generation = tiered.archiveWindow(
                start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED").orElseThrow();
        tiered.maintainArchiveLifecycle();
        ageGeneration(generation.id());
        archiveConfig.deleteHotAfterArchive = true;
        tiered.maintainArchiveLifecycle();
        assertEquals(0, hotRows());

        tiered.close();
        tiered = new TestableTieredStore(url, DB_NAME, login, password, TABLE,
                DATABASE_NUM, IdProviderType.SEQUENTIAL, archiveConfig);
        postgisObsStore = tiered;
        BigId next = tiered.add(new ObsData.Builder()
                .withDataStream(stream)
                .withPhenomenonTime(start.plus(2, ChronoUnit.HOURS))
                .withResult(values(701))
                .build());
        assertEquals(archivedMaximum + 1, next.getIdAsLong());
        tiered.commit();
        assertEquals(3, tiered.getNumRecords());
        assertEquals(3, selectAll(tiered));
    }

    @Test
    public void coldOpenBoundsPreserveLimitCountAndRemoval() throws Exception {
        TestableTieredStore tiered = (TestableTieredStore) postgisObsStore;
        Instant start = Instant.parse("2026-02-08T00:00:00Z");
        BigId stream = addSimpleDataStream(bigId(67), "gammaCounts");
        Map<BigId, org.sensorhub.api.data.IObsData> inserted =
                addSimpleObsWithoutResultTime(stream, BigId.NONE, start, 3, 60_000);
        forceReadBackFromStorage();
        ArchiveGeneration generation = tiered.archiveWindow(
                start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED").orElseThrow();
        tiered.maintainArchiveLifecycle();
        ageGeneration(generation.id());
        archiveConfig.deleteHotAfterArchive = true;
        tiered.maintainArchiveLifecycle();
        assertEquals(0, hotRows());

        ObsFilter limited = new ObsFilter.Builder()
                .withPhenomenonTime(new TemporalFilter.Builder()
                        .withRange(Range.openClosed(start, start.plus(2, ChronoUnit.MINUTES)))
                        .build())
                .withLimit(1)
                .build();
        try (var selected = tiered.selectEntries(limited)) {
            var rows = selected.toList();
            assertEquals(1, rows.size());
            assertEquals(start.plus(1, ChronoUnit.MINUTES),
                    rows.get(0).getValue().getPhenomenonTime());
        }
        ObsFilter openCount = new ObsFilter.Builder()
                .withPhenomenonTime(new TemporalFilter.Builder()
                        .withRange(Range.open(start, start.plus(2, ChronoUnit.MINUTES)))
                        .build())
                .build();
        assertEquals(1, tiered.countMatchingEntries(openCount));
        assertEquals(1, tiered.removeEntries(limited));

        BigId first = inserted.entrySet().stream()
                .filter(entry -> entry.getValue().getPhenomenonTime().equals(start))
                .map(Map.Entry::getKey).findFirst().orElseThrow();
        BigId middle = inserted.entrySet().stream()
                .filter(entry -> entry.getValue().getPhenomenonTime().equals(
                        start.plus(1, ChronoUnit.MINUTES)))
                .map(Map.Entry::getKey).findFirst().orElseThrow();
        assertTrue(tiered.get(first) != null);
        assertTrue(tiered.get(middle) == null);
    }

    @Test
    public void lateRowsCreateANewRevisionOfTheAnchoredUtcHour() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Instant firstTime = Instant.parse("2026-03-04T12:34:56.123456789Z");
        Instant windowStart = OscarTieredObsSystemDatabase.utcHourWindowStart(firstTime);
        Instant windowEnd = windowStart.plus(1, ChronoUnit.HOURS);
        BigId stream = addSimpleDataStream(bigId(20), "gammaCounts");

        addSimpleObsWithoutResultTime(stream, BigId.NONE, firstTime, 1, 0);
        forceReadBackFromStorage();
        assertEquals(UtcJdbcTime.toDatabasePrecision(firstTime),
                tiered.nextArchiveWindowStart(windowEnd, "RAW_GATED").orElseThrow());
        ArchiveGeneration first = tiered.archiveWindow(windowStart, windowEnd, "RAW_GATED").orElseThrow();
        assertEquals(1, first.rowCount());

        Instant lateTime = Instant.parse("2026-03-04T12:45:00.987654321Z");
        addSimpleObsWithoutResultTime(stream, BigId.NONE, lateTime, 1, 0);
        forceReadBackFromStorage();
        assertEquals(UtcJdbcTime.toDatabasePrecision(lateTime),
                tiered.nextArchiveWindowStart(windowEnd, "RAW_GATED").orElseThrow());
        ArchiveGeneration revision = tiered.archiveWindow(windowStart, windowEnd, "RAW_GATED").orElseThrow();
        assertEquals(1, revision.rowCount());
        assertFalse(first.id().equals(revision.id()));

        List<Integer> revisions = new ArrayList<>();
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT revision FROM oscar_archive_job WHERE profile='RAW_GATED' " +
                        "AND store_key=? AND id_scope=? AND window_start=? AND window_end=? ORDER BY revision")) {
            statement.setString(1, TABLE);
            statement.setInt(2, DATABASE_NUM);
            UtcJdbcTime.setInstant(statement, 3, windowStart);
            UtcJdbcTime.setInstant(statement, 4, windowEnd);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next())
                    revisions.add(result.getInt(1));
            }
        }
        assertEquals(List.of(1, 2), revisions);
        assertEquals(2, tiered.getArchiveCatalog().candidates(windowStart, windowEnd, null).size());
        assertEquals(2, tiered.getNumRecords());
        try (var rows = tiered.selectEntries(new ObsFilter.Builder().build())) {
            var selected = rows.map(entry -> entry.getKey().getIdAsLong() + "@" +
                    entry.getValue().getPhenomenonTime()).toList();
            assertEquals(selected.toString(), 2, selected.size());
        }
    }

    @Test
    public void failedActivationRemovesPublishedAttemptObjects() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Path root = Path.of(archiveConfig.rootDirectory);
        Instant start = Instant.parse("2026-05-06T12:00:00Z");
        BigId stream = addSimpleDataStream(bigId(30), "gammaCounts");
        addSimpleObsWithoutResultTime(stream, BigId.NONE, start, 2, 60_000);
        forceReadBackFromStorage();

        try (Connection connection = open(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE OR REPLACE FUNCTION codex_storage_it_block_archive_mark() " +
                    "RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN " +
                    "IF NEW.archivegenerationid IS NOT NULL THEN RETURN NULL; END IF; RETURN NEW; END $$");
            statement.execute("CREATE TRIGGER codex_storage_it_block_archive_mark " +
                    "BEFORE UPDATE ON " + TABLE + " FOR EACH ROW " +
                    "EXECUTE FUNCTION codex_storage_it_block_archive_mark()");
        }
        try {
            assertThrows(IllegalStateException.class, () -> tiered.archiveWindow(
                    start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED"));
            try (var paths = Files.walk(root)) {
                assertEquals(0, paths.filter(Files::isRegularFile).count());
            }
            try (Connection connection = open(); Statement statement = connection.createStatement();
                 ResultSet result = statement.executeQuery(
                         "SELECT count(*) FROM oscar_archive_job WHERE state='FAILED'")) {
                assertTrue(result.next());
                assertEquals(1, result.getLong(1));
            }
        } finally {
            try (Connection connection = open(); Statement statement = connection.createStatement()) {
                statement.execute("DROP TRIGGER IF EXISTS codex_storage_it_block_archive_mark ON " + TABLE);
                statement.execute("DROP FUNCTION IF EXISTS codex_storage_it_block_archive_mark()");
            }
        }
    }

    @Test
    public void concurrentResultCorrectionRejectsStaleExportAndRetriesExactly() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Path root = Path.of(archiveConfig.rootDirectory);
        Instant start = Instant.parse("2026-05-07T12:00:00Z");
        BigId stream = addSimpleDataStream(bigId(31), "gammaCounts");
        Map<BigId, org.sensorhub.api.data.IObsData> inserted =
                addSimpleObsWithoutResultTime(stream, BigId.NONE, start, 2, 60_000);
        forceReadBackFromStorage();
        long correctedId = inserted.keySet().stream().mapToLong(BigId::getIdAsLong).min().orElseThrow();
        String correctedJson = correctedResultJson(hotResultJson(correctedId), 10_000.0);

        installActivationBarrier();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Connection barrier = open(); Statement lock = barrier.createStatement()) {
            lock.execute("SELECT pg_advisory_lock(" + ACTIVATION_LOCK_CLASS + "," +
                    ACTIVATION_LOCK_OBJECT + ")");
            Future<?> export = executor.submit(() -> tiered.archiveWindow(
                    start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED"));
            waitForBlockedActivation();

            try (Connection connection = open(); PreparedStatement update = connection.prepareStatement(
                    "UPDATE " + TABLE + " SET result=?::jsonb WHERE id=?")) {
                update.setString(1, correctedJson);
                update.setLong(2, correctedId);
                assertEquals(1, update.executeUpdate());
            }
            lock.execute("SELECT pg_advisory_unlock(" + ACTIVATION_LOCK_CLASS + "," +
                    ACTIVATION_LOCK_OBJECT + ")");

            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> export.get(30, TimeUnit.SECONDS));
            assertTrue(failure.getCause() instanceof IllegalStateException);
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(30, TimeUnit.SECONDS);
            dropActivationBarrier();
        }

        assertEquals(JsonParser.parseString(correctedJson),
                JsonParser.parseString(hotResultJson(correctedId)));
        assertEquals(0, archivedHotRows());
        assertTrue(tiered.getArchiveCatalog().candidates(
                start, start.plus(1, ChronoUnit.HOURS), null).isEmpty());
        assertEquals(0, regularArchiveFiles(root));

        // FAILED reuses the same job/generation identity, but regenerates its
        // deterministic files from the now-current exact row values.
        ArchiveGeneration recovered = tiered.archiveWindow(
                start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED").orElseThrow();
        assertEquals(2, recovered.rowCount());
        assertEquals(2, archivedHotRows());
        assertEquals(JsonParser.parseString(correctedJson),
                JsonParser.parseString(hotResultJson(correctedId)));
        assertEquals(2, selectAll(tiered));
    }

    @Test
    public void concurrentDeleteAndInsertCannotSubstituteForExportedIdentity() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        TestableTieredStore writable = (TestableTieredStore) postgisObsStore;
        Instant start = Instant.parse("2026-05-08T12:00:00Z");
        BigId stream = addSimpleDataStream(bigId(32), "gammaCounts");
        var schema = tiered.getDataStreams().get(new DataStreamKey(stream));
        String firstJson = SerializerUtils.writeDataBlockToJson(
                schema.getRecordStructure(), schema.getRecordEncoding(), values(1));
        String secondJson = SerializerUtils.writeDataBlockToJson(
                schema.getRecordStructure(), schema.getRecordEncoding(), values(101));
        long firstId = 8_000_000L;
        long removedId = firstId + 2;
        long replacementId = firstId + 1;
        try (Connection connection = open()) {
            connection.setAutoCommit(false);
            writable.insertVerified(connection, firstId, stream.getIdAsLong(), null,
                    start, null, firstJson);
            writable.insertVerified(connection, removedId, stream.getIdAsLong(), null,
                    start.plusSeconds(2), null, secondJson);
            connection.commit();
        }

        installActivationBarrier();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Connection barrier = open(); Statement lock = barrier.createStatement()) {
            lock.execute("SELECT pg_advisory_lock(" + ACTIVATION_LOCK_CLASS + "," +
                    ACTIVATION_LOCK_OBJECT + ")");
            Future<?> export = executor.submit(() -> tiered.archiveWindow(
                    start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED"));
            waitForBlockedActivation();

            try (Connection connection = open()) {
                connection.setAutoCommit(false);
                try (PreparedStatement delete = connection.prepareStatement(
                        "DELETE FROM " + TABLE + " WHERE id=?")) {
                    delete.setLong(1, removedId);
                    assertEquals(1, delete.executeUpdate());
                }
                writable.insertVerified(connection, replacementId, stream.getIdAsLong(), null,
                        start.plusSeconds(1), null, secondJson);
                connection.commit();
            }
            lock.execute("SELECT pg_advisory_unlock(" + ACTIVATION_LOCK_CLASS + "," +
                    ACTIVATION_LOCK_OBJECT + ")");

            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> export.get(30, TimeUnit.SECONDS));
            assertTrue(failure.getCause() instanceof IllegalStateException);
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(30, TimeUnit.SECONDS);
            dropActivationBarrier();
        }

        // The old range/count update saw two rows before and two rows after,
        // so it could silently assign the inserted replacement to a Parquet
        // generation that did not contain it. Exact activation rolls back.
        assertEquals(0, archivedHotRows());
        assertFalse(hotRowExists(removedId));
        assertTrue(hotRowExists(replacementId));
        assertTrue(tiered.getArchiveCatalog().candidates(
                start, start.plus(1, ChronoUnit.HOURS), null).isEmpty());

        ArchiveGeneration recovered = tiered.archiveWindow(
                start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED").orElseThrow();
        assertEquals(2, recovered.rowCount());
        assertEquals(firstId, recovered.minimumId());
        assertEquals(replacementId, recovered.maximumId());
        assertEquals(2, archivedHotRows());
        assertEquals(2, selectAll(tiered));
    }

    @Test
    public void rejectedNaturalKeyDoesNotPoisonTheBatchQueue() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        BigId stream = addSimpleDataStream(bigId(40), "gammaCounts");
        Instant time = Instant.parse("2026-06-01T00:00:00.000002500Z");
        DataBlockDouble initial = values(1);
        DataBlockDouble correction = values(101);

        BigId stableId = tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(time).withResult(initial).build());
        tiered.commit();

        // Reopen to prove the batch store initializes its per-datastream
        // high-water mark from PostgreSQL rather than relying on process-local
        // state to return the stable logical ID.
        tiered.close();
        tiered = new TestableTieredStore(url, DB_NAME, login, password, TABLE,
                DATABASE_NUM, IdProviderType.SEQUENTIAL, archiveConfig);
        postgisObsStore = tiered;

        // Exact replay is idempotent. Several corrected samples at the same
        // OSH identity in one timed batch coalesce to the newest result, while
        // an unrelated row commits normally.
        BigId replayId = tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(time).withResult(initial).build());
        BigId firstCorrectionId = tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(time).withResult(values(51)).build());
        BigId finalCorrectionId = tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(time).withResult(correction).build());
        assertEquals(stableId, replayId);
        assertEquals(stableId, firstCorrectionId);
        assertEquals(stableId, finalCorrectionId);
        tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(time.plusSeconds(1)).withResult(initial).build());
        tiered.commit();
        assertEquals(2, tiered.getNumRecords());
        assertEquals(101.0, tiered.get(stableId).getResult().getDoubleValue(0), 0.0);

        // A later exact replay neither changes the row nor poisons the next
        // timed commit, proving restart-style duplicate delivery is harmless.
        tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(time).withResult(correction).build());
        tiered.commit();
        tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(time.plusSeconds(2)).withResult(initial).build());
        tiered.commit();
        assertEquals(3, tiered.getNumRecords());
    }

    @Test
    public void naturalKeyCorrectionOfArchivedRowUsesOverlayThroughHotDeletion() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        ArchiveCatalog catalog = tiered.getArchiveCatalog();
        BigId stream = addSimpleDataStream(bigId(41), "gammaCounts");
        Instant time = Instant.parse("2026-06-01T01:00:00.000002500Z");
        Instant baseOnlyTime = time.plus(1, ChronoUnit.MINUTES);
        DataBlockDouble initial = values(1);

        BigId stableId = tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(time).withResult(initial).build());
        BigId baseOnlyStableId = tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(baseOnlyTime).withResult(values(301)).build());
        tiered.commit();
        String immutableBase = hotResultJson(stableId.getIdAsLong());

        ArchiveGeneration generation = tiered.archiveWindow(
                time.truncatedTo(ChronoUnit.HOURS),
                time.truncatedTo(ChronoUnit.HOURS).plus(1, ChronoUnit.HOURS),
                "RAW_GATED").orElseThrow();
        tiered.maintainArchiveLifecycle();
        assertTrue(oneGeneration(catalog).parityVerified());

        // The batch coalescer must retain the original logical ID and apply
        // the final submitted value. Because the base now belongs to an
        // immutable archive generation, the correction belongs in an overlay
        // rather than in the hot parity source.
        BigId firstCorrection = tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(time).withResult(values(51)).build());
        BigId finalCorrection = tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(time).withResult(values(101)).build());
        assertEquals(stableId, firstCorrection);
        assertEquals(stableId, finalCorrection);
        tiered.commit();

        assertEquals(immutableBase, hotResultJson(stableId.getIdAsLong()));
        assertEquals(101.0, tiered.get(stableId).getResult().getDoubleValue(0), 0.0);
        ArchiveCatalog.ArchivedOverlay overlay = catalog.overlayWithGeneration(
                stableId.getIdAsLong(), DATABASE_NUM, "OVERLAY").orElseThrow();
        assertEquals(generation.id(), overlay.generationId());
        assertEquals(JsonParser.parseString(SerializerUtils.writeDataBlockToJson(
                        tiered.getDataStreams().get(new DataStreamKey(stream)).getRecordStructure(),
                        tiered.getDataStreams().get(new DataStreamKey(stream)).getRecordEncoding(),
                        values(101))),
                JsonParser.parseString(overlay.observation().resultJson()));

        // Once the verified grace copy is deleted, normal point and filtered
        // reads must still expose exactly the corrected logical observation.
        ageGeneration(generation.id());
        archiveConfig.deleteHotAfterArchive = true;
        tiered.maintainArchiveLifecycle();
        assertFalse(hotRowExists(stableId.getIdAsLong()));
        assertFalse(hotRowExists(baseOnlyStableId.getIdAsLong()));
        assertEquals("SEALED", oneGeneration(catalog).state());
        assertEquals(101.0, tiered.get(stableId).getResult().getDoubleValue(0), 0.0);
        try (var selected = tiered.selectEntries(new ObsFilter.Builder()
                .withInternalIDs(stableId).build())) {
            var entries = selected.toList();
            assertEquals(1, entries.size());
            assertEquals(stableId, entries.get(0).getKey());
            assertEquals(101.0, entries.get(0).getValue().getResult().getDoubleValue(0), 0.0);
        }

        // Reopen after every hot row is gone. The tiered high-water mark must
        // still recognize this as an old identity, find its stable ID in cold
        // storage, and avoid inserting a duplicate PostgreSQL row.
        tiered.close();
        tiered = new TestableTieredStore(url, DB_NAME, login, password, TABLE,
                DATABASE_NUM, IdProviderType.SEQUENTIAL, archiveConfig);
        postgisObsStore = tiered;
        ((TestableTieredStore) tiered).limitObservationPool(1);
        BigId coldReplay = tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(time).withResult(values(101)).build());
        BigId coldBaseReplay = tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(baseOnlyTime).withResult(values(301)).build());
        assertEquals(stableId, coldReplay);
        assertEquals(baseOnlyStableId, coldBaseReplay);
        tiered.commit();
        assertEquals(0, hotRows());
        assertEquals(2, tiered.getNumRecords());
        assertEquals(2, selectAll(tiered));
        assertTrue(tiered.getArchiveCatalog().overlayWithGeneration(
                baseOnlyStableId.getIdAsLong(), DATABASE_NUM, "OVERLAY").isEmpty());
        assertEquals(301.0, tiered.get(baseOnlyStableId).getResult().getDoubleValue(0), 0.0);

        BigId coldFirstCorrection = tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(time).withResult(values(151)).build());
        BigId coldFinalCorrection = tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(time).withResult(values(201)).build());
        assertEquals(stableId, coldFirstCorrection);
        assertEquals(stableId, coldFinalCorrection);
        // Force reconcileProtectedObservations to resolve the schema through
        // the datastream store's independent pool while the observation pool's
        // sole connection is held by the batch transaction.
        ((PostgisDataStreamStoreImpl) tiered.getDataStreams()).clearCache();
        tiered.commit();
        assertEquals(0, hotRows());
        assertEquals(2, tiered.getNumRecords());
        assertEquals(2, selectAll(tiered));
        assertEquals(201.0, tiered.get(stableId).getResult().getDoubleValue(0), 0.0);
    }

    private static DataBlockDouble values(double first) {
        DataBlockDouble values = new DataBlockDouble(5);
        for (int i = 0; i < 5; i++) values.setDoubleValue(i, first + i);
        return values;
    }

    @Test
    public void existingObservationTableReceivesNullSafeNaturalKeyIndex() throws Exception {
        try (Connection connection = open(); Statement statement = connection.createStatement()) {
            statement.execute("DROP INDEX " + TABLE + "_unique_constraint_v2");
        }
        TestableTieredStore reopened = new TestableTieredStore(
                url, DB_NAME, login, password, TABLE, DATABASE_NUM,
                IdProviderType.SEQUENTIAL, archiveConfig);
        try (Connection connection = open(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(
                     "SELECT indisvalid FROM pg_index WHERE indexrelid='" +
                             TABLE + "_unique_constraint_v2'::regclass")) {
            assertTrue(result.next());
            assertTrue(result.getBoolean(1));
        } finally {
            reopened.close();
        }
    }

    @Test
    public void closeSurfacesFinalFlushFailureAndRetainsPendingObservation() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        BigId stream = addSimpleDataStream(bigId(45), "gammaCounts");
        DataBlockDouble values = new DataBlockDouble(5);
        for (int i = 0; i < 5; i++) values.setDoubleValue(i, i + 10);
        tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(Instant.parse("2026-06-02T00:00:00Z"))
                .withResult(values).build());

        String unavailable = TABLE + "_temporarily_unavailable";
        try (Connection connection = open(); Statement statement = connection.createStatement()) {
            statement.execute("ALTER TABLE " + TABLE + " RENAME TO " + unavailable);
        }
        try {
            assertThrows(IllegalStateException.class, tiered::close);
        } finally {
            try (Connection connection = open(); Statement statement = connection.createStatement()) {
                statement.execute("ALTER TABLE " + unavailable + " RENAME TO " + TABLE);
            }
        }
        tiered.commit();
        assertEquals(1, tiered.getNumRecords());
    }

    @Test
    public void archivePayloadByteCapFailsBeforePublication() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Instant start = Instant.parse("2026-06-03T00:00:00Z");
        BigId stream = addSimpleDataStream(bigId(46), "gammaCounts");
        addSimpleObsWithoutResultTime(stream, BigId.NONE, start, 2, 1_000);
        forceReadBackFromStorage();
        archiveConfig.maximumUncompressedBytesPerGeneration = 1;

        IllegalStateException planningFailure = assertThrows(IllegalStateException.class, () ->
                tiered.selectArchiveWindowEnd(start, start.plusSeconds(1)));
        assertTrue(planningFailure.getMessage().contains("minimum one-second window"));
        assertThrows(IllegalStateException.class, () -> tiered.archiveWindow(
                start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED"));
        assertEquals(2, hotRows());
        assertTrue(tiered.getArchiveCatalog().candidates(
                start, start.plus(1, ChronoUnit.HOURS), null).isEmpty());
        try (var paths = Files.walk(Path.of(archiveConfig.rootDirectory))) {
            assertEquals(0, paths.filter(Files::isRegularFile).count());
        }
    }

    @Test
    public void byteHeavyArchiveWindowSplitsAndActivatesBothPrefixes() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Instant start = Instant.parse("2026-06-03T12:00:00Z");
        Instant midpoint = start.plus(30, ChronoUnit.MINUTES);
        Instant end = start.plus(1, ChronoUnit.HOURS);
        BigId stream = addSimpleDataStream(bigId(49), "gammaCounts");
        addSimpleObsWithoutResultTime(stream, BigId.NONE, start, 4, 15 * 60_000);
        forceReadBackFromStorage();

        archiveConfig.maximumRowsPerGeneration = 100;
        OscarTieredObsStore.ArchiveCandidateFootprint full =
                tiered.measureArchiveCandidates(start, end, 100);
        OscarTieredObsStore.ArchiveCandidateFootprint first =
                tiered.measureArchiveCandidates(start, midpoint, 100);
        OscarTieredObsStore.ArchiveCandidateFootprint second =
                tiered.measureArchiveCandidates(midpoint, end, 100);
        assertEquals(4, full.rowCount());
        assertEquals(2, first.rowCount());
        assertEquals(2, second.rowCount());
        long exporterEstimate = 0;
        try (Connection connection = open(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(
                     "SELECT result::text FROM " + TABLE + " ORDER BY id")) {
            while (result.next())
                exporterEstimate = Math.addExact(exporterEstimate,
                        ArchiveExporter.estimateUncompressedRowBytes(result.getString(1)));
        }
        assertEquals(exporterEstimate, full.estimatedUncompressedBytes());
        long byteCap = Math.max(first.estimatedUncompressedBytes(),
                second.estimatedUncompressedBytes());
        assertTrue(full.estimatedUncompressedBytes() > byteCap);
        archiveConfig.maximumUncompressedBytesPerGeneration = byteCap;

        assertEquals(midpoint, tiered.selectArchiveWindowEnd(start, end));
        ArchiveGeneration firstGeneration = tiered.archiveWindow(
                start, midpoint, "RAW_GATED").orElseThrow();
        assertEquals(2, firstGeneration.rowCount());
        assertEquals(midpoint, tiered.nextArchiveWindowStart(end, "RAW_GATED").orElseThrow());
        assertEquals(end, tiered.selectArchiveWindowEnd(midpoint, end));
        ArchiveGeneration secondGeneration = tiered.archiveWindow(
                midpoint, end, "RAW_GATED").orElseThrow();
        assertEquals(2, secondGeneration.rowCount());
        assertEquals(2, tiered.getArchiveCatalog().candidates(start, end, null).size());

        try (Connection connection = open(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("SELECT count(*), " +
                     "count(DISTINCT archivegenerationid) FROM " + TABLE +
                     " WHERE archivegenerationid IS NOT NULL")) {
            assertTrue(result.next());
            assertEquals(4, result.getLong(1));
            assertEquals(2, result.getLong(2));
        }
    }

    @Test
    public void sparseByteHeavyHourDoesNotSelectAnEmptyPrefix() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Instant start = Instant.parse("2026-06-03T18:00:00Z");
        Instant firstObservation = start.plus(45, ChronoUnit.MINUTES);
        Instant end = start.plus(1, ChronoUnit.HOURS);
        BigId stream = addSimpleDataStream(bigId(50), "gammaCounts");
        addSimpleObsWithoutResultTime(stream, BigId.NONE, firstObservation, 2, 10_000);
        forceReadBackFromStorage();

        archiveConfig.maximumRowsPerGeneration = 100;
        OscarTieredObsStore.ArchiveCandidateFootprint firstRow =
                tiered.measureArchiveCandidates(start, firstObservation.plusSeconds(1), 100);
        archiveConfig.maximumUncompressedBytesPerGeneration =
                firstRow.estimatedUncompressedBytes();

        Instant selectedEnd = tiered.selectArchiveWindowEnd(start, end);
        assertEquals(firstObservation.plusSeconds(1), selectedEnd);
        ArchiveGeneration generation = tiered.archiveWindow(
                start, selectedEnd, "RAW_GATED").orElseThrow();
        assertEquals(1, generation.rowCount());
        assertEquals(firstObservation.plusSeconds(10),
                tiered.nextArchiveWindowStart(end, "RAW_GATED").orElseThrow());
    }

    @Test
    public void staleJobRetryReplacesItsDeterministicCrashObjects() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        ArchiveCatalog catalog = tiered.getArchiveCatalog();
        Instant start = Instant.parse("2026-06-04T00:00:00Z");
        Instant end = start.plus(1, ChronoUnit.HOURS);
        BigId stream = addSimpleDataStream(bigId(47), "gammaCounts");
        addSimpleObsWithoutResultTime(stream, BigId.NONE, start, 1, 0);
        forceReadBackFromStorage();

        UUID jobId = catalog.claimJob("RAW_GATED", start, end).orElseThrow();
        UUID expectedGeneration = UUID.nameUUIDFromBytes(
                ("oscar-archive-generation-v1:" + jobId).getBytes(StandardCharsets.UTF_8));
        Path directory = Path.of(archiveConfig.rootDirectory)
                .resolve("datasets/observations/date=2026-06-04/profile=RAW_GATED");
        Files.createDirectories(directory);
        for (String suffix : List.of(".parquet", ".schemas.json.gz", ".manifest.json"))
            Files.writeString(directory.resolve("generation-" + expectedGeneration + suffix), "crash-leftover");
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "UPDATE oscar_archive_job SET updated_at=now()-interval '3 hours' " +
                        "WHERE id=? AND store_key=? AND id_scope=?")) {
            statement.setObject(1, jobId);
            statement.setString(2, TABLE);
            statement.setInt(3, DATABASE_NUM);
            assertEquals(1, statement.executeUpdate());
        }

        ArchiveGeneration recovered = tiered.archiveWindow(start, end, "RAW_GATED").orElseThrow();
        assertEquals(expectedGeneration, recovered.id());
        assertTrue(Files.size(Path.of(archiveConfig.rootDirectory)
                .resolve(recovered.objectPath())) > "crash-leftover".length());
        assertEquals(1, catalog.candidates(start, end, null).size());
    }

    @Test
    public void archivePublicationRejectsSymlinkedPartitionPath() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Instant start = Instant.parse("2026-06-05T00:00:00Z");
        BigId stream = addSimpleDataStream(bigId(48), "gammaCounts");
        addSimpleObsWithoutResultTime(stream, BigId.NONE, start, 1, 0);
        forceReadBackFromStorage();
        Path root = Path.of(archiveConfig.rootDirectory);
        Path outside = Files.createTempDirectory("codex-oscar-archive-outside-");
        try {
            try {
                Files.createSymbolicLink(root.resolve("datasets"), outside);
            } catch (UnsupportedOperationException | java.nio.file.FileSystemException unsupported) {
                return;
            }
            assertThrows(IllegalStateException.class, () -> tiered.archiveWindow(
                    start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED"));
            try (var paths = Files.list(outside)) {
                assertEquals(0, paths.count());
            }
        } finally {
            Files.deleteIfExists(root.resolve("datasets"));
            Files.deleteIfExists(outside);
        }
    }

    @Test
    public void archiveCatalogNamespacesIndependentObservationStores() throws Exception {
        String secondTable = TABLE + "_second";
        int secondScope = DATABASE_NUM + 100;
        TestableTieredStore second = new TestableTieredStore(
                url, DB_NAME, login, password, secondTable, secondScope,
                IdProviderType.SEQUENTIAL, archiveConfig);
        try {
            ArchiveCatalog firstCatalog = ((OscarTieredObsStore) postgisObsStore).getArchiveCatalog();
            ArchiveCatalog secondCatalog = second.getArchiveCatalog();
            Instant start = Instant.parse("2026-07-01T00:00:00Z");
            Instant end = start.plus(1, ChronoUnit.HOURS);
            UUID firstJob = firstCatalog.claimJob("RAW_GATED", start, end).orElseThrow();
            UUID secondJob = secondCatalog.claimJob("RAW_GATED", start, end).orElseThrow();
            assertFalse(firstJob.equals(secondJob));
            UUID firstGeneration = insertManualGeneration(
                    firstJob, TABLE, DATABASE_NUM, start, end, "first.parquet");
            UUID secondGeneration = insertManualGeneration(
                    secondJob, secondTable, secondScope, start, end, "second.parquet");
            try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                    "UPDATE oscar_archive_generation SET max_id=? WHERE id=?")) {
                statement.setLong(1, 9_000_000L);
                statement.setObject(2, secondGeneration);
                assertEquals(1, statement.executeUpdate());
            }
            assertEquals(Long.valueOf(42), firstCatalog.maximumObservationId().orElseThrow());
            assertEquals(Long.valueOf(9_000_000L), secondCatalog.maximumObservationId().orElseThrow());

            ArchivedObservation firstOverlay = new ArchivedObservation(
                    DATABASE_NUM, 42, DATABASE_NUM, 10, null, null,
                    start, start, "{\"value\":1}", "OVERLAY");
            ArchivedObservation secondOverlay = new ArchivedObservation(
                    secondScope, 42, secondScope, 10, null, null,
                    start, start, "{\"value\":2}", "OVERLAY");
            assertThrows(IllegalArgumentException.class,
                    () -> firstCatalog.upsertOverlay(secondGeneration, firstOverlay, "{}"));
            assertThrows(IllegalArgumentException.class,
                    () -> firstCatalog.tombstone(42, secondGeneration, "wrong namespace", "TEST"));
            assertThrows(IllegalStateException.class,
                    () -> firstCatalog.recordVerification(secondGeneration,
                            "HOT_COLD_PARITY", true, 1, 1, "wrong namespace"));
            firstCatalog.upsertOverlay(firstGeneration, firstOverlay, "{}");
            secondCatalog.upsertOverlay(secondGeneration, secondOverlay, "{}");
            assertEquals(com.google.gson.JsonParser.parseString("{\"value\":1}"),
                    com.google.gson.JsonParser.parseString(firstCatalog.overlayWithGeneration(
                            42, DATABASE_NUM, "OVERLAY").orElseThrow().observation().resultJson()));
            assertEquals(com.google.gson.JsonParser.parseString("{\"value\":2}"),
                    com.google.gson.JsonParser.parseString(secondCatalog.overlayWithGeneration(
                            42, secondScope, "OVERLAY").orElseThrow().observation().resultJson()));

            firstCatalog.tombstone(42, firstGeneration, "first only", "TEST");
            assertTrue(firstCatalog.isTombstoned(42));
            assertFalse(secondCatalog.isTombstoned(42));
            assertEquals(1, firstCatalog.candidates(start, end, null).size());
            assertEquals(1, secondCatalog.candidates(start, end, null).size());
            assertEquals(0, firstCatalog.countTieredRecords(TABLE));
            assertEquals(1, secondCatalog.countTieredRecords(secondTable));
        } finally {
            second.drop();
            ((PostgisDataStreamStoreImpl) second.getDataStreams()).drop();
            ((PostgisDataStreamStoreImpl) second.getDataStreams()).clearCache();
        }
    }

    private UUID insertManualGeneration(UUID jobId, String storeKey, int scope,
                                        Instant start, Instant end, String objectPath) throws Exception {
        UUID generationId = UUID.randomUUID();
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO oscar_archive_generation(id,job_id,store_key,id_scope,profile,window_start,window_end,state," +
                        "object_path,manifest_path,sha256,row_count,min_id,max_id) " +
                        "VALUES (?,?,?,?,?,?,?,'ACTIVE',?,?,'none',1,42,42)")) {
            statement.setObject(1, generationId);
            statement.setObject(2, jobId);
            statement.setString(3, storeKey);
            statement.setInt(4, scope);
            statement.setString(5, "RAW_GATED");
            UtcJdbcTime.setInstant(statement, 6, start);
            UtcJdbcTime.setInstant(statement, 7, end);
            statement.setString(8, objectPath);
            statement.setString(9, objectPath + ".manifest.json");
            statement.executeUpdate();
        }
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO oscar_archive_id_block(generation_id,min_id,max_id) VALUES (?,42,42)")) {
            statement.setObject(1, generationId);
            statement.executeUpdate();
        }
        return generationId;
    }

    private static long selectAll(OscarTieredObsStore store) {
        try (var rows = store.selectEntries(new ObsFilter.Builder().build())) {
            return rows.count();
        }
    }

    private void assertStatementTriggerRange(BigId stream, Instant minimum, Instant maximum) throws Exception {
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT min_phenomenon_time, max_phenomenon_time FROM " + TABLE + "_datastreams WHERE id=?")) {
            statement.setLong(1, stream.getIdAsLong());
            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                assertEquals(minimum, result.getObject(1, LocalDateTime.class).toInstant(ZoneOffset.UTC));
                assertEquals(maximum, result.getObject(2, LocalDateTime.class).toInstant(ZoneOffset.UTC));
            }
        }
    }

    private void assertIdempotentInsertContract(long existingId) throws Exception {
        TestableTieredStore store = (TestableTieredStore) postgisObsStore;
        long streamId;
        Long foiId;
        Instant phenomenon;
        Instant resultTime;
        String resultJson;
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT datastreamid, foiid, phenomenontime, resulttime, result::text " +
                        "FROM " + TABLE + " WHERE id=?")) {
            statement.setLong(1, existingId);
            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                streamId = result.getLong(1);
                foiId = result.getObject(2, Long.class);
                phenomenon = UtcJdbcTime.getInstant(result, 3);
                resultTime = UtcJdbcTime.getInstant(result, 4);
                resultJson = result.getString(5);
            }
        }

        try (Connection connection = open()) {
            connection.setAutoCommit(false);
            String correctedJson = correctedResultJson(resultJson, 1000.0);
            // An exact primary-key replay with identical content is idempotent.
            store.insertVerified(connection, existingId, streamId, foiId,
                    phenomenon, resultTime, resultJson);
            connection.commit();
            // A corrected result at the same OSH observation identity uses
            // last-write-wins while retaining the stable stored ID.
            store.insertVerified(connection, existingId, streamId, foiId,
                    phenomenon, resultTime, correctedJson);
            connection.commit();

            // Establish a row for different-requested-ID replay/correction.
            long firstNaturalId = existingId + 1_000_000;
            Instant naturalPhenomenon = Instant.parse("2030-01-01T00:00:00Z");
            // Null result time is the common OSCAR shape and was not protected
            // by the former PostgreSQL unique-index definition.
            Instant naturalResult = null;
            store.insertVerified(connection, firstNaturalId, streamId, null,
                    naturalPhenomenon, naturalResult, resultJson);
            connection.commit();
            store.insertVerified(connection, firstNaturalId + 1,
                    streamId, null, naturalPhenomenon, naturalResult, resultJson);
            connection.commit();
            store.insertVerified(connection, firstNaturalId + 2,
                    streamId, null, naturalPhenomenon, naturalResult, correctedJson);
            connection.commit();
            try (PreparedStatement selected = connection.prepareStatement(
                    "SELECT id, result::text FROM " + TABLE + " WHERE datastreamid=? " +
                            "AND foiid IS NULL AND phenomenontime=?::timestamp AND resulttime IS NULL")) {
                selected.setLong(1, streamId);
                UtcJdbcTime.setInstant(selected, 2, naturalPhenomenon);
                try (ResultSet actual = selected.executeQuery()) {
                    assertTrue(actual.next());
                    assertEquals(firstNaturalId, actual.getLong(1));
                    assertEquals(JsonParser.parseString(correctedJson),
                            JsonParser.parseString(actual.getString(2)));
                    assertFalse(actual.next());
                }
            }

            // Reusing a primary key for a genuinely different observation is
            // still an integrity error; last-write-wins is scoped only to the
            // natural observation identity.
            assertThrows(SQLException.class, () -> store.insertVerified(connection, firstNaturalId,
                    streamId, null, naturalPhenomenon.plusSeconds(1), naturalResult, resultJson));
            connection.rollback();

            // pgjdbc rounds an exact 500-nanosecond tie upward. Conflict
            // verification must normalize identically or this valid insert
            // wedges the batch retry queue.
            long tieId = firstNaturalId + 3;
            Instant tie = Instant.parse("2031-01-01T00:00:00.000002500Z");
            store.insertVerified(connection, tieId, streamId, null, tie, tie, resultJson);
            connection.commit();
            try (PreparedStatement select = connection.prepareStatement(
                    "SELECT phenomenontime FROM " + TABLE + " WHERE id=?")) {
                select.setLong(1, tieId);
                try (ResultSet actual = select.executeQuery()) {
                    assertTrue(actual.next());
                    assertEquals(Instant.parse("2031-01-01T00:00:00.000003Z"),
                            UtcJdbcTime.getInstant(actual, 1));
                }
            }
            try (PreparedStatement delete = connection.prepareStatement("DELETE FROM " + TABLE + " WHERE id >= ?")) {
                delete.setLong(1, firstNaturalId);
                assertEquals(2, delete.executeUpdate());
            }
            connection.commit();
        }
    }

    private static String correctedResultJson(String original, double delta) {
        JsonObject corrected = JsonParser.parseString(original).getAsJsonObject().deepCopy();
        String numericField = corrected.entrySet().stream()
                .filter(entry -> entry.getValue().isJsonPrimitive() &&
                        entry.getValue().getAsJsonPrimitive().isNumber())
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow(() -> new AssertionError("Test observation result has no numeric field"));
        corrected.addProperty(numericField, corrected.get(numericField).getAsDouble() + delta);
        return corrected.toString();
    }

    private static final class TestableTieredStore extends OscarTieredObsStore {
        private Runnable beforeRemoveBatch;
        private Runnable afterRemoveBatchLocked;

        TestableTieredStore(String url, String dbName, String login, String password, String dataStoreName,
                            int idScope, IdProviderType idProviderType, TieredArchiveConfig config) {
            super(url, dbName, login, password, dataStoreName, idScope, idProviderType, config);
        }

        void beforeNextRemoveBatch(Runnable action) {
            beforeRemoveBatch = action;
        }

        void afterNextRemoveBatchLocked(Runnable action) {
            afterRemoveBatchLocked = action;
        }

        void limitObservationPool(int maximumPoolSize) {
            connectionManager.setMaximumPoolSize(maximumPoolSize);
        }

        @Override
        protected void beforeRemoveBatchRevalidation(List<BigId> candidateIds) {
            Runnable action = beforeRemoveBatch;
            beforeRemoveBatch = null;
            if (action != null)
                action.run();
        }

        @Override
        protected void afterRemoveBatchLocked(List<BigId> candidateIds) {
            Runnable action = afterRemoveBatchLocked;
            afterRemoveBatchLocked = null;
            if (action != null)
                action.run();
        }

        void insertVerified(Connection connection, long id, long streamId, Long foiId,
                            Instant phenomenon, Instant resultTime, String resultJson) throws SQLException {
            PreparedObservation observation = preparedObservation(
                    id, streamId, foiId, phenomenon, resultTime, resultJson);
            try (PreparedStatement statement = connection.prepareStatement(queryBuilder.insertObsPreparedQuery())) {
                bindPreparedObservation(statement, observation);
                statement.executeUpdate();
            }
            validateInsertOutcomes(connection, List.of(observation));
        }
    }

    private void assertCatalogV4AndActivation(ArchiveGeneration generation) throws Exception {
        try (Connection connection = open(); Statement statement = connection.createStatement()) {
            try (ResultSet result = statement.executeQuery(
                    "SELECT checksum FROM oscar_storage_schema_version WHERE version=4")) {
                assertTrue(result.next());
                assertEquals(ArchiveCatalog.SCHEMA_CHECKSUM, result.getString(1));
            }
            try (ResultSet result = statement.executeQuery(
                    "SELECT column_name, is_nullable FROM information_schema.columns " +
                            "WHERE table_name='oscar_obs_overlay' " +
                            "AND column_name IN ('store_key','id_scope','generation_id','schema_json') " +
                            "ORDER BY column_name")) {
                List<String> columns = new ArrayList<>();
                while (result.next()) {
                    columns.add(result.getString(1));
                    assertEquals("NO", result.getString(2));
                }
                assertEquals(List.of("generation_id", "id_scope", "schema_json", "store_key"), columns);
            }
            for (String table : List.of("oscar_deletion_plan", "oscar_deletion_ledger")) {
                try (ResultSet result = statement.executeQuery(
                        "SELECT column_name, is_nullable FROM information_schema.columns " +
                                "WHERE table_name='" + table + "' " +
                                "AND column_name IN ('store_key','id_scope') ORDER BY column_name")) {
                    List<String> columns = new ArrayList<>();
                    while (result.next()) {
                        columns.add(result.getString(1));
                        assertEquals("NO", result.getString(2));
                    }
                    assertEquals(List.of("id_scope", "store_key"), columns);
                }
            }
            try (ResultSet result = statement.executeQuery(
                    "SELECT indexdef FROM pg_indexes " +
                            "WHERE indexname='oscar_archive_object_relative_path_idx'")) {
                assertTrue(result.next());
                assertTrue(result.getString(1).startsWith("CREATE UNIQUE INDEX"));
            }
            try (ResultSet result = statement.executeQuery(
                    "SELECT state, row_count, parity_verified FROM oscar_archive_generation WHERE id='" +
                            generation.id() + "'")) {
                assertTrue(result.next());
                assertEquals("ACTIVE", result.getString(1));
                assertEquals(6, result.getLong(2));
                assertFalse(result.getBoolean(3));
            }
            try (ResultSet result = statement.executeQuery(
                    "SELECT count(*) FROM oscar_archive_object WHERE generation_id='" + generation.id() + "'")) {
                assertTrue(result.next());
                assertEquals(3, result.getLong(1));
            }
            try (ResultSet result = statement.executeQuery(
                    "SELECT count(*) FROM " + TABLE + " WHERE archivegenerationid='" + generation.id() + "'")) {
                assertTrue(result.next());
                assertEquals(6, result.getLong(1));
            }
            try (ResultSet result = statement.executeQuery(
                    "SELECT to_regclass('public." + TABLE + "_unique_constraint_v2')")) {
                assertTrue(result.next());
                assertTrue(result.getString(1) != null);
            }
        }
    }

    private static void assertGlobalOrderingAndLatest(OscarTieredObsStore store, Instant start) {
        List<Instant> ascending;
        try (var rows = store.selectEntries(new ObsFilter.Builder().build())) {
            ascending = rows.map(entry -> entry.getValue().getPhenomenonTime()).toList();
        }
        List<Instant> expectedAscending = new ArrayList<>();
        for (int i = 0; i < 6; i++)
            expectedAscending.add(start.plus(i, ChronoUnit.MINUTES));
        assertEquals(expectedAscending, ascending);

        ObsFilter descending = new ObsFilter.Builder().withPhenomenonTime()
                .withAllTimes().descendingOrder(true).done().build();
        try (var rows = store.selectEntries(descending)) {
            assertEquals(expectedAscending.stream().sorted(Comparator.reverseOrder()).toList(),
                    rows.map(entry -> entry.getValue().getPhenomenonTime()).toList());
        }

        ObsFilter latest = new ObsFilter.Builder().withPhenomenonTime()
                .withLatestTime().done().build();
        try (var rows = store.selectEntries(latest)) {
            var selected = rows.toList();
            assertEquals(1, selected.size());
            assertEquals(start.plus(5, ChronoUnit.MINUTES),
                    selected.get(0).getValue().getPhenomenonTime());
        }
        // DATASTREAM_ID can be omitted from the external projection, but the
        // tiered union still needs it internally to compute latest-per-stream.
        try (var rows = store.selectEntries(latest,
                Set.of(IObsStore.ObsField.PHENOMENON_TIME, IObsStore.ObsField.RESULT))) {
            var selected = rows.toList();
            assertEquals(1, selected.size());
            assertEquals(start.plus(5, ChronoUnit.MINUTES),
                    selected.get(0).getValue().getPhenomenonTime());
            assertTrue(selected.get(0).getValue().getResult() != null);
        }
        assertEquals(0, store.getOpenArchiveCursorCount());
    }

    private static ArchiveGeneration oneGeneration(ArchiveCatalog catalog) {
        List<ArchiveGeneration> generations = catalog.candidates(
                Instant.parse("2025-01-01T00:00:00Z"), Instant.parse("2027-01-01T00:00:00Z"), null);
        assertEquals(1, generations.size());
        return generations.get(0);
    }

    private void ageGeneration(UUID generationId) throws Exception {
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "UPDATE oscar_archive_generation SET activated_at=? WHERE id=?")) {
            UtcJdbcTime.setInstant(statement, 1, Instant.now().minus(2, ChronoUnit.DAYS));
            statement.setObject(2, generationId);
            assertEquals(1, statement.executeUpdate());
        }
    }

    private long hotRows() throws Exception {
        try (Connection connection = open(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("SELECT count(*) FROM " + TABLE)) {
            assertTrue(result.next());
            return result.getLong(1);
        }
    }

    private long archivedHotRows() throws Exception {
        try (Connection connection = open(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(
                     "SELECT count(*) FROM " + TABLE + " WHERE archivegenerationid IS NOT NULL")) {
            assertTrue(result.next());
            return result.getLong(1);
        }
    }

    private UUID tombstoneGeneration(long observationId) throws Exception {
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT generation_id FROM oscar_tombstone WHERE store_key=? AND id_scope=? " +
                        "AND subject_type='OBSERVATION' AND subject_id=?")) {
            statement.setString(1, TABLE);
            statement.setInt(2, DATABASE_NUM);
            statement.setString(3, Long.toString(observationId));
            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                return result.getObject(1, UUID.class);
            }
        }
    }

    private static long regularArchiveFiles(Path root) throws Exception {
        try (var paths = Files.walk(root)) {
            return paths.filter(Files::isRegularFile).count();
        }
    }

    private void installActivationBarrier() throws Exception {
        try (Connection connection = open(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE OR REPLACE FUNCTION codex_storage_it_pause_archive_activation() " +
                    "RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN " +
                    "PERFORM pg_advisory_xact_lock(" + ACTIVATION_LOCK_CLASS + "," +
                    ACTIVATION_LOCK_OBJECT + "); RETURN NULL; END $$");
            statement.execute("DROP TRIGGER IF EXISTS codex_storage_it_pause_archive_activation ON " + TABLE);
            statement.execute("CREATE TRIGGER codex_storage_it_pause_archive_activation " +
                    "BEFORE UPDATE OF archivegenerationid ON " + TABLE + " FOR EACH STATEMENT " +
                    "EXECUTE FUNCTION codex_storage_it_pause_archive_activation()");
        }
    }

    private void waitForBlockedActivation() throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                    "SELECT 1 FROM pg_locks WHERE locktype='advisory' AND classid=?::oid " +
                            "AND objid=?::oid AND NOT granted")) {
                statement.setInt(1, ACTIVATION_LOCK_CLASS);
                statement.setInt(2, ACTIVATION_LOCK_OBJECT);
                try (ResultSet result = statement.executeQuery()) {
                    if (result.next())
                        return;
                }
            }
            Thread.sleep(20);
        }
        throw new AssertionError("Archive activation did not reach the PostgreSQL barrier");
    }

    private void waitForAdvisoryWaiters(int minimum) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            try (Connection connection = open(); Statement statement = connection.createStatement();
                 ResultSet result = statement.executeQuery(
                         "SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND NOT granted")) {
                assertTrue(result.next());
                if (result.getLong(1) >= minimum)
                    return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("Expected at least " + minimum + " waiting advisory locks");
    }

    private void dropActivationBarrier() throws Exception {
        try (Connection connection = open(); Statement statement = connection.createStatement()) {
            statement.execute("DROP TRIGGER IF EXISTS codex_storage_it_pause_archive_activation ON " + TABLE);
            statement.execute("DROP FUNCTION IF EXISTS codex_storage_it_pause_archive_activation()");
        }
    }

    private String hotResultJson(long observationId) throws Exception {
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT result::text FROM " + TABLE + " WHERE id=?")) {
            statement.setLong(1, observationId);
            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                return result.getString(1);
            }
        }
    }

    private boolean hotRowExists(long observationId) throws Exception {
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT 1 FROM " + TABLE + " WHERE id=?")) {
            statement.setLong(1, observationId);
            try (ResultSet result = statement.executeQuery()) {
                return result.next();
            }
        }
    }

    private static void waitForActivePgSleep(Connection observer, int backendPid) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            try (PreparedStatement statement = observer.prepareStatement(
                    "SELECT 1 FROM pg_stat_activity WHERE pid=? AND state='active' " +
                            "AND query LIKE '%pg_sleep%'") ) {
                statement.setInt(1, backendPid);
                try (ResultSet result = statement.executeQuery()) {
                    if (result.next())
                        return;
                }
            }
            Thread.sleep(20L);
        }
        throw new AssertionError("Worker did not enter pg_sleep on PostgreSQL backend " + backendPid);
    }

    private static long queryLong(Connection connection, String sql) throws Exception {
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            assertTrue(result.next());
            return result.getLong(1);
        }
    }

    private static void waitForBackendExit(Connection observer, int backendPid) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            try (PreparedStatement statement = observer.prepareStatement(
                    "SELECT 1 FROM pg_stat_activity WHERE pid=?")) {
                statement.setInt(1, backendPid);
                try (ResultSet result = statement.executeQuery()) {
                    if (!result.next())
                        return;
                }
            }
            Thread.sleep(20L);
        }
        throw new AssertionError("Cancelled PostgreSQL backend remained active: " + backendPid);
    }

    private Connection open() throws Exception {
        return DriverManager.getConnection("jdbc:postgresql://" + url + "/" + DB_NAME, login, password);
    }
}

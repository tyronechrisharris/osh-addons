package org.sensorhub.impl.datastore.postgis.store.obs;

import com.google.gson.JsonParser;
import net.opengis.swe.v20.DataBlock;
import org.postgresql.util.PGobject;
import org.sensorhub.api.common.BigId;
import org.sensorhub.api.data.IDataStreamInfo;
import org.sensorhub.api.data.IObsData;
import org.sensorhub.api.data.ObsData;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.feature.FoiFilter;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.api.datastore.obs.ObsStats;
import org.sensorhub.api.datastore.obs.ObsStatsQuery;
import org.sensorhub.api.feature.FeatureId;
import org.sensorhub.impl.datastore.postgis.IdProviderType;
import org.sensorhub.impl.datastore.postgis.archive.ArchiveCatalog;
import org.sensorhub.impl.datastore.postgis.archive.ArchiveExporter;
import org.sensorhub.impl.datastore.postgis.archive.ArchiveGeneration;
import org.sensorhub.impl.datastore.postgis.archive.ArchiveLifecycleManager;
import org.sensorhub.impl.datastore.postgis.archive.ArchivedObservation;
import org.sensorhub.impl.datastore.postgis.archive.ParquetArchiveReader;
import org.sensorhub.impl.datastore.postgis.archive.TieredArchiveConfig;
import org.sensorhub.impl.datastore.postgis.archive.UtcJdbcTime;
import org.sensorhub.impl.datastore.postgis.builder.filter.JsonCqlEvaluator;
import org.sensorhub.impl.datastore.postgis.builder.ParameterizedQuery;
import org.sensorhub.impl.datastore.postgis.utils.SerializerUtils;
import org.vast.util.TimeExtent;
import org.vast.data.DataBlockByte;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * PostgreSQL/Parquet observation store preserving the standard OSH IObsStore contract.
 * PostgreSQL remains authoritative for metadata and recent observations; immutable
 * Parquet generations are merged transparently with mutation overlays and tombstones.
 */
public class OscarTieredObsStore extends PostgisBatchObsStoreImpl {
    private static final Instant MIN_ARCHIVE_TIME = Instant.parse("1000-01-01T00:00:00Z");
    private static final Instant MAX_ARCHIVE_TIME = Instant.parse("3000-01-01T00:00:00Z");
    private static final Duration MINIMUM_ARCHIVE_WINDOW = Duration.ofSeconds(1);
    private static final int REMOVE_BATCH_SIZE = 512;

    private final TieredArchiveConfig archiveConfig;
    private final ArchiveCatalog archiveCatalog;
    private final ArchiveExporter archiveExporter;
    private final ArchiveLifecycleManager archiveLifecycle;
    private final ParquetArchiveReader archiveReader;
    private final Map<String, IDataStreamInfo> archivedSchemas = new ConcurrentHashMap<>();

    public OscarTieredObsStore(String url, String dbName, String login, String password, String dataStoreName,
                               int idScope, IdProviderType idProviderType, TieredArchiveConfig archiveConfig) {
        super(url, dbName, login, password, dataStoreName, idScope, idProviderType);
        if (idProviderType != IdProviderType.SEQUENTIAL)
            throw new IllegalArgumentException("OSCAR tiered observations require sequential IDs for race-free archive publication");
        archiveConfig.validate();
        try (java.sql.Connection connection = connectionManager.getConnection()) {
            org.sensorhub.impl.datastore.postgis.utils.PostgisUtils.executeQueries(connection, new String[]{
                    queryBuilder.addArchiveGenerationColumnQuery(),
                    queryBuilder.createUnarchivedTimeIndexQuery(),
                    queryBuilder.createArchiveGenerationIndexQuery()
            });
        } catch (java.sql.SQLException e) {
            throw new IllegalStateException("Cannot initialize tiered observation markers", e);
        }
        this.archiveConfig = archiveConfig;
        this.archiveCatalog = new ArchiveCatalog(connectionManager::getConnection, dataStoreName, idScope);
        this.archiveCatalog.migrate();
        this.archiveCatalog.maximumObservationId().ifPresent(archiveMaximum ->
                lastId.accumulateAndGet(archiveMaximum, Math::max));
        this.archiveExporter = new ArchiveExporter(connectionManager::getConnection, archiveCatalog, archiveConfig,
                idScope, dataStoreName, dataStoreName + "_datastreams");
        this.archiveLifecycle = new ArchiveLifecycleManager(connectionManager::getConnection, archiveCatalog,
                archiveConfig, dataStoreName);
        this.archiveReader = new ParquetArchiveReader(Path.of(archiveConfig.rootDirectory));
    }

    public ArchiveCatalog getArchiveCatalog() {
        return archiveCatalog;
    }

    public TieredArchiveConfig getArchiveConfig() {
        return archiveConfig;
    }

    /** Visible to health checks and cursor-lifecycle regression tests. */
    public int getOpenArchiveCursorCount() {
        return archiveReader.getOpenCursorCount();
    }

    public void maintainArchiveLifecycle() {
        archiveLifecycle.maintain();
    }

    public java.util.Optional<ArchiveGeneration> archiveWindow(Instant start, Instant end, String profile) {
        return archiveExporter.exportWindow(start, end, profile);
    }

    /**
     * Natural-key corrections must never rewrite a base row that belongs to a
     * query-visible immutable archive generation. The guarded PostgreSQL
     * upsert leaves that row untouched; {@link #reconcileProtectedObservations}
     * records the logical replacement in the generation-owned overlay within
     * the same database transaction.
     */
    @Override
    protected String preparedObservationUpsertQuery() {
        return queryBuilder.insertTieredObsPreparedQuery();
    }

    @Override
    protected void beforeObservationWrite(Connection connection,
                                          Collection<PreparedObservation> observations)
            throws Exception {
        if (archiveConfig.enabled) {
            archiveCatalog.lockForMutation(connection, observations.stream()
                    .map(observation -> observation.id)
                    .toList());
        }
    }

    @Override
    protected Instant getStoredPhenomenonTimeHighWater(long dataStreamId) {
        Instant hot = super.getStoredPhenomenonTimeHighWater(dataStreamId);
        if (!archiveConfig.enabled)
            return hot;
        Instant cold = archiveCatalog.latestWindowEnd().orElse(null);
        if (hot == null)
            return cold;
        return cold == null || hot.isAfter(cold) ? hot : cold;
    }

    @Override
    protected Long findStoredObservationId(PreparedObservation observation) {
        Long hot = super.findStoredObservationId(observation);
        if (hot != null || !archiveConfig.enabled)
            return hot;
        return findArchivedNaturalKey(observation)
                .map(match -> match.observation().id())
                .orElse(null);
    }

    @Override
    protected Set<Long> reconcileProtectedObservations(Connection connection,
                                                        Collection<PreparedObservation> observations)
            throws Exception {
        if (!archiveConfig.enabled || observations.isEmpty())
            return Set.of();

        Map<Long, PreparedObservation> expectedById = new HashMap<>();
        for (PreparedObservation observation : observations)
            expectedById.put(observation.id, observation);

        List<ProtectedCorrection> corrections = new ArrayList<>();
        Set<Long> protectedHotIds = new HashSet<>();
        String sql = "SELECT observation.id, generation.id, " +
                "COALESCE(overlay_row.result_json, observation.result)::text " +
                "FROM " + getDatastoreName() + " observation " +
                "JOIN oscar_archive_generation generation " +
                "ON generation.id=observation.archivegenerationid " +
                "LEFT JOIN oscar_obs_overlay overlay_row " +
                "ON overlay_row.store_key=? AND overlay_row.id_scope=? " +
                "AND overlay_row.observation_id=observation.id " +
                "AND overlay_row.generation_id=generation.id " +
                "WHERE observation.id=ANY(?) AND generation.store_key=? " +
                "AND generation.id_scope=? AND generation.state IN ('ACTIVE','SEALED')";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, getDatastoreName());
            statement.setInt(2, idScope);
            statement.setArray(3, connection.createArrayOf("bigint",
                    expectedById.keySet().toArray(Long[]::new)));
            statement.setString(4, getDatastoreName());
            statement.setInt(5, idScope);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    PreparedObservation expected = expectedById.get(result.getLong(1));
                    if (expected != null) {
                        protectedHotIds.add(expected.id);
                        if (!jsonEquals(expected.resultJson, result.getString(3))) {
                            corrections.add(new ProtectedCorrection(
                                    result.getObject(2, UUID.class), expected));
                        }
                    }
                }
            }
        }

        // After grace deletion PostgreSQL no longer contains the stable row,
        // so the upsert temporarily inserts it. Resolve the exact immutable
        // Parquet identity, remove that uncommitted duplicate, and apply only
        // a logical correction as an overlay. Exact replay leaves no hot row
        // and creates no overlay.
        for (PreparedObservation expected : expectedById.values()) {
            if (protectedHotIds.contains(expected.id))
                continue;
            java.util.Optional<ArchivedNaturalMatch> archived =
                    findArchivedNaturalKey(connection, expected);
            if (archived.isEmpty())
                continue;
            ArchivedNaturalMatch match = archived.get();
            if (match.observation().id() != expected.id)
                throw new IllegalStateException("Archived observation identity resolved after add returned a different ID");
            protectedHotIds.add(expected.id);
            try (PreparedStatement delete = connection.prepareStatement(
                    "DELETE FROM " + getDatastoreName() +
                            " WHERE id=? AND archivegenerationid IS NULL")) {
                delete.setLong(1, expected.id);
                delete.executeUpdate();
            }
            String logicalResult = archiveCatalog.overlayWithGeneration(
                            connection, expected.id, idScope, "OVERLAY", true)
                    .map(overlay -> overlay.observation().resultJson())
                    .orElse(match.observation().resultJson());
            if (!jsonEquals(expected.resultJson, logicalResult))
                corrections.add(new ProtectedCorrection(match.generation().id(), expected));
        }

        Map<Long, String> schemaByDataStream = new HashMap<>();
        for (ProtectedCorrection correction : corrections) {
            PreparedObservation observation = correction.observation();
            String schemaJson = schemaByDataStream.get(observation.dataStreamId);
            if (schemaJson == null) {
                // PostgisDataStreamStoreImpl owns a distinct ConnectionManager;
                // this lookup therefore cannot recursively borrow the
                // observation connection held by this transaction. A linked
                // non-PostgreSQL store likewise does not use this pool.
                IDataStreamInfo schema = dataStreamStore.get(new DataStreamKey(
                        BigId.fromLong(idScope, observation.dataStreamId)));
                if (schema == null)
                    throw new IllegalStateException(
                            "Cannot correct archived observation without a live datastream schema");
                schemaJson = SerializerUtils.writeIDataStreamInfoToJson(schema);
                schemaByDataStream.put(observation.dataStreamId, schemaJson);
            }
            archiveCatalog.upsertOverlay(connection, correction.generationId(),
                    new ArchivedObservation(idScope, observation.id, idScope,
                            observation.dataStreamId,
                            observation.foiId == null ? null : idScope, observation.foiId,
                            observation.phenomenonTime, observation.resultTime,
                            observation.resultJson, "OVERLAY"), schemaJson);
        }
        return protectedHotIds;
    }

    private record ProtectedCorrection(UUID generationId, PreparedObservation observation) {}

    private java.util.Optional<ArchivedNaturalMatch> findArchivedNaturalKey(
            PreparedObservation observation) {
        try {
            return findArchivedNaturalKey(null, observation);
        } catch (Exception error) {
            throw new IllegalStateException("Cannot resolve archived observation identity", error);
        }
    }

    private java.util.Optional<ArchivedNaturalMatch> findArchivedNaturalKey(
            Connection transaction, PreparedObservation observation) throws Exception {
        if (observation.phenomenonTime == null)
            return java.util.Optional.empty();
        Instant end;
        try {
            end = observation.phenomenonTime.plus(1, ChronoUnit.MICROS);
        } catch (RuntimeException overflow) {
            end = MAX_ARCHIVE_TIME;
        }
        List<ArchiveGeneration> candidates = transaction == null
                ? archiveCatalog.candidates(observation.phenomenonTime, end, null)
                : archiveCatalog.candidates(transaction, observation.phenomenonTime, end, null);
        if (candidates.isEmpty())
            return java.util.Optional.empty();
        ObsFilter.Builder filter = new ObsFilter.Builder().withPhenomenonTime(
                new TemporalFilter.Builder().withSingleValue(observation.phenomenonTime).build());
        if (observation.resultTime != null) {
            filter.withResultTime(new TemporalFilter.Builder()
                    .withSingleValue(observation.resultTime).build());
        }
        Set<Long> dataStreams = Set.of(observation.dataStreamId);
        Set<Long> fois = Set.of(observation.foiId == null ? 0L : observation.foiId);
        try (Stream<ParquetArchiveReader.ArchivedRow> rows = archiveReader.stream(
                candidates, filter.build(), dataStreams, fois, Long.MAX_VALUE)) {
            return rows.filter(row -> {
                        ArchivedObservation archived = row.observation();
                        return archived.dataStreamId() == observation.dataStreamId &&
                                Objects.equals(archived.foiId(), observation.foiId) &&
                                Objects.equals(archived.phenomenonTime(), observation.phenomenonTime) &&
                                Objects.equals(archived.resultTime(), observation.resultTime) &&
                                !isArchiveTombstoned(transaction, archived.id());
                    })
                    .map(row -> new ArchivedNaturalMatch(row.generation(), row.observation()))
                    .findFirst();
        }
    }

    private boolean isArchiveTombstoned(Connection transaction, long observationId) {
        try {
            return transaction == null
                    ? archiveCatalog.isTombstoned(observationId)
                    : archiveCatalog.isTombstoned(transaction, observationId);
        } catch (Exception error) {
            throw new IllegalStateException(
                    "Cannot inspect archive tombstone for observation " + observationId, error);
        }
    }

    private static boolean jsonEquals(String left, String right) {
        return JsonParser.parseString(left).equals(JsonParser.parseString(right));
    }

    private record ArchivedNaturalMatch(ArchiveGeneration generation,
                                        ArchivedObservation observation) {}

    /** Returns the oldest unarchived eligible row, including late arrivals. */
    public java.util.Optional<Instant> nextArchiveWindowStart(Instant cutoff, String profile) {
        String sql = "SELECT min(o.phenomenontime) FROM " + getDatastoreName() +
                " o JOIN " + getDataStreams().getDatastoreName() + " d ON d.id=o.datastreamid " +
                "WHERE o.archivegenerationid IS NULL AND o.phenomenontime < ? AND d.data->>'outputName' = ANY (?)";
        try (java.sql.Connection connection = connectionManager.getConnection();
             java.sql.PreparedStatement statement = connection.prepareStatement(sql)) {
            UtcJdbcTime.setInstant(statement, 1, cutoff);
            statement.setArray(2, connection.createArrayOf("text", archiveConfig.outputNames.toArray()));
            try (java.sql.ResultSet result = statement.executeQuery()) {
                if (!result.next()) return java.util.Optional.empty();
                Instant value = UtcJdbcTime.getInstant(result, 1);
                return value == null ? java.util.Optional.empty() : java.util.Optional.of(value);
            }
        } catch (java.sql.SQLException e) {
            throw new IllegalStateException("Cannot determine next archive window", e);
        }
    }

    /**
     * Measures at most {@code rowCap + 1} candidates using the same uncompressed-byte
     * accounting as {@link ArchiveExporter}. If the returned row count is at most the
     * cap, the byte count covers the complete window; once the row cap is exceeded the
     * scheduler must split regardless of the partial byte total.
     */
    public ArchiveCandidateFootprint measureArchiveCandidates(Instant start, Instant end,
                                                               long rowCap) {
        if (start == null || end == null || !start.isBefore(end))
            throw new IllegalArgumentException("Archive candidate window must be non-empty");
        if (rowCap < 1)
            throw new IllegalArgumentException("Archive candidate row cap must be positive");
        String sql = "SELECT count(*), COALESCE(sum(" +
                ArchiveExporter.ESTIMATED_FIXED_BYTES_PER_ROW + "::bigint + " +
                "COALESCE(octet_length(convert_to(candidate.result::text, 'UTF8')), 0)), 0), " +
                "min(candidate.phenomenontime) " +
                "FROM (SELECT o.result, o.phenomenontime FROM " + getDatastoreName() +
                " o JOIN " + getDataStreams().getDatastoreName() + " d ON d.id=o.datastreamid " +
                "WHERE o.archivegenerationid IS NULL AND o.phenomenontime >= ? AND o.phenomenontime < ? " +
                "AND d.data->>'outputName' = ANY (?) " +
                "ORDER BY o.phenomenontime, o.id LIMIT ?) candidate";
        try (java.sql.Connection connection = connectionManager.getConnection();
             java.sql.PreparedStatement statement = connection.prepareStatement(sql)) {
            UtcJdbcTime.setInstant(statement, 1, start);
            UtcJdbcTime.setInstant(statement, 2, end);
            statement.setArray(3, connection.createArrayOf("text", archiveConfig.outputNames.toArray()));
            statement.setLong(4, Math.addExact(rowCap, 1));
            try (java.sql.ResultSet result = statement.executeQuery()) {
                if (!result.next())
                    return new ArchiveCandidateFootprint(0, 0, null);
                BigDecimal measuredBytes = result.getBigDecimal(2);
                long bytes = measuredBytes.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) >= 0
                        ? Long.MAX_VALUE : measuredBytes.longValueExact();
                return new ArchiveCandidateFootprint(result.getLong(1), bytes,
                        UtcJdbcTime.getInstant(result, 3));
            }
        } catch (java.sql.SQLException | ArithmeticException e) {
            throw new IllegalStateException("Cannot size next archive window", e);
        }
    }

    /** Selects the largest deterministic prefix that satisfies both generation caps. */
    public Instant selectArchiveWindowEnd(Instant start, Instant proposedEnd) {
        Instant end = proposedEnd;
        Instant minimumCandidateEnd = null;
        while (true) {
            ArchiveCandidateFootprint footprint = measureArchiveCandidates(
                    start, end, archiveConfig.maximumRowsPerGeneration);
            boolean rowLimitExceeded = footprint.rowCount() > archiveConfig.maximumRowsPerGeneration;
            boolean byteLimitExceeded = footprint.estimatedUncompressedBytes() >
                    archiveConfig.maximumUncompressedBytesPerGeneration;
            if (!rowLimitExceeded && !byteLimitExceeded)
                return end;

            if (minimumCandidateEnd == null) {
                Instant earliest = footprint.earliestPhenomenonTime();
                if (earliest == null)
                    throw new IllegalStateException("Oversized archive footprint did not identify its first row");
                // Do not let wall-clock bisection choose an empty prefix when
                // a sparse hour's first observation is in its latter half.
                // PostgreSQL timestamps have microsecond precision, but the
                // documented archive split floor is one whole UTC second.
                minimumCandidateEnd = earliest.truncatedTo(java.time.temporal.ChronoUnit.SECONDS)
                        .plus(MINIMUM_ARCHIVE_WINDOW);
                Instant absoluteMinimumEnd = start.plus(MINIMUM_ARCHIVE_WINDOW);
                if (minimumCandidateEnd.isBefore(absoluteMinimumEnd))
                    minimumCandidateEnd = absoluteMinimumEnd;
                if (minimumCandidateEnd.isAfter(proposedEnd))
                    minimumCandidateEnd = proposedEnd;
            }

            if (Duration.between(start, end).compareTo(MINIMUM_ARCHIVE_WINDOW) <= 0 ||
                    end.equals(minimumCandidateEnd)) {
                throw new IllegalStateException("Archive candidates in the minimum one-second window " +
                        "exceed configured generation limits: rows=" + footprint.rowCount() + "/" +
                        archiveConfig.maximumRowsPerGeneration + ", estimatedBytes=" +
                        footprint.estimatedUncompressedBytes() + "/" +
                        archiveConfig.maximumUncompressedBytesPerGeneration);
            }
            Instant bisected = bisectArchiveWindowEnd(start, end);
            end = bisected.isBefore(minimumCandidateEnd) ? minimumCandidateEnd : bisected;
        }
    }

    /** Deterministic UTC window bisection, clamped to the documented one-second minimum. */
    public static Instant bisectArchiveWindowEnd(Instant start, Instant end) {
        if (start == null || end == null || !start.isBefore(end))
            throw new IllegalArgumentException("Archive candidate window must be non-empty");
        Duration width = Duration.between(start, end);
        if (width.compareTo(MINIMUM_ARCHIVE_WINDOW) <= 0)
            throw new IllegalArgumentException("The minimum one-second archive window cannot be bisected");
        Duration half = width.dividedBy(2);
        if (half.compareTo(MINIMUM_ARCHIVE_WINDOW) < 0)
            half = MINIMUM_ARCHIVE_WINDOW;
        return start.plus(half);
    }

    /** Counts at most cap+1 rows for compatibility with existing diagnostics. */
    public long countArchiveCandidates(Instant start, Instant end, long cap) {
        return measureArchiveCandidates(start, end, cap).rowCount();
    }

    public record ArchiveCandidateFootprint(long rowCount, long estimatedUncompressedBytes,
                                            Instant earliestPhenomenonTime) {
        public ArchiveCandidateFootprint {
            if (rowCount < 0 || estimatedUncompressedBytes < 0)
                throw new IllegalArgumentException("Archive candidate footprint must not be negative");
            if ((rowCount == 0) != (earliestPhenomenonTime == null))
                throw new IllegalArgumentException("Archive candidate time must be present exactly when rows are present");
        }
    }

    @Override
    public Stream<Entry<BigId, IObsData>> selectEntries(ObsFilter filter, Set<ObsField> fields) {
        if (!archiveConfig.enabled)
            return super.selectEntries(filter, fields);

        return selectTieredEntries(filter, fields, archiveCatalog.hasObservationMutations());
    }

    private Stream<Entry<BigId, IObsData>> selectTieredEntries(
            ObsFilter filter, Set<ObsField> fields,
            boolean hasMutations) {
        Set<Long> dataStreams = resolveDataStreams(filter.getDataStreamFilter());
        Set<Long> fois = resolveFois(filter.getFoiFilter());
        boolean latest = hasLatestSelection(filter);

        // The top N of a union can only contain rows from the top N of either
        // ordered tier. Push the bound into both engines in the common,
        // mutation-free case; fall back to a complete merge when a predicate,
        // overlay, tombstone, or latest-per-stream reduction can invalidate a
        // pre-limit row.
        boolean boundedMerge = !latest && filter.getLimit() != Long.MAX_VALUE &&
                filter.getCQLFilter() == null && filter.getValuePredicate() == null &&
                filter.getPhenomenonLocation() == null && !hasMutations;
        long tierLimit = boundedMerge ? filter.getLimit() : Long.MAX_VALUE;
        ObsFilter tierFilter = latest ? withoutLatestSelection(filter, tierLimit) :
                ObsFilter.Builder.from(filter).withLimit(tierLimit).build();
        Comparator<Entry<BigId, IObsData>> comparator = entryComparator(filter);

        // Read complete internal rows even when the caller requested a narrow
        // projection. The union, latest-per-stream reduction, predicates, and
        // ordering all need datastream/time/result fields. Projection is
        // applied only after those semantics are complete.
        Stream<Entry<BigId, IObsData>> hot = super.selectEntries(tierFilter, (Set<ObsField>) null);
        if (hasMutations)
            hot = excludeMutated(hot, entry -> entry.getKey().getIdAsLong());

        Stream<Entry<BigId, IObsData>> cold = streamColdBases(tierFilter, tierLimit,
                dataStreams, fois, hasMutations);
        Stream<Entry<BigId, IObsData>> overlays = streamOverlays(tierFilter,
                dataStreams, fois, hasMutations);

        // Each source is already ordered by phenomenon time and ID. Merge it
        // lazily so cold payloads are never accumulated in a JVM collection.
        // The left side wins equal keys, making the deliberate hot/cold grace
        // overlap deterministic without retaining a global ID set.
        Stream<Entry<BigId, IObsData>> merged = mergeSorted(hot, cold, comparator);
        merged = mergeSorted(merged, overlays, comparator);
        if (latest)
            merged = applyLatestAndOrder(merged, filter);
        Stream<Entry<BigId, IObsData>> limited = limitAndClose(merged, filter.getLimit());
        return fields == null || fields.isEmpty() ? limited :
                limited.map(entry -> Map.<BigId, IObsData>entry(
                        entry.getKey(), project(entry.getValue(), fields)));
    }

    private static boolean hasLatestSelection(ObsFilter filter) {
        return (filter.getPhenomenonTime() != null && filter.getPhenomenonTime().isLatestTime()) ||
                (filter.getResultTime() != null && filter.getResultTime().isLatestTime());
    }

    /** Latest selection is evaluated only after the logical hot/cold union. */
    private static ObsFilter withoutLatestSelection(ObsFilter filter, long limit) {
        ObsFilter.Builder builder = ObsFilter.Builder.from(filter).withLimit(limit);
        if (filter.getPhenomenonTime() != null && filter.getPhenomenonTime().isLatestTime()) {
            builder.withPhenomenonTime(new TemporalFilter.Builder().withAllTimes()
                    .descendingOrder(filter.getPhenomenonTime().descendingOrder()).build());
        }
        if (filter.getResultTime() != null && filter.getResultTime().isLatestTime()) {
            builder.withResultTime(new TemporalFilter.Builder().withAllTimes()
                    .descendingOrder(filter.getResultTime().descendingOrder()).build());
        }
        return builder.build();
    }

    private java.util.Optional<IObsData> readCold(ObsFilter filter) {
        Set<Long> dataStreams = resolveDataStreams(filter.getDataStreamFilter());
        Set<Long> fois = resolveFois(filter.getFoiFilter());
        boolean hasMutations = archiveCatalog.hasObservationMutations();
        Comparator<Entry<BigId, IObsData>> comparator = entryComparator(filter);
        try (Stream<Entry<BigId, IObsData>> bases = streamColdBases(filter, Long.MAX_VALUE,
                dataStreams, fois, hasMutations);
             Stream<Entry<BigId, IObsData>> overlays = streamOverlays(filter,
                     dataStreams, fois, hasMutations);
             Stream<Entry<BigId, IObsData>> merged = mergeSorted(bases, overlays, comparator)) {
            return merged.map(Entry::getValue).findFirst();
        }
    }

    private Stream<Entry<BigId, IObsData>> streamColdBases(
            ObsFilter filter, long readerLimit, Set<Long> dataStreams, Set<Long> fois,
            boolean hasMutations) {
        ArchiveBounds bounds = archiveBounds(filter);
        Long oneId = filter.getInternalIDs() != null && filter.getInternalIDs().size() == 1
                ? filter.getInternalIDs().first().getIdAsLong() : null;

        List<ArchiveGeneration> candidates = archiveCatalog.candidates(bounds.start(), bounds.end(), oneId);
        Stream<ParquetArchiveReader.ArchivedRow> rows = archiveReader.stream(
                candidates, filter, dataStreams, fois, readerLimit);
        if (hasMutations)
            rows = excludeMutated(rows, row -> row.observation().id());
        return rows
                .map(row -> {
                    long id = row.observation().id();
                    IObsData observation = toObservation(row.generation().id(), null, row.observation());
                    return Map.entry(BigId.fromLong(idScope, id), observation);
                })
                .filter(entry -> filter.test(entry.getValue()));
    }

    private Stream<Entry<BigId, IObsData>> streamOverlays(
            ObsFilter filter, Set<Long> dataStreams, Set<Long> fois,
            boolean hasMutations) {
        if (!hasMutations)
            return Stream.empty();
        return archiveCatalog.streamOverlays(idScope, filter, dataStreams, fois)
                .flatMap(mutation -> {
                    ArchivedObservation overlay = mutation.observation();
                    long id = overlay.id();
                    IObsData observation = toObservation(
                            mutation.generationId(), mutation.schemaJson(), overlay);
                    return matchesObservation(filter, dataStreams, fois, id,
                            overlay.resultJson(), observation)
                            ? Stream.of(Map.entry(BigId.fromLong(idScope, id), observation))
                            : Stream.empty();
                });
    }

    private Set<Long> resolveDataStreams(DataStreamFilter filter) {
        if (filter == null)
            return null;
        try (Stream<DataStreamKey> keys = dataStreamStore.selectKeys(filter)) {
            return keys.map(key -> key.getInternalID().getIdAsLong()).collect(Collectors.toCollection(HashSet::new));
        }
    }

    private Set<Long> resolveFois(FoiFilter filter) {
        if (filter == null)
            return null;
        if (foiStore == null)
            throw new IllegalStateException("Cannot evaluate FOI filter before an FOI store is linked");
        Set<Long> ids;
        try (Stream<? extends BigId> keys = foiStore.selectKeys(filter).map(key -> key.getInternalID())) {
            ids = keys.map(BigId::getIdAsLong).collect(Collectors.toCollection(HashSet::new));
        }
        if (filter.getInternalIDs() != null && filter.getInternalIDs().contains(BigId.NONE))
            ids.add(0L); // reader interprets zero as SQL NULL as well
        return ids;
    }

    private ArchiveBounds archiveBounds(ObsFilter filter) {
        Instant start = MIN_ARCHIVE_TIME;
        Instant end = MAX_ARCHIVE_TIME;
        if (filter.getPhenomenonTime() != null && !filter.getPhenomenonTime().isLatestTime()) {
            Instant minimum = filter.getPhenomenonTime().getMin();
            Instant maximum = filter.getPhenomenonTime().getMax();
            if (minimum != null && !Instant.MIN.equals(minimum))
                start = minimum;
            if (maximum != null && !Instant.MAX.equals(maximum))
                end = maximum.plusNanos(1);
        }
        return new ArchiveBounds(start, end);
    }

    private boolean matchesObservation(ObsFilter filter, Set<Long> dataStreams, Set<Long> fois,
                                       long id, String resultJson, IObsData observation) {
        if (filter.getInternalIDs() != null &&
                !filter.getInternalIDs().contains(BigId.fromLong(idScope, id)))
            return false;
        if (dataStreams != null && !dataStreams.contains(observation.getDataStreamID().getIdAsLong()))
            return false;
        if (fois != null) {
            long foi = observation.hasFoi() ? observation.getFoiID().getIdAsLong() : 0L;
            if (!fois.contains(foi))
                return false;
        }
        if (!filter.test(observation))
            return false;
        if (!JsonCqlEvaluator.matches(filter.getCQLFilter(), resultJson))
            return false;
        return true;
    }

    private boolean matchesLockedObservation(ObsFilter filter, Set<Long> dataStreams,
                                             Set<Long> fois, long id,
                                             LockedLogicalObservation locked) {
        if (!matchesTemporal(filter.getPhenomenonTime(), locked.phenomenonTime()) ||
                !matchesTemporal(filter.getResultTime(), locked.resultTime()))
            return false;
        return matchesObservation(filter, dataStreams, fois, id,
                locked.resultJson(), locked.observation());
    }

    private static boolean matchesTemporal(TemporalFilter filter, Instant value) {
        if (filter == null || filter.isLatestTime())
            return true;
        return value != null && filter.getRange().contains(value);
    }

    private static IObsData project(IObsData observation, Set<ObsField> fields) {
        ObsData.Builder builder = new ObsData.Builder();
        if (fields.contains(ObsField.DATASTREAM_ID))
            builder.withDataStream(observation.getDataStreamID());
        if (fields.contains(ObsField.FOI_ID) && observation.hasFoi())
            builder.withFoi(observation.getFoiID());
        // The base PostgreSQL store always includes phenomenon time as the
        // stable result-order field, even for a narrow projection.
        if (observation.getPhenomenonTime() != null)
            builder.withPhenomenonTime(observation.getPhenomenonTime());
        if (fields.contains(ObsField.RESULT_TIME) && observation.getResultTime() != null)
            builder.withResultTime(observation.getResultTime());
        builder.withResult(fields.contains(ObsField.RESULT)
                ? observation.getResult() : new DataBlockByte());
        return builder.build();
    }

    private IObsData toObservation(UUID generationId, String overlaySchemaJson,
                                   ArchivedObservation archived) {
        return toObservation(null, generationId, overlaySchemaJson, archived);
    }

    private IObsData toObservation(Connection transaction, UUID generationId,
                                   String overlaySchemaJson, ArchivedObservation archived) {
        BigId dataStreamId = BigId.fromLong(archived.dataStreamScope(), archived.dataStreamId());
        boolean overlay = "OVERLAY".equals(archived.profile());
        IDataStreamInfo schema = null;
        if (overlay && overlaySchemaJson != null) {
            try {
                schema = SerializerUtils.readIDataStreamInfoFromJson(overlaySchemaJson);
            } catch (IOException e) {
                throw new IllegalStateException("Cannot decode archived overlay schema", e);
            }
        }
        if (schema == null && generationId != null) {
            String cacheKey = generationId + ":" + archived.dataStreamId();
            schema = archivedSchemas.get(cacheKey);
            if (schema == null) {
                try {
                    java.util.Optional<String> serialized = transaction == null
                            ? archiveCatalog.schemaJson(generationId, archived.dataStreamId())
                            : archiveCatalog.schemaJson(transaction, generationId, archived.dataStreamId());
                    if (serialized.isPresent()) {
                        IDataStreamInfo decoded = SerializerUtils.readIDataStreamInfoFromJson(serialized.get());
                        IDataStreamInfo existing = archivedSchemas.putIfAbsent(cacheKey, decoded);
                        schema = existing == null ? decoded : existing;
                    }
                } catch (Exception e) {
                    throw new IllegalStateException("Cannot decode archived datastream schema", e);
                }
            }
        }
        if (schema == null)
            throw new IllegalStateException((overlay ? "No live or archived schema for overlaid datastream " :
                    "No generation schema for archived datastream ") + archived.dataStreamId());
        DataBlock result = SerializerUtils.readDataBlockFromJson(schema.getRecordStructure(), archived.resultJson());
        ObsData.Builder builder = new ObsData.Builder().withDataStream(dataStreamId).withResult(result);
        if (archived.foiId() != null)
            builder.withFoi(BigId.fromLong(archived.foiScope(), archived.foiId()));
        if (archived.phenomenonTime() != null)
            builder.withPhenomenonTime(archived.phenomenonTime());
        if (archived.resultTime() != null)
            builder.withResultTime(archived.resultTime());
        return builder.build();
    }

    private static Comparator<Entry<BigId, IObsData>> entryComparator(ObsFilter filter) {
        boolean descending = filter.getPhenomenonTime() != null &&
                filter.getPhenomenonTime().descendingOrder();
        Comparator<Instant> timeOrder = descending
                ? Comparator.nullsFirst(Comparator.reverseOrder())
                : Comparator.nullsFirst(Comparator.naturalOrder());
        return Comparator.<Entry<BigId, IObsData>, Instant>comparing(
                        entry -> entry.getValue().getPhenomenonTime(), timeOrder)
                .thenComparingLong(entry -> entry.getKey().getIdAsLong());
    }

    private Stream<Entry<BigId, IObsData>> applyLatestAndOrder(
            Stream<Entry<BigId, IObsData>> entries, ObsFilter filter) {
        boolean latestPhenomenon = filter.getPhenomenonTime() != null && filter.getPhenomenonTime().isLatestTime();
        boolean latestResult = filter.getResultTime() != null && filter.getResultTime().isLatestTime();
        if (!latestPhenomenon && !latestResult)
            return entries;

        // Latest selection is intentionally deferred until after the hot/cold
        // union and sparse mutation layer. State is bounded by the number of
        // datastreams, rather than the number of archived observations.
        Map<BigId, Entry<BigId, IObsData>> latest;
        try (entries) {
            latest = entries.collect(Collectors.toMap(
                    entry -> entry.getValue().getDataStreamID(), Function.identity(), (left, right) -> {
                        Instant leftTime = latestResult ? left.getValue().getResultTime() :
                                left.getValue().getPhenomenonTime();
                        Instant rightTime = latestResult ? right.getValue().getResultTime() :
                                right.getValue().getPhenomenonTime();
                        if (leftTime == null && rightTime == null)
                            return right.getKey().getIdAsLong() > left.getKey().getIdAsLong() ? right : left;
                        if (leftTime == null) return right;
                        if (rightTime == null) return left;
                        int compared = rightTime.compareTo(leftTime);
                        return compared > 0 || (compared == 0 &&
                                right.getKey().getIdAsLong() > left.getKey().getIdAsLong()) ? right : left;
                    }, HashMap::new));
        }
        return latest.values().stream().sorted(entryComparator(filter));
    }

    /**
     * Merges two ordered streams without materializing either side. Equal
     * entries are emitted once and the preferred (left) tier wins. Both
     * streams are closed on exhaustion, errors, and explicit close.
     */
    private static <T> Stream<T> mergeSorted(
            Stream<T> preferred, Stream<T> other, Comparator<? super T> comparator) {
        MergingIterator<T> iterator = new MergingIterator<>(preferred, other, comparator);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator,
                        Spliterator.ORDERED | Spliterator.NONNULL), false)
                .onClose(iterator::close);
    }

    /**
     * Filters base rows through bounded set-based mutation lookups. Only one
     * page of IDs and surviving rows is retained; the source closes on
     * exhaustion, failure, or an explicit short-circuit close.
     */
    private <T> Stream<T> excludeMutated(Stream<T> source, ToLongFunction<T> idExtractor) {
        MutationFilteringIterator<T> iterator = new MutationFilteringIterator<>(source, idExtractor);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator,
                        Spliterator.ORDERED | Spliterator.NONNULL), false)
                .onClose(iterator::close);
    }

    /** Closes the upstream stream as soon as its externally visible limit is reached. */
    private static <T> Stream<T> limitAndClose(Stream<T> source, long limit) {
        if (limit == Long.MAX_VALUE)
            return source;
        if (limit < 0) {
            source.close();
            throw new IllegalArgumentException("Observation query limit cannot be negative");
        }
        ClosingLimitIterator<T> iterator = new ClosingLimitIterator<>(source, limit);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator,
                        Spliterator.ORDERED | Spliterator.NONNULL), false)
                .onClose(iterator::close);
    }

    private static final class MergingIterator<T> implements Iterator<T>, AutoCloseable {
        private final Stream<T> leftStream;
        private final Stream<T> rightStream;
        private final Iterator<T> left;
        private final Iterator<T> right;
        private final Comparator<? super T> comparator;
        private final AtomicBoolean closed = new AtomicBoolean();
        private T leftValue;
        private T rightValue;
        private boolean leftReady;
        private boolean rightReady;
        private boolean leftEnded;
        private boolean rightEnded;

        MergingIterator(Stream<T> leftStream, Stream<T> rightStream,
                        Comparator<? super T> comparator) {
            this.leftStream = leftStream;
            this.rightStream = rightStream;
            this.left = leftStream.iterator();
            this.right = rightStream.iterator();
            this.comparator = comparator;
        }

        @Override
        public boolean hasNext() {
            if (closed.get())
                return false;
            try {
                loadLeft();
                loadRight();
                boolean available = leftReady || rightReady;
                if (!available)
                    close();
                return available;
            } catch (RuntimeException | Error error) {
                close();
                throw error;
            }
        }

        @Override
        public T next() {
            if (!hasNext())
                throw new NoSuchElementException();
            if (!leftReady) {
                T value = rightValue;
                rightValue = null;
                rightReady = false;
                return value;
            }
            if (!rightReady) {
                T value = leftValue;
                leftValue = null;
                leftReady = false;
                return value;
            }

            int compared = comparator.compare(leftValue, rightValue);
            if (compared <= 0) {
                T value = leftValue;
                leftValue = null;
                leftReady = false;
                if (compared == 0) {
                    // The left (preferred) tier wins the overlap.
                    rightValue = null;
                    rightReady = false;
                }
                return value;
            }
            T value = rightValue;
            rightValue = null;
            rightReady = false;
            return value;
        }

        private void loadLeft() {
            if (!leftReady && !leftEnded) {
                if (left.hasNext()) {
                    leftValue = left.next();
                    leftReady = true;
                } else {
                    leftEnded = true;
                }
            }
        }

        private void loadRight() {
            if (!rightReady && !rightEnded) {
                if (right.hasNext()) {
                    rightValue = right.next();
                    rightReady = true;
                } else {
                    rightEnded = true;
                }
            }
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true))
                return;
            try {
                leftStream.close();
            } finally {
                rightStream.close();
            }
            leftValue = null;
            rightValue = null;
            leftReady = false;
            rightReady = false;
        }
    }

    private final class MutationFilteringIterator<T> implements Iterator<T>, AutoCloseable {
        private static final int LOOKUP_BATCH_SIZE = 1_000;

        private final Stream<T> source;
        private final Iterator<T> sourceIterator;
        private final ToLongFunction<T> idExtractor;
        private final ArrayDeque<T> ready = new ArrayDeque<>();
        private final AtomicBoolean closed = new AtomicBoolean();
        private boolean sourceEnded;

        MutationFilteringIterator(Stream<T> source, ToLongFunction<T> idExtractor) {
            this.source = source;
            this.sourceIterator = source.iterator();
            this.idExtractor = idExtractor;
        }

        @Override
        public boolean hasNext() {
            if (closed.get())
                return false;
            try {
                while (ready.isEmpty() && !sourceEnded)
                    fillPage();
                if (ready.isEmpty())
                    close();
                return !ready.isEmpty();
            } catch (RuntimeException | Error error) {
                close();
                throw error;
            }
        }

        private void fillPage() {
            List<T> page = new ArrayList<>(LOOKUP_BATCH_SIZE);
            List<Long> ids = new ArrayList<>(LOOKUP_BATCH_SIZE);
            while (page.size() < LOOKUP_BATCH_SIZE && sourceIterator.hasNext()) {
                T value = sourceIterator.next();
                page.add(value);
                ids.add(idExtractor.applyAsLong(value));
            }
            if (page.size() < LOOKUP_BATCH_SIZE)
                sourceEnded = true;
            Set<Long> mutated = archiveCatalog.mutatedObservationIds(ids);
            for (int i = 0; i < page.size(); i++) {
                if (!mutated.contains(ids.get(i)))
                    ready.addLast(page.get(i));
            }
        }

        @Override
        public T next() {
            if (!hasNext())
                throw new NoSuchElementException();
            return ready.removeFirst();
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true))
                return;
            ready.clear();
            source.close();
        }
    }

    private static final class ClosingLimitIterator<T> implements Iterator<T>, AutoCloseable {
        private final Stream<T> source;
        private final Iterator<T> iterator;
        private final long limit;
        private final AtomicBoolean closed = new AtomicBoolean();
        private long emitted;

        ClosingLimitIterator(Stream<T> source, long limit) {
            this.source = source;
            this.iterator = source.iterator();
            this.limit = limit;
            if (limit == 0)
                close();
        }

        @Override
        public boolean hasNext() {
            if (closed.get() || emitted >= limit)
                return false;
            try {
                boolean available = iterator.hasNext();
                if (!available)
                    close();
                return available;
            } catch (RuntimeException | Error error) {
                close();
                throw error;
            }
        }

        @Override
        public T next() {
            if (!hasNext())
                throw new NoSuchElementException();
            T value = iterator.next();
            emitted++;
            if (emitted >= limit)
                close();
            return value;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true))
                source.close();
        }
    }

    private boolean supportsMetadataScan(ObsFilter filter) {
        return filter.getCQLFilter() == null && filter.getValuePredicate() == null &&
                filter.getPhenomenonLocation() == null && filter.getLimit() == Long.MAX_VALUE &&
                (filter.getPhenomenonTime() == null ||
                        (!filter.getPhenomenonTime().isLatestTime() && !filter.getPhenomenonTime().isCurrentTime())) &&
                (filter.getResultTime() == null ||
                        (!filter.getResultTime().isLatestTime() && !filter.getResultTime().isCurrentTime()));
    }

    private MetadataScanPlan prepareMetadataScan(ObsFilter filter) {
        Set<Long> dataStreams = resolveDataStreams(filter.getDataStreamFilter());
        Set<Long> fois = resolveFois(filter.getFoiFilter());
        ArchiveBounds bounds = archiveBounds(filter);
        Long oneId = filter.getInternalIDs() != null && filter.getInternalIDs().size() == 1
                ? filter.getInternalIDs().first().getIdAsLong() : null;
        List<ArchiveGeneration> candidates = archiveCatalog.candidates(bounds.start(), bounds.end(), oneId);
        return new MetadataScanPlan(filter, dataStreams, fois, candidates);
    }

    /**
     * Emits the logical hot/cold union as signed metadata contributions.
     * Positive/negative pairs remove the intentional grace overlap without
     * retaining millions of IDs. Sparse overlays/tombstones are then applied
     * as a correction layer. includeRange is false for rows whose base time
     * was superseded, so reported min/max ranges remain exact.
     */
    private void scanMetadata(MetadataScanPlan plan, MetadataVisitor visitor) {
        scanHotMetadata(plan.filter(), hot -> {
            visitor.visit(hot.metadata(), 1, true);
            if (hot.coldOverlap())
                visitor.visit(hot.metadata(), -1, false);
        });

        archiveReader.forEachMetadata(plan.candidates(), plan.filter(), plan.dataStreams(), plan.fois(),
                Long.MAX_VALUE, row -> {
                    ObservationMetadata metadata = fromArchived(row);
                    visitor.visit(metadata, 1, true);
                });
    }

    private void scanHotMetadata(ObsFilter filter, Consumer<HotMetadata> consumer) {
        ObsFilter unbounded = ObsFilter.Builder.from(filter).withLimit(Long.MAX_VALUE).build();
        Set<ObsField> fields = Set.of(ObsField.DATASTREAM_ID, ObsField.FOI_ID,
                ObsField.PHENOMENON_TIME, ObsField.RESULT_TIME);
        ParameterizedQuery selected = queryBuilder.createParameterizedSelectEntriesQuery(unbounded, fields);
        String table = getDatastoreName();
        String sql = "SELECT matched.id, matched.datastreamid, matched.foiid, matched.phenomenontime, " +
                "matched.resulttime, EXISTS (SELECT 1 FROM oscar_archive_generation generation " +
                "WHERE generation.id=marker.archivegenerationid " +
                "AND generation.store_key='" + getDatastoreName() + "' " +
                "AND generation.id_scope=" + idScope + " " +
                "AND generation.state IN ('ACTIVE','SEALED')) AS cold_overlap " +
                "FROM (" + selected.sql() + ") matched JOIN " + table + " marker ON marker.id=matched.id";
        try (Connection connection = connectionManager.getConnection()) {
            connection.setReadOnly(true);
            connection.setAutoCommit(false); // PostgreSQL cursor fetch requires a transaction
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                selected.bind(statement);
                statement.setFetchSize(STREAM_FETCH_SIZE);
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) {
                        Long foi = result.getObject(3, Long.class);
                        Instant phenomenon = UtcJdbcTime.getInstant(result, 4);
                        Instant resultTime = UtcJdbcTime.getInstant(result, 5);
                        consumer.accept(new HotMetadata(new ObservationMetadata(
                                result.getLong(1), idScope, result.getLong(2),
                                foi == null ? null : idScope, foi,
                                phenomenon,
                                resultTime), result.getBoolean(6)));
                    }
                }
            }
            connection.rollback();
        } catch (Exception e) {
            throw new IllegalStateException("Cannot scan hot observation metadata", e);
        }
    }

    private ObservationMetadata fromArchived(ParquetArchiveReader.ArchivedMetadataRow row) {
        return new ObservationMetadata(row.id(), row.dataStreamScope(), row.dataStreamId(),
                row.foiScope(), row.foiId(), row.phenomenonTime(), row.resultTime());
    }

    @Override
    public Stream<ObsStats> getStatistics(ObsStatsQuery query) {
        if (!archiveConfig.enabled)
            return super.getStatistics(query);
        ObsFilter filter = query.getObsFilter() == null ? selectAllFilter() : query.getObsFilter();
        StatsAccumulator accumulator = new StatsAccumulator(query, filter);

        if (supportsMetadataScan(filter) && !archiveCatalog.hasObservationMutations()) {
            MetadataScanPlan plan = prepareMetadataScan(filter);
            scanMetadata(plan, accumulator::accept);
            accumulator.prepareHistogram();
            if (query.getHistogramBinSize() != null)
                scanMetadata(plan, accumulator::acceptHistogram);
        } else {
            // Arbitrary value/spatial predicates and latest/current selection
            // require normal OSH observation semantics. Reduce each payload
            // immediately rather than retaining an unbounded cold result set.
            try (Stream<Entry<BigId, IObsData>> entries = selectEntries(filter)) {
                entries.map(entry -> fromObservation(entry.getKey(), entry.getValue()))
                        .forEach(metadata -> accumulator.accept(metadata, 1, true));
            }
            accumulator.prepareHistogram();
            if (query.getHistogramBinSize() != null) {
                // A histogram needs the first pass's exact range. Re-read the
                // lazy union for the second pass; this trades bounded I/O for
                // bounded heap instead of retaining every observation.
                try (Stream<Entry<BigId, IObsData>> entries = selectEntries(filter)) {
                    entries.map(entry -> fromObservation(entry.getKey(), entry.getValue()))
                            .forEach(metadata -> accumulator.acceptHistogram(metadata, 1, false));
                }
            }
        }
        return accumulator.results().stream().limit(query.getLimit());
    }

    private ObservationMetadata fromObservation(BigId id, IObsData observation) {
        return new ObservationMetadata(id.getIdAsLong(), observation.getDataStreamID().getScope(),
                observation.getDataStreamID().getIdAsLong(),
                observation.hasFoi() ? observation.getFoiID().getScope() : null,
                observation.hasFoi() ? observation.getFoiID().getIdAsLong() : null,
                observation.getPhenomenonTime(), observation.getResultTime());
    }

    private final class StatsAccumulator {
        private static final int MAX_HISTOGRAM_BINS = 5_000_000;

        private final ObsStatsQuery query;
        private final ObsFilter filter;
        private final Map<StatsKey, StatsBucket> buckets = new HashMap<>();
        private long histogramSeconds;

        StatsAccumulator(ObsStatsQuery query, ObsFilter filter) {
            this.query = query;
            this.filter = filter;
        }

        void accept(ObservationMetadata metadata, long delta, boolean includeRange) {
            StatsKey key = new StatsKey(metadata.dataStreamId(),
                    query.isAggregateFois() ? null : metadata.foiId());
            StatsBucket bucket = buckets.computeIfAbsent(key,
                    ignored -> new StatsBucket(metadata.dataStreamScope(), metadata.foiScope()));
            bucket.count = Math.addExact(bucket.count, delta);
            if (includeRange)
                bucket.includeRange(metadata.phenomenonTime(), metadata.resultTime());
        }

        void prepareHistogram() {
            buckets.entrySet().removeIf(entry -> {
                if (entry.getValue().count < 0)
                    throw new IllegalStateException("Negative tiered statistics count for " + entry.getKey());
                return entry.getValue().count == 0;
            });
            Duration size = query.getHistogramBinSize();
            if (size == null)
                return;
            histogramSeconds = size.getSeconds();
            if (histogramSeconds <= 0)
                throw new IllegalArgumentException("Observation histogram bin size must be at least one second");

            Instant requestedStart = boundedMinimum(filter);
            Instant requestedEnd = boundedMaximum(filter);
            long totalBins = 0;
            for (StatsBucket bucket : buckets.values()) {
                if (bucket.minimumPhenomenon == null)
                    continue;
                bucket.histogramOrigin = requestedStart != null ? requestedStart : bucket.minimumPhenomenon;
                Instant end = requestedEnd != null ? requestedEnd : bucket.maximumPhenomenon;
                if (end.isBefore(bucket.histogramOrigin))
                    continue;
                Duration span = Duration.between(bucket.histogramOrigin, end);
                double spanSeconds = span.getSeconds() + span.getNano() / 1_000_000_000.0;
                long bins = Math.max(1L, (long)Math.ceil(spanSeconds / histogramSeconds));
                if (bins > MAX_HISTOGRAM_BINS)
                    throw new IllegalArgumentException("Observation histogram would require " + bins +
                            " bins; choose a larger bin size");
                totalBins = Math.addExact(totalBins, bins);
                if (totalBins > MAX_HISTOGRAM_BINS)
                    throw new IllegalArgumentException("Observation histograms would require " + totalBins +
                            " total bins; narrow the query or choose a larger bin size");
                bucket.histogram = new long[(int)bins];
            }
        }

        void acceptHistogram(ObservationMetadata metadata, long delta, boolean ignored) {
            StatsKey key = new StatsKey(metadata.dataStreamId(),
                    query.isAggregateFois() ? null : metadata.foiId());
            StatsBucket bucket = buckets.get(key);
            if (bucket == null || bucket.histogram == null || metadata.phenomenonTime() == null)
                return;
            long seconds = Duration.between(bucket.histogramOrigin, metadata.phenomenonTime()).getSeconds();
            if (seconds < 0)
                return;
            long index = seconds / histogramSeconds;
            if (index >= bucket.histogram.length)
                index = bucket.histogram.length - 1L; // inclusive upper query bound
            bucket.histogram[(int)index] = Math.addExact(bucket.histogram[(int)index], delta);
        }

        List<ObsStats> results() {
            List<Map.Entry<StatsKey, StatsBucket>> ordered = buckets.entrySet().stream()
                    .sorted(Comparator.comparingLong((Map.Entry<StatsKey, StatsBucket> entry) ->
                                    entry.getKey().dataStreamId())
                            .thenComparing(entry -> entry.getKey().foiId(),
                                    Comparator.nullsFirst(Comparator.naturalOrder())))
                    .toList();
            List<ObsStats> results = new ArrayList<>(ordered.size());
            for (Map.Entry<StatsKey, StatsBucket> entry : ordered) {
                StatsKey key = entry.getKey();
                StatsBucket bucket = entry.getValue();
                ObsStats.Builder builder = new ObsStats.Builder()
                        .withDataStreamID(BigId.fromLong(bucket.dataStreamScope, key.dataStreamId()))
                        .withTotalObsCount(bucket.count);
                if (!query.isAggregateFois()) {
                    FeatureId foi = key.foiId() == null ? FeatureId.NULL_FEATURE :
                            new FeatureId(BigId.fromLong(
                                    bucket.foiScope == null ? idScope : bucket.foiScope, key.foiId()),
                                    "urn:foi:unknown");
                    builder.withFoiID(foi);
                }
                if (bucket.minimumPhenomenon != null)
                    builder.withPhenomenonTimeRange(timeExtent(bucket.minimumPhenomenon, bucket.maximumPhenomenon));
                if (bucket.minimumResult != null)
                    builder.withResultTimeRange(timeExtent(bucket.minimumResult, bucket.maximumResult));
                if (bucket.minimumPhenomenon == null && bucket.minimumResult == null)
                    throw new IllegalStateException("Observation statistics row has no time value");
                if (bucket.histogram != null) {
                    int[] histogram = new int[bucket.histogram.length];
                    for (int i = 0; i < histogram.length; i++) {
                        if (bucket.histogram[i] < 0)
                            throw new IllegalStateException("Negative tiered observation histogram count");
                        histogram[i] = (int)Math.min(Integer.MAX_VALUE, bucket.histogram[i]);
                    }
                    builder.withObsCountByTime(histogram);
                }
                results.add(builder.build());
            }
            return results;
        }

        private Instant boundedMinimum(ObsFilter filter) {
            if (filter.getPhenomenonTime() == null)
                return null;
            Instant minimum = filter.getPhenomenonTime().getMin();
            return minimum == null || Instant.MIN.equals(minimum) ? null : minimum;
        }

        private Instant boundedMaximum(ObsFilter filter) {
            if (filter.getPhenomenonTime() == null)
                return null;
            Instant maximum = filter.getPhenomenonTime().getMax();
            return maximum == null || Instant.MAX.equals(maximum) ? null : maximum;
        }
    }

    private static final class StatsBucket {
        final int dataStreamScope;
        final Integer foiScope;
        long count;
        Instant minimumPhenomenon;
        Instant maximumPhenomenon;
        Instant minimumResult;
        Instant maximumResult;
        Instant histogramOrigin;
        long[] histogram;

        StatsBucket(int dataStreamScope, Integer foiScope) {
            this.dataStreamScope = dataStreamScope;
            this.foiScope = foiScope;
        }

        void includeRange(Instant phenomenon, Instant result) {
            if (phenomenon != null) {
                if (minimumPhenomenon == null || phenomenon.isBefore(minimumPhenomenon))
                    minimumPhenomenon = phenomenon;
                if (maximumPhenomenon == null || phenomenon.isAfter(maximumPhenomenon))
                    maximumPhenomenon = phenomenon;
            }
            if (result != null) {
                if (minimumResult == null || result.isBefore(minimumResult))
                    minimumResult = result;
                if (maximumResult == null || result.isAfter(maximumResult))
                    maximumResult = result;
            }
        }
    }

    private static TimeExtent timeExtent(Instant minimum, Instant maximum) {
        return minimum.equals(maximum) ? TimeExtent.instant(minimum) : TimeExtent.period(minimum, maximum);
    }

    @FunctionalInterface
    private interface MetadataVisitor {
        void visit(ObservationMetadata metadata, long delta, boolean includeRange);
    }

    private record ArchiveBounds(Instant start, Instant end) {}
    private record HotMetadata(ObservationMetadata metadata, boolean coldOverlap) {}
    private record ObservationMetadata(long id, int dataStreamScope, long dataStreamId,
                                       Integer foiScope, Long foiId, Instant phenomenonTime,
                                       Instant resultTime) {}
    private record StatsKey(long dataStreamId, Long foiId) {}
    private record MetadataScanPlan(ObsFilter filter, Set<Long> dataStreams, Set<Long> fois,
                                    List<ArchiveGeneration> candidates) {}

    @Override
    public IObsData get(Object key) {
        if (!(key instanceof BigId id) || !archiveConfig.enabled)
            return super.get(key);
        if (archiveCatalog.isTombstoned(id.getIdAsLong()))
            return null;
        IObsData hot = super.get(key);
        java.util.Optional<ArchiveCatalog.ArchivedOverlay> overlay = archiveCatalog.overlayWithGeneration(
                id.getIdAsLong(), idScope, "OVERLAY");
        if (overlay.isPresent())
            return toObservation(overlay.get().generationId(), overlay.get().schemaJson(),
                    overlay.get().observation());
        if (hot != null)
            return hot;
        ObsFilter filter = new ObsFilter.Builder().withInternalIDs(id).build();
        return readCold(filter).orElse(null);
    }

    @Override
    public IObsData put(BigId key, IObsData observation) {
        if (!archiveConfig.enabled)
            return super.put(key, observation);
        flushPendingForMutation("update");

        OverlayPayload overlay = prepareOverlay(key, observation);
        try (Connection connection = connectionManager.getConnection()) {
            boolean oldAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                long id = key.getIdAsLong();
                archiveCatalog.lockForMutation(connection, id);
                java.util.Optional<LockedLogicalObservation> locked =
                        loadLockedLogicalObservation(connection, id);
                if (locked.isEmpty())
                    throw new UnsupportedOperationException(
                            "Put can only be used to update existing entries");

                LockedLogicalObservation previous = locked.get();
                if (previous.generationId() != null) {
                    // The grace-period hot row is the immutable parity source.
                    // Logical updates live only in the generation-owned sparse
                    // overlay until lifecycle deletion removes that hot base.
                    archiveCatalog.upsertOverlay(connection, previous.generationId(),
                            overlay.observation(), overlay.schemaJson());
                } else {
                    updateLiveObservation(connection, key, observation,
                            overlay.observation().resultJson());
                }
                connection.commit();
                return previous.observation();
            } catch (Exception error) {
                connection.rollback();
                throw error;
            } finally {
                connection.setAutoCommit(oldAutoCommit);
            }
        } catch (UnsupportedOperationException error) {
            throw error;
        } catch (Exception error) {
            throw new IllegalStateException("Cannot atomically update tiered observation " + key, error);
        }
    }

    private OverlayPayload prepareOverlay(BigId key, IObsData observation) {
        IDataStreamInfo schema = dataStreamStore.get(new DataStreamKey(observation.getDataStreamID()));
        if (schema == null)
            throw new IllegalStateException("Cannot mutate archived observation without a live datastream schema");
        String resultJson = SerializerUtils.writeDataBlockToJson(
                schema.getRecordStructure(), schema.getRecordEncoding(), observation.getResult());
        try {
            return new OverlayPayload(new ArchivedObservation(
                    idScope, key.getIdAsLong(), observation.getDataStreamID().getScope(), observation.getDataStreamID().getIdAsLong(),
                    observation.hasFoi() ? observation.getFoiID().getScope() : null,
                    observation.hasFoi() ? observation.getFoiID().getIdAsLong() : null,
                    observation.getPhenomenonTime(), observation.getResultTime(), resultJson, "OVERLAY"),
                    SerializerUtils.writeIDataStreamInfoToJson(schema));
        } catch (IOException e) {
            throw new IllegalStateException("Cannot snapshot archived overlay datastream schema", e);
        }
    }

    private void updateLiveObservation(Connection connection, BigId key, IObsData observation,
                                       String resultJson) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(
                queryBuilder.updateByIdQuery() + " AND archivegenerationid IS NULL")) {
            statement.setLong(1, observation.getDataStreamID().getIdAsLong());
            if (observation.hasFoi())
                statement.setLong(2, observation.getFoiID().getIdAsLong());
            else
                statement.setNull(2, Types.BIGINT);
            if (observation.getPhenomenonTime() == null)
                statement.setNull(3, Types.TIMESTAMP);
            else
                UtcJdbcTime.setInstant(statement, 3, observation.getPhenomenonTime());
            if (observation.getResultTime() == null)
                statement.setNull(4, Types.TIMESTAMP);
            else
                UtcJdbcTime.setInstant(statement, 4, observation.getResultTime());
            PGobject json = new PGobject();
            json.setType("json");
            json.setValue(resultJson);
            statement.setObject(5, json);
            statement.setLong(6, key.getIdAsLong());
            if (statement.executeUpdate() != 1)
                throw new IllegalStateException(
                        "Live observation became archive-owned during update: " + key);
        }
    }

    private java.util.Optional<UUID> findArchiveGeneration(Connection connection,
                                                            long observationId) throws Exception {
        List<ArchiveGeneration> candidates = archiveCatalog.candidates(
                connection, MIN_ARCHIVE_TIME, MAX_ARCHIVE_TIME, observationId);
        if (candidates.isEmpty())
            return java.util.Optional.empty();
        ObsFilter filter = new ObsFilter.Builder()
                .withInternalIDs(BigId.fromLong(idScope, observationId)).build();
        UUID[] generation = {null};
        archiveReader.forEachMetadata(candidates, filter, null, null, 1,
                row -> generation[0] = row.generation().id());
        return java.util.Optional.ofNullable(generation[0]);
    }

    @Override
    public IObsData remove(Object key) {
        if (!(key instanceof BigId id) || !archiveConfig.enabled)
            return super.remove(key);
        flushPendingForMutation("remove");
        try (Connection connection = connectionManager.getConnection()) {
            boolean oldAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                long observationId = id.getIdAsLong();
                archiveCatalog.lockForMutation(connection, observationId);
                java.util.Optional<LockedLogicalObservation> locked =
                        loadLockedLogicalObservation(connection, observationId);
                if (locked.isEmpty()) {
                    connection.commit();
                    return null;
                }

                LockedLogicalObservation previous = locked.get();
                if (previous.generationId() != null) {
                    // Keep the immutable hot base through parity and grace
                    // deletion; the exact-generation tombstone is the logical
                    // deletion.
                    archiveCatalog.tombstoneIfAbsent(connection, observationId,
                            previous.generationId(), "Removed through IObsStore",
                            "USER_OR_SYSTEM_DELETE");
                } else {
                    try (PreparedStatement delete = connection.prepareStatement(
                            "DELETE FROM " + getDatastoreName() +
                                    " WHERE id=? AND archivegenerationid IS NULL")) {
                        delete.setLong(1, observationId);
                        if (delete.executeUpdate() != 1)
                            throw new IllegalStateException(
                                    "Live observation became archive-owned during removal: " + id);
                    }
                }
                connection.commit();
                return previous.observation();
            } catch (Exception error) {
                connection.rollback();
                throw error;
            } finally {
                connection.setAutoCommit(oldAutoCommit);
            }
        } catch (Exception error) {
            throw new IllegalStateException("Cannot atomically remove tiered observation " + id, error);
        }
    }

    private void flushPendingForMutation(String operation) {
        try {
            commit();
        } catch (DataStoreException error) {
            throw new IllegalStateException(
                    "Cannot flush observations before tiered " + operation, error);
        }
    }

    @Override
    public long removeEntries(ObsFilter filter) {
        if (!archiveConfig.enabled)
            return super.removeEntries(filter);

        if ((filter.getPhenomenonTime() != null && filter.getPhenomenonTime().isLatestTime()) ||
                (filter.getResultTime() != null && filter.getResultTime().isLatestTime())) {
            throw new UnsupportedOperationException("Latest-time filters do not support REMOVE operations");
        }
        if (filter.getLimit() == 0)
            return 0;

        long removed = 0;
        List<Long> previousStalledBatch = null;
        while (removed < filter.getLimit()) {
            long remaining = filter.getLimit() == Long.MAX_VALUE
                    ? REMOVE_BATCH_SIZE : filter.getLimit() - removed;
            long batchLimit = Math.min(REMOVE_BATCH_SIZE, remaining);
            ObsFilter candidateFilter = ObsFilter.Builder.from(filter)
                    .withLimit(batchLimit)
                    .build();
            List<BigId> candidates;
            try (Stream<BigId> stream = selectKeys(candidateFilter)) {
                candidates = stream.toList();
            }
            if (candidates.isEmpty())
                break;

            // Test subclasses use this hook to reproduce a mutation in the
            // former select-then-delete race window. Production does nothing.
            beforeRemoveBatchRevalidation(List.copyOf(candidates));
            long batchRemoved = removeRevalidatedBatch(filter, candidates);
            removed = Math.addExact(removed, batchRemoved);

            // A short page whose every logical row was removed proves that
            // the selection was exhausted at this operation's linearization
            // point. Avoid a redundant post-tombstone tier merge (and another
            // full archive scan) merely to rediscover an empty result.
            if (batchRemoved == candidates.size() && candidates.size() < batchLimit)
                break;

            List<Long> batchIds = candidates.stream()
                    .map(BigId::getIdAsLong)
                    .distinct()
                    .sorted()
                    .toList();
            if (batchRemoved == 0 && batchIds.equals(previousStalledBatch)) {
                throw new IllegalStateException(
                        "Tiered observation removal made no progress after predicate revalidation");
            }
            previousStalledBatch = batchRemoved == 0 ? batchIds : null;
        }
        return removed;
    }

    /** Visible only to deterministic concurrency tests. */
    protected void beforeRemoveBatchRevalidation(List<BigId> candidateIds) {
        // no-op
    }

    /** Invoked after mutation locks are held, for deterministic race tests. */
    protected void afterRemoveBatchLocked(List<BigId> candidateIds) {
        // no-op
    }

    private long removeRevalidatedBatch(ObsFilter filter, List<BigId> candidates) {
        List<BigId> ordered = candidates.stream()
                .distinct()
                .sorted(Comparator.comparingLong(BigId::getIdAsLong))
                .toList();
        try (Connection connection = connectionManager.getConnection()) {
            boolean oldAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                archiveCatalog.lockForMutation(connection, ordered.stream()
                        .map(BigId::getIdAsLong)
                        .toList());
                afterRemoveBatchLocked(ordered);
                // Resolve relational predicates again after the observation
                // locks instead of trusting the earlier candidate page. The
                // PostgreSQL datastream and FOI stores own independent pools,
                // so these lookups cannot recursively borrow this observation
                // connection. Their metadata transactions remain independent;
                // archive/observation mutation atomicity is provided by the
                // store and per-ID locks above, not by a cross-store snapshot.
                Set<Long> dataStreams = resolveDataStreams(filter.getDataStreamFilter());
                Set<Long> fois = resolveFois(filter.getFoiFilter());
                long removed = 0;
                for (BigId key : ordered) {
                    long id = key.getIdAsLong();
                    java.util.Optional<LockedLogicalObservation> hot =
                            lockHotLogicalObservation(connection, id);
                    if (hot.isPresent()) {
                        LockedLogicalObservation current = hot.get();
                        if (!matchesLockedObservation(filter, dataStreams, fois, id, current)) {
                            continue;
                        }
                        if (current.generationId() != null) {
                            if (archiveCatalog.tombstoneIfAbsent(connection, id,
                                    current.generationId(), "Removed through IObsStore filter",
                                    "USER_OR_SYSTEM_DELETE")) {
                                removed = Math.addExact(removed, 1L);
                            }
                        } else {
                            try (PreparedStatement delete = connection.prepareStatement(
                                    "DELETE FROM " + getDatastoreName() +
                                            " WHERE id=? AND archivegenerationid IS NULL")) {
                                delete.setLong(1, id);
                                if (delete.executeUpdate() == 1)
                                    removed = Math.addExact(removed, 1L);
                            }
                        }
                        continue;
                    }

                    // Once hot rows have aged out, an overlay is the only
                    // mutable representation. Lock and recheck it just like a
                    // hot row. An unmodified Parquet base is immutable, so its
                    // original candidate match remains valid.
                    java.util.Optional<LockedLogicalObservation> overlay =
                            lockColdOverlay(connection, id);
                    if (overlay.isPresent()) {
                        LockedLogicalObservation current = overlay.get();
                        if (!matchesLockedObservation(filter, dataStreams, fois, id, current)) {
                            continue;
                        }
                        if (archiveCatalog.tombstoneIfAbsent(connection, id,
                                current.generationId(), "Removed through IObsStore filter",
                                "USER_OR_SYSTEM_DELETE")) {
                            removed = Math.addExact(removed, 1L);
                        }
                        continue;
                    }

                    java.util.Optional<UUID> generation = findArchiveGeneration(connection, id);
                    if (generation.isPresent() && archiveCatalog.tombstoneIfAbsent(
                            connection, id, generation.get(), "Removed through IObsStore filter",
                            "USER_OR_SYSTEM_DELETE")) {
                        removed = Math.addExact(removed, 1L);
                    }
                }
                connection.commit();
                return removed;
            } catch (Exception error) {
                connection.rollback();
                throw error;
            } finally {
                connection.setAutoCommit(oldAutoCommit);
            }
        } catch (Exception error) {
            throw new IllegalStateException("Cannot atomically remove tiered observations", error);
        }
    }

    private java.util.Optional<LockedLogicalObservation> lockHotLogicalObservation(
            Connection connection, long observationId) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT datastreamid, foiid, phenomenontime, resulttime, result::text, archivegenerationid " +
                        "FROM " + getDatastoreName() + " WHERE id=? FOR UPDATE")) {
            statement.setLong(1, observationId);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next())
                    return java.util.Optional.empty();
                long dataStreamId = result.getLong(1);
                Long foiId = result.getObject(2, Long.class);
                Instant phenomenonTime = UtcJdbcTime.getInstant(result, 3);
                Instant resultTime = UtcJdbcTime.getInstant(result, 4);
                String resultJson = result.getString(5);
                UUID generationId = result.getObject(6, UUID.class);

                if (generationId != null) {
                    java.util.Optional<LockedLogicalObservation> overlay =
                            lockOverlay(connection, observationId, generationId);
                    if (overlay.isPresent())
                        return overlay;
                }

                ArchivedObservation archived = new ArchivedObservation(
                        idScope, observationId, idScope, dataStreamId,
                        foiId == null ? null : idScope, foiId,
                        phenomenonTime, resultTime, resultJson, "HOT");
                IObsData observation = generationId == null
                        ? decodeLiveObservation(archived)
                        : toObservation(connection, generationId, null, archived);
                return java.util.Optional.of(new LockedLogicalObservation(
                        generationId, resultJson, phenomenonTime, resultTime, observation));
            }
        }
    }

    private java.util.Optional<LockedLogicalObservation> lockColdOverlay(
            Connection connection, long observationId) throws Exception {
        return lockOverlay(connection, observationId, null);
    }

    private java.util.Optional<LockedLogicalObservation> lockOverlay(
            Connection connection, long observationId, UUID expectedGeneration) throws Exception {
        String generationClause = expectedGeneration == null ? "" : " AND overlay_row.generation_id=?";
        String sql = "SELECT overlay_row.generation_id, overlay_row.datastream_id, overlay_row.foi_id, " +
                "overlay_row.phenomenon_time, overlay_row.result_time, overlay_row.result_json::text, " +
                "overlay_row.schema_json::text FROM oscar_obs_overlay overlay_row " +
                "JOIN oscar_archive_generation generation ON generation.id=overlay_row.generation_id " +
                "WHERE overlay_row.store_key=? AND overlay_row.id_scope=? AND overlay_row.observation_id=? " +
                "AND generation.store_key=overlay_row.store_key AND generation.id_scope=overlay_row.id_scope " +
                "AND generation.state IN ('ACTIVE','SEALED')" + generationClause +
                " FOR UPDATE OF overlay_row";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, getDatastoreName());
            statement.setInt(2, idScope);
            statement.setLong(3, observationId);
            if (expectedGeneration != null)
                statement.setObject(4, expectedGeneration);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next())
                    return java.util.Optional.empty();
                UUID generationId = result.getObject(1, UUID.class);
                long dataStreamId = result.getLong(2);
                Long foiId = result.getObject(3, Long.class);
                Instant phenomenonTime = UtcJdbcTime.getInstant(result, 4);
                Instant resultTime = UtcJdbcTime.getInstant(result, 5);
                String resultJson = result.getString(6);
                String schemaJson = result.getString(7);
                ArchivedObservation archived = new ArchivedObservation(
                        idScope, observationId, idScope, dataStreamId,
                        foiId == null ? null : idScope, foiId,
                        phenomenonTime, resultTime, resultJson, "OVERLAY");
                return java.util.Optional.of(new LockedLogicalObservation(
                        generationId, resultJson, phenomenonTime, resultTime,
                        toObservation(connection, generationId, schemaJson, archived)));
            }
        }
    }

    private java.util.Optional<LockedLogicalObservation> loadLockedLogicalObservation(
            Connection connection, long observationId) throws Exception {
        if (archiveCatalog.isTombstoned(connection, observationId))
            return java.util.Optional.empty();

        java.util.Optional<LockedLogicalObservation> hot =
                lockHotLogicalObservation(connection, observationId);
        if (hot.isPresent())
            return hot;

        java.util.Optional<LockedLogicalObservation> overlay =
                lockColdOverlay(connection, observationId);
        if (overlay.isPresent())
            return overlay;

        List<ArchiveGeneration> generations = archiveCatalog.candidates(
                connection, MIN_ARCHIVE_TIME, MAX_ARCHIVE_TIME, observationId);
        if (generations.isEmpty())
            return java.util.Optional.empty();
        ObsFilter filter = new ObsFilter.Builder()
                .withInternalIDs(BigId.fromLong(idScope, observationId))
                .build();
        List<ParquetArchiveReader.ArchivedRow> rows =
                archiveReader.read(generations, filter, null, null, 1);
        if (rows.isEmpty())
            return java.util.Optional.empty();
        ParquetArchiveReader.ArchivedRow row = rows.get(0);
        ArchivedObservation archived = row.observation();
        return java.util.Optional.of(new LockedLogicalObservation(
                row.generation().id(), archived.resultJson(),
                archived.phenomenonTime(), archived.resultTime(),
                toObservation(connection, row.generation().id(), null, archived)));
    }

    private IObsData decodeLiveObservation(ArchivedObservation archived) {
        BigId dataStreamId = BigId.fromLong(idScope, archived.dataStreamId());
        IDataStreamInfo schema = dataStreamStore.get(new DataStreamKey(dataStreamId));
        if (schema == null)
            throw new IllegalStateException(
                    "Cannot revalidate observation without datastream schema " + archived.dataStreamId());
        DataBlock result = SerializerUtils.readDataBlockFromJson(
                schema.getRecordStructure(), archived.resultJson());
        ObsData.Builder builder = new ObsData.Builder()
                .withDataStream(dataStreamId)
                .withResult(result);
        if (archived.foiId() != null)
            builder.withFoi(BigId.fromLong(idScope, archived.foiId()));
        if (archived.phenomenonTime() != null)
            builder.withPhenomenonTime(archived.phenomenonTime());
        if (archived.resultTime() != null)
            builder.withResultTime(archived.resultTime());
        return builder.build();
    }

    private record LockedLogicalObservation(UUID generationId, String resultJson,
                                            Instant phenomenonTime, Instant resultTime,
                                            IObsData observation) {}
    private record OverlayPayload(ArchivedObservation observation, String schemaJson) {}

    @Override
    public long countMatchingEntries(ObsFilter filter) {
        if (!archiveConfig.enabled)
            return super.countMatchingEntries(filter);
        if (isUnfiltered(filter))
            return getNumRecords();

        ObsFilter unbounded = ObsFilter.Builder.from(filter).withLimit(Long.MAX_VALUE).build();
        if (supportsMetadataScan(unbounded) && !archiveCatalog.hasObservationMutations()) {
            long[] count = {0L};
            scanMetadata(prepareMetadataScan(unbounded), (metadata, delta, includeRange) ->
                    count[0] = Math.addExact(count[0], delta));
            if (count[0] < 0)
                throw new IllegalStateException("Negative tiered observation count");
            return filter.getLimit() == Long.MAX_VALUE ? count[0] : Math.min(count[0], filter.getLimit());
        }
        try (Stream<Entry<BigId, IObsData>> stream = selectEntries(filter)) {
            return stream.count();
        }
    }

    private boolean isUnfiltered(ObsFilter filter) {
        return filter.getInternalIDs() == null && filter.getPhenomenonTime() == null &&
                filter.getResultTime() == null && filter.getPhenomenonLocation() == null &&
                filter.getDataStreamFilter() == null && filter.getFoiFilter() == null &&
                filter.getValuePredicate() == null && filter.getCQLFilter() == null &&
                filter.getLimit() == Long.MAX_VALUE;
    }

    @Override
    public long getNumRecords() {
        if (!archiveConfig.enabled)
            return super.getNumRecords();
        return archiveCatalog.countTieredRecords(getDatastoreName());
    }
}

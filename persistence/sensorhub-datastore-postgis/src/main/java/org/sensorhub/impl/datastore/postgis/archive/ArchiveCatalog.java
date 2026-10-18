package org.sensorhub.impl.datastore.postgis.archive;

import org.sensorhub.api.common.BigId;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.obs.ObsFilter;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Transactional PostgreSQL catalog for immutable archive generations.
 *
 * <p>Object publication is manifest-last and cannot share a transaction with
 * PostgreSQL.  The explicit state machine makes every intermediate state
 * recoverable and prevents a partial file from becoming query-visible.</p>
 */
public final class ArchiveCatalog {
    public static final int SCHEMA_VERSION = 4;
    public static final String SCHEMA_CHECKSUM =
            "sha256:7aa2b6dd22cbed4499eb9fb16966323bbf90d14bff4d35e03a1670868fb2f8e8";
    private static final long MIGRATION_LOCK = 0x4f5343415253544fL;

    private final SqlConnectionProvider connections;
    private final String storeKey;
    private final int idScope;
    private final AtomicInteger openOverlayCursors = new AtomicInteger();
    private final AtomicLong overlayRowsRead = new AtomicLong();

    public ArchiveCatalog(SqlConnectionProvider connections, String storeKey, int idScope) {
        this.connections = connections;
        this.storeKey = safeIdentifier(storeKey);
        this.idScope = idScope;
    }

    public String storeKey() {
        return storeKey;
    }

    public int idScope() {
        return idScope;
    }

    public void migrate() {
        try (Connection connection = connections.open()) {
            boolean oldAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                statement.execute("SELECT pg_advisory_xact_lock(" + MIGRATION_LOCK + ")");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_storage_schema_version (" +
                        "version integer PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_archive_job (" +
                        "id uuid PRIMARY KEY, store_key text NOT NULL, id_scope integer NOT NULL, " +
                        "profile text NOT NULL, window_start timestamptz NOT NULL, " +
                        "window_end timestamptz NOT NULL, revision integer NOT NULL DEFAULT 1, state text NOT NULL, attempt integer NOT NULL DEFAULT 1, " +
                        "error text, created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now())");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_archive_generation (" +
                        "id uuid PRIMARY KEY, job_id uuid NOT NULL REFERENCES oscar_archive_job(id), " +
                        "store_key text NOT NULL, id_scope integer NOT NULL, profile text NOT NULL, " +
                        "window_start timestamptz NOT NULL, window_end timestamptz NOT NULL, state text NOT NULL, " +
                        "object_path text NOT NULL, manifest_path text NOT NULL, sha256 text NOT NULL, row_count bigint NOT NULL, " +
                        "min_id bigint NOT NULL, max_id bigint NOT NULL, parity_verified boolean NOT NULL DEFAULT false, " +
                        "activated_at timestamptz NOT NULL DEFAULT now(), sealed_at timestamptz)");
                statement.execute("ALTER TABLE oscar_archive_job ADD COLUMN IF NOT EXISTS revision integer NOT NULL DEFAULT 1");
                statement.execute("ALTER TABLE oscar_archive_job ADD COLUMN IF NOT EXISTS store_key text NOT NULL DEFAULT 'legacy'");
                statement.execute("ALTER TABLE oscar_archive_job ADD COLUMN IF NOT EXISTS id_scope integer NOT NULL DEFAULT -1");
                statement.execute("ALTER TABLE oscar_archive_job ALTER COLUMN store_key DROP DEFAULT");
                statement.execute("ALTER TABLE oscar_archive_job ALTER COLUMN id_scope DROP DEFAULT");
                statement.execute("ALTER TABLE oscar_archive_generation ADD COLUMN IF NOT EXISTS store_key text NOT NULL DEFAULT 'legacy'");
                statement.execute("ALTER TABLE oscar_archive_generation ADD COLUMN IF NOT EXISTS id_scope integer NOT NULL DEFAULT -1");
                statement.execute("ALTER TABLE oscar_archive_generation ALTER COLUMN store_key DROP DEFAULT");
                statement.execute("ALTER TABLE oscar_archive_generation ALTER COLUMN id_scope DROP DEFAULT");
                statement.execute("ALTER TABLE oscar_archive_job DROP CONSTRAINT IF EXISTS oscar_archive_job_profile_window_start_window_end_key");
                statement.execute("ALTER TABLE oscar_archive_job DROP CONSTRAINT IF EXISTS oscar_archive_job_profile_window_start_window_end_revision_key");
                statement.execute("ALTER TABLE oscar_archive_generation DROP CONSTRAINT IF EXISTS oscar_archive_generation_profile_window_start_window_end_key");
                statement.execute("DROP INDEX IF EXISTS oscar_archive_job_window_revision_idx");
                statement.execute("CREATE UNIQUE INDEX IF NOT EXISTS oscar_archive_job_window_revision_idx ON oscar_archive_job " +
                        "(store_key, id_scope, profile, window_start, window_end, revision)");
                statement.execute("DROP INDEX IF EXISTS oscar_archive_generation_time_idx");
                statement.execute("CREATE INDEX IF NOT EXISTS oscar_archive_generation_time_idx ON oscar_archive_generation " +
                        "(store_key, id_scope, profile, window_start, window_end) WHERE state IN ('ACTIVE','SEALED')");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_archive_object (" +
                        "generation_id uuid NOT NULL REFERENCES oscar_archive_generation(id) ON DELETE CASCADE, " +
                        "relative_path text NOT NULL, kind text NOT NULL, sha256 text NOT NULL, bytes bigint NOT NULL, " +
                        "state text NOT NULL DEFAULT 'ACTIVE', PRIMARY KEY(generation_id, relative_path))");
                // An immutable object may belong to exactly one generation.
                // This also prevents a future catalog bug from making one
                // generation's purge delete another generation's object.
                statement.execute("CREATE UNIQUE INDEX IF NOT EXISTS oscar_archive_object_relative_path_idx " +
                        "ON oscar_archive_object(relative_path)");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_archive_id_block (" +
                        "generation_id uuid NOT NULL REFERENCES oscar_archive_generation(id) ON DELETE CASCADE, " +
                        "min_id bigint NOT NULL, max_id bigint NOT NULL, PRIMARY KEY(generation_id, min_id))");
                statement.execute("CREATE INDEX IF NOT EXISTS oscar_archive_id_lookup_idx ON oscar_archive_id_block (min_id, max_id)");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_archive_schema (" +
                        "generation_id uuid NOT NULL REFERENCES oscar_archive_generation(id) ON DELETE CASCADE, " +
                        "datastream_id bigint NOT NULL, schema_json jsonb NOT NULL, PRIMARY KEY(generation_id, datastream_id))");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_event_locator (" +
                        "store_key text NOT NULL, id_scope integer NOT NULL, public_id text NOT NULL, " +
                        "observation_id bigint NOT NULL, generation_id uuid REFERENCES oscar_archive_generation(id), " +
                        "state text NOT NULL DEFAULT 'ACTIVE', updated_at timestamptz NOT NULL DEFAULT now(), " +
                        "PRIMARY KEY(store_key, id_scope, public_id))");
                statement.execute("ALTER TABLE oscar_event_locator ADD COLUMN IF NOT EXISTS store_key text NOT NULL DEFAULT 'legacy'");
                statement.execute("ALTER TABLE oscar_event_locator ADD COLUMN IF NOT EXISTS id_scope integer NOT NULL DEFAULT -1");
                statement.execute("ALTER TABLE oscar_event_locator ALTER COLUMN store_key DROP DEFAULT");
                statement.execute("ALTER TABLE oscar_event_locator ALTER COLUMN id_scope DROP DEFAULT");
                statement.execute("DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='oscar_event_locator'::regclass " +
                        "AND conname='oscar_event_locator_pkey' AND pg_get_constraintdef(oid) NOT LIKE '%store_key%') THEN " +
                        "ALTER TABLE oscar_event_locator DROP CONSTRAINT oscar_event_locator_pkey; " +
                        "ALTER TABLE oscar_event_locator ADD CONSTRAINT oscar_event_locator_pkey " +
                        "PRIMARY KEY(store_key, id_scope, public_id); END IF; END $$");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_obs_overlay (" +
                        "store_key text NOT NULL, id_scope integer NOT NULL, observation_id bigint NOT NULL, generation_id uuid NOT NULL " +
                        "REFERENCES oscar_archive_generation(id), datastream_id bigint NOT NULL, foi_id bigint, " +
                        "phenomenon_time timestamptz, result_time timestamptz, result_json jsonb NOT NULL, " +
                        "schema_json jsonb NOT NULL, " +
                        "updated_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(store_key, id_scope, observation_id))");
                statement.execute("ALTER TABLE oscar_obs_overlay ADD COLUMN IF NOT EXISTS store_key text NOT NULL DEFAULT 'legacy'");
                statement.execute("ALTER TABLE oscar_obs_overlay ADD COLUMN IF NOT EXISTS id_scope integer NOT NULL DEFAULT -1");
                statement.execute("ALTER TABLE oscar_obs_overlay ALTER COLUMN store_key DROP DEFAULT");
                statement.execute("ALTER TABLE oscar_obs_overlay ALTER COLUMN id_scope DROP DEFAULT");
                statement.execute("ALTER TABLE oscar_obs_overlay ADD COLUMN IF NOT EXISTS generation_id uuid " +
                        "REFERENCES oscar_archive_generation(id)");
                statement.execute("ALTER TABLE oscar_obs_overlay ADD COLUMN IF NOT EXISTS schema_json jsonb");
                statement.execute("UPDATE oscar_obs_overlay overlay_row SET schema_json=schema_row.schema_json " +
                        "FROM oscar_archive_schema schema_row WHERE overlay_row.schema_json IS NULL " +
                        "AND overlay_row.generation_id=schema_row.generation_id " +
                        "AND overlay_row.datastream_id=schema_row.datastream_id");
                // A pre-v4 overlay has no exact generation owner. Coarse ID
                // ranges are not ownership, so retain it for an operator-led
                // migration without allowing it to become query-visible.
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_obs_overlay_legacy_unowned (" +
                        "store_key text NOT NULL, id_scope integer NOT NULL, observation_id bigint NOT NULL, " +
                        "datastream_id bigint NOT NULL, foi_id bigint, phenomenon_time timestamptz, " +
                        "result_time timestamptz, result_json jsonb NOT NULL, schema_json jsonb, updated_at timestamptz, " +
                        "quarantined_at timestamptz NOT NULL DEFAULT now())");
                statement.execute("ALTER TABLE oscar_obs_overlay_legacy_unowned " +
                        "ADD COLUMN IF NOT EXISTS schema_json jsonb");
                statement.execute("INSERT INTO oscar_obs_overlay_legacy_unowned(" +
                        "store_key,id_scope,observation_id,datastream_id,foi_id,phenomenon_time,result_time,result_json,schema_json,updated_at) " +
                        "SELECT store_key,id_scope,observation_id,datastream_id,foi_id,phenomenon_time,result_time,result_json,schema_json,updated_at " +
                        "FROM oscar_obs_overlay WHERE generation_id IS NULL OR schema_json IS NULL");
                statement.execute("DELETE FROM oscar_obs_overlay WHERE generation_id IS NULL OR schema_json IS NULL");
                statement.execute("ALTER TABLE oscar_obs_overlay ALTER COLUMN generation_id SET NOT NULL");
                statement.execute("ALTER TABLE oscar_obs_overlay ALTER COLUMN schema_json SET NOT NULL");
                statement.execute("DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='oscar_obs_overlay'::regclass " +
                        "AND conname='oscar_obs_overlay_pkey' AND pg_get_constraintdef(oid)='PRIMARY KEY (observation_id)') THEN " +
                        "ALTER TABLE oscar_obs_overlay DROP CONSTRAINT oscar_obs_overlay_pkey; " +
                        "ALTER TABLE oscar_obs_overlay ADD CONSTRAINT oscar_obs_overlay_pkey " +
                        "PRIMARY KEY(store_key, id_scope, observation_id); END IF; END $$");
                statement.execute("CREATE INDEX IF NOT EXISTS oscar_obs_overlay_generation_idx " +
                        "ON oscar_obs_overlay(generation_id)");
                statement.execute("CREATE INDEX IF NOT EXISTS oscar_obs_overlay_query_idx " +
                        "ON oscar_obs_overlay(generation_id, phenomenon_time, observation_id)");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_retention_hold (" +
                        "id uuid PRIMARY KEY, store_key text NOT NULL, id_scope integer NOT NULL, " +
                        "subject_type text NOT NULL, subject_id text NOT NULL, reason text NOT NULL, " +
                        "created_by text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), " +
                        "released_at timestamptz, released_by text)");
                statement.execute("ALTER TABLE oscar_retention_hold ADD COLUMN IF NOT EXISTS released_by text");
                statement.execute("ALTER TABLE oscar_retention_hold ADD COLUMN IF NOT EXISTS store_key text NOT NULL DEFAULT 'legacy'");
                statement.execute("ALTER TABLE oscar_retention_hold ADD COLUMN IF NOT EXISTS id_scope integer NOT NULL DEFAULT -1");
                statement.execute("ALTER TABLE oscar_retention_hold ALTER COLUMN store_key DROP DEFAULT");
                statement.execute("ALTER TABLE oscar_retention_hold ALTER COLUMN id_scope DROP DEFAULT");
                statement.execute("DROP INDEX IF EXISTS oscar_retention_hold_active_idx");
                statement.execute("CREATE INDEX IF NOT EXISTS oscar_retention_hold_active_idx ON oscar_retention_hold " +
                        "(store_key, id_scope, subject_type, subject_id) WHERE released_at IS NULL");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_tombstone (" +
                        "store_key text NOT NULL, id_scope integer NOT NULL, subject_type text NOT NULL, " +
                        "subject_id text NOT NULL, deleted_at timestamptz NOT NULL DEFAULT now(), " +
                        "reason text NOT NULL, policy text NOT NULL, generation_id uuid " +
                        "REFERENCES oscar_archive_generation(id), PRIMARY KEY(subject_type, store_key, id_scope, subject_id))");
                statement.execute("ALTER TABLE oscar_tombstone ADD COLUMN IF NOT EXISTS store_key text NOT NULL DEFAULT 'legacy'");
                statement.execute("ALTER TABLE oscar_tombstone ADD COLUMN IF NOT EXISTS id_scope integer NOT NULL DEFAULT -1");
                statement.execute("ALTER TABLE oscar_tombstone ALTER COLUMN store_key DROP DEFAULT");
                statement.execute("ALTER TABLE oscar_tombstone ALTER COLUMN id_scope DROP DEFAULT");
                statement.execute("DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='oscar_tombstone'::regclass " +
                        "AND conname='oscar_tombstone_pkey' AND pg_get_constraintdef(oid)='PRIMARY KEY (subject_type, subject_id)') THEN " +
                        "ALTER TABLE oscar_tombstone DROP CONSTRAINT oscar_tombstone_pkey; " +
                        "ALTER TABLE oscar_tombstone ADD CONSTRAINT oscar_tombstone_pkey " +
                        "PRIMARY KEY(subject_type, store_key, id_scope, subject_id); END IF; END $$");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_tombstone_legacy_unowned (" +
                        "store_key text NOT NULL, id_scope integer NOT NULL, subject_type text NOT NULL, " +
                        "subject_id text NOT NULL, deleted_at timestamptz NOT NULL, reason text NOT NULL, " +
                        "policy text NOT NULL, quarantined_at timestamptz NOT NULL DEFAULT now())");
                statement.execute("INSERT INTO oscar_tombstone_legacy_unowned(" +
                        "store_key,id_scope,subject_type,subject_id,deleted_at,reason,policy) " +
                        "SELECT store_key,id_scope,subject_type,subject_id,deleted_at,reason,policy " +
                        "FROM oscar_tombstone WHERE subject_type='OBSERVATION' AND generation_id IS NULL");
                statement.execute("DELETE FROM oscar_tombstone " +
                        "WHERE subject_type='OBSERVATION' AND generation_id IS NULL");
                statement.execute("ALTER TABLE oscar_tombstone DROP CONSTRAINT IF EXISTS oscar_tombstone_observation_owner_check");
                statement.execute("ALTER TABLE oscar_tombstone ADD CONSTRAINT oscar_tombstone_observation_owner_check " +
                        "CHECK (subject_type <> 'OBSERVATION' OR generation_id IS NOT NULL)");
                statement.execute("CREATE INDEX IF NOT EXISTS oscar_tombstone_generation_idx " +
                        "ON oscar_tombstone(generation_id) WHERE subject_type='OBSERVATION'");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_media_manifest (" +
                        "event_id text NOT NULL, camera_uid text NOT NULL, lane_uid text NOT NULL, manifest_path text NOT NULL, " +
                        "state text NOT NULL, source_sha256 text, policy_version integer NOT NULL, legal_hold boolean NOT NULL DEFAULT false, " +
                        "updated_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(event_id, camera_uid))");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_media_asset (" +
                        "event_id text NOT NULL, camera_uid text NOT NULL, ordinal integer NOT NULL, asset_path text NOT NULL, " +
                        "mime_type text NOT NULL, sha256 text NOT NULL, bytes bigint NOT NULL, state text NOT NULL, " +
                        "PRIMARY KEY(event_id, camera_uid, ordinal), FOREIGN KEY(event_id, camera_uid) " +
                        "REFERENCES oscar_media_manifest(event_id, camera_uid) ON DELETE CASCADE)");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_backup_epoch (" +
                        "id uuid PRIMARY KEY, state text NOT NULL, postgres_lsn pg_lsn, archive_generation_ids uuid[] NOT NULL DEFAULT '{}', " +
                        "manifest_path text, sha256 text, created_at timestamptz NOT NULL DEFAULT now(), verified_at timestamptz)");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_archive_verification (" +
                        "generation_id uuid NOT NULL REFERENCES oscar_archive_generation(id) ON DELETE CASCADE, " +
                        "verification_type text NOT NULL, status text NOT NULL, expected_rows bigint, actual_rows bigint, " +
                        "detail text, verified_at timestamptz NOT NULL DEFAULT now(), " +
                        "PRIMARY KEY(generation_id, verification_type, verified_at))");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_deletion_plan (" +
                        "id uuid PRIMARY KEY, store_key text NOT NULL, id_scope integer NOT NULL, " +
                        "subject_type text NOT NULL, subject_id text NOT NULL, state text NOT NULL, " +
                        "estimated_rows bigint NOT NULL, estimated_bytes bigint NOT NULL, hold_count bigint NOT NULL, " +
                        "confirmation_hash text NOT NULL, requested_by text NOT NULL, confirmed_by text, " +
                        "created_at timestamptz NOT NULL DEFAULT now(), expires_at timestamptz NOT NULL, confirmed_at timestamptz)");
                statement.execute("ALTER TABLE oscar_deletion_plan ADD COLUMN IF NOT EXISTS store_key text NOT NULL DEFAULT 'legacy'");
                statement.execute("ALTER TABLE oscar_deletion_plan ADD COLUMN IF NOT EXISTS id_scope integer NOT NULL DEFAULT -1");
                statement.execute("ALTER TABLE oscar_deletion_plan ALTER COLUMN store_key DROP DEFAULT");
                statement.execute("ALTER TABLE oscar_deletion_plan ALTER COLUMN id_scope DROP DEFAULT");
                statement.execute("CREATE INDEX IF NOT EXISTS oscar_deletion_plan_scope_idx ON oscar_deletion_plan " +
                        "(store_key, id_scope, state, created_at)");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_deletion_plan_item (" +
                        "plan_id uuid NOT NULL REFERENCES oscar_deletion_plan(id) ON DELETE CASCADE, " +
                        "generation_id uuid NOT NULL REFERENCES oscar_archive_generation(id), " +
                        "state text NOT NULL DEFAULT 'PLANNED', detail text, updated_at timestamptz NOT NULL DEFAULT now(), " +
                        "PRIMARY KEY(plan_id, generation_id))");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_deletion_ledger (" +
                        "id bigserial PRIMARY KEY, plan_id uuid NOT NULL REFERENCES oscar_deletion_plan(id), " +
                        "store_key text NOT NULL, id_scope integer NOT NULL, subject_type text NOT NULL, " +
                        "subject_id text NOT NULL, object_path text, outcome text NOT NULL, " +
                        "detail text, recorded_at timestamptz NOT NULL DEFAULT now())");
                statement.execute("ALTER TABLE oscar_deletion_ledger ADD COLUMN IF NOT EXISTS store_key text NOT NULL DEFAULT 'legacy'");
                statement.execute("ALTER TABLE oscar_deletion_ledger ADD COLUMN IF NOT EXISTS id_scope integer NOT NULL DEFAULT -1");
                statement.execute("ALTER TABLE oscar_deletion_ledger ALTER COLUMN store_key DROP DEFAULT");
                statement.execute("ALTER TABLE oscar_deletion_ledger ALTER COLUMN id_scope DROP DEFAULT");
                statement.execute("CREATE INDEX IF NOT EXISTS oscar_deletion_ledger_scope_idx ON oscar_deletion_ledger " +
                        "(store_key, id_scope, recorded_at)");

                // Pre-v4 catalogs supported only one observation store. The
                // first tiered store opened after upgrade adopts that legacy
                // namespace; subsequent stores start clean and isolated.
                String namespace = "store_key='" + storeKey + "', id_scope=" + idScope;
                statement.execute("UPDATE oscar_archive_job SET " + namespace +
                        " WHERE store_key='legacy' AND id_scope=-1");
                statement.execute("UPDATE oscar_archive_generation generation SET " + namespace +
                        " FROM oscar_archive_job job WHERE generation.job_id=job.id " +
                        "AND generation.store_key='legacy' AND generation.id_scope=-1 " +
                        "AND job.store_key='" + storeKey + "' AND job.id_scope=" + idScope);
                statement.execute("UPDATE oscar_obs_overlay overlay_row SET " + namespace +
                        " FROM oscar_archive_generation generation WHERE overlay_row.generation_id=generation.id " +
                        "AND overlay_row.store_key='legacy' AND overlay_row.id_scope=-1 " +
                        "AND generation.store_key='" + storeKey + "' AND generation.id_scope=" + idScope);
                statement.execute("UPDATE oscar_tombstone tombstone SET " + namespace +
                        " FROM oscar_archive_generation generation WHERE tombstone.generation_id=generation.id " +
                        "AND tombstone.store_key='legacy' AND tombstone.id_scope=-1 " +
                        "AND generation.store_key='" + storeKey + "' AND generation.id_scope=" + idScope);
                statement.execute("UPDATE oscar_event_locator locator SET " + namespace +
                        " FROM oscar_archive_generation generation WHERE locator.generation_id=generation.id " +
                        "AND locator.store_key='legacy' AND locator.id_scope=-1 " +
                        "AND generation.store_key='" + storeKey + "' AND generation.id_scope=" + idScope);
                statement.execute("UPDATE oscar_retention_hold SET " + namespace +
                        " WHERE store_key='legacy' AND id_scope=-1");
                statement.execute("UPDATE oscar_deletion_plan SET " + namespace +
                        " WHERE store_key='legacy' AND id_scope=-1");
                statement.execute("UPDATE oscar_deletion_ledger ledger SET " + namespace +
                        " FROM oscar_deletion_plan plan WHERE ledger.plan_id=plan.id " +
                        "AND ledger.store_key='legacy' AND ledger.id_scope=-1 " +
                        "AND plan.store_key='" + storeKey + "' AND plan.id_scope=" + idScope);

                try (PreparedStatement select = connection.prepareStatement(
                        "SELECT checksum FROM oscar_storage_schema_version WHERE version = ?")) {
                    select.setInt(1, SCHEMA_VERSION);
                    try (ResultSet result = select.executeQuery()) {
                        if (result.next() && !SCHEMA_CHECKSUM.equals(result.getString(1)))
                            throw new IllegalStateException("Storage schema migration checksum mismatch for version " + SCHEMA_VERSION);
                    }
                }
                try (PreparedStatement insert = connection.prepareStatement(
                        "INSERT INTO oscar_storage_schema_version(version, checksum) VALUES (?, ?) ON CONFLICT (version) DO NOTHING")) {
                    insert.setInt(1, SCHEMA_VERSION);
                    insert.setString(2, SCHEMA_CHECKSUM);
                    insert.executeUpdate();
                }
                connection.commit();
            } catch (Exception e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(oldAutoCommit);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Cannot migrate OSCAR archive catalog", e);
        }
    }

    public Optional<UUID> claimJob(String profile, Instant start, Instant end) {
        try (Connection connection = connections.open()) {
            boolean oldAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                String lockKey = storeKey + '|' + idScope + '|' + profile + '|' + start + '|' + end;
                try (PreparedStatement lock = connection.prepareStatement(
                        "SELECT pg_advisory_xact_lock(hashtextextended(?, 0))")) {
                    lock.setString(1, lockKey);
                    lock.execute();
                }

                UUID previousId = null;
                String previousState = null;
                int revision = 0;
                boolean stale = false;
                try (PreparedStatement latest = connection.prepareStatement(
                        "SELECT id, revision, state, updated_at < now()-interval '2 hours' stale " +
                                "FROM oscar_archive_job WHERE store_key=? AND id_scope=? " +
                                "AND profile=? AND window_start=? AND window_end=? " +
                                "ORDER BY revision DESC LIMIT 1")) {
                    latest.setString(1, storeKey);
                    latest.setInt(2, idScope);
                    latest.setString(3, profile);
                    UtcJdbcTime.setInstant(latest, 4, start);
                    UtcJdbcTime.setInstant(latest, 5, end);
                    try (ResultSet result = latest.executeQuery()) {
                        if (result.next()) {
                            previousId = result.getObject(1, UUID.class);
                            revision = result.getInt(2);
                            previousState = result.getString(3);
                            stale = result.getBoolean(4);
                        }
                    }
                }

                if ("EXPORTING".equals(previousState) && !stale) {
                    connection.rollback();
                    return Optional.empty();
                }
                if (shouldRetryJob(previousState, stale)) {
                    try (PreparedStatement retry = connection.prepareStatement(
                            "UPDATE oscar_archive_job SET state='EXPORTING', error=NULL, attempt=attempt+1, updated_at=now() " +
                                    "WHERE id=? AND store_key=? AND id_scope=?")) {
                        retry.setObject(1, previousId);
                        retry.setString(2, storeKey);
                        retry.setInt(3, idScope);
                        retry.executeUpdate();
                    }
                    connection.commit();
                    return Optional.of(previousId);
                }

                UUID id = UUID.randomUUID();
                try (PreparedStatement insert = connection.prepareStatement(
                        "INSERT INTO oscar_archive_job(id, store_key, id_scope, profile, window_start, window_end, revision, state) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, 'EXPORTING')")) {
                    insert.setObject(1, id);
                    insert.setString(2, storeKey);
                    insert.setInt(3, idScope);
                    insert.setString(4, profile);
                    UtcJdbcTime.setInstant(insert, 5, start);
                    UtcJdbcTime.setInstant(insert, 6, end);
                    insert.setInt(7, nextRevision(revision));
                    insert.executeUpdate();
                }
                connection.commit();
                return Optional.of(id);
            } catch (Exception e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(oldAutoCommit);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Cannot claim archive window", e);
        }
    }

    public void activate(UUID jobId, ArchiveGeneration generation, Path parquetPath,
                         Path manifestPath, long bytes,
                         String manifestSha256, long manifestBytes, Path schemaPath, long schemaBytes,
                         String schemaSha256, List<SchemaSnapshot> schemas,
                         String observationTable, String dataStreamTable, List<String> outputNames) {
        try (Connection connection = connections.open()) {
            boolean oldAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                String observations = safeIdentifier(observationTable);
                String dataStreams = safeIdentifier(dataStreamTable);
                stageExportedRows(connection, parquetPath, generation);
                try (PreparedStatement mark = connection.prepareStatement(
                        "UPDATE " + observations + " o SET archivegenerationid=? " +
                                "FROM oscar_archive_activation_row exported, " + dataStreams + " d " +
                                "WHERE o.id=exported.observation_id AND d.id=o.datastreamid " +
                                "AND o.archivegenerationid IS NULL " +
                                "AND o.datastreamid=exported.datastream_id " +
                                "AND o.foiid IS NOT DISTINCT FROM exported.foi_id " +
                                "AND o.phenomenontime IS NOT DISTINCT FROM exported.phenomenon_time " +
                                "AND o.resulttime IS NOT DISTINCT FROM exported.result_time " +
                                "AND o.result IS NOT DISTINCT FROM exported.result_json " +
                                "AND o.phenomenontime>=? AND o.phenomenontime<? " +
                                "AND d.data->>'outputName'=ANY (?)")) {
                    mark.setObject(1, generation.id());
                    UtcJdbcTime.setInstant(mark, 2, generation.start());
                    UtcJdbcTime.setInstant(mark, 3, generation.end());
                    mark.setArray(4, connection.createArrayOf("text", outputNames.toArray()));
                    int marked = mark.executeUpdate();
                    if (marked != generation.rowCount())
                        throw new IllegalStateException("Archive snapshot changed before activation: expected " +
                                generation.rowCount() + " exact rows but marked " + marked);
                }
                try (PreparedStatement statement = connection.prepareStatement(
                        "INSERT INTO oscar_archive_generation(id, job_id, store_key, id_scope, profile, window_start, window_end, state, " +
                                "object_path, manifest_path, sha256, row_count, min_id, max_id) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?, ?, ?, ?, ?)")) {
                    statement.setObject(1, generation.id());
                    statement.setObject(2, jobId);
                    statement.setString(3, storeKey);
                    statement.setInt(4, idScope);
                    statement.setString(5, generation.profile());
                    UtcJdbcTime.setInstant(statement, 6, generation.start());
                    UtcJdbcTime.setInstant(statement, 7, generation.end());
                    statement.setString(8, generation.objectPath().toString());
                    statement.setString(9, manifestPath.toString());
                    statement.setString(10, generation.sha256());
                    statement.setLong(11, generation.rowCount());
                    statement.setLong(12, generation.minimumId());
                    statement.setLong(13, generation.maximumId());
                    statement.executeUpdate();
                }
                try (PreparedStatement statement = connection.prepareStatement(
                        "INSERT INTO oscar_archive_object(generation_id, relative_path, kind, sha256, bytes) VALUES (?, ?, ?, ?, ?)")) {
                    statement.setObject(1, generation.id());
                    statement.setString(2, generation.objectPath().toString());
                    statement.setString(3, "OBSERVATIONS_PARQUET");
                    statement.setString(4, generation.sha256());
                    statement.setLong(5, bytes);
                    statement.executeUpdate();
                    statement.setObject(1, generation.id());
                    statement.setString(2, manifestPath.toString());
                    statement.setString(3, "GENERATION_MANIFEST");
                    statement.setString(4, manifestSha256);
                    statement.setLong(5, manifestBytes);
                    statement.executeUpdate();
                    statement.setObject(1, generation.id());
                    statement.setString(2, schemaPath.toString());
                    statement.setString(3, "DATASTREAM_SCHEMAS");
                    statement.setString(4, schemaSha256);
                    statement.setLong(5, schemaBytes);
                    statement.executeUpdate();
                }
                try (PreparedStatement statement = connection.prepareStatement(
                        "INSERT INTO oscar_archive_id_block(generation_id, min_id, max_id) VALUES (?, ?, ?)")) {
                    statement.setObject(1, generation.id());
                    statement.setLong(2, generation.minimumId());
                    statement.setLong(3, generation.maximumId());
                    statement.executeUpdate();
                }
                try (PreparedStatement statement = connection.prepareStatement(
                        "INSERT INTO oscar_archive_schema(generation_id, datastream_id, schema_json) VALUES (?, ?, ?::jsonb)")) {
                    for (SchemaSnapshot schema : schemas) {
                        statement.setObject(1, generation.id());
                        statement.setLong(2, schema.dataStreamId());
                        statement.setString(3, schema.json());
                        statement.addBatch();
                    }
                    statement.executeBatch();
                }
                try (PreparedStatement statement = connection.prepareStatement(
                        "UPDATE oscar_archive_job SET state='ACTIVE', updated_at=now() " +
                                "WHERE id=? AND store_key=? AND id_scope=?")) {
                    statement.setObject(1, jobId);
                    statement.setString(2, storeKey);
                    statement.setInt(3, idScope);
                    statement.executeUpdate();
                }
                connection.commit();
            } catch (Exception e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(oldAutoCommit);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Cannot activate archive generation", e);
        }
    }

    /**
     * Streams the immutable export back into a transaction-local relation.
     * Activation can then compare every mutable source value instead of
     * treating a coarse ID/time range and a matching count as row ownership.
     * Keeping the ledger in PostgreSQL also lets the final UPDATE lock and
     * recheck each live tuple atomically under normal READ COMMITTED rules.
     */
    private void stageExportedRows(Connection postgres, Path parquetPath,
                                   ArchiveGeneration generation) throws Exception {
        try (Statement create = postgres.createStatement()) {
            create.execute("CREATE TEMP TABLE oscar_archive_activation_row (" +
                    "observation_id bigint PRIMARY KEY, datastream_id bigint NOT NULL, foi_id bigint, " +
                    "phenomenon_time timestamp, result_time timestamp, result_json jsonb) ON COMMIT DROP");
        }

        String parquet = parquetPath.toAbsolutePath().normalize().toString().replace("'", "''");
        String select = "SELECT id_scope, id_value, datastream_scope, datastream_id, foi_scope, foi_id, " +
                "phenomenon_time, result_time, result_json, archive_version, profile " +
                "FROM read_parquet('" + parquet + "')";
        String insert = "INSERT INTO oscar_archive_activation_row(" +
                "observation_id,datastream_id,foi_id,phenomenon_time,result_time,result_json) " +
                "VALUES (?,?,?,?::timestamp,?::timestamp,?::jsonb)";
        long count = 0;
        long minimumId = Long.MAX_VALUE;
        long maximumId = Long.MIN_VALUE;
        try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
             Statement read = duck.createStatement();
             ResultSet rows = read.executeQuery(select);
             PreparedStatement stage = postgres.prepareStatement(insert)) {
            while (rows.next()) {
                int exportedIdScope = rows.getInt(1);
                long observationId = rows.getLong(2);
                int exportedDataStreamScope = rows.getInt(3);
                long dataStreamId = rows.getLong(4);
                Integer foiScope = rows.getObject(5, Integer.class);
                Long foiId = rows.getObject(6, Long.class);
                int archiveVersion = rows.getInt(10);
                String profile = rows.getString(11);
                if (exportedIdScope != idScope || exportedDataStreamScope != idScope ||
                        (foiId == null ? foiScope != null : foiScope == null || foiScope != idScope) ||
                        archiveVersion != 1 || !generation.profile().equals(profile))
                    throw new IllegalStateException("Archive activation ledger metadata does not match its generation");

                stage.setLong(1, observationId);
                stage.setLong(2, dataStreamId);
                if (foiId == null)
                    stage.setNull(3, Types.BIGINT);
                else
                    stage.setLong(3, foiId);
                var phenomenonTime = UtcJdbcTime.getDuckDbInstant(rows, 7);
                if (phenomenonTime == null)
                    stage.setNull(4, Types.TIMESTAMP);
                else
                    UtcJdbcTime.setInstant(stage, 4, phenomenonTime);
                var resultTime = UtcJdbcTime.getDuckDbInstant(rows, 8);
                if (resultTime == null)
                    stage.setNull(5, Types.TIMESTAMP);
                else
                    UtcJdbcTime.setInstant(stage, 5, resultTime);
                String resultJson = rows.getString(9);
                if (resultJson == null)
                    stage.setNull(6, Types.VARCHAR);
                else
                    stage.setString(6, resultJson);
                stage.addBatch();

                count++;
                minimumId = Math.min(minimumId, observationId);
                maximumId = Math.max(maximumId, observationId);
                if (count % 5_000 == 0)
                    stage.executeBatch();
            }
            stage.executeBatch();
        }
        if (count != generation.rowCount() || minimumId != generation.minimumId() ||
                maximumId != generation.maximumId())
            throw new IllegalStateException("Archive activation ledger count/range does not match its generation");
    }

    public void failJob(UUID jobId, Throwable error) {
        String message = error == null ? "unknown failure" : String.valueOf(error.getMessage());
        if (message.length() > 4000)
            message = message.substring(0, 4000);
        try (Connection connection = connections.open(); PreparedStatement statement = connection.prepareStatement(
                "UPDATE oscar_archive_job SET state='FAILED', error=?, updated_at=now() " +
                        "WHERE id=? AND store_key=? AND id_scope=?")) {
            statement.setString(1, message);
            statement.setObject(2, jobId);
            statement.setString(3, storeKey);
            statement.setInt(4, idScope);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot record archive failure", e);
        }
    }

    /** True only after activation has committed the generation catalog row. */
    public boolean isGenerationRegistered(UUID generationId) {
        try (Connection connection = connections.open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT 1 FROM oscar_archive_generation WHERE id=? AND store_key=? AND id_scope=?")) {
            statement.setObject(1, generationId);
            statement.setString(2, storeKey);
            statement.setInt(3, idScope);
            try (ResultSet result = statement.executeQuery()) {
                return result.next();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot inspect archive generation registration", e);
        }
    }

    static boolean shouldRetryJob(String state, boolean stale) {
        return "FAILED".equals(state) || ("EXPORTING".equals(state) && stale);
    }

    static int nextRevision(int previousRevision) {
        return Math.incrementExact(previousRevision);
    }

    public List<ArchiveGeneration> candidates(Instant start, Instant end, Long observationId) {
        String sql = "SELECT DISTINCT g.id, g.profile, g.window_start, g.window_end, g.object_path, g.sha256, " +
                "g.row_count, g.min_id, g.max_id, g.state, g.parity_verified FROM oscar_archive_generation g ";
        if (observationId != null)
            sql += "JOIN oscar_archive_id_block b ON b.generation_id=g.id ";
        sql += "WHERE g.store_key=? AND g.id_scope=? AND g.state IN ('ACTIVE','SEALED') " +
                "AND g.window_start < ? AND g.window_end > ? ";
        if (observationId != null)
            sql += "AND ? BETWEEN b.min_id AND b.max_id ";
        sql += "ORDER BY g.window_start, g.id";

        List<ArchiveGeneration> generations = new ArrayList<>();
        try (Connection connection = connections.open(); PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            UtcJdbcTime.setInstant(statement, 3, end);
            UtcJdbcTime.setInstant(statement, 4, start);
            if (observationId != null)
                statement.setLong(5, observationId);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    generations.add(new ArchiveGeneration(
                            result.getObject(1, UUID.class), result.getString(2), UtcJdbcTime.getInstant(result, 3),
                            UtcJdbcTime.getInstant(result, 4), Path.of(result.getString(5)), result.getString(6),
                            result.getLong(7), result.getLong(8), result.getLong(9), result.getString(10), result.getBoolean(11)));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot plan archive query", e);
        }
        return generations;
    }

    public Optional<Instant> latestWindowEnd(String profile) {
        try (Connection connection = connections.open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT max(window_end) FROM oscar_archive_generation WHERE store_key=? AND id_scope=? " +
                        "AND profile=? AND state IN ('ACTIVE','SEALED')")) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            statement.setString(3, profile);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) return Optional.empty();
                Instant value = UtcJdbcTime.getInstant(result, 1);
                return value == null ? Optional.empty() : Optional.of(value);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot inspect archive high-water mark", e);
        }
    }

    /**
     * Conservative observation high-water mark across every query-visible
     * archive profile in this store namespace. A window end can be later than
     * its last observation, which is intentional: it only causes a bounded
     * cold identity probe for an out-of-order add and never skips one.
     */
    public Optional<Instant> latestWindowEnd() {
        try (Connection connection = connections.open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT max(window_end) FROM oscar_archive_generation " +
                        "WHERE store_key=? AND id_scope=? AND state IN ('ACTIVE','SEALED')")) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) return Optional.empty();
                Instant value = UtcJdbcTime.getInstant(result, 1);
                return value == null ? Optional.empty() : Optional.of(value);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot inspect archive high-water mark", e);
        }
    }

    public void recordVerification(UUID generationId, String type, boolean success,
                                   long expectedRows, long actualRows, String detail) {
        try (Connection connection = connections.open()) {
            boolean oldAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try (PreparedStatement receipt = connection.prepareStatement(
                    "INSERT INTO oscar_archive_verification(generation_id, verification_type, status, expected_rows, actual_rows, detail) " +
                            "SELECT generation.id, ?, ?, ?, ?, ? FROM oscar_archive_generation generation " +
                            "WHERE generation.id=? AND generation.store_key=? AND generation.id_scope=?");
                 PreparedStatement generation = connection.prepareStatement(
                         "UPDATE oscar_archive_generation SET parity_verified=? " +
                                 "WHERE id=? AND store_key=? AND id_scope=?")) {
                receipt.setString(1, type);
                receipt.setString(2, success ? "PASSED" : "FAILED");
                receipt.setLong(3, expectedRows);
                receipt.setLong(4, actualRows);
                receipt.setString(5, detail);
                receipt.setObject(6, generationId);
                receipt.setString(7, storeKey);
                receipt.setInt(8, idScope);
                if (receipt.executeUpdate() != 1)
                    throw new IllegalArgumentException("Archive generation does not belong to this observation store: " + generationId);
                if ("HOT_COLD_PARITY".equals(type)) {
                    generation.setBoolean(1, success);
                    generation.setObject(2, generationId);
                    generation.setString(3, storeKey);
                    generation.setInt(4, idScope);
                    if (generation.executeUpdate() != 1)
                        throw new IllegalStateException("Archive generation disappeared while recording parity: " + generationId);
                }
                connection.commit();
            } catch (Exception e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(oldAutoCommit);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Cannot record archive verification", e);
        }
    }

    public List<ArchiveGeneration> readyForHotDeletion(Instant activatedBefore) {
        List<ArchiveGeneration> generations = new ArrayList<>();
        String sql = "SELECT g.id, g.profile, g.window_start, g.window_end, g.object_path, g.sha256, " +
                "g.row_count, g.min_id, g.max_id, g.state, g.parity_verified " +
                "FROM oscar_archive_generation g WHERE g.store_key=? AND g.id_scope=? " +
                "AND g.state='ACTIVE' AND g.parity_verified=true " +
                "AND g.activated_at <= ? AND NOT EXISTS (SELECT 1 FROM oscar_retention_hold h " +
                "WHERE h.store_key=g.store_key AND h.id_scope=g.id_scope AND h.released_at IS NULL " +
                "AND h.subject_type='ARCHIVE_GENERATION' AND h.subject_id=g.id::text) " +
                "ORDER BY g.activated_at LIMIT 10";
        try (Connection connection = connections.open(); PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            UtcJdbcTime.setInstant(statement, 3, activatedBefore);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    generations.add(new ArchiveGeneration(result.getObject(1, UUID.class), result.getString(2),
                            UtcJdbcTime.getInstant(result, 3), UtcJdbcTime.getInstant(result, 4),
                            Path.of(result.getString(5)), result.getString(6), result.getLong(7), result.getLong(8),
                            result.getLong(9), result.getString(10), result.getBoolean(11)));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot list generations ready for hot deletion", e);
        }
        return generations;
    }

    public List<ArchiveGeneration> unverifiedGenerations(int limit) {
        List<ArchiveGeneration> generations = new ArrayList<>();
        String sql = "SELECT id, profile, window_start, window_end, object_path, sha256, row_count, min_id, max_id, state, parity_verified " +
                "FROM oscar_archive_generation WHERE store_key=? AND id_scope=? " +
                        "AND state='ACTIVE' AND parity_verified=false ORDER BY activated_at LIMIT ?";
        try (Connection connection = connections.open(); PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            statement.setInt(3, limit);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    generations.add(new ArchiveGeneration(result.getObject(1, UUID.class), result.getString(2),
                            UtcJdbcTime.getInstant(result, 3), UtcJdbcTime.getInstant(result, 4), Path.of(result.getString(5)),
                            result.getString(6), result.getLong(7), result.getLong(8), result.getLong(9),
                            result.getString(10), result.getBoolean(11)));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot list unverified archive generations", e);
        }
        return generations;
    }

    public void markSealed(UUID generationId) {
        try (Connection connection = connections.open(); PreparedStatement statement = connection.prepareStatement(
                "UPDATE oscar_archive_generation SET state='SEALED', sealed_at=now() " +
                        "WHERE id=? AND store_key=? AND id_scope=? AND state='ACTIVE' AND parity_verified=true")) {
            statement.setObject(1, generationId);
            statement.setString(2, storeKey);
            statement.setInt(3, idScope);
            if (statement.executeUpdate() != 1)
                throw new IllegalStateException("Generation is not eligible to seal: " + generationId);
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot seal archive generation", e);
        }
    }

    public boolean hasObservationHold(long observationId) {
        try (Connection connection = connections.open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT 1 FROM oscar_retention_hold WHERE store_key=? AND id_scope=? AND released_at IS NULL " +
                        "AND subject_type='OBSERVATION' AND subject_id=?")) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            statement.setString(3, Long.toString(observationId));
            try (ResultSet result = statement.executeQuery()) {
                return result.next();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot inspect observation hold", e);
        }
    }

    public Optional<String> schemaJson(UUID generationId, long dataStreamId) {
        try (Connection connection = connections.open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT schema.schema_json::text FROM oscar_archive_schema schema " +
                        "JOIN oscar_archive_generation generation ON generation.id=schema.generation_id " +
                        "WHERE schema.generation_id=? AND schema.datastream_id=? " +
                        "AND generation.store_key=? AND generation.id_scope=?")) {
            statement.setObject(1, generationId);
            statement.setLong(2, dataStreamId);
            statement.setString(3, storeKey);
            statement.setInt(4, idScope);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(result.getString(1)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot load archived datastream schema", e);
        }
    }

    public Optional<ArchivedObservation> overlay(long observationId, int scope, String profile) {
        return overlayWithGeneration(observationId, scope, profile).map(ArchivedOverlay::observation);
    }

    public Optional<ArchivedOverlay> overlayWithGeneration(long observationId, int scope, String profile) {
        try (Connection connection = connections.open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT overlay_row.generation_id, datastream_id, foi_id, phenomenon_time, result_time, " +
                        "result_json::text, schema_json::text " +
                        "FROM oscar_obs_overlay overlay_row JOIN oscar_archive_generation generation " +
                        "ON generation.id=overlay_row.generation_id " +
                        "WHERE overlay_row.store_key=? AND overlay_row.id_scope=? AND observation_id=? " +
                        "AND generation.store_key=overlay_row.store_key AND generation.id_scope=overlay_row.id_scope " +
                        "AND generation.state IN ('ACTIVE','SEALED')")) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            statement.setLong(3, observationId);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next())
                    return Optional.empty();
                UUID generationId = result.getObject(1, UUID.class);
                Long foi = result.getObject(3, Long.class);
                Instant phenomenon = UtcJdbcTime.getInstant(result, 4);
                Instant resultTime = UtcJdbcTime.getInstant(result, 5);
                ArchivedObservation observation = new ArchivedObservation(
                        scope, observationId, scope, result.getLong(2),
                        foi == null ? null : scope, foi, phenomenon,
                        resultTime == null ? null : resultTime, result.getString(6), profile);
                return Optional.of(new ArchivedOverlay(generationId, result.getString(7), observation));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot read archive overlay", e);
        }
    }

    public void upsertOverlay(UUID generationId, ArchivedObservation observation, String schemaJson) {
        if (schemaJson == null || schemaJson.isBlank())
            throw new IllegalArgumentException("Archived observation overlays require an immutable schema snapshot");
        try (Connection connection = connections.open()) {
            upsertOverlay(connection, generationId, observation, schemaJson);
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot update archived observation overlay", e);
        }
    }

    /**
     * Transaction-scoped overlay upsert used when ingestion discovers that a
     * natural-key correction targets an immutable generation-owned hot row.
     * The caller owns commit/rollback and connection lifecycle.
     */
    public void upsertOverlay(Connection connection, UUID generationId,
                              ArchivedObservation observation, String schemaJson) throws SQLException {
        if (schemaJson == null || schemaJson.isBlank())
            throw new IllegalArgumentException("Archived observation overlays require an immutable schema snapshot");
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO oscar_obs_overlay(store_key, id_scope, observation_id, generation_id, datastream_id, foi_id, phenomenon_time, result_time, result_json, schema_json) " +
                        "SELECT ?, ?, ?, generation.id, ?, ?, ?, ?, ?::jsonb, ?::jsonb " +
                        "FROM oscar_archive_generation generation WHERE generation.id=? " +
                        "AND generation.store_key=? AND generation.id_scope=? " +
                        "AND generation.state IN ('ACTIVE','SEALED') " +
                        "ON CONFLICT (store_key, id_scope, observation_id) DO UPDATE SET " +
                        "generation_id=EXCLUDED.generation_id, datastream_id=EXCLUDED.datastream_id, " +
                        "foi_id=EXCLUDED.foi_id, phenomenon_time=EXCLUDED.phenomenon_time, " +
                        "result_time=EXCLUDED.result_time, result_json=EXCLUDED.result_json, " +
                        "schema_json=EXCLUDED.schema_json, updated_at=now()")) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            statement.setLong(3, observation.id());
            statement.setLong(4, observation.dataStreamId());
            if (observation.foiId() == null) statement.setNull(5, java.sql.Types.BIGINT); else statement.setLong(5, observation.foiId());
            if (observation.phenomenonTime() == null) statement.setNull(6, java.sql.Types.TIMESTAMP_WITH_TIMEZONE); else UtcJdbcTime.setInstant(statement, 6, observation.phenomenonTime());
            if (observation.resultTime() == null) statement.setNull(7, java.sql.Types.TIMESTAMP_WITH_TIMEZONE); else UtcJdbcTime.setInstant(statement, 7, observation.resultTime());
            statement.setString(8, observation.resultJson());
            statement.setString(9, schemaJson);
            statement.setObject(10, generationId);
            statement.setString(11, storeKey);
            statement.setInt(12, idScope);
            if (statement.executeUpdate() != 1)
                throw new IllegalArgumentException("Archive generation does not belong to this observation store: " + generationId);
        }
    }

    public boolean isTombstoned(long observationId) {
        try (Connection connection = connections.open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT 1 FROM oscar_tombstone tombstone " +
                        "WHERE tombstone.store_key=? AND tombstone.id_scope=? " +
                        "AND subject_type='OBSERVATION' AND subject_id=? AND generation_id IS NOT NULL " +
                        "AND EXISTS (SELECT 1 FROM oscar_archive_generation generation " +
                        "WHERE generation.id=tombstone.generation_id AND generation.store_key=tombstone.store_key " +
                        "AND generation.id_scope=tombstone.id_scope AND generation.state IN ('ACTIVE','SEALED'))")) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            statement.setString(3, Long.toString(observationId));
            try (ResultSet result = statement.executeQuery()) {
                return result.next();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot read archive tombstone", e);
        }
    }

    public boolean hasObservationMutations() {
        try (Connection connection = connections.open(); PreparedStatement statement = connection.prepareStatement(
                     "SELECT EXISTS (SELECT 1 FROM oscar_obs_overlay overlay_row " +
                             "JOIN oscar_archive_generation generation ON generation.id=overlay_row.generation_id " +
                             "WHERE overlay_row.store_key=? AND overlay_row.id_scope=? " +
                             "AND generation.store_key=overlay_row.store_key AND generation.id_scope=overlay_row.id_scope " +
                             "AND generation.state IN ('ACTIVE','SEALED')) OR " +
                             "EXISTS (SELECT 1 FROM oscar_tombstone tombstone " +
                             "WHERE tombstone.store_key=? AND tombstone.id_scope=? " +
                             "AND subject_type='OBSERVATION' AND generation_id IS NOT NULL AND EXISTS (" +
                             "SELECT 1 FROM oscar_archive_generation generation " +
                             "WHERE generation.id=tombstone.generation_id AND generation.store_key=tombstone.store_key " +
                             "AND generation.id_scope=tombstone.id_scope AND generation.state IN ('ACTIVE','SEALED')))")) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            statement.setString(3, storeKey);
            statement.setInt(4, idScope);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() && result.getBoolean(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot inspect archive mutations", e);
        }
    }

    /**
     * Resolves mutation membership for one bounded page of observation IDs.
     * This avoids retaining every historical tombstone and overlay in heap for
     * each query while preserving exact suppression of cold/hot base rows.
     */
    public Set<Long> mutatedObservationIds(Collection<Long> observationIds) {
        if (observationIds == null || observationIds.isEmpty())
            return Set.of();
        String sql = "WITH requested(id) AS (SELECT unnest(?::bigint[])) SELECT requested.id " +
                "FROM requested WHERE EXISTS (SELECT 1 FROM oscar_obs_overlay overlay_row " +
                "JOIN oscar_archive_generation generation ON generation.id=overlay_row.generation_id " +
                "WHERE overlay_row.observation_id=requested.id " +
                "AND overlay_row.store_key=? AND overlay_row.id_scope=? " +
                "AND generation.store_key=overlay_row.store_key AND generation.id_scope=overlay_row.id_scope " +
                "AND generation.state IN ('ACTIVE','SEALED')) OR EXISTS (" +
                "SELECT 1 FROM oscar_tombstone tombstone WHERE tombstone.subject_type='OBSERVATION' " +
                "AND tombstone.store_key=? AND tombstone.id_scope=? " +
                "AND tombstone.subject_id=requested.id::text AND tombstone.generation_id IS NOT NULL AND EXISTS (" +
                "SELECT 1 FROM oscar_archive_generation generation WHERE generation.id=tombstone.generation_id " +
                "AND generation.store_key=tombstone.store_key AND generation.id_scope=tombstone.id_scope " +
                "AND generation.state IN ('ACTIVE','SEALED')))";
        Set<Long> mutated = new HashSet<>();
        try (Connection connection = connections.open(); PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setArray(1, connection.createArrayOf("bigint", observationIds.toArray()));
            statement.setString(2, storeKey);
            statement.setInt(3, idScope);
            statement.setString(4, storeKey);
            statement.setInt(5, idScope);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next())
                    mutated.add(result.getLong(1));
            }
            return mutated;
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot resolve observation mutations", e);
        }
    }

    /**
     * Streams only query-relevant overlays in the same global order used by
     * hot and Parquet observations. Structured predicates are pushed into
     * PostgreSQL; payload predicates remain in the tiered store after decode.
     */
    public Stream<ArchivedOverlay> streamOverlays(int scope, ObsFilter filter,
                                                   Set<Long> dataStreams, Set<Long> fois) {
        if (dataStreams != null && dataStreams.isEmpty())
            return Stream.empty();
        if (fois != null && fois.isEmpty())
            return Stream.empty();

        StringBuilder sql = new StringBuilder(
                "SELECT overlay_row.generation_id, overlay_row.observation_id, overlay_row.datastream_id, " +
                        "overlay_row.foi_id, overlay_row.phenomenon_time, overlay_row.result_time, " +
                        "overlay_row.result_json::text, overlay_row.schema_json::text FROM oscar_obs_overlay overlay_row " +
                        "JOIN oscar_archive_generation generation ON generation.id=overlay_row.generation_id " +
                        "WHERE overlay_row.store_key=? AND overlay_row.id_scope=? " +
                        "AND generation.store_key=overlay_row.store_key AND generation.id_scope=overlay_row.id_scope " +
                        "AND generation.state IN ('ACTIVE','SEALED')");
        List<QueryParameter> parameters = new ArrayList<>();
        parameters.add(new StringParameter(storeKey));
        parameters.add(new IntegerParameter(idScope));

        if (filter.getInternalIDs() != null && !filter.getInternalIDs().isEmpty()) {
            List<Long> ids = filter.getInternalIDs().stream().map(BigId::getIdAsLong).toList();
            appendLongArray(sql, "overlay_row.observation_id", ids, parameters);
        }
        if (dataStreams != null)
            appendLongArray(sql, "overlay_row.datastream_id", dataStreams, parameters);
        if (fois != null) {
            Set<Long> explicit = new HashSet<>(fois);
            boolean includeNull = explicit.remove(0L);
            if (includeNull) {
                sql.append(" AND (overlay_row.foi_id IS NULL");
                if (!explicit.isEmpty()) {
                    sql.append(" OR overlay_row.foi_id=ANY (?::bigint[])");
                    parameters.add(new LongArrayParameter(explicit.toArray()));
                }
                sql.append(')');
            } else {
                appendLongArray(sql, "overlay_row.foi_id", explicit, parameters);
            }
        }
        appendTime(sql, "overlay_row.phenomenon_time", filter.getPhenomenonTime(), parameters);
        appendTime(sql, "overlay_row.result_time", filter.getResultTime(), parameters);
        boolean descending = filter.getPhenomenonTime() != null &&
                filter.getPhenomenonTime().descendingOrder();
        sql.append(" ORDER BY overlay_row.phenomenon_time ")
                .append(descending ? "DESC" : "ASC")
                .append(" NULLS FIRST, overlay_row.observation_id ASC");

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet result = null;
        try {
            connection = connections.open();
            connection.setReadOnly(true);
            connection.setAutoCommit(false);
            statement = connection.prepareStatement(sql.toString());
            statement.setFetchSize(1_000);
            for (int i = 0; i < parameters.size(); i++)
                parameters.get(i).bind(connection, statement, i + 1);
            result = statement.executeQuery();
            OverlayCursor cursor = new OverlayCursor(scope, connection, statement, result);
            openOverlayCursors.incrementAndGet();
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(cursor,
                            Spliterator.ORDERED | Spliterator.NONNULL), false)
                    .onClose(cursor::close);
        } catch (Exception error) {
            closeQuietly(result);
            closeQuietly(statement);
            closeQuietly(connection);
            throw new IllegalStateException("Cannot stream observation overlays", error);
        }
    }

    public int getOpenOverlayCursorCount() {
        return openOverlayCursors.get();
    }

    public long getOverlayRowsRead() {
        return overlayRowsRead.get();
    }

    private static void appendLongArray(StringBuilder sql, String column, Collection<Long> values,
                                        List<QueryParameter> parameters) {
        if (values.isEmpty()) {
            sql.append(" AND false");
            return;
        }
        sql.append(" AND ").append(column).append("=ANY (?::bigint[])");
        parameters.add(new LongArrayParameter(values.toArray()));
    }

    private static void appendTime(StringBuilder sql, String column, TemporalFilter filter,
                                   List<QueryParameter> parameters) {
        if (filter == null || filter.isLatestTime())
            return;
        Instant minimum = filter.getMin();
        Instant maximum = filter.getMax();
        if (minimum != null && !Instant.MIN.equals(minimum)) {
            sql.append(" AND ").append(column).append(">=?");
            parameters.add(new InstantParameter(minimum));
        }
        if (maximum != null && !Instant.MAX.equals(maximum)) {
            sql.append(" AND ").append(column).append("<=?");
            parameters.add(new InstantParameter(maximum));
        }
    }

    private static void closeQuietly(AutoCloseable resource) {
        if (resource == null)
            return;
        try {
            resource.close();
        } catch (Exception ignored) {
            // Preserve the primary query/decoding error and continue closing.
        }
    }

    private interface QueryParameter {
        void bind(Connection connection, PreparedStatement statement, int index) throws SQLException;
    }

    private record LongArrayParameter(Object[] values) implements QueryParameter {
        @Override
        public void bind(Connection connection, PreparedStatement statement, int index) throws SQLException {
            statement.setArray(index, connection.createArrayOf("bigint", values));
        }
    }

    private record InstantParameter(Instant value) implements QueryParameter {
        @Override
        public void bind(Connection connection, PreparedStatement statement, int index) throws SQLException {
            UtcJdbcTime.setInstant(statement, index, value);
        }
    }

    private record StringParameter(String value) implements QueryParameter {
        @Override
        public void bind(Connection connection, PreparedStatement statement, int index) throws SQLException {
            statement.setString(index, value);
        }
    }

    private record IntegerParameter(int value) implements QueryParameter {
        @Override
        public void bind(Connection connection, PreparedStatement statement, int index) throws SQLException {
            statement.setInt(index, value);
        }
    }

    private final class OverlayCursor implements Iterator<ArchivedOverlay>, AutoCloseable {
        private final int scope;
        private final Connection connection;
        private final PreparedStatement statement;
        private final ResultSet result;
        private final AtomicBoolean closed = new AtomicBoolean();
        private ArchivedOverlay next;
        private boolean prefetched;

        OverlayCursor(int scope, Connection connection, PreparedStatement statement, ResultSet result) {
            this.scope = scope;
            this.connection = connection;
            this.statement = statement;
            this.result = result;
        }

        @Override
        public boolean hasNext() {
            if (prefetched)
                return true;
            if (closed.get())
                return false;
            try {
                if (!result.next()) {
                    close();
                    return false;
                }
                UUID generationId = result.getObject(1, UUID.class);
                long id = result.getLong(2);
                Long foi = result.getObject(4, Long.class);
                next = new ArchivedOverlay(generationId, result.getString(8), new ArchivedObservation(
                        scope, id, scope, result.getLong(3), foi == null ? null : scope, foi,
                        UtcJdbcTime.getInstant(result, 5), UtcJdbcTime.getInstant(result, 6),
                        result.getString(7), "OVERLAY"));
                overlayRowsRead.incrementAndGet();
                prefetched = true;
                return true;
            } catch (Exception error) {
                close();
                throw new IllegalStateException("Cannot decode observation overlay", error);
            }
        }

        @Override
        public ArchivedOverlay next() {
            if (!hasNext())
                throw new NoSuchElementException();
            ArchivedOverlay value = next;
            next = null;
            prefetched = false;
            return value;
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true))
                return;
            closeQuietly(result);
            closeQuietly(statement);
            try {
                connection.rollback();
            } catch (Exception ignored) {
                // The cursor resources are already closed; pool close below
                // performs its own reset as a final safeguard.
            }
            closeQuietly(connection);
            openOverlayCursors.decrementAndGet();
            next = null;
            prefetched = false;
        }
    }

    /**
     * Returns the exact logical observation cardinality without opening or
     * decoding Parquet payloads. Generation row IDs are disjoint by contract;
     * hot rows carrying an active generation marker are the deliberate grace
     * overlap and are therefore subtracted once. Observation tombstones are
     * subtracted only while their generation is query-visible.
     */
    public long countTieredRecords(String observationTable) {
        String observations = safeIdentifier(observationTable);
        String sql = "WITH cold AS (" +
                "SELECT COALESCE(sum(row_count), 0)::bigint count FROM oscar_archive_generation " +
                "WHERE store_key=? AND id_scope=? AND state IN ('ACTIVE','SEALED')), " +
                "overlap AS (SELECT count(*)::bigint count FROM " + observations + " o " +
                "JOIN oscar_archive_generation g ON g.id=o.archivegenerationid " +
                "WHERE g.store_key=? AND g.id_scope=? AND g.state IN ('ACTIVE','SEALED')), " +
                "deleted AS (SELECT count(*)::bigint count FROM oscar_tombstone t " +
                "WHERE t.store_key=? AND t.id_scope=? AND t.subject_type='OBSERVATION' " +
                "AND t.subject_id ~ '^[0-9]+$' " +
                "AND t.generation_id IS NOT NULL AND EXISTS (SELECT 1 FROM oscar_archive_generation exact_generation " +
                "WHERE exact_generation.id=t.generation_id AND exact_generation.store_key=t.store_key " +
                "AND exact_generation.id_scope=t.id_scope AND exact_generation.state IN ('ACTIVE','SEALED'))) " +
                "SELECT (SELECT count(*) FROM " + observations + ") + cold.count - overlap.count - deleted.count " +
                "FROM cold, overlap, deleted";
        try (Connection connection = connections.open(); PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            statement.setString(3, storeKey);
            statement.setInt(4, idScope);
            statement.setString(5, storeKey);
            statement.setInt(6, idScope);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next())
                    return 0;
                long count = result.getLong(1);
                if (count < 0)
                    throw new IllegalStateException("Tiered observation count invariant was violated");
                return count;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot count tiered observations", e);
        }
    }

    public void tombstone(long observationId, String reason, String policy) {
        throw new IllegalArgumentException("Archived observation tombstones require an exact generation ID");
    }

    public void tombstone(long observationId, UUID generationId, String reason, String policy) {
        if (generationId == null)
            throw new IllegalArgumentException("Archived observation tombstones require an exact generation ID");
        try (Connection connection = connections.open(); PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO oscar_tombstone(store_key, id_scope, subject_type, subject_id, generation_id, reason, policy) " +
                        "SELECT ?, ?, 'OBSERVATION', ?, generation.id, ?, ? " +
                        "FROM oscar_archive_generation generation WHERE generation.id=? " +
                        "AND generation.store_key=? AND generation.id_scope=? " +
                        "AND generation.state IN ('ACTIVE','SEALED') " +
                        "ON CONFLICT (subject_type, store_key, id_scope, subject_id) DO UPDATE SET " +
                        "generation_id=EXCLUDED.generation_id, reason=EXCLUDED.reason, policy=EXCLUDED.policy, " +
                        "deleted_at=now()")) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            statement.setString(3, Long.toString(observationId));
            statement.setString(4, reason);
            statement.setString(5, policy);
            statement.setObject(6, generationId);
            statement.setString(7, storeKey);
            statement.setInt(8, idScope);
            if (statement.executeUpdate() != 1)
                throw new IllegalArgumentException("Archive generation does not belong to this observation store: " + generationId);
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot tombstone archived observation", e);
        }
    }

    /**
     * Creates an observation tombstone on a caller-owned transaction and
     * reports whether this call performed the logical visible-to-deleted
     * transition.  A concurrent or repeated deletion returns {@code false}
     * instead of being double-counted.
     */
    public boolean tombstoneIfAbsent(Connection connection, long observationId,
                                     UUID generationId, String reason, String policy)
            throws SQLException {
        if (connection == null)
            throw new IllegalArgumentException("A transaction connection is required");
        if (generationId == null)
            throw new IllegalArgumentException("Archived observation tombstones require an exact generation ID");
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO oscar_tombstone(store_key, id_scope, subject_type, subject_id, generation_id, reason, policy) " +
                        "SELECT ?, ?, 'OBSERVATION', ?, generation.id, ?, ? " +
                        "FROM oscar_archive_generation generation WHERE generation.id=? " +
                        "AND generation.store_key=? AND generation.id_scope=? " +
                        "AND generation.state IN ('ACTIVE','SEALED') " +
                        "ON CONFLICT (subject_type, store_key, id_scope, subject_id) DO NOTHING")) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            statement.setString(3, Long.toString(observationId));
            statement.setString(4, reason);
            statement.setString(5, policy);
            statement.setObject(6, generationId);
            statement.setString(7, storeKey);
            statement.setInt(8, idScope);
            int inserted = statement.executeUpdate();
            if (inserted == 1)
                return true;
        }

        // Zero rows is valid only for an already deleted observation.  Do not
        // silently convert a stale/cross-store generation reference into a
        // successful removal.
        try (PreparedStatement existing = connection.prepareStatement(
                "SELECT generation_id FROM oscar_tombstone WHERE store_key=? AND id_scope=? " +
                        "AND subject_type='OBSERVATION' AND subject_id=?")) {
            existing.setString(1, storeKey);
            existing.setInt(2, idScope);
            existing.setString(3, Long.toString(observationId));
            try (ResultSet result = existing.executeQuery()) {
                if (result.next())
                    return false;
            }
        }
        throw new IllegalArgumentException(
                "Archive generation is not query-visible for this observation store: " + generationId);
    }

    private static String safeIdentifier(String identifier) {
        if (identifier == null || !identifier.matches("[A-Za-z_][A-Za-z0-9_]*"))
            throw new IllegalArgumentException("Unsafe SQL identifier: " + identifier);
        return identifier;
    }

    public record SchemaSnapshot(long dataStreamId, String json) {}
    public record ArchivedOverlay(UUID generationId, String schemaJson, ArchivedObservation observation) {}
}

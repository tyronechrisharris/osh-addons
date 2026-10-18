package org.sensorhub.impl.datastore.postgis.archive;

import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TestArchiveLifecycleInvariants {
    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void onlyFailedOrStaleExportingJobsAreRetried() {
        assertTrue(ArchiveCatalog.shouldRetryJob("FAILED", false));
        assertTrue(ArchiveCatalog.shouldRetryJob("EXPORTING", true));
        assertFalse(ArchiveCatalog.shouldRetryJob("EXPORTING", false));
        assertFalse(ArchiveCatalog.shouldRetryJob("ACTIVE", true));
        assertFalse(ArchiveCatalog.shouldRetryJob("SEALED", true));
    }

    @Test
    public void archiveJobRevisionsIncreaseMonotonically() {
        assertEquals(1, ArchiveCatalog.nextRevision(0));
        assertEquals(42, ArchiveCatalog.nextRevision(41));
        assertThrows(ArithmeticException.class, () -> ArchiveCatalog.nextRevision(Integer.MAX_VALUE));
    }

    @Test
    public void legacyBackupInventoryMigrationEnforcesCanonicalLauncherBounds() throws Exception {
        ArchiveCatalog.requireBackupGenerationInventoryBounds(0, 0);
        ArchiveCatalog.requireBackupGenerationInventoryBounds(1_000_000, 37_000_000);

        SQLException tooMany = assertThrows(SQLException.class,
                () -> ArchiveCatalog.requireBackupGenerationInventoryBounds(1_000_001, 37_000_037));
        assertTrue(tooMany.getMessage().contains("1,000,000-entry migration limit"));
        SQLException tooLarge = assertThrows(SQLException.class,
                () -> ArchiveCatalog.requireBackupGenerationInventoryBounds(1_000_000, 37_000_001));
        assertTrue(tooLarge.getMessage().contains("37,000,000-byte migration limit"));
        SQLException noncanonical = assertThrows(SQLException.class,
                () -> ArchiveCatalog.requireBackupGenerationInventoryBounds(1, 36));
        assertTrue(noncanonical.getMessage().contains("canonical UUID-line form"));
    }

    @Test
    public void exactObservationHoldsAreExcludedFromDeletionBatch() {
        assertEquals(List.of(10L, 12L), ArchiveLifecycleManager.withoutHeld(
                List.of(10L, 11L, 12L, 13L), Set.of(11L, 13L)));
    }

    @Test
    public void mutationLocksUseOneOrderedSetBasedStatementForLargeBatches() throws Exception {
        List<String> sql = new ArrayList<>();
        AtomicInteger executions = new AtomicInteger();
        AtomicReference<List<Long>> boundIds = new AtomicReference<>();
        AtomicBoolean arrayFreed = new AtomicBoolean();

        Array array = (Array) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{Array.class}, (proxy, method, arguments) -> {
                    if (method.getName().equals("free")) {
                        arrayFreed.set(true);
                        return null;
                    }
                    return defaultValue(method.getReturnType());
                });
        Connection connection = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{Connection.class}, (proxy, method, arguments) -> switch (method.getName()) {
                    case "getAutoCommit" -> false;
                    case "createArrayOf" -> {
                        assertEquals("bigint", arguments[0]);
                        Object[] values = (Object[]) arguments[1];
                        boundIds.set(java.util.Arrays.stream(values).map(value -> (Long) value).toList());
                        yield array;
                    }
                    case "prepareStatement" -> {
                        String statementSql = (String) arguments[0];
                        sql.add(statementSql);
                        yield preparedStatement(executions, boundIds);
                    }
                    default -> defaultValue(method.getReturnType());
                });

        List<Long> requested = new ArrayList<>(10_001);
        for (long id = 10_000; id >= 1; id--)
            requested.add(id);
        requested.add(5_000L); // duplicates must not request the same xact lock twice

        ArchiveCatalog catalog = new ArchiveCatalog(
                () -> { throw new AssertionError("provider must not be used"); }, "lock_probe", 7);
        catalog.lockForMutation(connection, requested);

        assertEquals("store and all observation locks need only two executions", 2, executions.get());
        assertEquals(2, sql.size());
        assertTrue(sql.get(0).contains("pg_advisory_xact_lock_shared"));
        assertTrue(sql.get(1).contains("WITH ORDINALITY"));
        assertTrue(sql.get(1).contains("ORDER BY ordinal"));
        assertEquals(10_000, boundIds.get().size());
        for (int index = 0; index < boundIds.get().size(); index++)
            assertEquals(index + 1L, boundIds.get().get(index).longValue());
        assertTrue(arrayFreed.get());
    }

    @Test
    public void removalHoldSnapshotIsSetBasedScopedAndUsesCallerTransaction() throws Exception {
        List<String> sql = new ArrayList<>();
        AtomicReference<List<Long>> boundIds = new AtomicReference<>();
        AtomicBoolean arrayFreed = new AtomicBoolean();
        UUID generation = UUID.randomUUID();

        Array array = (Array) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{Array.class}, (proxy, method, arguments) -> {
                    if (method.getName().equals("free")) {
                        arrayFreed.set(true);
                        return null;
                    }
                    return defaultValue(method.getReturnType());
                });
        Statement lock = (Statement) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{Statement.class}, (proxy, method, arguments) -> {
                    if (method.getName().equals("execute")) {
                        sql.add((String) arguments[0]);
                        return true;
                    }
                    return defaultValue(method.getReturnType());
                });
        Connection connection = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{Connection.class}, (proxy, method, arguments) -> switch (method.getName()) {
                    case "getAutoCommit" -> false;
                    case "createStatement" -> lock;
                    case "createArrayOf" -> {
                        assertEquals("bigint", arguments[0]);
                        boundIds.set(java.util.Arrays.stream((Object[]) arguments[1])
                                .map(value -> (Long) value).toList());
                        yield array;
                    }
                    case "prepareStatement" -> {
                        sql.add((String) arguments[0]);
                        yield holdSnapshotStatement(generation);
                    }
                    default -> defaultValue(method.getReturnType());
                });

        ArchiveCatalog catalog = new ArchiveCatalog(
                () -> { throw new AssertionError("provider must not be used"); }, "hold_probe", 9);
        ArchiveCatalog.ActiveRemovalHolds holds = catalog.lockActiveRemovalHolds(
                connection, List.of(12L, 11L, 11L));

        assertEquals(List.of(11L, 12L), boundIds.get());
        assertTrue(sql.get(0).contains("LOCK TABLE oscar_retention_hold IN SHARE MODE"));
        assertTrue(sql.get(1).contains("WITH requested(id)"));
        assertTrue(sql.get(1).contains("oscar_archive_id_block"));
        assertTrue(sql.get(1).contains("requested.id BETWEEN block.min_id AND block.max_id"));
        assertEquals(Set.of(11L), holds.observationIds());
        assertEquals(Set.of(generation), holds.generationIds());
        assertTrue(holds.blocks(11L, null));
        assertTrue(holds.blocks(12L, generation));
        assertFalse(holds.blocks(12L, UUID.randomUUID()));
        assertTrue(arrayFreed.get());
    }

    @Test
    public void lifecycleRejectsSymbolicLinkArchiveObjects() throws Exception {
        Path root = temporary.newFolder("archive-root").toPath();
        Path outside = temporary.newFile("outside.parquet").toPath();
        Path link = root.resolve("linked.parquet");
        try {
            Files.createSymbolicLink(link, outside);
        } catch (UnsupportedOperationException | java.nio.file.FileSystemException unsupported) {
            return;
        }

        assertThrows(IllegalStateException.class,
                () -> ArchiveLifecycleManager.resolveArchiveObject(root, Path.of("linked.parquet")));
    }

    private PreparedStatement preparedStatement(AtomicInteger executions,
                                                AtomicReference<List<Long>> boundIds) {
        return (PreparedStatement) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{PreparedStatement.class}, (proxy, method, arguments) -> switch (method.getName()) {
                    case "execute" -> {
                        executions.incrementAndGet();
                        yield true;
                    }
                    case "executeQuery" -> {
                        executions.incrementAndGet();
                        AtomicBoolean first = new AtomicBoolean(true);
                        yield Proxy.newProxyInstance(getClass().getClassLoader(),
                                new Class<?>[]{ResultSet.class}, (resultProxy, resultMethod, resultArguments) ->
                                        switch (resultMethod.getName()) {
                                            case "next" -> first.getAndSet(false);
                                            case "getLong" -> (long) boundIds.get().size();
                                            default -> defaultValue(resultMethod.getReturnType());
                                        });
                    }
                    default -> defaultValue(method.getReturnType());
                });
    }

    private PreparedStatement holdSnapshotStatement(UUID generation) {
        return (PreparedStatement) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{PreparedStatement.class}, (proxy, method, arguments) -> {
                    if (!method.getName().equals("executeQuery"))
                        return defaultValue(method.getReturnType());
                    AtomicInteger row = new AtomicInteger(-1);
                    String[][] values = {{"OBSERVATION", "11"},
                            {"ARCHIVE_GENERATION", generation.toString()}};
                    return Proxy.newProxyInstance(getClass().getClassLoader(),
                            new Class<?>[]{ResultSet.class}, (resultProxy, resultMethod, resultArguments) ->
                                    switch (resultMethod.getName()) {
                                        case "next" -> row.incrementAndGet() < values.length;
                                        case "getString" -> values[row.get()][(int) resultArguments[0] - 1];
                                        default -> defaultValue(resultMethod.getReturnType());
                                    });
                });
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive() || type == void.class)
            return null;
        if (type == boolean.class)
            return false;
        if (type == byte.class)
            return (byte) 0;
        if (type == short.class)
            return (short) 0;
        if (type == int.class)
            return 0;
        if (type == long.class)
            return 0L;
        if (type == float.class)
            return 0F;
        if (type == double.class)
            return 0D;
        if (type == char.class)
            return '\0';
        throw new AssertionError("Unhandled primitive " + type);
    }
}

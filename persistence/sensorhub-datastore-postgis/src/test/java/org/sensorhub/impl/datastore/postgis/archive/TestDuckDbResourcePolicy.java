package org.sensorhub.impl.datastore.postgis.archive;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sensorhub.api.datastore.ICancellableDataStore;
import org.sensorhub.impl.datastore.postgis.connection.ConnectionManager;

import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.ResultSet;
import java.sql.SQLTransientException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TestDuckDbResourcePolicy {
    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void appliesEveryBoundAndRemovesOperationDirectory() throws Exception {
        Path temporaryRoot = temporary.getRoot().toPath().resolve("duckdb-spill");
        TieredArchiveConfig config = config(temporaryRoot);
        Path operationDirectory;

        try (DuckDbResourcePolicy.Session session = DuckDbResourcePolicy.open(config, "near-cap")) {
            operationDirectory = session.operationDirectory();
            assertEquals(temporaryRoot.toRealPath(), operationDirectory.getParent());
            assertTrue(Files.isDirectory(operationDirectory));
            if (supportsPosix(operationDirectory)) {
                assertEquals(PosixFilePermissions.fromString("rwx------"),
                        Files.getPosixFilePermissions(operationDirectory, LinkOption.NOFOLLOW_LINKS));
                assertEquals(PosixFilePermissions.fromString("rwx------"),
                        Files.getPosixFilePermissions(temporaryRoot, LinkOption.NOFOLLOW_LINKS));
            }
            try (Statement statement = session.connection().createStatement()) {
                assertEquals(1, numericSetting(statement, "threads"));
                assertTrue(byteSetting(statement, "memory_limit") <= 64L * 1024 * 1024);
                assertTrue(byteSetting(statement, "max_temp_directory_size") <= 96L * 1024 * 1024);
                assertEquals(operationDirectory.toString(), textSetting(statement, "temp_directory"));

                // The generated values plus the hash sort are deliberately
                // close to the 64 MiB memory cap. The operation must finish
                // under the configured one-thread/96 MiB spill envelope.
                try (ResultSet rows = statement.executeQuery(
                        "SELECT count(*) FROM (SELECT i, repeat('x', 192) payload " +
                                "FROM range(0, 120000) values(i) ORDER BY hash(i), payload) bounded")) {
                    assertTrue(rows.next());
                    assertEquals(120000L, rows.getLong(1));
                }
            }
        }

        assertFalse(Files.exists(operationDirectory));
        assertTrue(Files.isDirectory(temporaryRoot));
    }

    @Test
    public void concurrentOperationsReceiveDifferentConfinedDirectories() throws Exception {
        Path temporaryRoot = temporary.newFolder("separate-spill").toPath();
        TieredArchiveConfig config = config(temporaryRoot);
        Path firstDirectory;
        Path secondDirectory;
        try (DuckDbResourcePolicy.Session first = DuckDbResourcePolicy.open(config, "cold-query");
             DuckDbResourcePolicy.Session second = DuckDbResourcePolicy.open(config, "cold-query")) {
            firstDirectory = first.operationDirectory();
            secondDirectory = second.operationDirectory();
            assertNotEquals(firstDirectory, secondDirectory);
            assertEquals(temporaryRoot.toRealPath(), firstDirectory.getParent());
            assertEquals(temporaryRoot.toRealPath(), secondDirectory.getParent());
        }
        assertFalse(Files.exists(firstDirectory));
        assertFalse(Files.exists(secondDirectory));
    }

    @Test
    public void scopedCancellationInterruptsLongQueryAndRemovesScratchDirectory()
            throws Exception {
        Path temporaryRoot = temporary.newFolder("cancelled-query-spill").toPath();
        TieredArchiveConfig config = config(temporaryRoot);
        AtomicReference<ICancellableDataStore.CancellableOperation> operation =
                new AtomicReference<>();
        AtomicReference<Path> operationDirectory = new AtomicReference<>();
        AtomicReference<Throwable> workerFailure = new AtomicReference<>();
        AtomicBoolean completedNormally = new AtomicBoolean();
        CountDownLatch queryReady = new CountDownLatch(1);
        CountDownLatch workerExited = new CountDownLatch(1);

        Thread worker = new Thread(() -> {
            try (ICancellableDataStore.CancellableOperation scope =
                         ConnectionManager.beginCancellableOperation();
                 DuckDbResourcePolicy.Session session =
                         DuckDbResourcePolicy.open(config, "metadata-query");
                 Statement statement = ConnectionManager.trackCancellableStatement(
                         session.connection().createStatement(), "DuckDB metadata query")) {
                operation.set(scope);
                operationDirectory.set(session.operationDirectory());
                queryReady.countDown();
                try (ResultSet rows = statement.executeQuery(
                        "SELECT sum(hash(i)) FROM range(0, 1000000000000) values(i)")) {
                    rows.next();
                }
                completedNormally.set(true);
            } catch (Throwable error) {
                workerFailure.set(error);
            } finally {
                workerExited.countDown();
            }
        }, "long-cold-statistics-query");
        worker.setDaemon(true);
        worker.start();

        long elapsedMillis;
        try {
            assertTrue("The DuckDB statement must be ready before cancellation",
                    queryReady.await(5, TimeUnit.SECONDS));
            Thread.sleep(200L);
            long started = System.nanoTime();
            operation.get().cancel();
            assertTrue("DuckDB cancellation must release the statistics worker inside the stop budget",
                    workerExited.await(5, TimeUnit.SECONDS));
            elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        } finally {
            ICancellableDataStore.CancellableOperation active = operation.get();
            if (worker.isAlive() && active != null) {
                try {
                    active.cancel();
                } catch (RuntimeException ignored) {
                    // Preserve the original assertion while still requesting
                    // best-effort cleanup of a failed cancellation fixture.
                }
            }
            worker.interrupt();
            worker.join(5_000L);
        }

        assertFalse("The cancelled statistics worker must terminate", worker.isAlive());
        assertFalse("The cancelled cold query must not complete normally", completedNormally.get());
        assertTrue("The in-flight DuckDB query must observe cancellation",
                workerFailure.get() != null);
        assertTrue("DuckDB cancellation must preserve headroom inside the 5 second statistics stop budget: "
                + elapsedMillis + " ms", elapsedMillis < 4_000L);
        assertFalse("Closing the cancelled session must remove its private scratch directory",
                Files.exists(operationDirectory.get(), LinkOption.NOFOLLOW_LINKS));
        try (var children = Files.list(temporaryRoot)) {
            assertEquals("No cancelled DuckDB scratch directory may remain", 0L, children.count());
        }
    }

    @Test
    public void processWideAdmissionFailsBusyThenRecoversAfterClose() throws Exception {
        Path temporaryRoot = temporary.newFolder("admission-spill").toPath();
        TieredArchiveConfig config = config(temporaryRoot);
        config.duckDbMaximumConcurrentSessions = 1;
        config.duckDbSessionAcquireTimeoutSeconds = 1;

        DuckDbResourcePolicy.Session first = DuckDbResourcePolicy.open(config, "cold-query");
        try {
            long started = System.nanoTime();
            SQLTransientException busy = assertThrows(SQLTransientException.class,
                    () -> DuckDbResourcePolicy.open(config, "archive-export"));
            assertTrue(busy.getMessage().contains("capacity is busy"));
            assertTrue(System.nanoTime() - started >=
                    java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(750));
        } finally {
            first.close();
        }

        try (DuckDbResourcePolicy.Session recovered =
                     DuckDbResourcePolicy.open(config, "archive-export")) {
            assertTrue(Files.isDirectory(recovered.operationDirectory()));
        }
    }

    @Test
    public void smallestRegisteredLimitConstrainsDifferentlyConfiguredStores() throws Exception {
        Path temporaryRoot = temporary.newFolder("mixed-admission-spill").toPath();
        TieredArchiveConfig broad = config(temporaryRoot);
        broad.duckDbMaximumConcurrentSessions = 2;
        TieredArchiveConfig strict = config(temporaryRoot);
        strict.duckDbMaximumConcurrentSessions = 1;
        strict.duckDbSessionAcquireTimeoutSeconds = 1;

        try (DuckDbResourcePolicy.Session first = DuckDbResourcePolicy.open(broad, "cold-query");
             DuckDbResourcePolicy.Session second = DuckDbResourcePolicy.open(broad, "archive-export")) {
            assertThrows(SQLTransientException.class,
                    () -> DuckDbResourcePolicy.open(strict, "archive-verify"));
        }

        try (DuckDbResourcePolicy.Session recovered =
                     DuckDbResourcePolicy.open(strict, "archive-verify")) {
            assertTrue(Files.isDirectory(recovered.operationDirectory()));
        }
    }

    @Test
    public void rejectsDisabledResourceBoundsAndLinkedTemporaryRoot() throws Exception {
        Path temporaryRoot = temporary.newFolder("bad-spill").toPath();
        TieredArchiveConfig invalidBounds = config(temporaryRoot);
        invalidBounds.duckDbMemoryLimitMegabytes = 15;
        assertThrows(IllegalArgumentException.class,
                () -> DuckDbResourcePolicy.open(invalidBounds, "cold-query"));

        Path target = temporary.newFolder("real-spill").toPath();
        Path link = temporary.getRoot().toPath().resolve("linked-spill");
        try {
            Files.createSymbolicLink(link, target);
        } catch (UnsupportedOperationException | java.nio.file.FileSystemException unsupported) {
            return;
        }
        TieredArchiveConfig linkedConfig = config(link);
        assertThrows(IllegalStateException.class,
                () -> DuckDbResourcePolicy.open(linkedConfig, "cold-query"));
    }

    @Test
    public void rejectsOverlappingArchiveAndTemporaryRoots() throws Exception {
        Path temporaryRoot = temporary.newFolder("overlap-spill").toPath();
        TieredArchiveConfig config = config(temporaryRoot);
        config.rootDirectory = temporaryRoot.resolve("archive-inside-spill").toString();
        assertThrows(IllegalArgumentException.class,
                () -> DuckDbResourcePolicy.open(config, "cold-query"));

        config.rootDirectory = temporaryRoot.resolveSibling("archive-parent").toString();
        config.duckDbTemporaryDirectory = Path.of(config.rootDirectory).resolve("spill-inside-archive").toString();
        assertThrows(IllegalArgumentException.class,
                () -> DuckDbResourcePolicy.open(config, "cold-query"));
    }

    @Test
    public void rejectsExistingPosixTemporaryRootWithBroadPermissions() throws Exception {
        Path temporaryRoot = temporary.newFolder("broad-spill").toPath();
        if (!supportsPosix(temporaryRoot))
            return;
        Files.setPosixFilePermissions(temporaryRoot,
                PosixFilePermissions.fromString("rwxr-xr-x"));
        TieredArchiveConfig config = new TieredArchiveConfig();
        config.rootDirectory = temporaryRoot.resolveSibling("broad-archive").toString();
        config.duckDbTemporaryDirectory = temporaryRoot.toString();
        assertThrows(IllegalStateException.class,
                () -> DuckDbResourcePolicy.open(config, "cold-query"));
    }

    @Test
    public void closeSurfacesOperationDirectoryCleanupFailure() throws Exception {
        Path temporaryRoot = temporary.newFolder("cleanup-failure-spill").toPath();
        if (!supportsPosix(temporaryRoot))
            return;
        Files.setPosixFilePermissions(temporaryRoot,
                PosixFilePermissions.fromString("rwx------"));
        DuckDbResourcePolicy.Session session = DuckDbResourcePolicy.open(
                config(temporaryRoot), "cold-query");
        Path operationDirectory = session.operationDirectory();
        Files.writeString(operationDirectory.resolve("cleanup-proof"), "must be removed");
        Files.setPosixFilePermissions(operationDirectory,
                PosixFilePermissions.fromString("r-x------"));
        try {
            IllegalStateException failure = assertThrows(
                    IllegalStateException.class, session::close);
            assertTrue(failure.getMessage().contains("Cannot close bounded DuckDB session"));
            assertTrue(Files.exists(operationDirectory, LinkOption.NOFOLLOW_LINKS));
        } finally {
            Files.setPosixFilePermissions(operationDirectory,
                    PosixFilePermissions.fromString("rwx------"));
            Files.deleteIfExists(operationDirectory.resolve("cleanup-proof"));
            Files.deleteIfExists(operationDirectory);
        }

        TieredArchiveConfig recovery = config(temporaryRoot);
        recovery.duckDbMaximumConcurrentSessions = 1;
        recovery.duckDbSessionAcquireTimeoutSeconds = 1;
        try (DuckDbResourcePolicy.Session recovered =
                     DuckDbResourcePolicy.open(recovery, "cold-query")) {
            assertTrue(Files.isDirectory(recovered.operationDirectory()));
        }
    }

    private static TieredArchiveConfig config(Path temporaryRoot) throws Exception {
        if (Files.exists(temporaryRoot) && supportsPosix(temporaryRoot))
            Files.setPosixFilePermissions(temporaryRoot,
                    PosixFilePermissions.fromString("rwx------"));
        TieredArchiveConfig config = new TieredArchiveConfig();
        config.rootDirectory = temporaryRoot.resolveSibling(
                temporaryRoot.getFileName() + "-archive").toString();
        config.duckDbMemoryLimitMegabytes = 64;
        config.duckDbThreads = 1;
        config.duckDbMaximumTemporaryMegabytes = 96;
        config.duckDbMaximumConcurrentSessions = 2;
        config.duckDbSessionAcquireTimeoutSeconds = 2;
        config.duckDbTemporaryDirectory = temporaryRoot.toString();
        return config;
    }

    private static boolean supportsPosix(Path path) {
        return Files.getFileAttributeView(
                path, PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS) != null;
    }

    private static long numericSetting(Statement statement, String name) throws Exception {
        return Long.parseLong(textSetting(statement, name));
    }

    private static long byteSetting(Statement statement, String name) throws Exception {
        String value = textSetting(statement, name).trim().toUpperCase(java.util.Locale.ROOT);
        long multiplier = 1;
        if (value.endsWith("KIB")) {
            multiplier = 1024L;
            value = value.substring(0, value.length() - 3);
        } else if (value.endsWith("MIB")) {
            multiplier = 1024L * 1024;
            value = value.substring(0, value.length() - 3);
        } else if (value.endsWith("GIB")) {
            multiplier = 1024L * 1024 * 1024;
            value = value.substring(0, value.length() - 3);
        } else if (value.endsWith("KB")) {
            multiplier = 1000L;
            value = value.substring(0, value.length() - 2);
        } else if (value.endsWith("MB")) {
            multiplier = 1000L * 1000;
            value = value.substring(0, value.length() - 2);
        } else if (value.endsWith("GB")) {
            multiplier = 1000L * 1000 * 1000;
            value = value.substring(0, value.length() - 2);
        }
        return new java.math.BigDecimal(value.trim())
                .multiply(java.math.BigDecimal.valueOf(multiplier)).longValue();
    }

    private static String textSetting(Statement statement, String name) throws Exception {
        try (ResultSet result = statement.executeQuery("SELECT current_setting('" + name + "')")) {
            assertTrue(result.next());
            return result.getString(1);
        }
    }
}

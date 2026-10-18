package org.sensorhub.impl.datastore.postgis.archive;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLTransientException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.TreeMap;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/** Opens a bounded DuckDB connection with an operation-private spill directory. */
final class DuckDbResourcePolicy {
    private static final Set<PosixFilePermission> PRIVATE_DIRECTORY_PERMISSIONS =
            PosixFilePermissions.fromString("rwx------");
    private static final AdmissionController ADMISSION = new AdmissionController();

    private DuckDbResourcePolicy() {
    }

    static Session open(TieredArchiveConfig config, String operation) throws Exception {
        config.validate();
        if (operation == null || !operation.matches("[a-z][a-z0-9-]{0,31}"))
            throw new IllegalArgumentException("Unsafe DuckDB operation name");

        Path archiveRoot = Path.of(config.rootDirectory).toAbsolutePath().normalize();
        Path temporaryRoot = Path.of(config.duckDbTemporaryDirectory).toAbsolutePath().normalize();
        requireDisjointRoots(archiveRoot, temporaryRoot);
        AdmissionPermit permit = ADMISSION.acquire(
                config.duckDbMaximumConcurrentSessions,
                config.duckDbSessionAcquireTimeoutSeconds,
                operation);
        Connection connection = null;
        Path realTemporaryRoot = null;
        Path operationDirectory = null;
        try {
            preparePrivateDirectory(temporaryRoot);
            realTemporaryRoot = temporaryRoot.toRealPath();
            Path realArchiveRoot = Files.exists(archiveRoot, LinkOption.NOFOLLOW_LINKS) ?
                    archiveRoot.toRealPath() : archiveRoot;
            requireDisjointRoots(realArchiveRoot, realTemporaryRoot);
            operationDirectory = realTemporaryRoot.resolve(
                    operation + '-' + UUID.randomUUID()).normalize();
            if (!operationDirectory.getParent().equals(realTemporaryRoot))
                throw new IllegalStateException(
                        "DuckDB operation directory escaped its configured parent");
            if (supportsPosix(realTemporaryRoot))
                Files.createDirectory(operationDirectory,
                        PosixFilePermissions.asFileAttribute(PRIVATE_DIRECTORY_PERMISSIONS));
            else
                Files.createDirectory(operationDirectory);
            BasicFileAttributes attributes = Files.readAttributes(
                    operationDirectory, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!attributes.isDirectory() || attributes.isSymbolicLink())
                throw new IllegalStateException(
                        "DuckDB operation directory creation was redirected");
            requirePrivatePosixDirectory(operationDirectory);

            connection = DriverManager.getConnection("jdbc:duckdb:");
            try (Statement statement = connection.createStatement()) {
                statement.execute("SET memory_limit='" + config.duckDbMemoryLimitMegabytes + "MB'");
                statement.execute("SET threads=" + config.duckDbThreads);
                statement.execute("SET temp_directory='" + sqlLiteral(operationDirectory.toString()) + "'");
                statement.execute("SET max_temp_directory_size='" +
                        config.duckDbMaximumTemporaryMegabytes + "MB'");
                // Global insertion-order preservation is unnecessary for the
                // archive's explicitly ordered writes/reads and can retain a
                // large extra buffer during near-cap operations.
                statement.execute("SET preserve_insertion_order=false");
            }
            return new Session(connection, realTemporaryRoot, operationDirectory, permit);
        } catch (Exception | Error error) {
            closeQuietly(connection, error);
            if (realTemporaryRoot != null && operationDirectory != null)
                deleteOperationDirectory(realTemporaryRoot, operationDirectory, error);
            permit.close();
            throw error;
        }
    }

    private static void preparePrivateDirectory(Path directory) throws IOException {
        if (supportsPosix(nearestExistingAncestor(directory)))
            Files.createDirectories(directory,
                    PosixFilePermissions.asFileAttribute(PRIVATE_DIRECTORY_PERMISSIONS));
        else
            Files.createDirectories(directory);
        BasicFileAttributes attributes = Files.readAttributes(
                directory, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!attributes.isDirectory() || attributes.isSymbolicLink())
            throw new IllegalStateException("DuckDB temporary directory is not a real directory: " + directory);
        requirePrivatePosixDirectory(directory);
    }

    private static Path nearestExistingAncestor(Path path) {
        Path current = path;
        while (current != null && !Files.exists(current, LinkOption.NOFOLLOW_LINKS))
            current = current.getParent();
        if (current == null)
            throw new IllegalStateException("DuckDB temporary directory has no existing filesystem ancestor");
        return current;
    }

    private static boolean supportsPosix(Path path) {
        return Files.getFileAttributeView(
                path, PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS) != null;
    }

    private static void requirePrivatePosixDirectory(Path directory) throws IOException {
        if (!supportsPosix(directory))
            return;
        Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(
                directory, LinkOption.NOFOLLOW_LINKS);
        if (!permissions.equals(PRIVATE_DIRECTORY_PERMISSIONS))
            throw new IllegalStateException(
                    "DuckDB temporary directory must have POSIX mode 0700: " + directory);
    }

    private static void requireDisjointRoots(Path archiveRoot, Path temporaryRoot) {
        if (archiveRoot.equals(temporaryRoot) || archiveRoot.startsWith(temporaryRoot) ||
                temporaryRoot.startsWith(archiveRoot))
            throw new IllegalArgumentException(
                    "DuckDB temporary directory and immutable archive root must be disjoint");
    }

    private static void deleteOperationDirectory(Path realTemporaryRoot, Path operationDirectory,
                                                 Throwable primary) {
        try {
            Path normalized = operationDirectory.toAbsolutePath().normalize();
            if (!normalized.getParent().equals(realTemporaryRoot) || normalized.equals(realTemporaryRoot))
                throw new IllegalStateException("Refusing unsafe DuckDB temporary cleanup: " + normalized);
            // Files.exists() also returns false when existence cannot be
            // determined. Only a positively confirmed absence may make
            // cleanup a no-op; inaccessible/indeterminate paths must fail
            // visibly through walkFileTree below.
            if (Files.notExists(normalized, LinkOption.NOFOLLOW_LINKS))
                return;
            Files.walkFileTree(normalized, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path directory, IOException error) throws IOException {
                    if (error != null)
                        throw error;
                    Files.delete(directory);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (Throwable cleanupError) {
            primary.addSuppressed(cleanupError);
        }
    }

    private static void closeQuietly(Connection connection, Throwable primary) {
        if (connection == null)
            return;
        try {
            connection.close();
        } catch (Throwable closeError) {
            primary.addSuppressed(closeError);
        }
    }

    private static String sqlLiteral(String value) {
        return value.replace("'", "''");
    }

    static final class Session implements AutoCloseable {
        private final Connection connection;
        private final Path temporaryRoot;
        private final Path operationDirectory;
        private final AdmissionPermit permit;
        private boolean closed;

        private Session(Connection connection, Path temporaryRoot, Path operationDirectory,
                        AdmissionPermit permit) {
            this.connection = connection;
            this.temporaryRoot = temporaryRoot;
            this.operationDirectory = operationDirectory;
            this.permit = permit;
        }

        Connection connection() {
            return connection;
        }

        Path operationDirectory() {
            return operationDirectory;
        }

        @Override
        public synchronized void close() {
            if (closed)
                return;
            closed = true;
            Throwable failure = null;
            boolean connectionFailed = false;
            try {
                connection.close();
            } catch (Throwable error) {
                failure = error;
                connectionFailed = true;
            }
            if (failure == null)
                failure = new IllegalStateException("DuckDB temporary cleanup failed");
            int priorSuppressed = failure.getSuppressed().length;
            deleteOperationDirectory(temporaryRoot, operationDirectory, failure);
            boolean cleanupFailed = failure.getSuppressed().length != priorSuppressed;
            try {
                if (cleanupFailed || connectionFailed)
                    throw new IllegalStateException("Cannot close bounded DuckDB session", failure);
            } finally {
                permit.close();
            }
        }
    }

    /**
     * Fair process-wide admission.  Every active and waiting request keeps its
     * configured cap registered, so independently configured stores cannot
     * evade the smallest live cap.  FIFO admission prevents a stream of short
     * queries from starving an exporter already waiting at the head.
     */
    private static final class AdmissionController {
        private final ReentrantLock lock = new ReentrantLock(true);
        private final Condition capacityChanged = lock.newCondition();
        private final Deque<AdmissionRequest> waiting = new ArrayDeque<>();
        private final TreeMap<Integer, Integer> registeredLimits = new TreeMap<>();
        private int active;

        AdmissionPermit acquire(int limit, int timeoutSeconds, String operation) throws Exception {
            long remaining = TimeUnit.SECONDS.toNanos(timeoutSeconds);
            AdmissionRequest request = new AdmissionRequest();
            try {
                lock.lockInterruptibly();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new SQLTransientException(
                        "Interrupted while waiting for DuckDB session capacity for operation " +
                                operation, interrupted);
            }
            try {
                waiting.addLast(request);
                addLimit(limit);
                while (waiting.peekFirst() != request || active >= registeredLimits.firstKey()) {
                    if (remaining <= 0) {
                        waiting.remove(request);
                        removeLimit(limit);
                        capacityChanged.signalAll();
                        throw new SQLTransientException(
                                "DuckDB session capacity is busy for operation " + operation +
                                        " after " + timeoutSeconds + " seconds");
                    }
                    remaining = capacityChanged.awaitNanos(remaining);
                }
                waiting.removeFirst();
                active++;
                capacityChanged.signalAll();
                return new AdmissionPermit(this, limit);
            } catch (InterruptedException interrupted) {
                waiting.remove(request);
                removeLimit(limit);
                capacityChanged.signalAll();
                Thread.currentThread().interrupt();
                throw new SQLTransientException(
                        "Interrupted while waiting for DuckDB session capacity for operation " +
                                operation, interrupted);
            } finally {
                lock.unlock();
            }
        }

        private void release(int limit) {
            lock.lock();
            try {
                if (active <= 0)
                    throw new IllegalStateException("DuckDB admission permit released without an active session");
                active--;
                removeLimit(limit);
                capacityChanged.signalAll();
            } finally {
                lock.unlock();
            }
        }

        private void addLimit(int limit) {
            registeredLimits.merge(limit, 1, Integer::sum);
        }

        private void removeLimit(int limit) {
            Integer count = registeredLimits.get(limit);
            if (count == null)
                throw new IllegalStateException("DuckDB admission limit registration is missing");
            if (count == 1)
                registeredLimits.remove(limit);
            else
                registeredLimits.put(limit, count - 1);
        }
    }

    private static final class AdmissionRequest {
    }

    private static final class AdmissionPermit implements AutoCloseable {
        private final AdmissionController controller;
        private final int limit;
        private boolean closed;

        private AdmissionPermit(AdmissionController controller, int limit) {
            this.controller = controller;
            this.limit = limit;
        }

        @Override
        public synchronized void close() {
            if (closed)
                return;
            closed = true;
            controller.release(limit);
        }
    }
}

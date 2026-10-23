package org.sensorhub.impl.datastore.postgis.connection;

import org.junit.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TestCancellableConnectionScope {

    @Test
    public void cancellationAbortsOnlyConnectionsOwnedByTheScopedThread() throws Exception {
        ConnectionProbe owned = new ConnectionProbe();
        ConnectionProbe unrelated = new ConnectionProbe();
        CountDownLatch unrelatedReady = new CountDownLatch(1);
        CountDownLatch releaseUnrelated = new CountDownLatch(1);
        AtomicReference<Throwable> unrelatedFailure = new AtomicReference<>();

        Thread other = new Thread(() -> {
            try (CancellableConnectionScope ignored = CancellableConnectionScope.open();
                 Connection connection = CancellableConnectionScope.track(unrelated.connection())) {
                unrelatedReady.countDown();
                assertTrue(releaseUnrelated.await(2, TimeUnit.SECONDS));
            } catch (Throwable error) {
                unrelatedFailure.set(error);
            }
        }, "unrelated-postgis-query");

        try (CancellableConnectionScope scope = CancellableConnectionScope.open();
             Connection connection = CancellableConnectionScope.track(owned.connection())) {
            other.start();
            assertTrue(unrelatedReady.await(2, TimeUnit.SECONDS));
            scope.cancel();
            assertEquals(1, owned.abortCount.get());
            assertEquals("Cancellation must not touch another thread's operation",
                    0, unrelated.abortCount.get());
        } finally {
            releaseUnrelated.countDown();
            other.join(2_000L);
        }

        assertNull(unrelatedFailure.get());
        assertEquals(0, unrelated.abortCount.get());
        assertEquals(1, unrelated.closeCount.get());
    }

    @Test
    public void cancellationDoesNotTouchConnectionAcquiredBeforeTheScope() throws Exception {
        ConnectionProbe unscoped = new ConnectionProbe();
        Connection connection = CancellableConnectionScope.track(unscoped.connection());

        try (CancellableConnectionScope scope = CancellableConnectionScope.open()) {
            scope.cancel();
        }

        assertEquals(0, unscoped.abortCount.get());
        assertEquals(0, unscoped.closeCount.get());
        connection.close();
        assertEquals(1, unscoped.closeCount.get());
    }

    @Test
    public void completedConnectionIsNotAbortedByLaterCancellation() throws Exception {
        ConnectionProbe completed = new ConnectionProbe();
        try (CancellableConnectionScope scope = CancellableConnectionScope.open()) {
            Connection connection = CancellableConnectionScope.track(completed.connection());
            connection.close();
            scope.cancel();
        }

        assertEquals(0, completed.abortCount.get());
        assertEquals(1, completed.closeCount.get());
    }

    @Test
    public void standaloneStatementParticipatesInCancellationAndClosesExactlyOnce()
            throws Exception {
        StatementProbe duckDb = new StatementProbe();
        try (CancellableConnectionScope scope = CancellableConnectionScope.open();
             Statement statement = CancellableConnectionScope.trackStandaloneStatement(
                     duckDb.statement(), "DuckDB cold query")) {
            scope.cancel();
            scope.cancel();

            assertEquals(1, duckDb.cancelCount.get());
            assertThrows(CancellationException.class,
                    () -> statement.executeQuery("SELECT 1"));
            statement.close();
            statement.close();
        }

        assertEquals(1, duckDb.closeCount.get());
    }

    @Test
    public void cancelledScopeRejectsAndCleansUpLateStandaloneStatement() {
        StatementProbe duckDb = new StatementProbe();
        try (CancellableConnectionScope scope = CancellableConnectionScope.open()) {
            scope.cancel();
            assertThrows(CancellationException.class,
                    () -> CancellableConnectionScope.trackStandaloneStatement(
                            duckDb.statement(), "DuckDB metadata query"));
        }

        assertEquals(1, duckDb.cancelCount.get());
        assertEquals(1, duckDb.closeCount.get());
    }

    @Test
    public void scopeCloseClosesLeakedStandaloneStatement() {
        StatementProbe duckDb = new StatementProbe();
        try (CancellableConnectionScope ignored = CancellableConnectionScope.open()) {
            CancellableConnectionScope.trackStandaloneStatement(
                    duckDb.statement(), "DuckDB cold count");
        }

        assertEquals(0, duckDb.cancelCount.get());
        assertEquals(1, duckDb.closeCount.get());
    }

    @Test
    public void concurrentCancelTrackAndStandaloneCloseNeverLeaksOrDoubleCancels()
            throws Exception {
        for (int iteration = 0; iteration < 100; iteration++) {
            StatementProbe duckDb = new StatementProbe();
            CancellableConnectionScope scope = CancellableConnectionScope.open();
            CountDownLatch start = new CountDownLatch(1);
            AtomicReference<Throwable> cancellerFailure = new AtomicReference<>();
            Thread canceller = new Thread(() -> {
                try {
                    assertTrue(start.await(2, TimeUnit.SECONDS));
                    scope.cancel();
                    scope.cancel();
                } catch (Throwable error) {
                    cancellerFailure.set(error);
                }
            }, "standalone-scope-canceller-" + iteration);
            canceller.start();

            Statement statement = null;
            try {
                start.countDown();
                try {
                    statement = CancellableConnectionScope.trackStandaloneStatement(
                            duckDb.statement(), "DuckDB cold query");
                } catch (CancellationException expected) {
                    // Cancellation won before registration completed.
                }
                if (statement != null)
                    statement.close();
                canceller.join(2_000L);
                assertFalse(canceller.isAlive());
                assertNull(cancellerFailure.get());
            } finally {
                scope.close();
            }

            assertTrue("A scoped standalone statement must be cancelled at most once",
                    duckDb.cancelCount.get() <= 1);
            assertEquals("Every scoped standalone statement must be closed exactly once",
                    1, duckDb.closeCount.get());
        }
    }

    @Test
    public void cancelledScopeRejectsAndAbortsNewConnections() {
        ConnectionProbe late = new ConnectionProbe();
        try (CancellableConnectionScope scope = CancellableConnectionScope.open()) {
            scope.cancel();
            assertThrows(CancellationException.class,
                    () -> CancellableConnectionScope.track(late.connection()));
        }

        assertEquals(1, late.abortCount.get());
        assertEquals(1, late.closeCount.get());
    }

    @Test
    public void cancellationIsIdempotentAndOnlyTheOwnerMayCloseTheScope() throws Exception {
        ConnectionProbe probe = new ConnectionProbe();
        CancellableConnectionScope scope = CancellableConnectionScope.open();
        AtomicReference<Throwable> nonOwnerFailure = new AtomicReference<>();
        try (Connection ignored = CancellableConnectionScope.track(probe.connection())) {
            scope.cancel();
            scope.cancel();

            Thread nonOwner = new Thread(() -> {
                try {
                    scope.close();
                } catch (Throwable error) {
                    nonOwnerFailure.set(error);
                }
            }, "non-owner-scope-close");
            nonOwner.start();
            nonOwner.join(2_000L);

            assertFalse(nonOwner.isAlive());
            assertTrue(nonOwnerFailure.get() instanceof IllegalStateException);
            assertEquals(1, probe.abortCount.get());
        } finally {
            scope.close();
        }
        assertEquals(1, probe.closeCount.get());
    }

    @Test
    public void concurrentCancelTrackAndConnectionCloseNeverLeaksOrDoubleAborts() throws Exception {
        for (int iteration = 0; iteration < 250; iteration++) {
            ConnectionProbe probe = new ConnectionProbe();
            CancellableConnectionScope scope = CancellableConnectionScope.open();
            CountDownLatch start = new CountDownLatch(1);
            AtomicReference<Throwable> cancellerFailure = new AtomicReference<>();
            Thread canceller = new Thread(() -> {
                try {
                    assertTrue(start.await(2, TimeUnit.SECONDS));
                    scope.cancel();
                    scope.cancel();
                } catch (Throwable error) {
                    cancellerFailure.set(error);
                }
            }, "scope-canceller-" + iteration);
            canceller.start();

            Connection connection = null;
            try {
                start.countDown();
                try {
                    connection = CancellableConnectionScope.track(probe.connection());
                } catch (CancellationException expected) {
                    // Cancellation won before registration completed.
                }
                if (connection != null)
                    connection.close();
                canceller.join(2_000L);
                assertFalse(canceller.isAlive());
                assertNull(cancellerFailure.get());
            } finally {
                scope.close();
            }

            assertTrue("A scoped connection must be aborted at most once",
                    probe.abortCount.get() <= 1);
            assertEquals("Every tracked connection must be closed exactly once",
                    1, probe.closeCount.get());
        }
    }

    @Test
    public void persistentCloseFailureIsReportedAfterBestEffortAbortAndDoesNotPinThreadLocal() {
        ConnectionProbe probe = new ConnectionProbe();
        probe.closeFailures.set(Integer.MAX_VALUE);
        CancellableConnectionScope scope = CancellableConnectionScope.open();
        CancellableConnectionScope.track(probe.connection());

        IllegalStateException failure = assertThrows(IllegalStateException.class, scope::close);
        assertTrue(scope.isCancellationRequested());
        assertEquals("A failed close must trigger the mandatory abort fallback",
                1, probe.abortCount.get());
        assertEquals(2, probe.closeAttempts.get());
        assertEquals(0, probe.closeCount.get());
        assertSame("A terminal cleanup failure must never be forgotten",
                failure, assertThrows(IllegalStateException.class, scope::close));
        assertSame("Cancel must report the same terminal cleanup failure",
                failure, assertThrows(IllegalStateException.class, scope::cancel));

        try (CancellableConnectionScope ignored = CancellableConnectionScope.open()) {
            // Terminal failure was surfaced but did not leak the old scope.
        }
    }

    @Test
    public void closeFailureAfterSuccessfulAbortRemainsSticky() throws Exception {
        ConnectionProbe probe = new ConnectionProbe();
        probe.closeFailures.set(Integer.MAX_VALUE);
        CancellableConnectionScope scope = CancellableConnectionScope.open();
        CancellableConnectionScope.track(probe.connection());

        scope.cancel();
        IllegalStateException failure = assertThrows(IllegalStateException.class, scope::close);

        assertEquals(1, probe.abortCount.get());
        assertEquals(1, probe.closeAttempts.get());
        assertSame(failure, assertThrows(IllegalStateException.class, scope::close));
        try (CancellableConnectionScope ignored = CancellableConnectionScope.open()) {
            // An aborted connection's close error remains visible without
            // retaining a failed scope in this thread.
        }
    }

    @Test
    public void cancellationCancelsTrackedStatementThenAbortsConnectionAndRejectsNewWork()
            throws Exception {
        ConnectionProbe probe = new ConnectionProbe();
        try (CancellableConnectionScope scope = CancellableConnectionScope.open();
             Connection connection = CancellableConnectionScope.track(probe.connection());
             Statement statement = connection.createStatement()) {
            assertSame("Statement.getConnection must not escape the scoped proxy",
                    connection, statement.getConnection());
            scope.cancel();
            scope.cancel();

            assertEquals(1, probe.statement.cancelCount.get());
            assertEquals(1, probe.abortCount.get());
            assertThrows(CancellationException.class, connection::createStatement);
            assertEquals("No statement may be created after scope cancellation",
                    1, probe.statementCreateCount.get());
            statement.close();
            statement.close();
            assertEquals("Statement close must be idempotent", 1, probe.statement.closeCount.get());
        }
        assertEquals(1, probe.statement.closeCount.get());
    }

    @Test
    public void connectionAbortStillRunsWhenStatementCancellationFails() throws Exception {
        ConnectionProbe probe = new ConnectionProbe();
        probe.statement.cancelFailures.set(1);
        CancellableConnectionScope scope = CancellableConnectionScope.open();
        Connection connection = CancellableConnectionScope.track(probe.connection());
        Statement statement = connection.createStatement();
        try {
            IllegalStateException failure = assertThrows(IllegalStateException.class, scope::cancel);
            assertTrue(scope.isCancellationRequested());
            assertEquals(1, probe.statement.cancelCount.get());
            assertEquals("Connection.abort is the mandatory fallback",
                    1, probe.abortCount.get());
            assertSame("Cancellation failure must be sticky",
                    failure, assertThrows(IllegalStateException.class, scope::cancel));
        } finally {
            statement.close();
            connection.close();
        }
        IllegalStateException closeFailure =
                assertThrows(IllegalStateException.class, scope::close);
        assertSame("Scope close failure must remain sticky",
                closeFailure, assertThrows(IllegalStateException.class, scope::close));
    }

    @Test
    public void failedConnectionAbortPoisonsScopeEvenWhenConnectionLaterCloses() throws Exception {
        ConnectionProbe probe = new ConnectionProbe();
        probe.abortFailures.set(1);
        CancellableConnectionScope scope = CancellableConnectionScope.open();
        Connection connection = CancellableConnectionScope.track(probe.connection());

        IllegalStateException failure = assertThrows(IllegalStateException.class, scope::cancel);
        assertTrue(scope.isCancellationRequested());
        assertEquals(1, probe.abortAttempts.get());
        assertEquals(0, probe.abortCount.get());
        assertSame(failure, assertThrows(IllegalStateException.class, scope::cancel));

        connection.close();
        IllegalStateException closeFailure =
                assertThrows(IllegalStateException.class, scope::close);
        assertSame(closeFailure, assertThrows(IllegalStateException.class, scope::close));
    }

    @Test
    public void cancellationUsesOneDeadlineAcrossMultipleSlowResources() throws Exception {
        CountDownLatch started = new CountDownLatch(4);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(4);
        ConnectionProbe first = new ConnectionProbe();
        ConnectionProbe second = new ConnectionProbe();
        first.blockCancellation(started, release, finished);
        second.blockCancellation(started, release, finished);

        CancellableConnectionScope scope =
                CancellableConnectionScope.open(150L, TimeUnit.MILLISECONDS);
        Connection firstConnection = CancellableConnectionScope.track(first.connection());
        Connection secondConnection = CancellableConnectionScope.track(second.connection());
        Statement firstStatement = firstConnection.createStatement();
        Statement secondStatement = secondConnection.createStatement();

        long startedAt = System.nanoTime();
        IllegalStateException failure = assertThrows(IllegalStateException.class, scope::cancel);
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);

        assertTrue("All independent cancellation actions must start concurrently",
                started.await(1, TimeUnit.SECONDS));
        assertTrue("Four blocked resources must share one deadline, not four serial budgets: "
                        + elapsedMillis + " ms",
                elapsedMillis < 500L);
        assertTrue(scope.isCancellationRequested());
        assertSame("A timed-out cancellation result must be persistent",
                failure, assertThrows(IllegalStateException.class, scope::cancel));

        release.countDown();
        assertTrue(finished.await(2, TimeUnit.SECONDS));
        firstStatement.close();
        secondStatement.close();
        firstConnection.close();
        secondConnection.close();
        assertThrows(IllegalStateException.class, scope::close);
        assertEquals(1, first.abortCount.get());
        assertEquals(1, second.abortCount.get());
        assertEquals(1, first.statement.cancelCount.get());
        assertEquals(1, second.statement.cancelCount.get());
    }

    @Test
    public void statementCreatedDuringCancellationCannotEscapeTheScope() throws Exception {
        ConnectionProbe probe = new ConnectionProbe();
        CountDownLatch createStarted = new CountDownLatch(1);
        CountDownLatch releaseCreate = new CountDownLatch(1);
        probe.statementCreateStarted = createStarted;
        probe.releaseStatementCreate = releaseCreate;

        CancellableConnectionScope scope = CancellableConnectionScope.open();
        Connection connection = CancellableConnectionScope.track(probe.connection());
        AtomicReference<Throwable> cancellerFailure = new AtomicReference<>();
        Thread canceller = new Thread(() -> {
            try {
                assertTrue(createStarted.await(2, TimeUnit.SECONDS));
                scope.cancel();
            } catch (Throwable error) {
                cancellerFailure.set(error);
            } finally {
                releaseCreate.countDown();
            }
        }, "late-statement-canceller");
        canceller.start();

        assertThrows(CancellationException.class, connection::createStatement);
        canceller.join(2_000L);
        assertFalse(canceller.isAlive());
        assertNull(cancellerFailure.get());
        assertEquals(1, probe.abortCount.get());
        assertEquals(1, probe.statement.cancelCount.get());
        assertEquals(1, probe.statement.closeCount.get());
        connection.close();
        scope.close();
    }

    private static final class ConnectionProbe {
        final AtomicInteger abortCount = new AtomicInteger();
        final AtomicInteger abortAttempts = new AtomicInteger();
        final AtomicInteger abortFailures = new AtomicInteger();
        final AtomicInteger closeAttempts = new AtomicInteger();
        final AtomicInteger closeCount = new AtomicInteger();
        final AtomicInteger closeFailures = new AtomicInteger();
        final AtomicInteger statementCreateCount = new AtomicInteger();
        final AtomicBoolean closed = new AtomicBoolean();
        final StatementProbe statement = new StatementProbe();
        volatile CountDownLatch abortStarted;
        volatile CountDownLatch releaseAbort;
        volatile CountDownLatch abortFinished;
        volatile CountDownLatch statementCreateStarted;
        volatile CountDownLatch releaseStatementCreate;

        void blockCancellation(
                CountDownLatch started, CountDownLatch release, CountDownLatch finished) {
            abortStarted = started;
            releaseAbort = release;
            abortFinished = finished;
            statement.cancelStarted = started;
            statement.releaseCancel = release;
            statement.cancelFinished = finished;
        }

        Connection connection() {
            return (Connection) Proxy.newProxyInstance(
                    Connection.class.getClassLoader(),
                    new Class<?>[]{Connection.class},
                    (proxy, method, args) -> switch (method.getName()) {
                        case "abort" -> {
                            abortAttempts.incrementAndGet();
                            if (abortFailures.getAndUpdate(value -> Math.max(0, value - 1)) > 0)
                                throw new SQLException("fixture abort failure");
                            ((Executor) args[0]).execute(() -> {
                                abortCount.incrementAndGet();
                                countDown(abortStarted);
                                awaitUninterruptibly(releaseAbort);
                                closed.set(true);
                                countDown(abortFinished);
                            });
                            yield null;
                        }
                        case "close" -> {
                            closeAttempts.incrementAndGet();
                            if (closeFailures.getAndUpdate(value -> Math.max(0, value - 1)) > 0)
                                throw new SQLException("fixture close failure");
                            closeCount.incrementAndGet();
                            closed.set(true);
                            yield null;
                        }
                        case "createStatement" -> {
                            statementCreateCount.incrementAndGet();
                            countDown(statementCreateStarted);
                            awaitUninterruptibly(releaseStatementCreate);
                            yield statement.statement();
                        }
                        case "isClosed" -> closed.get();
                        case "equals" -> proxy == args[0];
                        case "hashCode" -> System.identityHashCode(proxy);
                        case "toString" -> "ConnectionProbe";
                        default -> defaultValue(method.getReturnType());
                    });
        }

        private static Object defaultValue(Class<?> type) {
            if (!type.isPrimitive())
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
            return null;
        }
    }

    private static final class StatementProbe {
        final AtomicInteger cancelCount = new AtomicInteger();
        final AtomicInteger cancelFailures = new AtomicInteger();
        final AtomicInteger closeCount = new AtomicInteger();
        volatile CountDownLatch cancelStarted;
        volatile CountDownLatch releaseCancel;
        volatile CountDownLatch cancelFinished;

        Statement statement() {
            return (Statement) Proxy.newProxyInstance(
                    Statement.class.getClassLoader(), new Class<?>[]{Statement.class},
                    (proxy, method, args) -> switch (method.getName()) {
                        case "cancel" -> {
                            cancelCount.incrementAndGet();
                            countDown(cancelStarted);
                            awaitUninterruptibly(releaseCancel);
                            if (cancelFailures.getAndUpdate(value -> Math.max(0, value - 1)) > 0)
                                throw new SQLException("fixture statement cancellation failure");
                            countDown(cancelFinished);
                            yield null;
                        }
                        case "close" -> {
                            closeCount.incrementAndGet();
                            yield null;
                        }
                        case "equals" -> proxy == args[0];
                        case "hashCode" -> System.identityHashCode(proxy);
                        case "toString" -> "StatementProbe";
                        default -> ConnectionProbe.defaultValue(method.getReturnType());
            });
        }
    }

    private static void countDown(CountDownLatch latch) {
        if (latch != null)
            latch.countDown();
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        if (latch == null)
            return;
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        if (interrupted)
            Thread.currentThread().interrupt();
    }
}

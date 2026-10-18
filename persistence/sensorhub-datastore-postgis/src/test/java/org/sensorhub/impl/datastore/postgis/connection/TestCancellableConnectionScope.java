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
    public void failedConnectionCloseRemainsTrackedAndScopeCloseCanBeRetried() {
        ConnectionProbe probe = new ConnectionProbe();
        probe.closeFailures.set(1);
        CancellableConnectionScope scope = CancellableConnectionScope.open();
        CancellableConnectionScope.track(probe.connection());

        assertThrows(IllegalStateException.class, scope::close);
        assertThrows("A failed close must leave the original scope current",
                IllegalStateException.class, CancellableConnectionScope::open);

        scope.cancel();
        assertEquals("The connection whose close failed must still be cancellable",
                1, probe.abortCount.get());
        scope.close();

        assertEquals(2, probe.closeAttempts.get());
        assertEquals(1, probe.closeCount.get());
        try (CancellableConnectionScope ignored = CancellableConnectionScope.open()) {
            // Successful retry removed the old thread-local scope.
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
        try (CancellableConnectionScope scope = CancellableConnectionScope.open();
             Connection connection = CancellableConnectionScope.track(probe.connection());
             Statement statement = connection.createStatement()) {
            assertThrows(IllegalStateException.class, scope::cancel);
            assertTrue(scope.isCancellationRequested());
            assertEquals(1, probe.statement.cancelCount.get());
            assertEquals("Connection.abort is the mandatory fallback",
                    1, probe.abortCount.get());
        }
    }

    private static final class ConnectionProbe {
        final AtomicInteger abortCount = new AtomicInteger();
        final AtomicInteger closeAttempts = new AtomicInteger();
        final AtomicInteger closeCount = new AtomicInteger();
        final AtomicInteger closeFailures = new AtomicInteger();
        final AtomicInteger statementCreateCount = new AtomicInteger();
        final AtomicBoolean closed = new AtomicBoolean();
        final StatementProbe statement = new StatementProbe();

        Connection connection() {
            return (Connection) Proxy.newProxyInstance(
                    Connection.class.getClassLoader(),
                    new Class<?>[]{Connection.class},
                    (proxy, method, args) -> switch (method.getName()) {
                        case "abort" -> {
                            ((Executor) args[0]).execute(abortCount::incrementAndGet);
                            closed.set(true);
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

        Statement statement() {
            return (Statement) Proxy.newProxyInstance(
                    Statement.class.getClassLoader(), new Class<?>[]{Statement.class},
                    (proxy, method, args) -> switch (method.getName()) {
                        case "cancel" -> {
                            cancelCount.incrementAndGet();
                            if (cancelFailures.getAndUpdate(value -> Math.max(0, value - 1)) > 0)
                                throw new SQLException("fixture statement cancellation failure");
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
}

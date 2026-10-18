package org.sensorhub.impl.datastore.postgis.connection;

import org.sensorhub.api.datastore.ICancellableDataStore;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Thread-bound cancellation scope shared by every PostgreSQL connection
 * manager. Sharing the thread-local scope is important because one logical
 * observation query can borrow connections from linked metadata stores.
 */
final class CancellableConnectionScope implements ICancellableDataStore.CancellableOperation {
    private static final ThreadLocal<CancellableConnectionScope> CURRENT = new ThreadLocal<>();
    private static final Executor DIRECT_EXECUTOR = Runnable::run;

    private final Thread owner = Thread.currentThread();
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Set<TrackedConnection> connections = ConcurrentHashMap.newKeySet();

    static CancellableConnectionScope open() {
        if (CURRENT.get() != null)
            throw new IllegalStateException("A cancellable datastore operation is already active on this thread");
        CancellableConnectionScope scope = new CancellableConnectionScope();
        CURRENT.set(scope);
        return scope;
    }

    static Connection track(Connection connection) {
        CancellableConnectionScope scope = CURRENT.get();
        return scope == null ? connection : scope.trackConnection(connection);
    }

    private Connection trackConnection(Connection connection) {
        TrackedConnection tracked = new TrackedConnection(connection);
        connections.add(tracked);
        if (cancelled.get()) {
            RuntimeException failure = null;
            try {
                tracked.abort();
            } catch (RuntimeException error) {
                failure = error;
            }
            try {
                tracked.close();
            } catch (RuntimeException error) {
                if (failure == null)
                    failure = error;
                else
                    failure.addSuppressed(error);
            }
            CancellationException cancellation =
                    new CancellationException("Datastore operation was cancelled");
            if (failure != null)
                cancellation.addSuppressed(failure);
            throw cancellation;
        }
        return tracked.proxy();
    }

    @Override
    public void cancel() {
        cancelled.set(true);
        IllegalStateException failure = null;
        for (TrackedConnection connection : connections) {
            try {
                connection.abort();
            } catch (RuntimeException error) {
                if (failure == null)
                    failure = new IllegalStateException("Could not cancel every scoped PostgreSQL connection");
                failure.addSuppressed(error);
            }
        }
        if (failure != null)
            throw failure;
    }

    @Override
    public boolean isCancellationRequested() {
        return cancelled.get();
    }

    @Override
    public void close() {
        if (Thread.currentThread() != owner)
            throw new IllegalStateException("Cancellable datastore operation must be closed by its owning thread");
        if (closed.get())
            return;
        if (CURRENT.get() != this)
            throw new IllegalStateException("Cancellable datastore operation is not current on this thread");

        IllegalStateException failure = null;
        for (TrackedConnection connection : connections) {
            try {
                connection.close();
            } catch (RuntimeException error) {
                if (failure == null)
                    failure = new IllegalStateException("Could not close every scoped PostgreSQL connection");
                failure.addSuppressed(error);
            }
        }
        if (failure != null)
            throw failure;
        closed.set(true);
        CURRENT.remove();
    }

    private final class TrackedConnection {
        private final Connection delegate;
        private final Connection proxy;
        private final Set<TrackedStatement> statements = ConcurrentHashMap.newKeySet();
        private boolean connectionClosed;
        private boolean connectionAborted;

        TrackedConnection(Connection delegate) {
            this.delegate = delegate;
            this.proxy = (Connection) Proxy.newProxyInstance(
                    Connection.class.getClassLoader(),
                    new Class<?>[]{Connection.class},
                    (ignored, method, args) -> {
                        if (method.getDeclaringClass() == Object.class) {
                            return switch (method.getName()) {
                                case "equals" -> ignored == args[0];
                                case "hashCode" -> System.identityHashCode(ignored);
                                case "toString" -> "Scoped[" + delegate + "]";
                                default -> throw new UnsupportedOperationException(method.getName());
                            };
                        }
                        if (method.getName().equals("close") && method.getParameterCount() == 0) {
                            close();
                            return null;
                        }
                        if (cancelled.get())
                            throw new CancellationException("Datastore operation was cancelled");
                        try {
                            Object result = method.invoke(delegate, args);
                            return result instanceof Statement statement
                                    ? trackStatement(statement) : result;
                        } catch (InvocationTargetException error) {
                            throw error.getCause();
                        }
                    });
        }

        Connection proxy() {
            return proxy;
        }

        synchronized void abort() {
            if (connectionClosed || connectionAborted)
                return;
            IllegalStateException failure = null;
            for (TrackedStatement statement : statements) {
                try {
                    statement.cancel();
                } catch (RuntimeException error) {
                    if (failure == null)
                        failure = new IllegalStateException(
                                "Could not cancel every scoped PostgreSQL statement");
                    failure.addSuppressed(error);
                }
            }
            try {
                delegate.abort(DIRECT_EXECUTOR);
                connectionAborted = true;
            } catch (SQLException error) {
                if (failure == null)
                    failure = new IllegalStateException(
                            "Could not abort scoped PostgreSQL connection");
                failure.addSuppressed(error);
            }
            if (failure != null)
                throw failure;
        }

        synchronized void close() {
            if (connectionClosed)
                return;
            try {
                delegate.close();
                connectionClosed = true;
                connections.remove(this);
            } catch (SQLException error) {
                // Hikari still evicts/recycles a broken pool entry in its
                // close() finally block, but may rethrow the rollback error
                // after JDBC abort has already closed the physical socket.
                if (connectionAborted) {
                    connectionClosed = true;
                    connections.remove(this);
                    return;
                }
                throw new IllegalStateException("Could not close scoped PostgreSQL connection", error);
            }
        }

        private Statement trackStatement(Statement statement) {
            TrackedStatement tracked = new TrackedStatement(statement);
            statements.add(tracked);
            if (cancelled.get()) {
                RuntimeException failure = null;
                try {
                    tracked.cancel();
                } catch (RuntimeException error) {
                    failure = error;
                }
                try {
                    tracked.close();
                } catch (RuntimeException error) {
                    if (failure == null)
                        failure = error;
                    else
                        failure.addSuppressed(error);
                }
                CancellationException cancellation =
                        new CancellationException("Datastore operation was cancelled");
                if (failure != null)
                    cancellation.addSuppressed(failure);
                throw cancellation;
            }
            return tracked.proxy();
        }

        private final class TrackedStatement {
            private final Statement delegate;
            private final Statement proxy;
            private boolean statementClosed;
            private boolean statementCancelled;

            TrackedStatement(Statement delegate) {
                this.delegate = delegate;
                Class<?> statementType = delegate instanceof CallableStatement
                        ? CallableStatement.class
                        : delegate instanceof PreparedStatement
                        ? PreparedStatement.class : Statement.class;
                this.proxy = (Statement) Proxy.newProxyInstance(
                        Statement.class.getClassLoader(), new Class<?>[]{statementType},
                        (ignored, method, args) -> {
                            if (method.getDeclaringClass() == Object.class) {
                                return switch (method.getName()) {
                                    case "equals" -> ignored == args[0];
                                    case "hashCode" -> System.identityHashCode(ignored);
                                    case "toString" -> "Scoped[" + delegate + "]";
                                    default -> throw new UnsupportedOperationException(method.getName());
                                };
                            }
                            if (method.getName().equals("close") && method.getParameterCount() == 0) {
                                close();
                                return null;
                            }
                            if (cancelled.get())
                                throw new CancellationException("Datastore operation was cancelled");
                            if (method.getName().equals("getConnection")
                                    && method.getParameterCount() == 0)
                                return TrackedConnection.this.proxy;
                            try {
                                return method.invoke(delegate, args);
                            } catch (InvocationTargetException error) {
                                throw error.getCause();
                            }
                        });
            }

            Statement proxy() {
                return proxy;
            }

            synchronized void cancel() {
                if (statementClosed || statementCancelled)
                    return;
                try {
                    delegate.cancel();
                    statementCancelled = true;
                } catch (SQLException error) {
                    throw new IllegalStateException("Could not cancel scoped PostgreSQL statement", error);
                }
            }

            synchronized void close() {
                if (statementClosed)
                    return;
                try {
                    delegate.close();
                    statementClosed = true;
                    statements.remove(this);
                } catch (SQLException error) {
                    throw new IllegalStateException("Could not close scoped PostgreSQL statement", error);
                }
            }
        }
    }
}

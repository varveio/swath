/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.store;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A fair, fixed-size pool over an already-opened {@code List<Connection>}, shared by {@link
 * DuckDbListingStore} and {@link SortedParquetStore} so interrupt- and close-discipline (borrow
 * blocking, a suppressed-exception-accumulating {@code close()}) lives in exactly <b>one</b>
 * place rather than being duplicated per store.
 *
 * <p>Connection <em>setup</em> (pragmas such as the Parquet metadata/object cache, temp-database
 * wiring, schema materialisation, etc.) is deliberately <b>not</b> this class's concern — it stays
 * with each store, which opens its own connections however it needs to and hands the finished
 * {@code List<Connection>} to this pool's constructor.
 *
 * <h2>Borrow/release contract</h2>
 * <p>Availability is guarded by a single fair {@link ReentrantLock}; borrowers wait on its {@link
 * Condition} for an idle connection. Because the closed-check and the take happen under the same
 * lock, they are atomic: no borrower can pass the closed-check and then be handed a connection that
 * {@link #close()} concurrently closed.
 * <ul>
 *   <li>{@link #borrow()} blocks on the fair lock's condition until a connection is idle. If the
 *       waiting thread is interrupted, the interrupt flag is restored ({@code
 *       Thread.currentThread().interrupt()}) before an {@link IllegalStateException} wrapping the
 *       {@link InterruptedException} is thrown.</li>
 *   <li>{@link #release(Connection)} must be called exactly once for each connection returned by
 *       {@link #borrow()}. Releasing back into an already-full pool (a caller bug, not normal
 *       operation) throws {@link IllegalStateException}; once the pool is {@link #close() closed}
 *       the connection has already been closed by {@code close()}, so release is a no-op.</li>
 *   <li>{@link #borrow()} on an already-{@link #close() closed} pool throws {@link
 *       IllegalStateException} rather than handing out a connection that may already be closed.
 *       A borrower that is <em>already blocked</em> in {@code borrow()} when {@link #close()} runs
 *       is woken by {@code signalAll()}; it re-checks {@code closed} under the lock and fails fast
 *       with the same {@link IllegalStateException} rather than hanging forever or receiving a
 *       closed connection.</li>
 *   <li>{@link #close()} is idempotent: the first call closes every pooled connection,
 *       accumulating any {@link SQLException}s as suppressed exceptions on a single returned
 *       {@link RuntimeException} (never thrown directly — callers that need to fold in their own
 *       close-time failures, e.g. deleting a temp database file, accumulate onto the returned
 *       exception before deciding whether to throw it); later calls are a no-op returning {@code
 *       null}.</li>
 * </ul>
 */
final class DuckDbConnectionPool {

    private final List<Connection> connections;
    private final ReentrantLock lock = new ReentrantLock(true);
    private final Condition available = lock.newCondition();
    private final Deque<Connection> idle = new ArrayDeque<>();
    private boolean closed = false;
    private final String interruptedMessage;
    private final String rejectedMessage;
    private final String closeFailureMessage;

    DuckDbConnectionPool(List<Connection> connections, String interruptedMessage, String rejectedMessage,
                         String closeFailureMessage) {
        this.connections = List.copyOf(connections);
        this.idle.addAll(this.connections);
        this.interruptedMessage = interruptedMessage;
        this.rejectedMessage = rejectedMessage;
        this.closeFailureMessage = closeFailureMessage;
    }

    /** The pooled connections, in construction order. */
    List<Connection> connections() {
        return connections;
    }

    Connection borrow() {
        lock.lock();
        try {
            while (!closed && idle.isEmpty()) {
                available.await();
            }
            if (closed) {
                throw new IllegalStateException("connection pool is closed");
            }
            return idle.removeFirst();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interruptedMessage, e);
        } finally {
            lock.unlock();
        }
    }

    void release(Connection connection) {
        lock.lock();
        try {
            if (closed) {
                // close() has already closed this connection, so there is nothing to return.
                return;
            }
            if (idle.size() >= connections.size()) {
                throw new IllegalStateException(rejectedMessage);
            }
            idle.addLast(connection);
            available.signal();
        } finally {
            lock.unlock();
        }
    }

    RuntimeException close() {
        lock.lock();
        try {
            if (closed) {
                return null;
            }
            closed = true;
            // Wake every waiter; each re-checks closed under the lock and fails fast.
            available.signalAll();
        } finally {
            lock.unlock();
        }
        // Closing outside the lock is safe: closed is already set, so no borrow can succeed.
        RuntimeException failure = null;
        for (Connection connection : connections) {
            try {
                connection.close();
            } catch (SQLException e) {
                if (failure == null) {
                    failure = new IllegalStateException(closeFailureMessage, e);
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        return failure;
    }
}

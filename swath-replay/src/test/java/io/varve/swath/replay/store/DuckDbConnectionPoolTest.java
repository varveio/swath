/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class DuckDbConnectionPoolTest {

    private static final String INTERRUPTED = "interrupted waiting for a test connection";
    private static final String REJECTED = "test connection pool rejected a returned connection";
    private static final String CLOSE_FAILURE = "failed to close test connection pool";

    @Test
    void borrowAllThenBlockThenRelease() throws Exception {
        Connection first = fakeConnection(false);
        Connection second = fakeConnection(false);
        DuckDbConnectionPool pool = new DuckDbConnectionPool(List.of(first, second), INTERRUPTED, REJECTED, CLOSE_FAILURE);

        Connection borrowed1 = pool.borrow();
        Connection borrowed2 = pool.borrow();
        assertThat(List.of(borrowed1, borrowed2)).containsExactlyInAnyOrder(first, second);

        CountDownLatch borrowedByOtherThread = new CountDownLatch(1);
        AtomicReference<Connection> received = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            Connection connection = pool.borrow();
            received.set(connection);
            borrowedByOtherThread.countDown();
        });
        waiter.start();

        // The waiter should be blocked: nothing is available yet.
        assertThat(borrowedByOtherThread.await(200, TimeUnit.MILLISECONDS)).isFalse();

        pool.release(borrowed1);
        assertThat(borrowedByOtherThread.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(received.get()).isEqualTo(borrowed1);

        waiter.join(5000);
        pool.release(borrowed2);
        pool.release(received.get());
    }

    @Test
    void closeAccumulatesSuppressedExceptionsFromEachFailingConnection() {
        Connection ok = fakeConnection(false);
        Connection failing1 = fakeConnection(true);
        Connection failing2 = fakeConnection(true);
        DuckDbConnectionPool pool = new DuckDbConnectionPool(
                List.of(ok, failing1, failing2), INTERRUPTED, REJECTED, CLOSE_FAILURE);

        RuntimeException failure = pool.close();

        assertThat(failure).isNotNull();
        assertThat(failure).isInstanceOf(IllegalStateException.class).hasMessage(CLOSE_FAILURE);
        assertThat(failure.getCause()).isInstanceOf(SQLException.class);
        assertThat(failure.getSuppressed()).hasSize(1);
        assertThat(failure.getSuppressed()[0]).isInstanceOf(SQLException.class);
    }

    @Test
    void closeIsIdempotentAndReturnsNullOnSecondCall() {
        Connection connection = fakeConnection(false);
        DuckDbConnectionPool pool = new DuckDbConnectionPool(List.of(connection), INTERRUPTED, REJECTED, CLOSE_FAILURE);

        assertThat(pool.close()).isNull();
        assertThat(pool.close()).isNull();
    }

    @Test
    void borrowAfterCloseFails() {
        Connection connection = fakeConnection(false);
        DuckDbConnectionPool pool = new DuckDbConnectionPool(List.of(connection), INTERRUPTED, REJECTED, CLOSE_FAILURE);

        pool.close();

        assertThatThrownBy(pool::borrow).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void closeWakesABlockedBorrowerWhichFailsFastInsteadOfHangingOrGettingAClosedConnection() throws Exception {
        Connection only = fakeConnection(false);
        DuckDbConnectionPool pool = new DuckDbConnectionPool(List.of(only), INTERRUPTED, REJECTED, CLOSE_FAILURE);
        pool.borrow(); // drain the only connection so the next borrow() blocks in take()

        AtomicReference<Throwable> thrown = new AtomicReference<>();
        AtomicReference<Connection> handedOut = new AtomicReference<>();
        CountDownLatch reachedBorrow = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(1);
        Thread waiter = new Thread(() -> {
            reachedBorrow.countDown();
            try {
                handedOut.set(pool.borrow());
            } catch (Throwable t) {
                thrown.set(t);
            } finally {
                done.countDown();
            }
        });
        waiter.setDaemon(true); // so a hang against the buggy pool cannot wedge JVM shutdown
        waiter.start();

        reachedBorrow.await();
        Thread.sleep(100); // give the waiter time to reach the blocking take()

        pool.close();

        // Against a broken pool this borrower blocks forever: the latch never fires and this
        // assertion (not a JVM hang) fails. Against the fix it is woken by signalAll() and fails
        // fast under the lock.
        assertThat(done.await(5, TimeUnit.SECONDS))
                .as("close() must wake the blocked borrower rather than leave it hung")
                .isTrue();
        assertThat(thrown.get())
                .as("a woken borrower on a closed pool must fail fast")
                .isInstanceOf(IllegalStateException.class);
        assertThat(handedOut.get())
                .as("a closed pool must never hand out a (closed) connection")
                .isNull();

        waiter.join(5000);
    }

    @Test
    void closeWakesEveryBlockedBorrowerNotJustOne() throws Exception {
        Connection only = fakeConnection(false);
        DuckDbConnectionPool pool = new DuckDbConnectionPool(List.of(only), INTERRUPTED, REJECTED, CLOSE_FAILURE);
        pool.borrow(); // drain the only connection so every subsequent borrow() blocks

        int waiters = 4;
        CountDownLatch reachedBorrow = new CountDownLatch(waiters);
        CountDownLatch done = new CountDownLatch(waiters);
        AtomicInteger illegalStates = new AtomicInteger();
        AtomicInteger handedOut = new AtomicInteger();
        for (int i = 0; i < waiters; i++) {
            Thread waiter = new Thread(() -> {
                reachedBorrow.countDown();
                try {
                    if (pool.borrow() != null) {
                        handedOut.incrementAndGet();
                    }
                } catch (IllegalStateException e) {
                    illegalStates.incrementAndGet();
                } finally {
                    done.countDown();
                }
            });
            waiter.setDaemon(true); // a hang against a broken pool cannot wedge JVM shutdown
            waiter.start();
        }

        reachedBorrow.await();
        Thread.sleep(100); // give every waiter time to reach the blocking await()

        pool.close();

        // Against a pool that signals only ONE waiter, the other three hang: this latch never
        // reaches zero and the assertion (not a JVM hang) fails.
        assertThat(done.await(5, TimeUnit.SECONDS))
                .as("close() must wake every blocked borrower, not just one")
                .isTrue();
        assertThat(illegalStates.get())
                .as("every woken borrower on a closed pool must fail fast")
                .isEqualTo(waiters);
        assertThat(handedOut.get())
                .as("a closed pool must never hand out a connection to any waiter")
                .isZero();
    }

    @Test
    void releaseAfterCloseIsASilentNoOp() {
        Connection connection = fakeConnection(false);
        DuckDbConnectionPool pool = new DuckDbConnectionPool(List.of(connection), INTERRUPTED, REJECTED, CLOSE_FAILURE);

        Connection borrowed = pool.borrow();
        pool.close();

        // close() has already closed the connection; returning it must not throw.
        pool.release(borrowed);
    }

    @Test
    void overReleaseWhileOpenThrowsRejected() {
        Connection connection = fakeConnection(false);
        DuckDbConnectionPool pool = new DuckDbConnectionPool(List.of(connection), INTERRUPTED, REJECTED, CLOSE_FAILURE);

        Connection borrowed = pool.borrow();
        pool.release(borrowed); // pool now back at capacity

        // Releasing again (a caller bug) overflows capacity and is rejected.
        assertThatThrownBy(() -> pool.release(borrowed))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(REJECTED);
    }

    @Test
    void interruptWhileBlockedOnBorrowRestoresTheInterruptFlagAndWraps() throws Exception {
        Connection only = fakeConnection(false);
        DuckDbConnectionPool pool = new DuckDbConnectionPool(List.of(only), INTERRUPTED, REJECTED, CLOSE_FAILURE);
        pool.borrow(); // drain the only connection so the next borrow() blocks

        AtomicReference<Throwable> thrown = new AtomicReference<>();
        AtomicBoolean stillInterrupted = new AtomicBoolean(false);
        Thread waiter = new Thread(() -> {
            try {
                pool.borrow();
            } catch (Throwable t) {
                thrown.set(t);
                stillInterrupted.set(Thread.currentThread().isInterrupted());
            }
        });
        waiter.start();
        Thread.sleep(100); // give the waiter time to reach the blocking take()
        waiter.interrupt();
        waiter.join(5000);

        assertThat(thrown.get()).isInstanceOf(IllegalStateException.class);
        assertThat(thrown.get()).hasMessage(INTERRUPTED);
        assertThat(thrown.get().getCause()).isInstanceOf(InterruptedException.class);
        assertThat(stillInterrupted.get()).isTrue();
    }

    private static Connection fakeConnection(boolean throwOnClose) {
        return (Connection) Proxy.newProxyInstance(
                DuckDbConnectionPoolTest.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                new FakeConnectionHandler(throwOnClose));
    }

    private static final class FakeConnectionHandler implements InvocationHandler {
        private final boolean throwOnClose;

        FakeConnectionHandler(boolean throwOnClose) {
            this.throwOnClose = throwOnClose;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            switch (method.getName()) {
                case "close":
                    if (throwOnClose) {
                        throw new SQLException("simulated close failure");
                    }
                    return null;
                case "equals":
                    return proxy == args[0];
                case "hashCode":
                    return System.identityHashCode(proxy);
                case "toString":
                    return "FakeConnection@" + System.identityHashCode(proxy);
                default:
                    throw new UnsupportedOperationException(method.getName());
            }
        }
    }
}

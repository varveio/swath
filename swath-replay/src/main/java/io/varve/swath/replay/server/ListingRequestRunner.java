/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import io.varve.swath.output.parquet.sorted.RowGroupOrderException;
import io.varve.swath.replay.metrics.ListingObservation;
import io.varve.swath.replay.metrics.ReplayMetrics;
import io.varve.swath.replay.protocol.S3Error;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.IteratingCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Shared page/render admission and response ownership for every listing protocol. */
final class ListingRequestRunner {
    private static final Logger log = LoggerFactory.getLogger(ListingRequestRunner.class);
    private static final int WRITE_VIEW_BYTES = 256 * 1024;

    private final ReplayMetrics metrics;
    private final Semaphore readPermits;
    private final Semaphore responsePermits;
    private final int maxResponses;
    private final ResponseByteBudget byteBudget;
    private final int maxResponseBytes;
    private final Duration writeTimeout;
    private final ScheduledThreadPoolExecutor writeDeadlines = new ScheduledThreadPoolExecutor(1, task -> {
        Thread thread = new Thread(task, "replay-write-deadline");
        thread.setDaemon(true);
        return thread;
    });
    private final Object drainLock = new Object();
    private int activeOperations;
    private int activeCallbacks;
    private boolean closing;
    private volatile long shutdownDeadlineNanos = Long.MAX_VALUE;
    private final Set<Thread> operationThreads = ConcurrentHashMap.newKeySet();
    private final Map<String, AtomicInteger> activeByProtocol = new ConcurrentHashMap<>();
    private Runnable afterOperations;
    private Runnable afterAll;
    private boolean storeCloseScheduled;
    private boolean storeCloseFinished;
    private boolean registryCloseScheduled;
    private boolean registryCloseFinished;

    ListingRequestRunner(ReplayMetrics metrics, int maxConcurrentReads, int maxResponses,
                         long responseBufferBudget, int maxResponseBytes, Duration writeTimeout) {
        if (maxResponses <= 0 || maxResponseBytes <= 0 || maxResponseBytes > responseBufferBudget) {
            throw new IllegalArgumentException("invalid response admission limits");
        }
        this.metrics = metrics;
        this.readPermits = maxConcurrentReads > 0 ? new Semaphore(maxConcurrentReads, true) : null;
        this.responsePermits = new Semaphore(maxResponses);
        this.maxResponses = maxResponses;
        this.byteBudget = new ResponseByteBudget(responseBufferBudget, metrics::recordChunkAllocation);
        this.maxResponseBytes = maxResponseBytes;
        this.writeTimeout = writeTimeout;
        writeDeadlines.setRemoveOnCancelPolicy(true);
        metrics.registerResponseGauges(this::chargedBytes, this::peakChargedBytes, this::activeResponses);
    }

    void serve(ListingHttpRequest request, Request jettyRequest, Response response, Callback callback,
               ListingProtocolHandler handler) {
        var sample = metrics.startTimer();
        String protocol = handler.protocol().metricName();
        if (!beginOperation()) {
            if (pastShutdownDeadline()) {
                callback.failed(new TimeoutException("replay shutdown deadline expired"));
                return;
            }
            metrics.recordAdmissionRefusal(protocol, "shutdown");
            try {
                writeSmall(jettyRequest, response, handler.error(new ReplayFailure(ReplayFailure.Kind.OVERLOAD,
                        "shutdown", "replay server is stopping"), request), callback, sample, null, protocol);
            } catch (Throwable error) {
                callback.failed(error);
            }
            return;
        }
        AtomicInteger protocolActive = activeByProtocol.computeIfAbsent(protocol, key -> {
            AtomicInteger active = new AtomicInteger();
            metrics.registerProtocolActiveGauge(key, active);
            return active;
        });
        protocolActive.incrementAndGet();
        boolean countHeld = false;
        Callback ownedCallback = null;
        BudgetedOutput output = null;
        try {
            ListingOperation operation = handler.parse(request);
            if (!responsePermits.tryAcquire()) {
                throw new ReplayOutputException(false, "response_count_exhausted");
            }
            countHeld = true;
            int initial = Math.min(maxResponseBytes, Math.max(1, operation.initialOutputBytes()));
            output = new BudgetedOutput(byteBudget, initial, maxResponseBytes);
            long startedNanos = System.nanoTime();
            operation.beforePage();
            var pageSample = metrics.startTimer();
            PreparedPage page;
            if (readPermits == null) {
                page = operation.page();
            } else {
                readPermits.acquire();
                try {
                    page = operation.page();
                } finally {
                    readPermits.release();
                }
            }
            metrics.recordRequestStage(pageSample, protocol, "page");
            output.setFirstChunkHint(page.initialOutputBytesHint());
            var renderSample = metrics.startTimer();
            RenderedResponse rendered = page.render(output);
            if (!output.owns(rendered.body())) {
                throw new IllegalStateException("provider response did not use owned output bytes");
            }
            metrics.recordRequestStage(renderSample, protocol, "render");
            ListingObservation observation = page.observation();
            Duration injected = page.injectedLatency();
            java.util.function.LongConsumer overrunRecorder = page.injectionOverrunRecorder();
            // Drop decoded rows before injected delay and network completion.
            page = null;
            if (injected != null && injected.isPositive()) {
                long remaining = injected.toNanos() - (System.nanoTime() - startedNanos);
                if (remaining > 0) {
                    var delaySample = metrics.startTimer();
                    Duration sleep = Duration.ofNanos(remaining);
                    Thread.sleep(sleep.toMillis(), sleep.minusMillis(sleep.toMillis()).getNano());
                    metrics.recordRequestStage(delaySample, protocol, "delay");
                } else {
                    overrunRecorder.accept(-remaining);
                }
            }
            if (pastShutdownDeadline()) {
                throw new TimeoutException("replay shutdown deadline expired");
            }
            BudgetedOutput ownedOutput = output;
            metrics.recordHttpRequest(sample, rendered.status());
            metrics.recordProtocolResponse(protocol, rendered.status());
            if (rendered.status() >= 200 && rendered.status() < 300) {
                metrics.recordListingObservation(protocol, observation, rendered.body().length());
            }
            var writeSample = metrics.startTimer();
            Callback completion = new Callback() {
                private final AtomicBoolean finished = new AtomicBoolean();

                private void finish(Throwable error) {
                    if (!finished.compareAndSet(false, true)) {
                        return;
                    }
                    ownedOutput.close();
                    responsePermits.release();
                    protocolActive.decrementAndGet();
                    Throwable completedError = error;
                    try {
                        metrics.recordRequestStage(writeSample, protocol, "write");
                    } catch (Throwable metricsError) {
                        if (completedError == null) completedError = metricsError;
                        else completedError.addSuppressed(metricsError);
                    }
                    try {
                        if (completedError == null) {
                            callback.succeeded();
                        } else {
                            callback.failed(completedError);
                        }
                    } finally {
                        synchronized (drainLock) {
                            activeCallbacks--;
                        }
                        fireDrainActions();
                    }
                }

                @Override public void succeeded() { finish(null); }
                @Override public void failed(Throwable x) { finish(x); }
            };
            synchronized (drainLock) {
                activeCallbacks++;
            }
            output = null;
            countHeld = false;
            ownedCallback = completion;
            writeOwned(jettyRequest, response, rendered, ownedCallback, protocol);
        } catch (Throwable error) {
            if (ownedCallback != null) {
                ownedCallback.failed(error);
                return;
            }
            if (output != null) {
                output.close();
            }
            if (countHeld) {
                responsePermits.release();
            }
            if (pastShutdownDeadline()) {
                protocolActive.decrementAndGet();
                callback.failed(error);
                return;
            }
            if (error instanceof ReplayOutputException outputError) {
                metrics.recordAdmissionRefusal(protocol, outputError.reason());
            }
            try {
                RenderedResponse failure = failure(handler, request, error);
                writeSmall(jettyRequest, response, failure, callback, sample, protocolActive, protocol);
            } catch (Throwable errorRenderingFailure) {
                protocolActive.decrementAndGet();
                errorRenderingFailure.addSuppressed(error);
                callback.failed(errorRenderingFailure);
            }
        } finally {
            endOperation();
        }
    }

    private static RenderedResponse failure(ListingProtocolHandler handler, ListingHttpRequest request,
                                            Throwable error) {
        if (handler instanceof S3ListingProtocolHandler s3 && error instanceof S3Error s3Error) {
            return s3.s3Error(s3Error, request.path());
        }
        if (error instanceof ReplayRequestException requestError) {
            return handler.error(requestError.failure(), request);
        }
        if (error instanceof RowGroupOrderException disorder) {
            log.error("replay refused a request over a disordered fixture: {}", disorder.getMessage(), disorder);
            return handler.error(new ReplayFailure(ReplayFailure.Kind.FIXTURE_DISORDERED,
                    "fixture_disordered", disorder.redactedMessage()), request);
        }
        if (error instanceof ReplayOutputException outputError) {
            return handler.error(new ReplayFailure(outputError.responseTooLarge()
                    ? ReplayFailure.Kind.RESPONSE_TOO_LARGE : ReplayFailure.Kind.OVERLOAD,
                    outputError.reason(),
                    outputError.getMessage()), request);
        }
        if (error instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            return handler.error(new ReplayFailure(ReplayFailure.Kind.OVERLOAD,
                    "shutdown", "replay request interrupted"), request);
        }
        log.error("replay failed to serve request", error);
        return handler.error(new ReplayFailure(ReplayFailure.Kind.INTERNAL,
                "internal", "internal replay error"), request);
    }

    private void writeSmall(Request request, Response response, RenderedResponse rendered, Callback callback,
                            io.micrometer.core.instrument.Timer.Sample sample, AtomicInteger protocolActive,
                            String protocol) {
        metrics.recordHttpRequest(sample, rendered.status());
        metrics.recordProtocolResponse(protocol, rendered.status());
        synchronized (drainLock) {
            activeCallbacks++;
        }
        WriteDeadline deadline;
        try {
            org.eclipse.jetty.io.Connection connection = request.getConnectionMetaData().getConnection();
            deadline = new WriteDeadline(writeDeadlines, connection, writeTimeout,
                    () -> metrics.recordWriteDeadlineExpiration(protocol));
        } catch (RuntimeException schedulingFailure) {
            if (protocolActive != null) {
                protocolActive.decrementAndGet();
            }
            synchronized (drainLock) {
                activeCallbacks--;
            }
            fireDrainActions();
            callback.failed(schedulingFailure);
            return;
        }
        Callback wrapped = new Callback() {
            private final AtomicBoolean finished = new AtomicBoolean();

            private void finish(Throwable error) {
                if (!finished.compareAndSet(false, true)) {
                    return;
                }
                Throwable completionError = deadline.finish(error);
                if (protocolActive != null) {
                    protocolActive.decrementAndGet();
                }
                try {
                    if (completionError == null) {
                        callback.succeeded();
                    } else {
                        callback.failed(completionError);
                    }
                } finally {
                    synchronized (drainLock) {
                        activeCallbacks--;
                    }
                    fireDrainActions();
                }
            }

            @Override public void succeeded() { finish(null); }
            @Override public void failed(Throwable x) { finish(x); }
        };
        try {
            writeHeaders(response, rendered);
            if (rendered.body().views().size() != 1) {
                throw new IllegalStateException("small replay error must have one byte view");
            }
            response.write(true, rendered.body().views().getFirst().duplicate(), wrapped);
        } catch (Throwable error) {
            wrapped.failed(error);
        }
    }

    private void writeOwned(Request request, Response response, RenderedResponse rendered, Callback callback,
                            String protocol) {
        writeHeaders(response, rendered);
        List<ByteBuffer> views = new java.util.ArrayList<>();
        for (ByteBuffer chunk : rendered.body().views()) {
            ByteBuffer remaining = chunk.duplicate();
            while (remaining.hasRemaining()) {
                int size = Math.min(remaining.remaining(), WRITE_VIEW_BYTES);
                ByteBuffer view = remaining.slice();
                view.limit(size);
                remaining.position(remaining.position() + size);
                views.add(view);
            }
        }
        if (views.isEmpty()) views.add(ByteBuffer.allocate(0));
        org.eclipse.jetty.io.Connection connection = request.getConnectionMetaData().getConnection();
        WriteDeadline deadline = new WriteDeadline(writeDeadlines, connection, writeTimeout,
                () -> metrics.recordWriteDeadlineExpiration(protocol));
        IteratingCallback writer = new IteratingCallback() {
            private int nextView;

            @Override
            protected Action process() {
                if (nextView == views.size()) {
                    return Action.SUCCEEDED;
                }
                ByteBuffer view = views.get(nextView++);
                response.write(nextView == views.size(), view, this);
                return Action.SCHEDULED;
            }

            @Override protected void onCompleteSuccess() {
                Throwable error = deadline.finish(null);
                if (error == null) callback.succeeded();
                else callback.failed(error);
            }
            @Override protected void onCompleteFailure(Throwable failure) {
                callback.failed(deadline.finish(failure));
            }
        };
        try {
            writer.iterate();
        } catch (Throwable failure) {
            callback.failed(deadline.finish(failure));
        }
    }

    private static void writeHeaders(Response response, RenderedResponse rendered) {
        response.setStatus(rendered.status());
        response.getHeaders().put(HttpHeader.CONTENT_TYPE, rendered.contentType());
        response.getHeaders().put(HttpHeader.CONTENT_LENGTH,
                Integer.toString(rendered.body().length()));
        rendered.headers().forEach((name, value) -> response.getHeaders().put(name, value));
    }

    private boolean beginOperation() {
        synchronized (drainLock) {
            if (closing) {
                return false;
            }
            activeOperations++;
            operationThreads.add(Thread.currentThread());
            return true;
        }
    }

    private void endOperation() {
        synchronized (drainLock) {
            activeOperations--;
            operationThreads.remove(Thread.currentThread());
        }
        fireDrainActions();
    }

    void beginShutdown() {
        beginShutdown(System.nanoTime());
    }

    void beginShutdown(long deadlineNanos) {
        synchronized (drainLock) {
            closing = true;
            shutdownDeadlineNanos = deadlineNanos;
            drainLock.notifyAll();
        }
        operationThreads.forEach(Thread::interrupt);
    }

    private boolean pastShutdownDeadline() {
        return System.nanoTime() >= shutdownDeadlineNanos;
    }

    boolean awaitOperations(long deadlineNanos) {
        return awaitDrain(deadlineNanos, false);
    }

    boolean awaitAll(long deadlineNanos) {
        return awaitDrain(deadlineNanos, true);
    }

    private boolean awaitDrain(long deadlineNanos, boolean callbacks) {
        synchronized (drainLock) {
            while (activeOperations != 0 || (callbacks && activeCallbacks != 0)
                    || (storeCloseScheduled && !storeCloseFinished)
                    || (callbacks && registryCloseScheduled && !registryCloseFinished)) {
                long remaining = deadlineNanos - System.nanoTime();
                if (remaining <= 0) {
                    return false;
                }
                try {
                    drainLock.wait(Math.max(1, remaining / 1_000_000L));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return true;
        }
    }

    void afterOperations(Runnable action) {
        synchronized (drainLock) {
            if (storeCloseScheduled) {
                throw new IllegalStateException("store close already scheduled");
            }
            storeCloseScheduled = true;
            afterOperations = action;
        }
        fireDrainActions();
    }

    void afterAll(Runnable action) {
        synchronized (drainLock) {
            if (registryCloseScheduled) {
                throw new IllegalStateException("registry close already scheduled");
            }
            registryCloseScheduled = true;
            afterAll = action;
        }
        fireDrainActions();
    }

    private void fireDrainActions() {
        Runnable storeAction = null;
        synchronized (drainLock) {
            if (activeOperations == 0 && afterOperations != null) {
                storeAction = afterOperations;
                afterOperations = null;
            }
            drainLock.notifyAll();
        }
        if (storeAction != null) {
            try {
                storeAction.run();
            } finally {
                synchronized (drainLock) {
                    storeCloseFinished = true;
                    drainLock.notifyAll();
                }
            }
        }
        Runnable registryAction = null;
        synchronized (drainLock) {
            if (activeOperations == 0 && activeCallbacks == 0
                    && (!storeCloseScheduled || storeCloseFinished) && afterAll != null) {
                registryAction = afterAll;
                afterAll = null;
            }
        }
        if (registryAction != null) {
            try {
                registryAction.run();
            } finally {
                synchronized (drainLock) {
                    registryCloseFinished = true;
                    drainLock.notifyAll();
                }
            }
        }
    }

    long chargedBytes() { return byteBudget.charged(); }
    long peakChargedBytes() { return byteBudget.peak(); }
    int activeResponses() { return maxResponses - responsePermits.availablePermits(); }
    int maxResponses() { return maxResponses; }
    long responseBufferBudget() { return byteBudget.limit(); }
    int maxResponseBytes() { return maxResponseBytes; }
    long writeTimeoutMs() { return writeTimeout.toMillis(); }
    int writeDeadlineQueueSize() { return writeDeadlines.getQueue().size(); }
    void stopDeadlines() { writeDeadlines.shutdownNow(); }

    /** Timer and callback arbitrate completion before a connection can be reused. */
    static final class WriteDeadline {
        private static final int ACTIVE = 0;
        private static final int COMPLETED = 1;
        private static final int EXPIRED = 2;
        private final AtomicInteger state = new AtomicInteger(ACTIVE);
        private final ScheduledFuture<?> timer;
        private final org.eclipse.jetty.io.Connection connection;
        private final Runnable onExpiry;
        private final CountDownLatch expirationFinished = new CountDownLatch(1);

        WriteDeadline(ScheduledThreadPoolExecutor scheduler, org.eclipse.jetty.io.Connection connection,
                      Duration timeout, Runnable onExpiry) {
            this.connection = connection;
            this.onExpiry = onExpiry;
            timer = scheduler.schedule(this::expire, timeout.toNanos(), TimeUnit.NANOSECONDS);
        }

        void expire() {
            if (state.compareAndSet(ACTIVE, EXPIRED)) {
                try {
                    onExpiry.run();
                } catch (Throwable observationFailure) {
                    log.error("failed to record replay write deadline expiration", observationFailure);
                } finally {
                    // close() may synchronously invoke the write callback, so the observer must
                    // finish before close() starts; otherwise that callback waits on itself.
                    expirationFinished.countDown();
                    connection.close();
                }
            }
        }

        Throwable finish(Throwable writeFailure) {
            if (state.compareAndSet(ACTIVE, COMPLETED)) {
                timer.cancel(false);
                return writeFailure;
            }
            if (state.get() == EXPIRED) {
                boolean interrupted = false;
                for (;;) {
                    try {
                        expirationFinished.await();
                        break;
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
                if (interrupted) Thread.currentThread().interrupt();
                return new TimeoutException("replay response write deadline expired");
            }
            return writeFailure;
        }
    }
}

/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.replay.metrics.ListingObservation;
import io.varve.swath.replay.metrics.ObservationShape;
import io.varve.swath.replay.metrics.ReplayMetrics;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.io.Connection;
import org.eclipse.jetty.server.ConnectionMetaData;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.jupiter.api.Test;

class ListingRequestRunnerTest {
    @Test
    void oversizedRenderUsesSmallUnbudgetedErrorThenRecovers() {
        ReplayMetrics metrics = new ReplayMetrics();
        ListingRequestRunner runner = new ListingRequestRunner(metrics, 1, 2,
                64, 64, Duration.ofSeconds(5));
        AtomicBoolean oversized = new AtomicBoolean(true);
        AtomicInteger status = new AtomicInteger();
        AtomicInteger writes = new AtomicInteger();
        Connection connection = proxy(Connection.class, (method, args) -> null);
        ConnectionMetaData metadata = proxy(ConnectionMetaData.class,
                (method, args) -> method.equals("getConnection") ? connection : null);
        Request request = proxy(Request.class,
                (method, args) -> method.equals("getConnectionMetaData") ? metadata : null);
        HttpFields.Mutable headers = HttpFields.build();
        Response response = proxy(Response.class, (method, args) -> {
            if (method.equals("getHeaders")) return headers;
            if (method.equals("setStatus")) status.set((int) args[0]);
            if (method.equals("write")) {
                writes.incrementAndGet();
                ((Callback) args[2]).succeeded();
            }
            return null;
        });
        ListingProtocolHandler handler = new ListingProtocolHandler() {
            @Override public Protocol protocol() { return Protocol.S3; }
            @Override public ListingOperation parse(ListingHttpRequest ignored) {
                return new ListingOperation() {
                    @Override public int initialOutputBytes() { return 32; }
                    @Override public PreparedPage page() {
                        return output -> {
                            int bytes = oversized.getAndSet(false) ? 65 : 1;
                            output.appendPercentEncoded(new byte[] {'x'});
                            output.write(new byte[bytes], 0, bytes);
                            return new RenderedResponse(200, "text/plain", Map.of(), output.body());
                        };
                    }
                };
            }
            @Override public RenderedResponse error(ReplayFailure failure, ListingHttpRequest ignored) {
                return new RenderedResponse(500, "text/plain",
                        Map.of(ReplayFailure.REASON_HEADER, failure.reason()),
                        ByteBuffer.wrap(new byte[] {'e'}));
            }
        };
        Callback outer = new Callback() {
            @Override public void succeeded() { }
            @Override public void failed(Throwable error) { throw new AssertionError(error); }
        };
        ListingHttpRequest http = new ListingHttpRequest("GET", "/bucket", null, Map.of());
        runner.serve(http, request, response, outer, handler);
        assertThat(status).hasValue(500);
        assertThat(headers.get(ReplayFailure.REASON_HEADER)).isEqualTo("response_too_large");
        assertThat(runner.chargedBytes()).isZero();
        assertThat(runner.activeResponses()).isZero();
        assertThat(metrics.registry().get("swath.replay.response.admission.refused")
                .tags("protocol", "s3", "reason", "response_too_large")
                .counter().count()).isEqualTo(1);
        assertThat(metrics.registry().find("swath.replay.response.percent.encoding.path")
                .tag("protocol", "s3").counter()).isNull();
        runner.serve(http, request, response, outer, handler);
        assertThat(status).hasValue(200);
        assertThat(writes).hasValue(2);
        assertThat(runner.chargedBytes()).isZero();
        assertThat(runner.activeResponses()).isZero();
        assertThat(metrics.registry().get("swath.replay.response.percent.encoding.path")
                .tags("protocol", "s3", "reason", "exact_length_fallback")
                .counter().count()).isEqualTo(1);
        runner.stopDeadlines();
        metrics.registry().close();
    }

    @Test
    void chunkViewsWriteIterativelyAndReleaseOnInlineSuccessOrDisconnect() {
        byte[] pattern = new byte[600_000];
        new java.util.Random(220).nextBytes(pattern);
        for (boolean disconnect : new boolean[] {false, true}) {
            ReplayMetrics metrics = new ReplayMetrics();
            ListingRequestRunner runner = new ListingRequestRunner(metrics, 1, 2,
                    2L * 1024 * 1024, 1024 * 1024, Duration.ofSeconds(5));
            AtomicInteger writes = new AtomicInteger();
            AtomicInteger lastWrites = new AtomicInteger();
            AtomicInteger successes = new AtomicInteger();
            AtomicInteger failures = new AtomicInteger();
            java.io.ByteArrayOutputStream emitted = new java.io.ByteArrayOutputStream();
            Connection connection = proxy(Connection.class, (method, args) -> null);
            ConnectionMetaData metadata = proxy(ConnectionMetaData.class,
                    (method, args) -> method.equals("getConnection") ? connection : null);
            Request request = proxy(Request.class,
                    (method, args) -> method.equals("getConnectionMetaData") ? metadata : null);
            HttpFields.Mutable headers = HttpFields.build();
            Response response = proxy(Response.class, (method, args) -> {
                if (method.equals("getHeaders")) return headers;
                if (method.equals("write")) {
                    int call = writes.incrementAndGet();
                    ByteBuffer view = ((ByteBuffer) args[1]).duplicate();
                    assertThat(view.remaining()).isLessThanOrEqualTo(256 * 1024);
                    byte[] transferred = new byte[view.remaining()];
                    view.get(transferred);
                    emitted.writeBytes(transferred);
                    if ((boolean) args[0]) lastWrites.incrementAndGet();
                    if (disconnect && call == 2) {
                        ((Callback) args[2]).failed(new java.io.IOException("disconnect"));
                    } else {
                        ((Callback) args[2]).succeeded();
                    }
                }
                return null;
            });
            ListingProtocolHandler handler = new ListingProtocolHandler() {
                @Override public Protocol protocol() { return Protocol.S3; }
                @Override public ListingOperation parse(ListingHttpRequest ignored) {
                    return new ListingOperation() {
                        @Override public int initialOutputBytes() { return 320_512; }
                        @Override public PreparedPage page() {
                            return new PreparedPage() {
                                @Override public ListingObservation observation() {
                                    return new ListingObservation(ObservationShape.PAGE, 7, 2);
                                }
                                @Override public RenderedResponse render(BudgetedOutput output) {
                                    output.write(pattern, 0, pattern.length);
                                    return new RenderedResponse(200, "text/plain", Map.of(), output.body());
                                }
                            };
                        }
                    };
                }
                @Override public RenderedResponse error(ReplayFailure failure, ListingHttpRequest ignored) {
                    throw new AssertionError(failure);
                }
            };
            runner.serve(new ListingHttpRequest("GET", "/bucket", null, Map.of()), request, response,
                    new Callback() {
                        @Override public void succeeded() {
                            successes.incrementAndGet();
                            assertThat(runner.chargedBytes()).isZero();
                            assertThat(runner.activeResponses()).isZero();
                        }
                        @Override public void failed(Throwable error) {
                            failures.incrementAndGet();
                            assertThat(runner.chargedBytes()).isZero();
                            assertThat(runner.activeResponses()).isZero();
                        }
                    }, handler);
            assertThat(writes).hasValue(disconnect ? 2 : 3);
            assertThat(lastWrites).hasValue(disconnect ? 0 : 1);
            assertThat(successes).hasValue(disconnect ? 0 : 1);
            assertThat(failures).hasValue(disconnect ? 1 : 0);
            assertThat(headers.get("Content-Length")).isEqualTo("600000");
            assertThat(emitted.toByteArray()).isEqualTo(disconnect
                    ? java.util.Arrays.copyOf(pattern, 2 * 256 * 1024) : pattern);
            assertThat(runner.writeDeadlineQueueSize()).isZero();
            assertThat(metrics.registry().get("swath.replay.response.chunk.allocation")
                    .tag("reason", "initial").counter().count()).isEqualTo(1);
            assertThat(metrics.registry().get("swath.replay.response.chunk.allocation")
                    .tag("reason", "continuation").counter().count()).isEqualTo(2);
            assertThat(metrics.registry().get("swath.replay.protocol.objects")
                    .tags("protocol", "s3", "shape", "page").counter().count()).isEqualTo(7);
            assertThat(metrics.registry().get("swath.replay.protocol.prefixes")
                    .tags("protocol", "s3", "shape", "page").counter().count()).isEqualTo(2);
            assertThat(metrics.registry().get("swath.replay.protocol.encoded.bytes")
                    .tags("protocol", "s3", "shape", "page").counter().count()).isEqualTo(600_000);
            runner.stopDeadlines();
            metrics.registry().close();
        }
    }

    @Test
    void synchronousConnectionCloseCallbackCannotDeadlockDeadlineObserver() throws Exception {
        ReplayMetrics metrics = new ReplayMetrics();
        AtomicReference<ListingRequestRunner.WriteDeadline> deadlineRef = new AtomicReference<>();
        AtomicReference<Throwable> callbackResult = new AtomicReference<>();
        Connection connection = proxy(Connection.class, (method, args) -> {
            if (method.equals("close")) {
                callbackResult.set(deadlineRef.get().finish(null));
            }
            return null;
        });
        ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
        try {
            var deadline = new ListingRequestRunner.WriteDeadline(scheduler, connection,
                    Duration.ofSeconds(10), () -> metrics.recordWriteDeadlineExpiration("s3"));
            deadlineRef.set(deadline);
            Thread expiration = Thread.ofVirtual().start(deadline::expire);
            expiration.join(Duration.ofSeconds(1));
            assertThat(expiration.isAlive()).isFalse();
            assertThat(callbackResult.get()).isInstanceOf(TimeoutException.class);
            assertThat(metrics.registry().get("swath.replay.response.write.deadline")
                    .tags("protocol", "s3", "reason", "total_deadline")
                    .counter().count()).isEqualTo(1);
        } finally {
            scheduler.shutdownNow();
            metrics.registry().close();
        }
    }

    @Test
    void deadlineObservationFinishesBeforeTimedOutCallbackCanReleaseRegistry() throws Exception {
        ReplayMetrics metrics = new ReplayMetrics();
        CountDownLatch observerEntered = new CountDownLatch(1);
        CountDownLatch releaseObserver = new CountDownLatch(1);
        AtomicInteger closes = new AtomicInteger();
        Connection connection = proxy(Connection.class, (method, args) -> {
            if (method.equals("close")) closes.incrementAndGet();
            return null;
        });
        ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
        var deadline = new ListingRequestRunner.WriteDeadline(scheduler, connection,
                Duration.ofSeconds(10), () -> {
                    observerEntered.countDown();
                    try {
                        releaseObserver.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                    metrics.recordWriteDeadlineExpiration("s3");
                });
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            try {
                var expiration = workers.submit(deadline::expire);
                assertThat(observerEntered.await(5, TimeUnit.SECONDS)).isTrue();
                var callback = workers.submit(() -> deadline.finish(null));
                assertThatThrownBy(() -> callback.get(100, TimeUnit.MILLISECONDS))
                        .isInstanceOf(TimeoutException.class);
                releaseObserver.countDown();
                expiration.get(5, TimeUnit.SECONDS);
                assertThat(callback.get(5, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
                assertThat(metrics.registry().get("swath.replay.response.write.deadline")
                        .tags("protocol", "s3", "reason", "total_deadline")
                        .counter().count()).isEqualTo(1);
                assertThat(closes).hasValue(1);
            } finally {
                releaseObserver.countDown();
            }
        } finally {
            releaseObserver.countDown();
            scheduler.shutdownNow();
            metrics.registry().close();
        }
    }

    @Test
    void countAdmissionRefusalKeepsSmallErrorOutsideByteBudgetAndRecovers() {
        ReplayMetrics metrics = new ReplayMetrics();
        ListingRequestRunner runner = new ListingRequestRunner(metrics, 1, 1,
                128, 64, Duration.ofSeconds(5));
        List<Callback> writes = new ArrayList<>();
        Connection connection = proxy(Connection.class, (method, args) -> null);
        ConnectionMetaData metadata = proxy(ConnectionMetaData.class,
                (method, args) -> method.equals("getConnection") ? connection : null);
        Request request = proxy(Request.class,
                (method, args) -> method.equals("getConnectionMetaData") ? metadata : null);
        Response response = proxy(Response.class, (method, args) -> {
            if (method.equals("getHeaders")) return HttpFields.build();
            if (method.equals("write")) writes.add((Callback) args[2]);
            return null;
        });
        ListingProtocolHandler handler = new ListingProtocolHandler() {
            @Override public Protocol protocol() { return Protocol.S3; }
            @Override public ListingOperation parse(ListingHttpRequest ignored) {
                return new ListingOperation() {
                    @Override public int initialOutputBytes() { return 64; }
                    @Override public PreparedPage page() {
                        return output -> {
                            output.write('x');
                            return new RenderedResponse(200, "text/plain", Map.of(), output.body());
                        };
                    }
                };
            }
            @Override public RenderedResponse error(ReplayFailure failure, ListingHttpRequest ignored) {
                return new RenderedResponse(503, "text/plain", Map.of(ReplayFailure.REASON_HEADER,
                        failure.reason()), java.nio.ByteBuffer.wrap(new byte[] {'e'}));
            }
        };
        Callback outer = new Callback() {
            @Override public void succeeded() { }
            @Override public void failed(Throwable failure) { throw new AssertionError(failure); }
        };
        ListingHttpRequest http = new ListingHttpRequest("GET", "/bucket", null, Map.of());
        runner.serve(http, request, response, outer, handler);
        runner.serve(http, request, response, outer, handler);
        assertThat(writes).hasSize(2);
        assertThat(runner.chargedBytes()).isEqualTo(64);
        assertThat(runner.activeResponses()).isEqualTo(1);
        assertThat(metrics.registry().get("swath.replay.response.admission.refused")
                .tags("protocol", "s3", "reason", "response_count_exhausted")
                .counter().count()).isEqualTo(1);
        writes.get(1).succeeded();
        writes.get(0).succeeded();
        assertThat(runner.chargedBytes()).isZero();
        assertThat(runner.activeResponses()).isZero();
        runner.stopDeadlines();
        metrics.registry().close();
    }

    @Test
    void successfulWriteWinsDeadlineRaceAndCannotCloseAReusedConnection() {
        AtomicInteger closes = new AtomicInteger();
        Connection connection = proxy(Connection.class, (method, args) -> {
            if (method.equals("close")) closes.incrementAndGet();
            return null;
        });
        ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
        scheduler.setRemoveOnCancelPolicy(true);
        ReplayMetrics metrics = new ReplayMetrics();
        Runnable recordDeadline = () -> metrics.recordWriteDeadlineExpiration("s3");
        try {
            var successWins = new ListingRequestRunner.WriteDeadline(scheduler, connection,
                    Duration.ofSeconds(10), recordDeadline);
            assertThat(successWins.finish(null)).isNull();
            successWins.expire();
            assertThat(closes).hasValue(0);
            assertThat(scheduler.getQueue()).isEmpty();

            var timeoutWins = new ListingRequestRunner.WriteDeadline(scheduler, connection,
                    Duration.ofSeconds(10), recordDeadline);
            timeoutWins.expire();
            assertThat(timeoutWins.finish(null)).isInstanceOf(TimeoutException.class);
            assertThat(closes).hasValue(1);
            assertThat(metrics.registry().get("swath.replay.response.write.deadline")
                    .tags("protocol", "s3", "reason", "total_deadline")
                    .counter().count()).isEqualTo(1);
        } finally {
            scheduler.shutdownNow();
            metrics.registry().close();
        }
    }

    @Test
    void drainWaitsForOutOfLockStoreAndRegistryCleanupActions() throws Exception {
        ReplayMetrics metrics = new ReplayMetrics();
        ListingRequestRunner runner = new ListingRequestRunner(metrics, 1, 2,
                1024, 64, Duration.ofSeconds(5));
        CountDownLatch storeCloseEntered = new CountDownLatch(1);
        CountDownLatch releaseStoreClose = new CountDownLatch(1);
        AtomicBoolean registryClosed = new AtomicBoolean();
        runner.beginShutdown(System.nanoTime() + Duration.ofSeconds(5).toNanos());
        Thread closer = Thread.ofVirtual().start(() -> runner.afterOperations(() -> {
            storeCloseEntered.countDown();
            try {
                releaseStoreClose.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }));
        try {
            assertThat(storeCloseEntered.await(5, TimeUnit.SECONDS)).isTrue();
            runner.afterAll(() -> registryClosed.set(true));
            assertThat(runner.awaitAll(System.nanoTime() + Duration.ofMillis(20).toNanos())).isFalse();
            assertThat(registryClosed).isFalse();
        } finally {
            releaseStoreClose.countDown();
            closer.join();
        }
        assertThat(runner.awaitAll(System.nanoTime() + Duration.ofSeconds(1).toNanos())).isTrue();
        assertThat(registryClosed).isTrue();
        runner.stopDeadlines();
        metrics.registry().close();
    }

    @Test
    void storeClosesAfterPagingButRegistryAndBytesStayOwnedThroughWriteCallback() {
        ReplayMetrics metrics = new ReplayMetrics();
        ListingRequestRunner runner = new ListingRequestRunner(metrics, 1, 2,
                1024, 64, Duration.ofSeconds(5));
        AtomicReference<Callback> write = new AtomicReference<>();
        AtomicBoolean storeClosed = new AtomicBoolean();
        AtomicBoolean registryClosed = new AtomicBoolean();
        AtomicBoolean registryVisibleInOuterCallback = new AtomicBoolean();
        Connection connection = proxy(Connection.class, (method, args) -> null);
        ConnectionMetaData metadata = proxy(ConnectionMetaData.class,
                (method, args) -> method.equals("getConnection") ? connection : null);
        Request request = proxy(Request.class,
                (method, args) -> method.equals("getConnectionMetaData") ? metadata : null);
        HttpFields.Mutable headers = HttpFields.build();
        Response response = proxy(Response.class, (method, args) -> {
            if (method.equals("getHeaders")) return headers;
            if (method.equals("write")) write.set((Callback) args[2]);
            return null;
        });
        ListingProtocolHandler handler = new ListingProtocolHandler() {
            @Override public Protocol protocol() { return Protocol.S3; }
            @Override public ListingOperation parse(ListingHttpRequest ignored) {
                return new ListingOperation() {
                    @Override public int initialOutputBytes() { return 64; }
                    @Override public PreparedPage page() {
                        return output -> {
                            output.write('x');
                            return new RenderedResponse(200, "text/plain", Map.of(), output.body());
                        };
                    }
                };
            }
            @Override public RenderedResponse error(ReplayFailure failure, ListingHttpRequest ignored) {
                throw new AssertionError(failure);
            }
        };
        runner.serve(new ListingHttpRequest("GET", "/bucket", null, Map.of()), request, response,
                new Callback() {
                    @Override public void succeeded() {
                        registryVisibleInOuterCallback.set(!registryClosed.get()
                                && !metrics.registry().getMeters().isEmpty());
                    }
                    @Override public void failed(Throwable failure) { throw new AssertionError(failure); }
                }, handler);
        runner.beginShutdown();
        runner.afterOperations(() -> storeClosed.set(true));
        runner.afterAll(() -> {
            registryClosed.set(true);
        });
        assertThat(storeClosed).isTrue();
        assertThat(registryClosed).isFalse();
        assertThat(runner.chargedBytes()).isEqualTo(64);
        assertThat(runner.activeResponses()).isEqualTo(1);
        assertThat(metrics.registry().get("swath.replay.response.bytes.live").gauge().value())
                .isEqualTo(64);
        assertThat(metrics.registry().get("swath.replay.response.active").gauge().value())
                .isEqualTo(1);
        assertThat(metrics.registry().get("swath.replay.protocol.requests.active")
                .tag("protocol", "s3").gauge().value()).isEqualTo(1);
        assertThat(metrics.registry().get("swath.replay.request.stage.latency")
                .tags("protocol", "s3", "stage", "page").timer().count()).isEqualTo(1);
        assertThat(metrics.registry().get("swath.replay.request.stage.latency")
                .tags("protocol", "s3", "stage", "render").timer().count()).isEqualTo(1);
        write.get().succeeded();
        assertThat(registryVisibleInOuterCallback).isTrue();
        assertThat(registryClosed).isTrue();
        assertThat(runner.chargedBytes()).isZero();
        assertThat(runner.activeResponses()).isZero();
        assertThat(runner.writeDeadlineQueueSize()).isZero();
        assertThat(metrics.registry().get("swath.replay.request.stage.latency")
                .tags("protocol", "s3", "stage", "write").timer().count()).isEqualTo(1);
        assertThat(metrics.registry().get("swath.replay.response.bytes.peak").gauge().value())
                .isEqualTo(64);
        runner.stopDeadlines();
        metrics.registry().close();
    }

    private interface Invocation {
        Object invoke(String method, Object[] args);
    }

    private static <T> T proxy(Class<T> type, Invocation invocation) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type},
                (instance, method, args) -> invocation.invoke(method.getName(), args)));
    }

    @Test
    void renderingReleasesReadPermitSoAnotherPageCanUseTheStore() throws Exception {
        ReplayMetrics metrics = new ReplayMetrics();
        ListingRequestRunner runner = new ListingRequestRunner(metrics, 1, 4,
                1024, 256, Duration.ofSeconds(5));
        CountDownLatch rendering = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger pages = new AtomicInteger();
        ListingProtocolHandler handler = new ListingProtocolHandler() {
            @Override public Protocol protocol() { return Protocol.S3; }

            @Override
            public ListingOperation parse(ListingHttpRequest request) {
                boolean first = request.query() != null && request.query().contains("first");
                return new ListingOperation() {
                    @Override public int initialOutputBytes() { return 64; }

                    @Override
                    public PreparedPage page() {
                        pages.incrementAndGet();
                        return output -> {
                            if (first) {
                                rendering.countDown();
                                try {
                                    release.await();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    throw new IllegalStateException(e);
                                }
                            }
                            output.write('x');
                            return new RenderedResponse(200, "text/plain", Map.of(), output.body());
                        };
                    }
                };
            }

            @Override
            public RenderedResponse error(ReplayFailure failure, ListingHttpRequest request) {
                return new RenderedResponse(500, "text/plain", Map.of(),
                        java.nio.ByteBuffer.wrap(new byte[0]));
            }
        };
        Server server = new Server(new QueuedThreadPool(8));
        ServerConnector connector = new ServerConnector(server);
        connector.setHost("127.0.0.1");
        connector.setPort(0);
        server.addConnector(connector);
        server.setHandler(new ReplayHandler(Map.of(Protocol.S3, handler), "replay", runner));
        server.start();
        try (HttpClient client = HttpClient.newHttpClient()) {
            String base = "http://127.0.0.1:" + connector.getLocalPort() + "/bucket?";
            var first = client.sendAsync(HttpRequest.newBuilder(URI.create(base + "first")).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(rendering.await(5, TimeUnit.SECONDS)).isTrue();
            var second = client.send(HttpRequest.newBuilder(URI.create(base + "second")).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(second.statusCode()).isEqualTo(200);
            assertThat(pages.get()).isEqualTo(2);
            release.countDown();
            assertThat(first.get(5, TimeUnit.SECONDS).statusCode()).isEqualTo(200);
            assertThat(runner.chargedBytes()).isZero();
            assertThat(runner.activeResponses()).isZero();
            assertThat(runner.writeDeadlineQueueSize()).isZero();
        } finally {
            release.countDown();
            runner.beginShutdown();
            server.stop();
            runner.stopDeadlines();
            metrics.registry().close();
        }
    }
}

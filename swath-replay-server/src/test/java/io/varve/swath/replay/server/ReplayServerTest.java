/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.protocol.ListingFixture;
import io.varve.swath.replay.protocol.S3ListRequest;
import io.varve.swath.replay.protocol.S3ListResult;
import io.varve.swath.replay.protocol.S3ResultEntry;
import io.varve.swath.replay.testkit.HttpProbe;
import io.varve.swath.sort.RowGroupOrderException;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class ReplayServerTest {

    @Test
    void requestCeilingMustFitJettysMinimumThreadPool() {
        assertThatThrownBy(() -> ReplayServer.validateMaxConcurrentRequests(7))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least 8");
        assertThatCode(() -> ReplayServer.validateMaxConcurrentRequests(8)).doesNotThrowAnyException();
    }

    @Test
    void servesPathStyleListObjectsV2Xml() throws Exception {
        try (ReplayServer server = new ReplayServer(
                "127.0.0.1", 0, "bucket", request -> new S3ListResult(request, List.of(
                new S3ResultEntry.ObjectResult(new ListedObject(bytes("a/1"), 1, 0, "etag", "STANDARD", null, null, null, null))),
                false, null))) {
            server.start();

            HttpResponse<String> response = HttpProbe.response(server, "/bucket?list-type=2&prefix=a%2F&encoding-type=url");

            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.headers().firstValue("content-type")).contains("application/xml");
            assertThat(response.body()).contains("<Name>bucket</Name>");
            assertThat(response.body()).contains("<Prefix>a/</Prefix>");
            assertThat(response.body()).contains("<Key>a/1</Key>");
            assertThat(server.metrics().snapshot().httpRequests()).isEqualTo(1);
        }
    }

    @Test
    void returnsS3ErrorForWrongBucket() throws Exception {
        try (ReplayServer server = new ReplayServer(
                "127.0.0.1", 0, "bucket", request -> new S3ListResult(request, List.of(), false, null))) {
            server.start();

            HttpResponse<String> response = HttpProbe.response(server, "/other?list-type=2");

            assertThat(response.statusCode()).isEqualTo(404);
            assertThat(response.body()).contains("<Code>NoSuchBucket</Code>");
        }
    }

    /**
     * A fixture that passed eligibility and is disordered <em>inside</em> a row group fails at request
     * time, with nothing to fall back to (documented in {@code docs/swath-replay-server.md}). The
     * response must carry enough to act on — the typed reason, the file NAME, the row group — and must
     * <b>not</b> carry the server's filesystem layout, which is what a bare {@code getMessage()} on
     * this exception spells out. The full path stays in the log, not on the wire.
     */
    @Test
    void aDisorderedFixtureFailsTheRequestWithoutPublishingItsPath() throws Exception {
        Path fixtureFile = Path.of("/srv/captures/some-bucket/part-00001.parquet");
        try (ReplayServer server = new ReplayServer("127.0.0.1", 0, "bucket", request -> {
            throw RowGroupOrderException.at(fixtureFile, 7, 42,
                    "its keys must be in strictly ascending unsigned order");
        })) {
            server.start();

            HttpResponse<String> response = HttpProbe.response(server, "/bucket?list-type=2&delimiter=%2F");

            assertThat(response.statusCode()).isEqualTo(500);
            assertThat(response.body())
                    .contains("<Code>InternalError</Code>")
                    .contains(RowGroupOrderException.ROW_GROUP_DISORDER)
                    .contains("row group 7 of part-00001.parquet")
                    .doesNotContain("/srv/captures");
        }
    }

    @Test
    void echoesRequestedMaxKeysButClampsInternalPageSize() throws Exception {
        AtomicReference<S3ListRequest> seen = new AtomicReference<>();
        try (ReplayServer server = new ReplayServer(
                "127.0.0.1", 0, "bucket", request -> {
                    seen.set(request);
                    return new S3ListResult(request, List.of(), false, null);
                })) {
            server.start();

            HttpResponse<String> response = HttpProbe.response(server, "/bucket?list-type=2&max-keys=5000");

            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.body()).contains("<MaxKeys>5000</MaxKeys>");
            assertThat(seen.get().maxKeys()).isEqualTo(5000);
            assertThat(seen.get().pageSize()).isEqualTo(1000);
        }
    }

    @Test
    void handlesConcurrentRequests() throws Exception {
        try (ReplayServer server = new ReplayServer(
                "127.0.0.1", 0, "bucket", request -> new S3ListResult(request, List.of(
                new S3ResultEntry.ObjectResult(new ListedObject(bytes("a/1"), 1, 0, "etag", "STANDARD", null, null, null, null))),
                false, null))) {
            server.start();
            try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
                List<Callable<Integer>> calls = IntStream.range(0, 64)
                        .mapToObj(i -> (Callable<Integer>) () -> HttpProbe.response(server, "/bucket?list-type=2").statusCode())
                        .toList();
                assertThat(pool.invokeAll(calls).stream().map(f -> {
                    try {
                        return f.get();
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                })).containsOnly(200);
            }
        }
    }

    @Test
    void boundsConcurrentFixtureReadsToConfiguredPermitCount() throws Exception {
        int bound = 2;
        int requests = 6;
        // A barrier sized to exactly `bound` parties: it only trips once `bound` calls are
        // simultaneously inside fixture.list(), proving the semaphore both admits up to the bound
        // (the barrier trips at all, rather than timing out) and never exceeds it (maxObserved).
        CyclicBarrier barrier = new CyclicBarrier(bound);
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxObserved = new AtomicInteger();
        ListingFixture blockingFixture = request -> {
            int concurrent = inFlight.incrementAndGet();
            maxObserved.updateAndGet(m -> Math.max(m, concurrent));
            try {
                barrier.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                throw new IllegalStateException("bound was not actually reached", e);
            } finally {
                inFlight.decrementAndGet();
            }
            return new S3ListResult(request, List.of(), false, null);
        };

        try (ReplayServer server = new ReplayServer(
                "127.0.0.1", 0, "bucket", blockingFixture,
                ReplayServerFixtureConfig.DEFAULT.withMaxConcurrentReads(bound))) {
            server.start();
            try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
                List<Callable<Integer>> calls = IntStream.range(0, requests)
                        .mapToObj(i -> (Callable<Integer>) () -> HttpProbe.response(server, "/bucket?list-type=2").statusCode())
                        .toList();
                assertThat(pool.invokeAll(calls).stream().map(f -> {
                    try {
                        return f.get();
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                })).containsOnly(200);
            }
            assertThat(maxObserved.get()).isEqualTo(bound);
        }
    }

    @Test
    void closeIsIdempotentAndSurfacesFixtureCloseFailureOnlyOnce() throws Exception {
        AtomicInteger fixtureCloseCalls = new AtomicInteger();
        AutoCloseable failingOwnedFixture = () -> {
            fixtureCloseCalls.incrementAndGet();
            throw new IllegalStateException("boom");
        };
        ReplayServer server = new ReplayServer(
                "127.0.0.1", 0, "bucket", request -> new S3ListResult(request, List.of(), false, null),
                ReplayServerFixtureConfig.DEFAULT.withOwnedFixture(failingOwnedFixture));
        server.start();

        assertThatThrownBy(server::close)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("failed to close S3 listing replay fixture");
        assertThat(fixtureCloseCalls.get()).isEqualTo(1);

        // Idempotent: a second close() is a no-op — it neither re-invokes the fixture's close()
        // nor re-throws/re-surfaces the first call's failure.
        server.close();
        assertThat(fixtureCloseCalls.get()).isEqualTo(1);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}

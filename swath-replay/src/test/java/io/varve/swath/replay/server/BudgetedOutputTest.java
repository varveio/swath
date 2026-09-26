/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.replay.metrics.ChunkAllocationReason;
import io.varve.swath.replay.metrics.ReplayMetrics;
import io.varve.swath.replay.protocol.ByteKeys;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.protocol.S3ListResult;
import io.varve.swath.replay.protocol.S3ResultEntry;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class BudgetedOutputTest {
    @Test
    void productionChunkSizeIsFixedWhileTestConstructorCanExerciseOtherBoundaries() {
        assertThat(BudgetedOutput.configuredChunkBytes()).isEqualTo(256 * 1024);
        try (BudgetedOutput output = new BudgetedOutput(new ResponseByteBudget(4096),
                1, 4096, 7)) {
            assertThat(output.chunkBytes()).isEqualTo(7);
        }
    }

    @Test
    void initialChunkReasonNamesOnlyActualCapClamping() {
        List<ChunkAllocationReason> reasons = new ArrayList<>();
        ResponseByteBudget budget = new ResponseByteBudget(128 * 1024,
                (reason, bytes) -> reasons.add(reason));
        try (BudgetedOutput sparse = new BudgetedOutput(budget, 4096, 64 * 1024, 256 * 1024)) {
            sparse.setFirstChunkHint(4096);
            sparse.write(1);
            assertThat(sparse.capacity()).isEqualTo(4096);
        }
        assertThat(reasons).containsExactly(ChunkAllocationReason.INITIAL);
        reasons.clear();
        try (BudgetedOutput capped = new BudgetedOutput(budget, 4096, 5000, 256 * 1024)) {
            capped.setFirstChunkHint(8192);
            capped.write(1);
            assertThat(capped.capacity()).isEqualTo(5000);
        }
        assertThat(reasons).containsExactly(ChunkAllocationReason.INITIAL_CAP_PARTIAL);
        assertThat(budget.charged()).isZero();
    }

    @Test
    void concurrentChunkGrowthNeverOverchargesAndRecoversAfterRelease() throws Exception {
        ResponseByteBudget budget = new ResponseByteBudget(1536);
        BudgetedOutput[] outputs = new BudgetedOutput[8];
        for (int i = 0; i < outputs.length; i++) {
            outputs[i] = new BudgetedOutput(budget, 128, 256, 128);
        }
        assertThat(budget.charged()).isEqualTo(1024);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(outputs.length);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger succeeded = new AtomicInteger();
        AtomicInteger refused = new AtomicInteger();
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            for (BudgetedOutput output : outputs) {
                workers.submit(() -> {
                    try {
                        start.await();
                        output.write(new byte[129], 0, 129);
                        succeeded.incrementAndGet();
                    } catch (ReplayOutputException exhausted) {
                        refused.incrementAndGet();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    } finally {
                        done.countDown();
                        try {
                            release.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        output.close();
                    }
                });
            }
            start.countDown();
            try {
                assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(succeeded).hasValue(4);
                assertThat(refused).hasValue(4);
                assertThat(budget.charged()).isEqualTo(1536);
                assertThat(budget.peak()).isEqualTo(1536);
            } finally {
                release.countDown();
            }
        }
        assertThat(budget.charged()).isZero();
    }

    @Test
    void actualS3OneKeyAndEmptyPagesUseSmallPostPageFirstChunks() {
        for (boolean empty : new boolean[] {false, true}) {
            ReplayMetrics metrics = new ReplayMetrics();
            ResponseByteBudget budget = new ResponseByteBudget(1024 * 1024);
            var entries = empty ? List.<S3ResultEntry>of()
                    : List.<S3ResultEntry>of(new S3ResultEntry.ObjectResult(
                            new ListedObject("a".getBytes(StandardCharsets.UTF_8), 1, 0,
                                    null, "STANDARD", null, null, null, null)));
            S3ListingProtocolHandler handler = new S3ListingProtocolHandler("bucket",
                    request -> new S3ListResult(request, entries, false, null), metrics,
                    (request, result) -> Duration.ZERO);
            ListingOperation operation = handler.parse(new ListingHttpRequest("GET", "/bucket",
                    "list-type=2&max-keys=1000", Map.of()));
            try (BudgetedOutput output = new BudgetedOutput(budget,
                    operation.initialOutputBytes(), 64 * 1024 * 1024)) {
                operation.beforePage();
                PreparedPage page = operation.page();
                output.setFirstChunkHint(page.initialOutputBytesHint());
                RenderedResponse rendered = page.render(output);
                assertThat(rendered.body().length()).isLessThan(4096);
                assertThat(output.capacity()).isEqualTo(4096);
                assertThat(budget.charged()).isEqualTo(4096);
            } finally {
                metrics.registry().close();
            }
            assertThat(budget.charged()).isZero();
        }
    }

    @Test
    void reservesBeforePagingThenReturnsUnusedCreditAfterSparseOrEmptyRender() {
        ResponseByteBudget budget = new ResponseByteBudget(1024 * 1024);
        try (BudgetedOutput sparse = new BudgetedOutput(budget, 320_512, 64 * 1024 * 1024)) {
            assertThat(budget.charged()).isEqualTo(320_512);
            assertThat(sparse.capacity()).isZero();
            sparse.setFirstChunkHint(4096);
            sparse.write(new byte[100], 0, 100);
            assertThat(sparse.capacity()).isEqualTo(4096);
            assertThat(budget.charged()).isEqualTo(320_512);
            assertThat(sparse.body().length()).isEqualTo(100);
            assertThat(budget.charged()).isEqualTo(4096);
            assertThatThrownBy(() -> sparse.write(1)).isInstanceOf(IllegalStateException.class);
        }
        assertThat(budget.charged()).isZero();
        try (BudgetedOutput empty = new BudgetedOutput(budget, 320_512, 64 * 1024 * 1024)) {
            assertThat(empty.capacity()).isZero();
            assertThat(empty.body().length()).isZero();
            assertThat(budget.charged()).isZero();
        }
        assertThat(budget.charged()).isZero();
    }

    @Test
    void chunksConsumeCreditThenRefuseAggregateGrowthWithoutLeaking() {
        ResponseByteBudget budget = new ResponseByteBudget(12);
        BudgetedOutput first = new BudgetedOutput(budget, 4, 12, 4);
        first.write(new byte[8], 0, 8);
        assertThat(first.capacity()).isEqualTo(8);
        assertThat(budget.charged()).isEqualTo(8);
        BudgetedOutput second = new BudgetedOutput(budget, 4, 12, 4);
        assertThatThrownBy(() -> first.write(1))
                .isInstanceOf(ReplayOutputException.class)
                .satisfies(error -> assertThat(((ReplayOutputException) error).responseTooLarge()).isFalse());
        assertThat(first.size()).isEqualTo(8);
        assertThat(first.capacity()).isEqualTo(8);
        assertThat(budget.charged()).isEqualTo(12);
        first.close();
        first.close();
        second.close();
        assertThat(budget.charged()).isZero();
    }

    @Test
    void allocationFailureRollsBackExtraChargeAndRetainsExistingChunks() {
        ResponseByteBudget budget = new ResponseByteBudget(16);
        AtomicInteger calls = new AtomicInteger();
        BudgetedOutput output = new BudgetedOutput(budget, 4, 12, 4, size -> {
            if (calls.incrementAndGet() == 2) throw new OutOfMemoryError("injected allocation failure");
            return new byte[size];
        });
        output.write(new byte[4], 0, 4);
        assertThatThrownBy(() -> output.write(1))
                .isInstanceOf(OutOfMemoryError.class).hasMessageContaining("injected");
        assertThat(output.size()).isEqualTo(4);
        assertThat(output.capacity()).isEqualTo(4);
        assertThat(budget.charged()).isEqualTo(4);
        output.close();
        assertThat(budget.charged()).isZero();
    }

    @Test
    void finalPartialChunkHonorsNonAlignedCapacityCapWithoutPadding() {
        ResponseByteBudget budget = new ResponseByteBudget(32);
        try (BudgetedOutput output = new BudgetedOutput(budget, 4, 10, 4)) {
            byte[] expected = "0123456789".getBytes(StandardCharsets.US_ASCII);
            output.write(expected, 0, expected.length);
            OwnedBody body = output.body();
            assertThat(body.views()).extracting(ByteBuffer::remaining).containsExactly(4, 4, 2);
            assertThat(bytes(body)).isEqualTo(expected);
            assertThat(output.capacity()).isEqualTo(10);
            assertThat(budget.charged()).isEqualTo(10);
            assertThatThrownBy(() -> output.write(1))
                    .isInstanceOf(IllegalStateException.class);
        }
        assertThat(budget.charged()).isZero();
        try (BudgetedOutput capped = new BudgetedOutput(budget, 4, 10, 4)) {
            assertThatThrownBy(() -> capped.write(new byte[11], 0, 11))
                    .isInstanceOf(ReplayOutputException.class)
                    .satisfies(error -> assertThat(((ReplayOutputException) error).responseTooLarge()).isTrue());
        }
        assertThat(budget.charged()).isZero();
    }

    @Test
    void entityUtf8AndPercentEscapesCrossChunkEdgesByteExactly() {
        ResponseByteBudget budget = new ResponseByteBudget(64);
        try (BudgetedOutput xml = new BudgetedOutput(budget, 4, 32, 4)) {
            xml.appendAscii("abc");
            xml.appendEscaped("é&d");
            assertThat(new String(bytes(xml.body()), StandardCharsets.UTF_8)).isEqualTo("abcé&amp;d");
            assertThat(xml.body().views().size()).isGreaterThan(1);
        }
        assertThat(budget.charged()).isZero();
        byte[] safe = "abc/XYZ-_.09".getBytes(StandardCharsets.US_ASCII);
        try (BudgetedOutput output = new BudgetedOutput(new ResponseByteBudget(128), 1, safe.length, 4)) {
            output.appendPercentEncoded(safe);
            assertThat(bytes(output.body())).isEqualTo(safe);
        }
        Random random = new Random(220);
        for (int n = 1; n <= 128; n++) {
            byte[] input = new byte[n];
            int expectedLength = 0;
            for (int i = 0; i < n; i++) {
                input[i] = (byte) (random.nextBoolean() ? 'a' : ' ');
                expectedLength += input[i] == 'a' ? 1 : 3;
            }
            try (BudgetedOutput output = new BudgetedOutput(
                    new ResponseByteBudget(4L * expectedLength), 1, expectedLength, 7)) {
                output.appendPercentEncoded(input);
                byte[] rendered = bytes(output.body());
                assertThat(rendered).hasSize(expectedLength);
                assertThat(ByteKeys.percentDecode(new String(rendered, StandardCharsets.US_ASCII)))
                        .isEqualTo(input);
            }
        }
    }

    @Test
    void percentEncodingReusesChargedChunkWithoutAllocatingAndRejectsFrozenOrClosedOutput() {
        ResponseByteBudget budget = new ResponseByteBudget(32);
        AtomicInteger allocations = new AtomicInteger();
        BudgetedOutput output = new BudgetedOutput(budget, 8, 32, 32, bytes -> {
            allocations.incrementAndGet();
            return new byte[bytes];
        });
        try {
            output.appendAscii("prefix");
            assertThat(allocations).hasValue(1);
            assertThat(budget.charged()).isEqualTo(32);
            output.appendPercentEncoded(new byte[] {'a', ' ', (byte) 0xe9});
            assertThat(allocations).hasValue(1);
            assertThat(output.capacity()).isEqualTo(32);
            assertThat(output.percentOnePassValues()).isEqualTo(1);
            assertThat(output.percentExactFallbackValues()).isZero();
            assertThat(new String(bytes(output.body()), StandardCharsets.US_ASCII))
                    .isEqualTo("prefixa%20%E9");
            assertThat(output.percentOnePassValues()).isEqualTo(1);
            assertThatThrownBy(() -> output.appendPercentEncoded(new byte[0]))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("frozen");
        } finally {
            output.close();
        }
        assertThat(budget.charged()).isZero();
        assertThatThrownBy(() -> output.appendPercentEncoded(new byte[0]))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("closed");
    }

    @Test
    void prefixedFastAndExactFallbackMatchIndependentPercentOracleForAllBytesAndRandomMix() {
        byte[] everyByte = new byte[256];
        for (int i = 0; i < everyByte.length; i++) everyByte[i] = (byte) i;
        byte[] mixed = new byte[512];
        new Random(220).nextBytes(mixed);
        for (byte[] value : List.of(everyByte, mixed)) {
            String encoded = ByteKeys.percentEncode(value);
            String fastPrefix = "p".repeat(50);
            try (BudgetedOutput fast = new BudgetedOutput(new ResponseByteBudget(4096),
                    64, 4096, 4096)) {
                fast.appendAscii(fastPrefix);
                fast.appendPercentEncoded(value);
                OwnedBody body = fast.body();
                assertThat(new String(bytes(body), StandardCharsets.US_ASCII))
                        .isEqualTo(fastPrefix + encoded);
                assertThat(body.views()).extracting(ByteBuffer::remaining)
                        .containsExactly(fastPrefix.length() + encoded.length());
                assertThat(fast.percentOnePassValues()).isEqualTo(1);
                assertThat(fast.percentExactFallbackValues()).isZero();
            }
            String exactPrefix = "p".repeat(4096 - encoded.length());
            assertThat(value.length).isGreaterThan(encoded.length() / 3);
            try (BudgetedOutput fallback = new BudgetedOutput(new ResponseByteBudget(4096),
                    64, 4096, 4096)) {
                fallback.appendAscii(exactPrefix);
                fallback.appendPercentEncoded(value);
                OwnedBody body = fallback.body();
                assertThat(new String(bytes(body), StandardCharsets.US_ASCII))
                        .isEqualTo(exactPrefix + encoded);
                assertThat(body.views()).extracting(ByteBuffer::remaining).containsExactly(4096);
                assertThat(fallback.percentOnePassValues()).isZero();
                assertThat(fallback.percentExactFallbackValues()).isEqualTo(1);
            }
        }
    }

    @Test
    void percentFastPathEngagesAtExactlyOneThirdRoomAndFallsBackOneByteBeyond() {
        ResponseByteBudget budget = new ResponseByteBudget(32);
        try (BudgetedOutput exactBoundary = new BudgetedOutput(budget, 8, 32, 32)) {
            exactBoundary.appendAscii("p".repeat(23));
            exactBoundary.appendPercentEncoded(new byte[] {0, 0x7f, (byte) 0xff});
            assertThat(new String(bytes(exactBoundary.body()), StandardCharsets.US_ASCII))
                    .isEqualTo("p".repeat(23) + "%00%7F%FF");
            assertThat(exactBoundary.percentOnePassValues()).isEqualTo(1);
            assertThat(exactBoundary.capacity()).isEqualTo(32);
        }
        assertThat(budget.charged()).isZero();
        try (BudgetedOutput oneBeyond = new BudgetedOutput(budget, 8, 32, 32)) {
            oneBeyond.appendAscii("p".repeat(23));
            oneBeyond.appendPercentEncoded("abcd".getBytes(StandardCharsets.US_ASCII));
            assertThat(new String(bytes(oneBeyond.body()), StandardCharsets.US_ASCII))
                    .isEqualTo("p".repeat(23) + "abcd");
            assertThat(oneBeyond.percentOnePassValues()).isZero();
            assertThat(oneBeyond.percentExactFallbackValues()).isEqualTo(1);
            assertThat(oneBeyond.capacity()).isEqualTo(32);
        }
        assertThat(budget.charged()).isZero();
    }

    @Test
    void conservativePercentFastPathFallsBackToExactCapAndBudgetDecisions() {
        ResponseByteBudget budget = new ResponseByteBudget(14);
        try (BudgetedOutput exactFit = new BudgetedOutput(budget, 5, 10, 10)) {
            exactFit.appendAscii("xxxxx");
            // Four safe bytes fit, though the 3x fast-path bound does not.
            exactFit.appendPercentEncoded("aaaa".getBytes(StandardCharsets.US_ASCII));
            assertThat(exactFit.percentOnePassValues()).isZero();
            assertThat(exactFit.percentExactFallbackValues()).isEqualTo(1);
            assertThat(new String(bytes(exactFit.body()), StandardCharsets.US_ASCII))
                    .isEqualTo("xxxxxaaaa");
            ReplayMetrics metrics = new ReplayMetrics();
            try {
                metrics.recordPercentEncodingPaths("s3", exactFit.percentOnePassValues(),
                        exactFit.percentExactFallbackValues());
                assertThat(metrics.registry().get("swath.replay.response.percent.encoding.path")
                        .tags("protocol", "s3", "reason", "exact_length_fallback")
                        .counter().count()).isEqualTo(1);
                assertThat(metrics.registry().find("swath.replay.response.percent.encoding.path")
                        .tags("protocol", "s3", "reason", "charged_chunk_one_pass")
                        .counter()).isNull();
            } finally {
                metrics.registry().close();
            }
        }
        assertThat(budget.charged()).isZero();
        try (BudgetedOutput capped = new BudgetedOutput(budget, 5, 10, 10)) {
            capped.appendAscii("xxxxxxx");
            assertThatThrownBy(() -> capped.appendPercentEncoded("a b".getBytes(StandardCharsets.US_ASCII)))
                    .isInstanceOf(ReplayOutputException.class)
                    .satisfies(error -> assertThat(((ReplayOutputException) error).responseTooLarge()).isTrue());
            assertThat(capped.size()).isEqualTo(7);
            assertThat(capped.capacity()).isEqualTo(10);
            assertThat(capped.percentExactFallbackValues()).isZero();
        }
        assertThat(budget.charged()).isZero();
        try (BudgetedOutput split = new BudgetedOutput(budget, 7, 14, 7)) {
            split.appendAscii("xxxxx");
            split.appendPercentEncoded(new byte[] {' '});
            assertThat(split.percentExactFallbackValues()).isEqualTo(1);
            assertThat(new String(bytes(split.body()), StandardCharsets.US_ASCII)).isEqualTo("xxxxx%20");
            assertThat(split.body().views()).extracting(ByteBuffer::remaining).containsExactly(7, 1);
        }
        assertThat(budget.charged()).isZero();
        ResponseByteBudget exhausted = new ResponseByteBudget(6);
        try (BudgetedOutput output = new BudgetedOutput(exhausted, 4, 8, 4)) {
            output.appendAscii("xxxx");
            assertThatThrownBy(() -> output.appendPercentEncoded(new byte[] {' '}))
                    .isInstanceOf(ReplayOutputException.class)
                    .satisfies(error -> assertThat(((ReplayOutputException) error).responseTooLarge()).isFalse());
            assertThat(output.size()).isEqualTo(4);
            assertThat(output.capacity()).isEqualTo(4);
            assertThat(exhausted.charged()).isEqualTo(4);
            assertThat(output.percentExactFallbackValues()).isZero();
        }
        assertThat(exhausted.charged()).isZero();
    }

    private static byte[] bytes(OwnedBody body) {
        byte[] result = new byte[body.length()];
        int offset = 0;
        for (ByteBuffer view : body.views()) {
            ByteBuffer copy = view.duplicate();
            int count = copy.remaining();
            copy.get(result, offset, count);
            offset += count;
        }
        return result;
    }
}

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
    void invalidChunkPropertyFailsInsteadOfSilentlyUsingTheDefault() throws Exception {
        String javaBinary = java.nio.file.Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String probe = BudgetedOutputTest.PropertyProbe.class.getName();
        Process valid = new ProcessBuilder(javaBinary, "-Dswath.replay.response-chunk-bytes=131072",
                "-cp", System.getProperty("java.class.path"), probe)
                .redirectErrorStream(true).start();
        String validOutput = new String(valid.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertThat(valid.waitFor()).isZero();
        assertThat(validOutput).contains("131072");
        Process invalid = new ProcessBuilder(javaBinary, "-Dswath.replay.response-chunk-bytes=128k",
                "-cp", System.getProperty("java.class.path"), probe)
                .redirectErrorStream(true).start();
        String invalidOutput = new String(invalid.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertThat(invalid.waitFor()).isNotZero();
        assertThat(invalidOutput).contains("must be 65536, 131072, or 262144");
    }

    public static final class PropertyProbe {
        public static void main(String[] ignored) {
            System.out.println(BudgetedOutput.configuredChunkBytes());
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

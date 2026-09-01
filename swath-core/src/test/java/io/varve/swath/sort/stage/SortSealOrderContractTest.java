/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.ListEntry;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * SORT-RESUME-1, out-of-order-finalize hazard case. The single ordered encoder MUST finalize
 * staging segments — and therefore advance {@code durable_cursor} through {@link SegmentSink} —
 * <b>strictly in seal order</b>, even when a LATER buffer is already sealed and queued while an
 * EARLIER one is still mid-encode. An out-of-order finalize would let {@code durable_cursor}
 * over-advance past keys still sitting in an earlier unfinalized buffer and silently lose them on
 * resume.
 *
 * <p>Rigged via the package-private {@code beforeEncodeHookForTesting} seam: the first segment's
 * encode is stalled while the drain thread seals additional buffers behind it (double-buffering is
 * proven engaged — {@link SortLane#maxObservedOffThreadBuffers()} &gt; 1). Despite that concurrency,
 * the {@link SegmentSink} must observe the per-node max keys strictly ascending in seal order (never
 * a later-sealed-but-earlier-finalized inversion), so {@code durable_cursor} never regresses or
 * over-advances.
 */
final class SortSealOrderContractTest {

    private final ListEntryComparator cmp = new ListEntryComparator();

    /** segment-entries=5, buffers=3 ⇒ each 10-key page seals immediately, up to 2 buffers off-thread. */
    private static SortConfig config() {
        return SortConfigs.base().withSegmentEntries(5).withBuffers(3);
    }

    @Test
    @Timeout(60)
    void sealsFinalizeStrictlyInSealOrderEvenWithLaterBuffersQueuedDuringAStalledEncode(
            @TempDir Path tmp) throws Exception {
        Path staging = Files.createDirectories(tmp.resolve("_staging"));

        // The SegmentSink records each finalized segment's node-1 max key, in the exact order the
        // single ordered encoder invokes it — this is the durable_cursor advance order.
        List<byte[]> finalizeOrderMaxKeys = Collections.synchronizedList(new ArrayList<>());
        SegmentSink recordingSink = result -> finalizeOrderMaxKeys.add(result.perNodeMaxKeys().get(1L));

        SortLane lane = new SortLane(config(), cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP,
                SortLaneMeters.NO_OP, staging, "seg-sealorder", recordingSink);

        // Stall ONLY the first segment's encode so later buffers seal + queue behind it.
        AtomicInteger encodeIndex = new AtomicInteger();
        lane.beforeEncodeHookForTesting = () -> {
            if (encodeIndex.getAndIncrement() == 0) {
                sleepUninterruptibly(200);
            }
        };

        // Four ascending 10-key pages under ONE node ⇒ four segments, max keys k009 < k019 < k029 < k039.
        List<byte[]> expectedMaxKeys = new ArrayList<>();
        for (int p = 0; p < 4; p++) {
            List<ListEntry> page = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                page.add(SortTestSupport.object(String.format("k%03d", p * 10 + i)));
            }
            lane.admit(1L, page);
            expectedMaxKeys.add(String.format("k%03d", p * 10 + 9).getBytes(StandardCharsets.UTF_8));
        }
        lane.close();

        // Double-buffering genuinely engaged: while segment 0 stalled, at least one later buffer was
        // live off-thread (queued/encoding) — so the seal-order guarantee below was actually tested
        // against concurrency, not a degenerate one-at-a-time drain.
        assertThat(lane.maxObservedOffThreadBuffers())
                .as("a later buffer was live while the first encode stalled (double-buffering active)")
                .isGreaterThan(1);

        // The finalize (durable_cursor advance) order is EXACTLY the seal order — strictly ascending,
        // never inverted. This is the over-advance guard.
        assertThat(finalizeOrderMaxKeys).as("every seal finalized exactly once").hasSize(4);
        for (int i = 0; i < expectedMaxKeys.size(); i++) {
            assertThat(finalizeOrderMaxKeys.get(i))
                    .as("segment %d finalized in seal order (durable_cursor advances monotonically)", i)
                    .isEqualTo(expectedMaxKeys.get(i));
        }
        for (int i = 1; i < finalizeOrderMaxKeys.size(); i++) {
            assertThat(Arrays.compareUnsigned(finalizeOrderMaxKeys.get(i - 1),
                    finalizeOrderMaxKeys.get(i)))
                    .as("durable_cursor never regresses nor over-advances across seals")
                    .isNegative();
        }
    }

    private static void sleepUninterruptibly(long millis) {
        long deadline = System.nanoTime() + millis * 1_000_000L;
        long remaining;
        while ((remaining = deadline - System.nanoTime()) > 0) {
            try {
                Thread.sleep(remaining / 1_000_000L, (int) (remaining % 1_000_000L));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}

/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.model.PageBatch;
import io.varve.swath.runtime.ListRunner;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.ParquetReads;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * <b>PERF-2</b> (I11): stream 100k keys through the
 * <b>real listing→parquet pipeline</b> ({@link ListRunner#runToParquet}, a virtual-thread
 * producer feeding the platform-thread writer pool) and assert the three things the contract
 * requires, all <b>measured</b> (not estimated):
 *
 * <ol>
 *   <li><b>measured peak heap</b> under the §7.2 Parquet budget (&lt; 1 GB) — the documented,
 *       re-measure-don't-guess release gate;</li>
 *   <li><b>measured peak RSS</b> (from {@code /proc/self/status} {@code VmRSS}) under a bounded
 *       budget — RSS is heap + JVM native (metaspace, code cache, thread stacks, GC, parquet-mr
 *       direct buffers); see the RSS-budget note below;</li>
 *   <li><b>no hot-path virtual-thread pinning</b> — the {@code ScanProducer} runs on a virtual
 *       carrier (the HTTP-I/O hot path) while the Parquet writer lanes are platform threads
 *       ({@link io.varve.swath.concurrent.Scope#ofPlatformThreads}) precisely so their blocking file
 *       I/O / fsync never pins a carrier. A pin would mean blocking I/O leaked onto the virtual
 *       producer carrier (or a {@code Channel} wait pins), defeating the decoupled design.</li>
 * </ol>
 *
 * <p>This is the gated full PERF-2 ({@code @Tag("perf")}, opt-in via {@code ./gradlew test -Pperf}).
 * It is mock-driven at 100k via {@link MockPageFetcher} — NO LocalStack, per the no-mass-populate
 * rule (docs/ops/dev/TESTING.md). The heavier ≤50M-key sweep stays on-demand/at-gates.
 *
 * <p><b>RSS budget — undocumented, flagged for doc follow-up.</b> The contract documents only the
 * peak-<i>heap</i> budget per format (Parquet &lt; 1 GB); it does NOT document an RSS budget. The
 * {@link #RSS_BUDGET_BYTES} bound below is therefore a justified choice — the 1 GB heap budget plus
 * ~2 GB of JVM/native headroom — asserting the invariant (I11) that RSS, like heap, is a function of
 * the writer/row-group knobs and JVM baseline, never of object count. A spec follow-up to document a
 * per-format RSS budget would let this assert against the pack instead of a derived bound.
 *
 * <p>{@code -Djdk.tracePinnedThreads} was removed in JDK 24 (JEP 491), so the pinning check asserts
 * against its successor signal: the {@code jdk.VirtualThreadPinned} JFR event (the programmatic
 * equivalent — the JVM flag can't be toggled mid-test). Heap stays a function of the writer/row-group
 * knobs, not object count (I11).
 */
@Tag("perf")
class ParquetPerf2Test {

    private static final long HEAP_BUDGET_BYTES = 1024L * 1024 * 1024;       // §7.2 Parquet heap budget (documented)
    private static final long RSS_BUDGET_BYTES = 3L * 1024 * 1024 * 1024;    // heap budget + ~2 GB JVM/native (undocumented; see class note)

    @Test
    void streams100kKeysThroughTheRealPipelineUnderHeapAndRssBudgetWithNoHotPathPinning(@TempDir Path dir)
            throws Exception {
        int n = 100_000;
        int writers = 3;

        Recording recording = new Recording();
        recording.enable("jdk.VirtualThreadPinned")
                .withThreshold(Duration.ofMillis(1));
        recording.start();

        AtomicBoolean done = new AtomicBoolean(false);
        AtomicLong peakHeap = new AtomicLong(0);
        AtomicLong peakRss = new AtomicLong(0);
        // Platform thread: the sampler reads /proc (blocking file I/O); on a virtual
        // thread that would pin and pollute the pinning measurement below.
        Thread sampler = Thread.ofPlatform().daemon(true).start(() -> {
            Runtime rt = Runtime.getRuntime();
            while (!done.get()) {
                peakHeap.accumulateAndGet(rt.totalMemory() - rt.freeMemory(), Math::max);
                long rss = readVmRssBytes();
                if (rss > 0) {
                    peakRss.accumulateAndGet(rss, Math::max);
                }
                try {
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    return;
                }
            }
        });

        // Drive the real pipeline: a virtual-thread ScanProducer (the hot path) over the
        // deterministic MockPageFetcher, feeding the 3-writer platform-thread pool. Tight queue
        // budgets keep heap a function of the knobs, not n (I11).
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(Keyspaces.exactly(n)).build();
        var spec = new ListRunner.ParquetSpec(new byte[0], 64, 1000, FilterChain.EMPTY,
                writers, PartWriter.ROW_GROUP_BYTES * 4, 32, "perf2", null, null, 0L, 0L, "");
        var stats = new ListRunner().runToParquet(RunContext.create(), fetcher, dir, spec);

        done.set(true);
        sampler.join(1000);

        recording.stop();
        Path jfr = dir.resolve("perf2.jfr");
        recording.dump(jfr);
        recording.close();

        // (1) measured peak heap under the documented §7.2 Parquet budget — not O(N).
        assertThat(peakHeap.get())
                .as("measured peak heap (%d MB) under §7.2 Parquet budget", peakHeap.get() / (1024 * 1024))
                .isLessThan(HEAP_BUDGET_BYTES);

        // (2) measured peak RSS under the (derived) budget where the OS exposes it (Linux /proc).
        //     Asserted whenever VmRSS is readable; silently skipped only on a platform without /proc.
        if (peakRss.get() > 0) {
            assertThat(peakRss.get())
                    .as("measured peak RSS (%d MB) bounded — function of knobs, not N (I11)",
                            peakRss.get() / (1024 * 1024))
                    .isLessThan(RSS_BUDGET_BYTES);
        }

        // (3) no virtual-thread pinning on the listing→parquet hot path (the virtual producer
        //     carrier and the Channel hand-off; file I/O lives on platform writer lanes).
        long pinnedEvents = countPinnedEvents(jfr);
        assertThat(pinnedEvents)
                .as("no jdk.VirtualThreadPinned events on the listing→parquet hot path").isZero();

        // And the data is all there: the union of parts equals the bucket, exactly once.
        assertThat(stats.objects()).isEqualTo((long) n);
        long rows = 0;
        for (Path part : DatasetLayout.of(dir).dataParts()) {
            rows += ParquetReads.keys(part).size();
        }
        assertThat(rows).isEqualTo((long) n);
    }

    /**
     * The 3-writer memory model with <b>all lanes active</b> (2–4 writers
     * decoupled from {@code T}). The sequential {@code ScanProducer} emits a single
     * {@code nodeId=0}, so the real pipeline above drives only one lane; the multi-lane heap
     * budget is exercised here by submitting batches across distinct node ids (routing is
     * {@code nodeId % numWriters}). Heap stays a function of {@code writers × row-group}, not
     * object count (I11).
     */
    @Test
    void threeWriterLanesStayUnderHeapBudget(@TempDir Path dir) throws Exception {
        int n = 100_000;
        int writers = 3;

        AtomicBoolean done = new AtomicBoolean(false);
        AtomicLong peakHeap = new AtomicLong(0);
        Thread sampler = Thread.ofPlatform().daemon(true).start(() -> {
            Runtime rt = Runtime.getRuntime();
            while (!done.get()) {
                peakHeap.accumulateAndGet(rt.totalMemory() - rt.freeMemory(), Math::max);
                try {
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    return;
                }
            }
        });

        long written;
        try (ParquetWriterPool pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "perf",
                writers, PartWriter.ROW_GROUP_BYTES * 4, 32)) {
            long seq = 0;
            int batchSize = 1000;
            for (int i = 0; i < n; i += batchSize) {
                int to = Math.min(n, i + batchSize);
                long nodeId = (i / batchSize) % writers;   // spread across all writers ⇒ all lanes active
                pool.submit(batch(nodeId, seq++, i, to));
            }
            written = n;
        }
        done.set(true);
        sampler.join(1000);

        assertThat(peakHeap.get())
                .as("3-lane measured peak heap (%d MB) under §7.2 budget", peakHeap.get() / (1024 * 1024))
                .isLessThan(HEAP_BUDGET_BYTES);

        long rows = 0;
        for (Path part : DatasetLayout.of(dir).dataParts()) {
            rows += ParquetReads.keys(part).size();
        }
        assertThat(rows).isEqualTo(written);
    }

    private static PageBatch batch(long nodeId, long seq, int from, int to) {
        List<ListEntry> entries = new ArrayList<>(to - from);
        for (int i = from; i < to; i++) {
            entries.add(ObjectEntry.withoutOwnerDisplayNameAndChecksumType(KeyBytes.ofUtf8(String.format("bucket/key-%09d", i)),
                    1234, 1_700_000_000_000_000L, "etag1234", "STANDARD", null, true, null, null));
        }
        return new PageBatch(nodeId, seq, entries);
    }

    private static long countPinnedEvents(Path jfr) throws IOException {
        long count = 0;
        try (RecordingFile rf = new RecordingFile(jfr)) {
            while (rf.hasMoreEvents()) {
                RecordedEvent e = rf.readEvent();
                if (e.getEventType().getName().equals("jdk.VirtualThreadPinned")) {
                    count++;
                }
            }
        }
        return count;
    }

    /** Linux RSS in bytes from {@code /proc/self/status}; {@code 0} when unavailable. */
    private static long readVmRssBytes() {
        Path status = Path.of("/proc/self/status");
        if (!Files.exists(status)) {
            return 0;
        }
        try {
            for (String line : Files.readAllLines(status)) {
                if (line.startsWith("VmRSS:")) {
                    String[] parts = line.split("\\s+");
                    return Long.parseLong(parts[1]) * 1024;   // kB → bytes
                }
            }
        } catch (IOException | RuntimeException ignored) {
            // best effort
        }
        return 0;
    }
}

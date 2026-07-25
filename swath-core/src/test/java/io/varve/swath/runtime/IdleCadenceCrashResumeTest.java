/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.PartRef;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.ListingException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ListingMode;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.model.PageBatch;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.output.parquet.ParquetResume;
import io.varve.swath.output.parquet.ParquetSchema;
import io.varve.swath.output.parquet.ParquetWriterPool;
import io.varve.swath.output.parquet.PartInfo;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.ParquetReads;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.io.TempDir;

/**
 * The highest-value RPO scenario: a crash DURING an idle window whose
 * only durability came from the idle-timeout cadence path (not a row-count or size
 * trigger). Distinct from both:
 * <ul>
 *   <li>{@link IdleLaneDurableCursorCadenceTest}, which proves the idle-timeout advances
 *       {@code durable_cursor} mid-run but always lets the run finish cleanly afterward
 *       (release → clean completion, never a crash).</li>
 *   <li>{@link ResumeParquetCadenceTest}, whose crash/resume guard is driven exclusively by
 *       the deterministic ROW-COUNT trigger (a lane that never goes idle — it's still
 *       actively receiving batches right up to the kill).</li>
 * </ul>
 * Here the lane finalizes a part purely because it went idle (no more rows arrive to
 * trigger a check-on-write, and neither the size nor row-count trigger is configured),
 * and only THEN does the run crash (the fetch throws, simulating a kill, the same
 * injected-exception shape {@code ResumeParquetCadenceTest}/{@code ResumeParquetTest}'s
 * res4 cases use). The finalized prefix must survive the crash, resume must re-list only
 * the bounded tail from that point (not the whole run), and the final union must equal
 * the clean run exactly once (I6).
 */
final class IdleCadenceCrashResumeTest {

    private static final int TOTAL_KEYS = 60;
    private static final int PAGE_SIZE = 5;
    /** Block (then crash) on the 4th fetch: pages 0-2 (15 rows) land in the lane's open
     *  part undisturbed before the idle window opens. */
    private static final int BLOCK_AT_PAGE_IDX = 3;
    private static final Duration ROTATION_INTERVAL = Duration.ofMillis(300);

    private static List<byte[]> keys(int n) {
        List<byte[]> ks = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            ks.add(String.format("k%04d", i).getBytes(StandardCharsets.UTF_8));
        }
        return ks;
    }

    private static RunKey runKey() {
        return new RunKey("s3", null, "bucket", new byte[0], "h1",
                "WORK_STEALING", ListingMode.OBJECTS, "", "parquet");
    }

    private static PageBatch warmupBatch() {
        List<ListEntry> entries = List.of(ObjectEntry.withoutOwnerDisplayNameAndChecksumType(KeyBytes.ofUtf8("warmup/key"),
                100, 1_700_000_000_000_000L, "etag", "STANDARD", null, true, null, null));
        return new PageBatch(999, 0, entries);
    }

    /** Same rationale as the sibling cadence tests: warm up parquet-mr/Hadoop classloading
     * off the timed critical section, so it can't blow past {@link #ROTATION_INTERVAL} on its own. */
    @BeforeEach
    void warmupParquetWriterClasses(@TempDir Path warmupDir) throws Exception {
        var warm = new ParquetWriterPool(warmupDir, ParquetSchema.canonical(), "warmup", 1, Long.MAX_VALUE, 4);
        warm.submit(warmupBatch());
        warm.close();
    }

    private static List<String> allPartKeys(Path dir) throws IOException {
        List<String> out = new ArrayList<>();
        for (Path part : DatasetLayout.of(dir).dataParts()) {
            out.addAll(ParquetReads.keys(part));
        }
        return out;
    }

    /** Mirror the CLI's resume reconciliation: discard non-finalized parts; carry finalized into the manifest. */
    private static List<PartInfo> reconcileResume(SqliteCheckpointStore store, long runId, Path dir)
            throws Exception {
        List<PartRef> finalized = store.finalizedParts(runId);
        Set<String> names = finalized.stream().map(PartRef::path).collect(Collectors.toSet());
        ParquetResume.discardNonFinalized(dir, names);
        return finalized.stream()
                .map(p -> new PartInfo(p.path(), p.writerId(), p.rows(), p.bytes(), ""))
                .toList();
    }

    /**
     * Blocks on {@code blockAtIdx} until {@code release} counts down, then THROWS instead
     * of returning a page — the same injected-crash shape as {@code ResumeParquetCadenceTest}
     * /{@code ResumeParquetTest}'s res4 cases, but only delivered after the caller has
     * observed the idle-timeout finalize, so the crash lands strictly inside the idle window.
     */
    private static MockPageFetcher blockThenCrash(List<byte[]> all, int blockAtIdx, CountDownLatch release) {
        return MockPageFetcher.builder().keys(all).maxKeysCap(PAGE_SIZE)
                .interceptor((req, idx, page) -> {
                    if (idx == blockAtIdx) {
                        release.await(20, TimeUnit.SECONDS);
                        throw new ListingException("injected crash during the idle-cadence window");
                    }
                    return page;
                }).build();
    }

    @RepeatedTest(10)
    void idleTimeoutFinalize_thenCrashInTheIdleWindow_resumeRelistsOnlyTheBoundedTail(@TempDir Path tmp)
            throws Exception {
        List<byte[]> all = keys(TOTAL_KEYS);
        List<String> clean = all.stream().map(b -> new String(b, StandardCharsets.UTF_8)).toList();
        Path dir = tmp.resolve("out");
        Files.createDirectories(dir);
        Path dbPath = tmp.resolve("c.sqlite");

        CountDownLatch release = new CountDownLatch(1);
        MockPageFetcher faulty = blockThenCrash(all, BLOCK_AT_PAGE_IDX, release);
        MockPageFetcher clean2 = MockPageFetcher.builder().keys(all).maxKeysCap(PAGE_SIZE).build();
        RunContext ctx = RunContext.create();
        // numWriters=1 (deterministic single ordered part stream); targetBytes huge and
        // rotationMaxRows=0 so the ONLY possible rotation trigger is the idle-timeout interval.
        ListRunner.ParquetSpec spec = new ListRunner.ParquetSpec(new byte[0], 1000, PAGE_SIZE, FilterChain.EMPTY,
                1, Long.MAX_VALUE, 8, "h1", null, null, 0L, 0L, "")
                        .withRotationIntervalNanos(ROTATION_INTERVAL.toNanos());

        AtomicReference<Throwable> bgFailure = new AtomicReference<>();
        long idleFinalizedRows;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dbPath)) {
            RunMeta run = store.openRun(runKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            Node node = store.loadResumable(run.id(), true).getFirst();

            Thread runner = new Thread(() -> {
                try {
                    new ListRunner().runToParquetCheckpointed(
                            ctx, faulty, dir, spec, store, run.id(), node, List.of());
                } catch (Throwable t) {
                    bgFailure.set(t);
                }
            }, "idle-cadence-crash-runner");
            runner.start();
            try {
                // The idle-timeout wake-up — not a row/size trigger — finalizes a part while
                // the run is blocked mid-fetch and has not been closed.
                await().atMost(Duration.ofSeconds(5))
                        .pollInterval(Duration.ofMillis(20))
                        .until(() -> !store.finalizedParts(run.id()).isEmpty());
                assertThat(runner.isAlive())
                        .as("still blocked in the idle window; the crash hasn't landed yet").isTrue();
            } finally {
                release.countDown();   // let the fetch throw now — the crash lands inside the idle window
                runner.join(Duration.ofSeconds(20).toMillis());
            }
            assertThat(runner.isAlive()).as("runner thread finished (crashed)").isFalse();
            assertThat(bgFailure.get())
                    .as("the run crashed as injected, inside the idle window").isInstanceOf(ListingException.class);

            // Only the idle-finalized part is durable; the crash added/corrupted nothing.
            List<PartRef> finalizedAfterCrash = store.finalizedParts(run.id());
            assertThat(finalizedAfterCrash)
                    .as("exactly the one idle-timeout-finalized part survives the crash").hasSize(1);
            idleFinalizedRows = finalizedAfterCrash.getFirst().rows();
            assertThat(idleFinalizedRows)
                    .as("the idle finalize captured only the rows fetched before the block, not the whole run")
                    .isGreaterThan(0).isLessThan(TOTAL_KEYS);

            Node before = store.loadResumable(run.id(), true).getFirst();
            assertThat(before.durableCursor())
                    .as("durable_cursor advanced via the idle-timeout path before the crash").isNotNull();

            List<PartInfo> existing = reconcileResume(store, run.id(), dir);
            assertThat(allPartKeys(dir))
                    .as("nothing but the idle-finalized prefix is durable on disk pre-resume")
                    .hasSize((int) idleFinalizedRows);

            Node resumed = store.loadResumable(run.id(), true).getFirst();
            new ListRunner().runToParquetCheckpointed(
                    ctx, clean2, dir, spec, store, run.id(), resumed, existing);
        }

        long tailRelisted = TOTAL_KEYS - idleFinalizedRows;
        assertThat(tailRelisted)
                .as("resume re-lists only the bounded tail after the idle-finalized prefix (%d rows durable), "
                        + "not the whole %d-row run", idleFinalizedRows, TOTAL_KEYS)
                .isLessThan(TOTAL_KEYS);

        List<String> union = allPartKeys(dir);
        assertThat(union).doesNotHaveDuplicates();
        assertThat(union).containsExactlyInAnyOrderElementsOf(clean);   // exactly-once (I6): nothing lost/duplicated
    }
}

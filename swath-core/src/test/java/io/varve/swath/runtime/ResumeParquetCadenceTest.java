/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.PartRef;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.ListingException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.output.parquet.ParquetResume;
import io.varve.swath.output.parquet.PartInfo;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.ParquetReads;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Guards rotation cadence at the resume level, distinct from the
 * mechanism unit tests in {@code ParquetRotationCadenceTest}: a
 * long steady run whose parts never hit the size trigger alone would leave {@code
 * durable_cursor} {@code NULL} for the run's <b>whole</b> duration — a
 * mid-run crash re-listing everything (the resume RPO gap; §0 I6). With {@code --part-rotation-max-rows} enabled, rotation is bounded
 * by row count even though every individual part stays far below the size
 * target, so {@code durable_cursor} advances mid-run and a crash re-lists
 * only a bounded tail.
 *
 * <p>Deliberately uses the deterministic <b>row-count</b> trigger only (no
 * wall-clock/time trigger) — see {@code ParquetRotationCadenceTest} for why
 * the time trigger needs an injected clock seam that isn't wired through
 * {@link ListRunner.ParquetSpec} at this layer.
 */
final class ResumeParquetCadenceTest {

    private static final int TOTAL_KEYS = 300;
    private static final int PAGE_SIZE = 5;
    /** Crash after this many successful page fetches (out of 60 total pages of 5). */
    private static final int KILL_AT_PAGE_IDX = 50;

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

    /**
     * numWriters 1 ⇒ a single ordered part stream (deterministic for the union check).
     * {@code targetBytes} is set to {@link Long#MAX_VALUE} so the size trigger never
     * fires — any rotation observed is exclusively due to {@code rotationMaxRows}.
     *
     * <p>{@code writerQueueCapacity} is deliberately tiny (2, not the usual 64):
     * {@link io.varve.swath.output.parquet.ParquetWriterPool#submit} blocks once a lane's
     * queue is full, so a small capacity makes the pipeline's consumer thread (which
     * synchronously drains every batch the producer emitted before it observes the
     * injected failure) backpressure tightly against the lane's writer thread. That
     * turns "how many rows got durably written before the abort" from a wall-clock
     * race (flaky, and biased toward "almost none") into something the lane thread
     * reliably keeps up with — a real property of the pool's own backpressure
     * design, not a test-only hack.
     */
    private static ListRunner.ParquetSpec cadenceSpec(long rotationMaxRows) {
        ListRunner.ParquetSpec base = new ListRunner.ParquetSpec(
                new byte[0], 1000, PAGE_SIZE, FilterChain.EMPTY, 1, Long.MAX_VALUE, 2,
                "h1", null, null, 0L, 0L, "");
        return rotationMaxRows == 0L ? base : base.withRotationMaxRows(rotationMaxRows);
    }

    private static List<String> allPartKeys(Path dir) throws IOException {
        List<String> keys = new ArrayList<>();
        for (Path part : DatasetLayout.of(dir).dataParts()) {
            keys.addAll(ParquetReads.keys(part));
        }
        return keys;
    }

    /** Mirror the CLI's resume reconciliation: discard non-finalized parts; retain finalized for publication. */
    private static List<PartInfo> reconcileResume(SqliteCheckpointStore store, long runId, Path dir)
            throws Exception {
        List<PartRef> finalized = store.finalizedParts(runId);
        Set<String> names = finalized.stream().map(PartRef::path).collect(Collectors.toSet());
        ParquetResume.discardNonFinalized(dir, names);
        return finalized.stream()
                .map(p -> new PartInfo(p.path(), p.writerId(), p.rows(), p.bytes(), ""))
                .toList();
    }

    /** Kill on the {@code KILL_AT_PAGE_IDX}-th fetch, same shape as the RES-4 fixtures. */
    private static MockPageFetcher killAtFixedPage(List<byte[]> all) {
        return MockPageFetcher.builder().keys(all).maxKeysCap(PAGE_SIZE)
                .interceptor((req, idx, page) -> {
                    if (idx == KILL_AT_PAGE_IDX) {
                        throw new ListingException("injected mid-run kill");
                    }
                    return page;
                }).build();
    }

    /**
     * The guard: with the row-count cadence enabled ({@code rotationMaxRows} far
     * smaller than the crash point), several parts finalize before the crash even
     * though {@code targetBytes} never trips ⇒ {@code durable_cursor} advances mid-run
     * and only a bounded tail — materially less than the full {@code TOTAL_KEYS} run —
     * is re-listed on resume. The final union is exactly the clean run (I6).
     */
    @RepeatedTest(10)
    void cadence_rowCountRotation_durableCursorAdvances_boundedTailRelisted(@TempDir Path tmp) throws Exception {
        List<byte[]> all = keys(TOTAL_KEYS);
        List<String> clean = all.stream().map(b -> new String(b, StandardCharsets.UTF_8)).toList();
        Path dir = tmp.resolve("out");
        Files.createDirectories(dir);

        MockPageFetcher faulty = killAtFixedPage(all);
        MockPageFetcher clean2 = MockPageFetcher.builder().keys(all).maxKeysCap(PAGE_SIZE).build();
        RunContext ctx = RunContext.create();
        ListRunner.ParquetSpec spec = cadenceSpec(30);

        int durableRowsBeforeResume;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(tmp.resolve("c.sqlite"))) {
            RunMeta run = store.openRun(runKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            Node node = store.loadResumable(run.id(), true).getFirst();

            assertThatThrownBy(() -> new ListRunner().runToParquetCheckpointed(
                    ctx, faulty, dir, spec, store, run.id(), node, List.of()))
                    .isInstanceOf(ListingException.class);

            // The cadence claim: rotation on row count alone (never on size) still
            // finalized part(s) before the crash ⇒ progress was made durable mid-run.
            List<PartRef> finalized = store.finalizedParts(run.id());
            assertThat(finalized).as("row-count cadence finalized part(s) before the kill").isNotEmpty();
            Node before = store.loadResumable(run.id(), true).getFirst();
            assertThat(before.durableCursor()).as("durable_cursor advanced mid-run").isNotNull();

            List<PartInfo> existing = reconcileResume(store, run.id(), dir);
            // Everything left on disk after discarding non-finalized parts is exactly
            // the durable rows — the bounded prefix the crash did NOT lose. The tiny
            // writerQueueCapacity (see cadenceSpec) makes this deterministic: the lane
            // reliably rotates on every 30-row boundary up to the last one completed
            // before the crash (8 × 30 = 240 of the 250 rows fetched by page 50).
            durableRowsBeforeResume = allPartKeys(dir).size();
            assertThat(durableRowsBeforeResume)
                    .as("a substantial majority of pre-crash progress was made durable, not just a token part")
                    .isGreaterThanOrEqualTo(KILL_AT_PAGE_IDX * PAGE_SIZE - 60);

            Node resumed = store.loadResumable(run.id(), true).getFirst();
            new ListRunner().runToParquetCheckpointed(
                    ctx, clean2, dir, spec, store, run.id(), resumed, existing);
        }

        int tailRelisted = TOTAL_KEYS - durableRowsBeforeResume;
        // The bounded-tail claim: the re-listed tail is a small remainder (the last
        // partial/open part plus whatever hadn't been fetched yet), NOT the whole run.
        assertThat(tailRelisted)
                .as("re-listed tail is bounded by the cadence, not the whole %d-row run", TOTAL_KEYS)
                .isLessThan(TOTAL_KEYS / 3);

        List<String> union = allPartKeys(dir);
        assertThat(union).doesNotHaveDuplicates();
        assertThat(union).containsExactlyInAnyOrderElementsOf(clean);   // exactly-once (I6): nothing lost/duplicated
    }

    /**
     * Guard-bites companion: the exact same scenario (same key count, same crash
     * point) with the cadence trigger DISABLED ({@code rotationMaxRows = 0}, huge
     * {@code targetBytes}). {@code durable_cursor} never
     * advances during the run, so a crash re-lists the ENTIRE run. This is what proves
     * {@code cadence_rowCountRotation_durableCursorAdvances_boundedTailRelisted} is a
     * real guard, not a vacuous pass: remove the cadence trigger and the
     * bounded-tail assertion above would fail exactly the way this test's "full
     * re-list" assertion below succeeds.
     */
    @Test
    void cadence_disabled_durableCursorStaysNull_wholeRunRelisted(@TempDir Path tmp) throws Exception {
        List<byte[]> all = keys(TOTAL_KEYS);
        List<String> clean = all.stream().map(b -> new String(b, StandardCharsets.UTF_8)).toList();
        Path dir = tmp.resolve("out");
        Files.createDirectories(dir);

        MockPageFetcher faulty = killAtFixedPage(all);
        MockPageFetcher clean2 = MockPageFetcher.builder().keys(all).maxKeysCap(PAGE_SIZE).build();
        RunContext ctx = RunContext.create();
        ListRunner.ParquetSpec spec = cadenceSpec(0);   // cadence OFF: size-only behavior

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(tmp.resolve("c.sqlite"))) {
            RunMeta run = store.openRun(runKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            Node node = store.loadResumable(run.id(), true).getFirst();

            assertThatThrownBy(() -> new ListRunner().runToParquetCheckpointed(
                    ctx, faulty, dir, spec, store, run.id(), node, List.of()))
                    .isInstanceOf(ListingException.class);

            // The cadence-disabled gap, reproduced: with no cadence and a size target never hit,
            // NOTHING finalized despite 50 successful pages (250 rows) of progress.
            assertThat(store.finalizedParts(run.id())).as("size-only ⇒ nothing rotated mid-run").isEmpty();
            Node before = store.loadResumable(run.id(), true).getFirst();
            assertThat(before.durableCursor()).as("durable_cursor never advanced").isNull();

            List<PartInfo> existing = reconcileResume(store, run.id(), dir);
            assertThat(allPartKeys(dir)).as("nothing durable survives the crash").isEmpty();

            Node resumed = store.loadResumable(run.id(), true).getFirst();
            new ListRunner().runToParquetCheckpointed(
                    ctx, clean2, dir, spec, store, run.id(), resumed, existing);
        }

        // The whole run was re-listed from scratch (the RPO gap row-count cadence fixes) — still
        // correct (exactly-once, I6), just with the worst possible re-work bound.
        List<String> union = allPartKeys(dir);
        assertThat(union).doesNotHaveDuplicates();
        assertThat(union).containsExactlyInAnyOrderElementsOf(clean);
    }
}

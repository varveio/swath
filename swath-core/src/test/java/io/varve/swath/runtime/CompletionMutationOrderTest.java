/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SoftRestoreContext;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.engine.RetryConfig;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.TraceSink;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortMode;
import io.varve.swath.store.PageFetcher;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.RecordingCheckpointStore;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Characterization test that pins the exact ordered chain of run-completion checkpoint-store
 * mutations each {@link ListRunner} entry point issues at the end of its lifecycle.
 *
 * <p>The rest of the runtime suite pins terminal <i>outcomes</i> (a run ends COMPLETED, a
 * sort run's durable phase ends PUBLISHED) but not the <i>sequence</i> of store calls that
 * reaches them — so a reordering of the completion chain that still lands on the same terminal
 * state would slip through. This test closes that gap ahead of the lifecycle-envelope
 * restructure: it drives the real runner paths end to end over the deterministic
 * {@link MockPageFetcher} and asserts the completion mutations in exact order and multiplicity.
 *
 * <p>The ordering it pins is load-bearing for I6 (durability &hArr; finalized, contracts.md
 * &sect;0): {@code markOutputComplete} is the I6 latch that advances {@code durable_cursor} for the
 * COMPLETED nodes, and it MUST precede {@code markRunFinished(COMPLETED)} — the run is recorded
 * finished only once its kept rows are durable. For the sort path the pinned chain also walks the
 * sort-phase state machine LISTING &rarr; MERGING &rarr; PUBLISHED (contracts.md &sect;6): the merge
 * runs between {@code MERGING} and {@code PUBLISHED}, and the run is recorded finished last.
 *
 * <p>It also pins the I6 <i>precondition</i> that a run's parts are all durable before the latch:
 * the LAST {@code partFinalized} MUST precede {@code markOutputComplete}, so {@code durable_cursor}
 * is never latched while a final part is still un-finalized. Without this a restructure that hoisted
 * {@code markOutputComplete} above {@code pool.close()}/{@code closeLane()} would keep the same
 * terminal chain yet break I6.
 *
 * <p><b>Two views (see {@link RecordingCheckpointStore}).</b> {@code partFinalized} fires once per
 * part/segment; the tail latch/phase/finish calls fire once. So the exact-chain assertions run
 * against {@link RecordingCheckpointStore#completionChain()} (the {@code partFinalized}-filtered view)
 * to stay robust to varying part counts, while the precondition assertions read the full
 * {@link RecordingCheckpointStore#completionMutations()} view. {@code commitPage}/{@code splitNode}
 * traffic is never recorded either way.
 */
final class CompletionMutationOrderTest {

    private static final String HASH = "completion-order-hash";
    private static final int MAX_KEYS = 16;

    private static RunKey parquetKey() {
        return new RunKey("s3", null, "bucket", new byte[0], HASH,
                "WORK_STEALING", ListingMode.OBJECTS, "", "parquet");
    }

    private static RunKey textKey() {
        return new RunKey("s3", null, "bucket", new byte[0], HASH,
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    private static RunKey sortKey() {
        return new RunKey("s3", null, "bucket", new byte[0], HASH,
                "WORK_STEALING", ListingMode.OBJECTS, "", "parquet",
                new SoftRestoreContext(false, null, null, false, false, "out", false, null, null), true);
    }

    private static ListRunner.ParquetSpec parquetSpec() {
        return new ListRunner.ParquetSpec(new byte[0], 256, MAX_KEYS, FilterChain.EMPTY,
                2, 256L * 1024, 16, HASH, null, null, 0L, 0L, "");
    }

    private static ListRunner.Spec textSpec() {
        return new ListRunner.Spec(new byte[0], OutputFormat.JSONL, true, 8000, 1000,
                FilterChain.EMPTY, null, null);
    }

    @Test
    @Timeout(30)
    void parquetWorkStealing_marksOutputCompleteThenRunFinished(@TempDir Path tmp) throws Exception {
        Path dir = Files.createDirectories(tmp.resolve("out"));
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(Keyspaces.exactly(1500)).build();

        try (SqliteCheckpointStore real = SqliteCheckpointStore.open(tmp.resolve("c.sqlite"))) {
            RunMeta run = real.openRun(parquetKey(), false, false);
            real.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = real.loadResumable(run.id(), true);

            RecordingCheckpointStore rec = new RecordingCheckpointStore(real);
            rec.reset();
            new ListRunner().runToParquetWorkStealing(
                    RunContext.create(), fetcher, dir, parquetSpec(), rec, run.id(), 4, seeds, List.of());

            assertThat(rec.completionChain())
                    .containsExactly("markOutputComplete", "markRunFinished");
            assertLastPartFinalizedPrecedesLatch(rec);
        }
    }

    @Test
    @Timeout(30)
    void parquetCheckpointed_marksOutputCompleteThenRunFinished(@TempDir Path tmp) throws Exception {
        Path dir = Files.createDirectories(tmp.resolve("out"));
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(Keyspaces.exactly(80)).maxKeysCap(MAX_KEYS).build();

        try (SqliteCheckpointStore real = SqliteCheckpointStore.open(tmp.resolve("c.sqlite"))) {
            RunMeta run = real.openRun(parquetKey(), false, false);
            real.insertNode(NodeSpec.rootRange(run.id()));
            Node node = real.loadResumable(run.id(), true).getFirst();

            RecordingCheckpointStore rec = new RecordingCheckpointStore(real);
            rec.reset();
            new ListRunner().runToParquetCheckpointed(
                    RunContext.create(), fetcher, dir, parquetSpec(), rec, run.id(), node, List.of());

            assertThat(rec.completionChain())
                    .containsExactly("markOutputComplete", "markRunFinished");
            assertLastPartFinalizedPrecedesLatch(rec);
        }
    }

    @Test
    @Timeout(60)
    void sortedWorkStealing_latchesThenWalksSortPhasesThenFinishes(@TempDir Path tmp) throws Exception {
        Path outputDir = Files.createDirectories(tmp.resolve("out"));
        Path stagingDir = Files.createDirectories(outputDir.resolve("_staging"));
        // Enough volume + long keys to force a genuine multi-pass merge (as SortCascadeContractTest).
        List<byte[]> keyspace = new ArrayList<>(Keyspaces.singlePrefixFlat(4000));
        keyspace.addAll(Keyspaces.longKeys1024(400));
        PageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).maxKeysCap(MAX_KEYS).build();
        SortConfig cascading = SortConfigs.base().withSegmentEntries(8).withFanIn(4);

        RunContext ctx = RunContext.create();
        try (SqliteCheckpointStore real = SqliteCheckpointStore.open(tmp.resolve("c.sqlite"), ctx.metrics())) {
            RunMeta run = real.openRun(sortKey(), false, false);
            real.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = real.loadResumable(run.id(), true);

            RecordingCheckpointStore rec = new RecordingCheckpointStore(real);
            rec.reset();
            new ListRunner().runToSortedParquetWorkStealing(ctx, fetcher, outputDir, stagingDir, parquetSpec(),
                    rec, run.id(), 4, seeds, cascading, SortMode.OBJECTS,
                    EngineToggles.DEFAULT, TraceSink.NONE, false);

            assertThat(rec.completionChain()).containsExactly(
                    "markOutputComplete", "setSortPhase:MERGING", "setSortPhase:PUBLISHED", "markRunFinished");
            assertLastPartFinalizedPrecedesLatch(rec);
        }
    }

    @Test
    @Timeout(30)
    void textWorkStealing_publishesOutputBeforeRecordingRunFinished(@TempDir Path tmp) throws Exception {
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(Keyspaces.exactly(1500)).build();
        // The text-sink publish is an atomic rename, not a store call. Model it with a marker file and
        // sample its existence at the instant markRunFinished is recorded (no wall-clock dependence).
        Path published = tmp.resolve("published.marker");

        try (SqliteCheckpointStore real = SqliteCheckpointStore.open(tmp.resolve("c.sqlite"))) {
            RunMeta run = real.openRun(textKey(), false, false);
            real.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = real.loadResumable(run.id(), false);

            RecordingCheckpointStore rec = new RecordingCheckpointStore(real, () -> Files.exists(published));
            rec.reset();
            OutputPublisher publisher = () -> Files.createFile(published);
            new ListRunner().runWorkStealing(RunContext.create(), fetcher, new StringWriter(), textSpec(),
                    rec, run.id(), 4, seeds, EngineToggles.DEFAULT, TraceSink.NONE,
                    RetryConfig.DEFAULT, publisher);

            // The text path's sole completion mutation is markRunFinished (no parts); publish is not a
            // store call, so the completion chain never contains markOutputComplete or partFinalized.
            assertThat(rec.completionChain()).containsExactly("markRunFinished");
            assertThat(rec.publishObservedAtRunFinished())
                    .as("output is published before the run is recorded finished")
                    .isTrue();
        }
    }

    /**
     * The I6 precondition: every part is durable before the latch. Pins that the run finalized at
     * least one part and that the LAST {@code partFinalized} strictly precedes {@code markOutputComplete}
     * (so {@code durable_cursor} is never latched while a final part is still un-finalized).
     */
    private static void assertLastPartFinalizedPrecedesLatch(RecordingCheckpointStore rec) {
        List<String> full = rec.completionMutations();
        int lastPartFinalized = full.lastIndexOf(RecordingCheckpointStore.PART_FINALIZED);
        int markOutputComplete = full.indexOf("markOutputComplete");
        assertThat(lastPartFinalized).as("the run finalized at least one part").isGreaterThanOrEqualTo(0);
        assertThat(markOutputComplete).as("markOutputComplete was recorded").isGreaterThanOrEqualTo(0);
        assertThat(lastPartFinalized)
                .as("the last partFinalized precedes markOutputComplete (I6: parts durable before the latch)")
                .isLessThan(markOutputComplete);
    }
}

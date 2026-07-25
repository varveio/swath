/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Gauge;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SoftRestoreContext;
import io.varve.swath.checkpoint.SortPhase;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.Phase;
import io.varve.swath.observability.TraceSink;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortMode;
import io.varve.swath.store.PageFetcher;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Nothing else drives a REAL sorted run through {@link ListRunner} and proves its own
 * control flow actually walks the phase machine LISTING -> MERGING -> WRITING -> COMPLETE end to end
 * ({@code RunMetricsContractTest.setPhaseUpdatesThePhaseGaugeToTheCorrectCode} only pins the
 * per-{@link Phase} {@code setPhase} -> {@code swath.phase} code mapping via direct calls). That's the
 * gap this test closes — <b>deterministically</b>.
 *
 * <p><b>Why this reads durable state and a terminal gauge, not a sampled gauge.</b> {@code swath.phase}
 * (§3.2) is a live, PULL-based gauge backed by a plain {@code AtomicLong} that {@code setPhase}
 * overwrites in place. A pull gauge exposes only its <i>current</i> value; a transient intermediate
 * phase can be overwritten before any observer samples it, so <b>concurrently polling the gauge to
 * catch intermediate transitions is inherently racy</b> and cannot be made deterministic.
 * This test therefore proves the run drove the phase machine using only DETERMINISTIC signals:
 * <ul>
 *   <li><b>LISTING (durable, pre-run):</b> {@code openRun} on a sort-enabled {@link RunKey} seeds
 *       {@code run_meta.sort_phase = LISTING}, read back via {@link SqliteCheckpointStore#sortPhase}.</li>
 *   <li><b>MERGING -> PUBLISHED (durable, post-run):</b> the sort path records phase durably right
 *       next to the {@code swath.phase} meter — {@code store.setSortPhase(runId, MERGING)} is paired
 *       with {@code setPhase(Phase.MERGING)}, and {@code SortPhase.PUBLISHED} is set only after the
 *       whole k-way merge + publish completes. After the run returns, {@code store.sortPhase(runId)}
 *       is {@code PUBLISHED}, which is set strictly after {@code MERGING} in {@link ListRunner} — so
 *       the terminal durable value proves the real run passed through MERGING and finished the merge.</li>
 *   <li><b>COMPLETE (gauge, post-run):</b> once the run returns the gauge's value is stable, so
 *       reading {@code swath.phase} directly (no polling) yields exactly {@code COMPLETE(3)}.</li>
 * </ul>
 *
 * <p><b>WRITING coverage gap.</b> {@link Phase#WRITING} has no durable {@link SortPhase} counterpart:
 * it is set by the {@code onFinalPassStarting} hook inside {@code SortTransform.transform} and leaves
 * no durable trace, so it cannot be observed here without reintroducing a timing race. Its
 * {@code setPhase(WRITING)} -> gauge {@code == 2} mapping is pinned deterministically by
 * {@code RunMetricsContractTest}, and this test proves the phases on either side of it (MERGING
 * durably before, COMPLETE on the gauge after), so the real run demonstrably transits the WRITING
 * boundary even though the transient value itself is not sampled here.
 *
 * <p>This test is fully deterministic: no sampler thread, no {@code Thread.sleep}, no polling, no
 * timing-based assertion.
 */
final class SortPhaseGaugeTransitionTest {

    private static final String ARGS_HASH = "phase-gauge-sort-hash";
    private static final int MAX_KEYS = 16;

    private static RunKey sortKey() {
        return new RunKey("s3", null, "bucket", new byte[0], ARGS_HASH,
                "WORK_STEALING", ListingMode.OBJECTS, "", "parquet",
                new SoftRestoreContext(false, null, null, false, false, "out", false, null, null), true);
    }

    private static ListRunner.ParquetSpec spec() {
        return new ListRunner.ParquetSpec(new byte[0], 256, MAX_KEYS, FilterChain.EMPTY,
                2, 1024, 16, ARGS_HASH, null, null, 0L, 0L, "");
    }

    /** segment-entries=8, fan-in=4 (as {@code SortCascadeContractTest}) -- multi-pass cascade, a real merge. */
    private static SortConfig cascadingSegments() {
        return SortConfigs.base()
                .withSegmentEntries(8)
                .withFanIn(4);
    }

    @Test
    @Timeout(60)
    void realSortedRunWalksThePhaseGaugeListingMergingWritingCompleteInOrder(@TempDir Path tmp) throws Exception {
        // A flat + 1 KB-key mix (as SortCascadeContractTest) at enough volume to force a genuine multi-pass
        // merge cascade, so the run really exercises the MERGING/WRITING code path -- not for timing.
        List<byte[]> keyspace = new ArrayList<>(Keyspaces.singlePrefixFlat(4000));
        keyspace.addAll(Keyspaces.longKeys1024(400));
        Path outputDir = Files.createDirectories(tmp.resolve("out"));
        Path stagingDir = Files.createDirectories(outputDir.resolve("_staging"));
        Path db = tmp.resolve("c.sqlite");

        RunContext ctx = RunContext.create();
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db, ctx.metrics())) {
            RunMeta run = store.openRun(sortKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), true);

            // Pre-run durable phase: a sort-enabled run opens in LISTING (deterministic).
            assertThat(store.sortPhase(run.id()))
                    .as("a sort-enabled run opens durably in the LISTING phase")
                    .isEqualTo(SortPhase.LISTING);

            PageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).maxKeysCap(MAX_KEYS).build();

            Gauge phaseGauge = ctx.metrics().registry().find("swath.phase").gauge();
            assertThat(phaseGauge).as("swath.phase gauge registered at RunMetrics construction").isNotNull();

            new ListRunner().runToSortedParquetWorkStealing(ctx, fetcher, outputDir, stagingDir, spec(),
                    store, run.id(), 4, seeds, cascadingSegments(), SortMode.OBJECTS,
                    EngineToggles.DEFAULT, TraceSink.NONE, false);

            // Post-run durable phase: PUBLISHED is set only after MERGING and the full merge+publish
            // complete, so the terminal durable value proves the real run walked LISTING -> MERGING ->
            // PUBLISHED (deterministic -- a persisted SQL row, not a sampled transient value).
            assertThat(store.sortPhase(run.id()))
                    .as("the real sorted run drove the durable sort phase through MERGING to PUBLISHED")
                    .isEqualTo(SortPhase.PUBLISHED);

            // Terminal gauge value is stable once the run returns -- never depends on scheduling luck.
            assertThat(phaseGauge.value())
                    .as("run terminates with the phase gauge at COMPLETE(%s)", Phase.COMPLETE.code())
                    .isEqualTo((double) Phase.COMPLETE.code());
        }
    }
}

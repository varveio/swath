/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageRequest;
import io.varve.swath.testkit.ApiCallBudget;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.EngineHarness;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import io.varve.swath.testkit.SeedSteps;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Demand-driven {@code delimiter=/} discovery during stealing. The shallow seed only
 * parallelizes a bucket that branches near the top; a seed range that is
 * <i>internally</i> a deep funnel (a long single-child chain that fans out only far below the
 * level the bounded seed reaches) reverts to byte-midpoint, which can only sliver at the cursor
 * over a deep cursor + far bound — baton-passing, near-serial. This proves the thief discovers the
 * hidden sub-directory structure on demand and lifts such a funnel from ~1–3 in-flight to near the
 * worker count, while staying exactly-once and bounded (INT-8), and NOT firing on flat/uniform.
 *
 * <p>These are ordinary unit guards of this mechanism; the cross-cutting
 * PROP-1/RES-3/CONC interleavings are covered separately.
 */
final class StealStructureProbeTest {

    /** A deep funnel: single-child chain {@code f/g/h/} then a broad fan-out the seed cannot reach. */
    private static List<byte[]> deepFunnel(int dirs, int keysPerDir) {
        List<byte[]> keys = new ArrayList<>(dirs * keysPerDir + 1);
        for (int d = 0; d < dirs; d++) {
            for (int k = 0; k < keysPerDir; k++) {
                keys.add(("f/g/h/%04d/obj-%03d".formatted(d, k)).getBytes(StandardCharsets.UTF_8));
            }
        }
        // A far high sentinel so the funnel range has a FINITE bound (byte-midpoint, not the open
        // frontier's extrapolate). 'z' ≫ 'f', so byte-midpoint lands in empty space above the
        // cluster — the empty-upper signal that triggers the structure probe.
        keys.add("zzz/end".getBytes(StandardCharsets.UTF_8));
        return keys;
    }

    private static RunKey key(String hash) {
        return new RunKey("s3", null, "bucket", new byte[0], hash,
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    // -------------------------------------------------------------------------
    // 1. The headline: a deep funnel reaches high parallelism (≫ the ~1–3 byte-midpoint gives).
    // -------------------------------------------------------------------------

    @Test
    @Timeout(120)
    void deepFunnelReachesHighParallelismExactlyOnce(@TempDir Path dir) throws Exception {
        int dirs = 1024;                 // > 1000 ⇒ the seed's delimiter=/ at f/g/h/ truncates and STOPS
        int keysPerDir = 4;
        List<byte[]> keyspace = deepFunnel(dirs, keysPerDir);
        int workers = 32;

        Run run = scan(dir, "funnel", keyspace, workers, 16, Duration.ofMillis(2));

        EngineHarness.assertExactlyOnce(run.emitted, keyspace);

        // The seed produced only a handful of ranges (the chain + the one dense range + sentinel):
        // it did NOT pre-parallelize the fan-out.
        assertThat(run.seedCount).as("seed leaves the funnel as one dense range").isLessThanOrEqualTo(6);

        // Demand-driven structure discovery fired during stealing...
        assertThat(run.structureProbes).as("the thief issued delimiter=/ structure probes")
                .isGreaterThan(0);

        // ...and lifted the funnel far past the ~1–3 in-flight a cursor-adjacent byte-midpoint
        // sliver sustains; a near-serial, baton-passing funnel fails this assertion.
        assertThat(run.peakInFlight)
                .as("deep funnel parallelizes via demand-driven structure discovery (peak=%d)", run.peakInFlight)
                .isGreaterThanOrEqualTo(8);
    }

    // -------------------------------------------------------------------------
    // 1b. Pivot-attribution: a winning empty-upper structure-probe split is tagged
    // PIVOT.structure_probe (distinct from the parent-empty sliver's PIVOT.adaptive_structure —
    // see ThiefParentEmptyPivotTest — so post-hoc analysis can tell the two durability mechanisms
    // apart instead of both landing in one merged "structure" tag).
    // -------------------------------------------------------------------------

    @Test
    @Timeout(120)
    void winningStructureProbeSplitIsTaggedDistinctlyFromAdaptiveBackout(@TempDir Path dir) throws Exception {
        int dirs = 1024;
        int keysPerDir = 4;
        List<byte[]> keyspace = deepFunnel(dirs, keysPerDir);

        Run run = scan(dir, "funnel-tag", keyspace, 32, 16, Duration.ofMillis(2));

        EngineHarness.assertExactlyOnce(run.emitted, keyspace);
        // 1024 directories is far more than one bounded structure probe returns, so every probe here
        // truncates and the winning pivot is the furthest boundary it could prove — tagged
        // PIVOT.structure_capped. Either plain-structure tag is the point: both are distinct from the
        // parent-empty sliver's PIVOT.adaptive_structure.
        long structure = run.stealReasons.getOrDefault("PIVOT.structure_capped", 0L)
                + run.stealReasons.getOrDefault("PIVOT.structure_probe", 0L);
        assertThat(structure)
                .as("the empty-upper funnel's winning structure probe carries a plain-structure PIVOT tag")
                .isPositive();
        assertThat(run.stealReasons.getOrDefault("PIVOT.structure_capped", 0L))
                .as("a 1024-directory funnel truncates every probe, so the capped regime is the one that wins")
                .isPositive();

    }

    // -------------------------------------------------------------------------
    // 2. INT-8 boundedness: the structure probes are NOT a per-directory walk.
    // -------------------------------------------------------------------------

    @Test
    @Timeout(120)
    void structureProbesAreBoundedNotPerDirectory(@TempDir Path dir) throws Exception {
        // The O(pages) shape bound must sit COMFORTABLY BELOW the directory count, otherwise a
        // one-structure-probe-per-directory regression would still fit under it and the test would
        // not actually prove "not per-directory". dirs=4096, keysPerDir=4, maxKeys=64 ⇒ pages=257,
        // shape_bound=4·257+4·32+64=1220 — comfortably < dirs (4096), a 3.4× margin, so a
        // per-directory-probe regression (~4096) CLEARLY exceeds the bound. (Contention margin
        // preserved: the coarse 2·dirs=8192 co-bound and the chained O(pages) shape bound below.)
        int dirs = 4096;
        int keysPerDir = 4;
        List<byte[]> keyspace = deepFunnel(dirs, keysPerDir);
        int maxKeys = 64;
        int workers = 32;

        Run run = scan(dir, "int8", keyspace, workers, maxKeys, Duration.ofMillis(1));

        EngineHarness.assertExactlyOnce(run.emitted, keyspace);

        long pages = (long) Math.ceil((double) keyspace.size() / maxKeys);
        long shapeBound = ApiCallBudget.int8Budget(pages, workers);   // ≈1220, comfortably < dirs (4096)
        assertThat(shapeBound).as("shape bound (%d) must sit below dirs (%d) to prove NOT-per-directory",
                shapeBound, dirs).isLessThan(dirs);

        // The probe is paced to ≤1 per non-productive steal attempt and progress-gating caps splits
        // at O(pages); it must stay well under O(directories) — the exact trap INT-8 guards. The
        // chained O(pages) shape bound is the real assertion; isLessThan(2*dirs) is the coarse "not
        // O(directories)" intuition, kept as a contention margin.
        // §3.
        assertThat(run.structureProbes)
                .as("structure probes stay O(pages)+slack, NOT O(directories=%d)", dirs)
                .isLessThan(2 * dirs)
                .isLessThanOrEqualTo((int) shapeBound);
        // Total LIST stays ≈ O(pages + structure), nowhere near per-directory (~O(N) = dirs*keys).
        // apiCalls includes the structure probes above, so the same load that pushes probes toward the
        // O(pages) shape max (~1220) + data pages (~257) also lifts this count; dirs*keysPerDir/2
        // keeps margin above that legitimate sum while staying well under the O(N) per-directory
        // regression it guards.
        // §3.
        assertThat(run.apiCalls)
                .as("total LIST ≈ O(pages+structure), not per-directory")
                .isLessThan((long) dirs * keysPerDir / 2);
    }

    // -------------------------------------------------------------------------
    // 3. Flat/uniform regression: a flat keyspace must NOT trigger structure probes.
    // -------------------------------------------------------------------------

    @Test
    @Timeout(120)
    void flatKeyspaceTriggersNoStructureProbes(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = Keyspaces.singlePrefixFlat(16_000);

        Run run = scan(dir, "flat", keyspace, 32, 1000, Duration.ZERO);

        EngineHarness.assertExactlyOnce(run.emitted, keyspace);
        // byte-midpoint (and the open-frontier extrapolate) split a flat run on the first try, so the
        // empty-upper signal essentially never fires. It CAN fire sporadically as a range drains (the
        // midpoint lands above the last few keys near a tail), so the guarantee is "no probe storm",
        // not exactly zero — a flat run (zero directories) must stay far below a per-directory walk.
        // 32 = the worker count: at most ~one tail-probe per worker; a regression to per-directory
        // probing would be hundreds+.
        // Contention margin: keep the "no probe storm" shape guard, loosen the coincidental
        // absolute bound. Under parallel CI load a few extra non-productive steal attempts add tail
        // probes; 96 (3× the worker count) still sits far below the hundreds+ a per-directory-probing
        // regression produces, so it guards the real shape while absorbing contention.
        // §3.
        assertThat(run.structureProbes).as("flat keyspace must not structure-probe-storm (no directories)")
                .isLessThanOrEqualTo(96);
    }

    // -------------------------------------------------------------------------

    private record Run(List<byte[]> emitted, int seedCount, long peakInFlight, int structureProbes, long apiCalls,
                       Map<String, Long> stealReasons) {
    }

    private static Run scan(Path baseDir, String name, List<byte[]> keyspace, int workers, int maxKeys,
                            Duration pageLatency) throws Exception {
        Path ckptDir = baseDir.resolve(name);
        Files.createDirectories(ckptDir);

        AtomicInteger structureProbes = new AtomicInteger();
        MockPageFetcher mock = MockPageFetcher.builder()
                .keys(keyspace)
                .latency(req -> req.maxKeys() > 1 && req.delimiter() == null ? pageLatency : Duration.ZERO)
                .interceptor((PageRequest req, int idx, ListPage page) -> {
                    // A steal-time structure probe = a delimiter listing WITH a start_after (the
                    // cursor). The seed's delimiter probes have start_after == null, so they are
                    // excluded — this counts only the demand-driven probes.
                    if (req.delimiter() != null && req.startAfter() != null) {
                        structureProbes.incrementAndGet();
                    }
                    return page;
                })
                .build();

        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<byte[]> emitted = new ArrayList<>(keyspace.size());
        int seedCount;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(ckptDir.resolve("ckpt.sqlite"))) {
            RunMeta run = store.openRun(key(name + "-hash"), false, false);
            List<NodeSpec> specs = SeedSteps.of(mock, new byte[0], workers).seedSpecs(run.id(), SeedMode.SHALLOW);
            store.insertNodes(specs);
            seedCount = specs.size();
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics),
                    mock, store, workers, maxKeys, seeds, FilterChain.EMPTY);

            PipelineDrain.collectKeys(5000, engine, emitted);
        }
        return new Run(emitted, seedCount, metrics.peakInFlight(), structureProbes.get(), mock.apiCalls(),
                metrics.diagnostics(Duration.ZERO).stealReasons());
    }

}

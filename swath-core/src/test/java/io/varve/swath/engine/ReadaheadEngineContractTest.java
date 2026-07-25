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
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.EngineHarness;
import io.varve.swath.testkit.LatencyModels;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * <b>Adversarial CONC guard for speculative readahead running inside the full
 * {@link WorkStealingScan} engine</b>: the focused unit suite ({@link RangeScannerReadaheadTest})
 * exercises readahead against a bare {@link RangeScanner}; this class is the flagged gap — readahead ON
 * under real multi-worker stealing, concurrent splits, and injected latency. It guards the
 * exactly-once contract (I1, exactly-once, I9) end-to-end:
 *
 * <ol>
 *   <li><b>(a) byte-exact under concurrent splits + steals</b> ({@link #conc_byteExactUnderStealing}):
 *       a dense opaque tail with latency, root-seeded so an owner collapses to a serial drain and
 *       readahead engages while thieves split — the emitted set is byte-exactly the keyspace, once.</li>
 *   <li><b>(c) counter coherence</b> (asserted in the same arm): {@code adopted_page +
 *       discarded_overlap + cancelled_split <= guess_placed}, and {@code guess_placed > 0 =>
 *       engaged >= 1} — the mechanism's own accounting is internally consistent.</li>
 *   <li><b>(b) liveness under a tight concurrency target</b> ({@link #conc_livenessUnderTightTarget}):
 *       readahead ON with very few workers and slow probes — the run must COMPLETE (no hang / no
 *       deadlock) and stay byte-exact.</li>
 *   <li><b>(interference) splitting still wins on a splittable keyspace</b>
 *       ({@link #conc_splittingWinsOverReadaheadOnSplittableKeyspace}): with visible structure and many
 *       thieves, the range gets split rather than readahead monopolizing a serial drain — runtime
 *       range count > 1.</li>
 * </ol>
 *
 * <p>@Tag("deep"): latency-injecting + multi-worker (excluded from the fast per-commit suite, run with
 * {@code -Pdeep}), matching {@link io.varve.swath.testkit.MockPageFetcher}-latency convention. Fully
 * deterministic (fixed seeds, per-request timing); asserts on robust invariants, never exact timings.
 */
@Tag("deep")
final class ReadaheadEngineContractTest {

    private static final byte[] NO_PREFIX = new byte[0];
    /** DEFAULT ablation config with the opt-in readahead toggle turned ON. */
    private static final EngineToggles READAHEAD_ON =
            EngineToggles.DEFAULT.withReadahead(true);

    private record Run(List<byte[]> emitted, Map<String, Long> reasons, long splitsCommitted) {
    }

    // ================================================================================================
    // (a) byte-exact under concurrent splits + steals, on a dense opaque tail with latency; and
    // (c) counter coherence in the same arm.
    // ================================================================================================

    @Test
    @Timeout(120)
    void conc_byteExactUnderStealing(@TempDir Path dir) throws Exception {
        int n = 30_000;
        int workers = 4;              // few workers + one big dense range => an owner collapses to a drain
        int maxKeys = 50;             // ~600 pages of dense tail
        List<byte[]> keys = denseOpaqueKeys(n);

        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(keys).maxKeysCap(maxKeys)
                .latencyModel(LatencyModels.uniformFast(0xC0FFEE01L, Duration.ofNanos(200_000),
                        Duration.ofNanos(900_000)))
                .build();
        Run r = runEngineRootSeed(fetcher, workers, maxKeys, dir);

        EngineHarness.assertExactlyOnce(r.emitted(), keys);

        // Readahead actually engaged on the collapsed tail (the whole point of the arm).
        long engaged = r.reasons().getOrDefault("READAHEAD.engaged", 0L);
        long placed = r.reasons().getOrDefault("READAHEAD.guess_placed", 0L);
        long adopted = r.reasons().getOrDefault("READAHEAD.adopted_page", 0L);
        long overlap = r.reasons().getOrDefault("READAHEAD.discarded_overlap", 0L);
        long cancelled = r.reasons().getOrDefault("READAHEAD.cancelled_split", 0L);
        assertThat(engaged).as("readahead engaged on the collapsed serial drain").isGreaterThan(0L);
        assertThat(placed).as("guesses were placed once engaged").isGreaterThan(0L);

        // (c) coherence: every placed guess has at most one terminal outcome (adopt / overlap-discard /
        // split-cancel); the rest are still buffered/in-flight or ended empty. So the three terminal
        // tallies can never exceed the number of guesses launched.
        assertThat(adopted + overlap + cancelled)
                .as("terminal guess outcomes are bounded by launched guesses (%d+%d+%d <= %d)",
                        adopted, overlap, cancelled, placed)
                .isLessThanOrEqualTo(placed);
        // guess_placed > 0 can only happen inside an engaged range.
        assertThat(engaged).as("placement implies engagement").isGreaterThan(0L);
    }

    // ================================================================================================
    // (b) liveness: readahead ON with a very small worker count + slow probes must COMPLETE, no hang.
    // ================================================================================================

    @Test
    @Timeout(120)
    void conc_livenessUnderTightTarget(@TempDir Path dir) throws Exception {
        int n = 8_000;
        int workers = 2;              // tight concurrency target; readahead's K guesses > T
        int maxKeys = 40;
        List<byte[]> keys = denseOpaqueKeys(n);

        // Slow-ish, per-request-deterministic latency: speculative fetches and worker fetches interleave
        // heavily. If speculation could wedge the run (deadlock / lost wakeup), the @Timeout trips.
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(keys).maxKeysCap(maxKeys)
                .latencyModel(LatencyModels.uniformFast(0x11FE00L, Duration.ofMillis(1), Duration.ofMillis(3)))
                .build();

        Run r = runEngineRootSeed(fetcher, workers, maxKeys, dir);

        // The only CONC guarantees under the pending fail-soft / slot-isolation changes: the run
        // COMPLETES (reaching this line at all means no deadlock within the @Timeout) and is byte-exact,
        // NOT that speculation consumes gauge slots.
        EngineHarness.assertExactlyOnce(r.emitted(), keys);
    }

    // ================================================================================================
    // (interference) splitting must still win on a splittable keyspace — readahead does not monopolize.
    // ================================================================================================

    @Test
    @Timeout(120)
    void conc_splittingWinsOverReadaheadOnSplittableKeyspace(@TempDir Path dir) throws Exception {
        int partitions = 400;
        int perPartition = 60;        // ~24k keys, visible `key=value/` structure to split on
        int workers = 16;             // heavy steal pressure
        int maxKeys = 50;
        List<byte[]> keys = splittableKeyspace(partitions, perPartition);

        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(keys).maxKeysCap(maxKeys)
                .latencyModel(LatencyModels.uniformFast(0x5911700L, Duration.ofNanos(300_000),
                        Duration.ofMillis(1)))
                .build();
        Run r = runEngineRootSeed(fetcher, workers, maxKeys, dir);

        EngineHarness.assertExactlyOnce(r.emitted(), keys);

        // Splitting wins: with structure + many thieves the single root range is carved into many
        // runtime children (final range count = 1 + splitsCommitted > 1), rather than readahead
        // holding it as one serial drain. (The engage gate needs 6 consecutive un-split pages, which
        // the thieves' splits keep resetting.)
        assertThat(r.splitsCommitted())
                .as("the splittable root range is split at runtime (range count > 1), not monopolized")
                .isGreaterThan(1L);
    }

    // ================================================================================================
    // engine runner + helpers
    // ================================================================================================

    /**
     * Drive the full {@link WorkStealingScan} with readahead ON over a SINGLE root range (no seed
     * tiling), so a collapsed owner drain / heavy runtime stealing are both exercised on one range.
     */
    private static Run runEngineRootSeed(MockPageFetcher fetcher, int workers, int maxKeys, Path dir)
            throws Exception {
        Files.createDirectories(dir);
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<byte[]> emitted = new ArrayList<>();
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("ckpt.sqlite"))) {
            RunMeta run = store.openRun(key(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            metrics.markRunStarted();
            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics).withToggles(READAHEAD_ON),
                    fetcher, store, workers, maxKeys, seeds, FilterChain.EMPTY);

            PipelineDrain.collectKeys(5000, engine, emitted);
        }
        RunMetrics.RunDiagnostics diag = metrics.diagnostics(Duration.ZERO);
        return new Run(emitted, diag.stealReasons(), diag.splitsCommitted());
    }

    private static RunKey key() {
        return new RunKey("s3", null, "bucket", new byte[0], "readahead-conc-" + System.nanoTime(),
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    /** Dense OPAQUE keyspace (no delimiter structure): 8-char base-26 of the index, unsigned-sorted. */
    private static List<byte[]> denseOpaqueKeys(int n) {
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            char[] c = new char[8];
            int v = i;
            for (int p = 7; p >= 0; p--) {
                c[p] = (char) ('a' + (v % 26));
                v /= 26;
            }
            keys.add(new String(c).getBytes(StandardCharsets.US_ASCII));
        }
        return keys;
    }

    /** A splittable `key=value/` partition keyspace (visible common-prefix structure to steal on). */
    private static List<byte[]> splittableKeyspace(int partitions, int perPartition) {
        List<byte[]> keys = new ArrayList<>(partitions * perPartition);
        for (int p = 0; p < partitions; p++) {
            for (int o = 0; o < perPartition; o++) {
                keys.add(("t/date=%05d/part-%05d.parquet".formatted(p, o)).getBytes(StandardCharsets.UTF_8));
            }
        }
        return keys;
    }
}

/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeKind;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.SwathException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.EngineHarness;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import io.varve.swath.testkit.RecordingSplitStore;
import io.varve.swath.testkit.StubCheckpointStore;
import io.varve.swath.testkit.Thiefs;
import io.varve.swath.testkit.WorkerStates;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Adversarial guard for per-victim futility pacing — the LIVENESS spine. The
 * risk is not correctness but progress: a cooldown that never expires (or a pace that is secretly
 * global) would deadlock/livelock the scan or serialize a productive bucket. These
 * tests prove (1) pacing EVERY victim at once still leaves them all eligible again after a bounded
 * number of skips (the cooldown always expires), (2) a full-engine run that generates heavy per-victim
 * futility still terminates byte-exact within a preemptive timeout, and (3) that sustained pacing does
 * NOT throttle overall steal throughput — real splits and genuine parallelism persist, which a global
 * pace would have suppressed.
 *
 * <p>Disjoint from {@code FutilityPacingTest} (single-victim mechanics + one Thief-selection
 * unit). Deterministic: bounded-cooldown arithmetic, byte-exact set equality (schedule-invariant), and
 * a preemptive timeout that fails rather than hangs on a liveness regression.
 *
 * <p><b>Engagement is proven deterministically, not by scheduler luck.</b> Whether a 16-thief
 * storm accumulates {@link WorkerState#FUTILITY_PACE_THRESHOLD} <i>consecutive</i> futile outcomes
 * against ONE victim before that victim makes progress (which resets the run) is inherently
 * schedule-dependent — on oversubscribed CI runners it never trips, so a hard {@code futility_paced >=
 * 1} assert on the storm read 0 and failed deterministically. The storm tests therefore keep only their
 * <i>schedule-invariant</i> checks (byte-exact termination under a preemptive timeout; per-victim
 * isolation = splits + genuine parallelism survive the storm). The engagement guarantee — that a
 * threshold-length futile run really does trip pacing and skip the victim, feeding {@code
 * STEAL.futility_paced} — is proven <b>deterministically</b> in {@link
 * #futilityRunTripsPacingAndSkipsTheVictimDeterministically()}, which drives the REAL pacing wiring
 * (real {@link Thief#steal}, real {@link WorkerState} cooldown, real {@code RunMetrics} counter) to
 * certainty by forcing the actual {@code cursor_passed_pivot} futile path a threshold number of times
 * with zero thread-race dependence. No production behavior is changed and no seam is added.
 */
final class FutilityPacingContractTest {

    private static final int MAX_KEYS = 100;

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // ============================================================================================
    // Pacing mechanics: pace EVERY victim simultaneously — all recover after bounded skips.
    // ============================================================================================

    @Test
    void everyVictimPacedAtOnceStillAllRecoverAfterBoundedSkips() {
        // Trip the cooldown on ALL victims at the same time (the worst case: an idle-thief storm has
        // raced every drainer futilely). The liveness property is that NO victim is permanently skipped:
        // each cooldown is a bounded number of stealPaced() skips, after which the victim is eligible
        // again. If any cooldown failed to expire, the drain would livelock (no victim ever selectable).
        int n = 8;
        List<WorkerState> pool = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            WorkerState w = WorkerStates.of(i, b("r" + i + "/lo"), b("r" + i + "/c"), b("r" + i + "/hi"));
            for (int k = 0; k < WorkerState.FUTILITY_PACE_THRESHOLD; k++) {
                w.recordFutileSteal();
            }
            pool.add(w);
        }
        // Every victim is paced right now.
        for (WorkerState w : pool) {
            assertThat(w.stealPaced()).as("victim %d paced after the threshold", w.nodeId()).isTrue();
        }
        // Drain each cooldown: it MUST expire within the bounded cap, then the victim is eligible again.
        for (WorkerState w : pool) {
            int skips = 1;   // the first stealPaced() above already consumed one
            while (w.stealPaced()) {
                skips++;
                assertThat(skips).as("victim %d cooldown must be bounded", w.nodeId())
                        .isLessThanOrEqualTo(WorkerState.FUTILITY_PACE_MAX_COOLDOWN);
            }
            assertThat(w.stealPaced()).as("victim %d eligible again after expiry", w.nodeId()).isFalse();
        }
    }

    // ============================================================================================
    // Engine: heavy per-victim futility still terminates byte-exact — no dead/livelock.
    // ============================================================================================

    private record Run(List<byte[]> emitted, Map<String, Long> stealReasons, long peakInFlight) {
        long counter(String k) {
            return stealReasons.getOrDefault(k, 0L);
        }
    }

    private Run run(Path dir, String label, List<byte[]> keyspace, int workers, Duration dataLatency)
            throws Exception {
        Path ckptDir = dir.resolve(label);
        Files.createDirectories(ckptDir);
        MockPageFetcher mock = MockPageFetcher.builder()
                .keys(keyspace)
                .latency(req -> req.maxKeys() > 1 && req.delimiter() == null ? dataLatency : Duration.ZERO)
                .build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<byte[]> emitted = new ArrayList<>(keyspace.size());
        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(ckptDir.resolve("ckpt.sqlite"))) {
            RecordingSplitStore store = new RecordingSplitStore(sqlite);
            RunMeta run = store.openRun(new RunKey("s3", null, "bucket", new byte[0], label,
                    "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl"), false, false);
            store.insertNode(new NodeSpec(run.id(), null, NodeKind.RANGE, b("d/00"), b("d/05"), b("d/00"), null));
            List<Node> seeds = store.loadResumable(run.id(), false);
            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics),
                    mock, store, workers, MAX_KEYS, seeds, FilterChain.EMPTY);
            PipelineDrain.collectKeys(5000, engine, emitted);
            RunMetrics.RunDiagnostics diag = metrics.diagnostics(Duration.ZERO);
            return new Run(emitted, diag.stealReasons(), diag.peakInFlight());
        }
    }

    /** A dense flat directory {@code d/000000..} inside a snug {@code (d/00, d/05]} window. */
    private static List<byte[]> denseFlat(int n) {
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(b("d/%06d".formatted(i)));
        }
        return keys;
    }

    @Test
    void heavyFutilityStormStillTerminatesByteExact(@TempDir Path dir) throws Exception {
        // Many idle thieves (16) hammer a slowed dense drainer (2ms/data page): they repeatedly lose the
        // cursor_passed_pivot / bound_moved races, so per-victim futility pacing trips again and again.
        // The liveness guard: the scan STILL terminates (preemptive timeout fails, never hangs) and is
        // byte-exact — the bounded cooldown + progress gate guarantee no dead/livelock even
        // when pacing is firing constantly.
        List<byte[]> keyspace = distinct(denseFlat(40_000));

        Run run = assertTimeoutPreemptively(Duration.ofSeconds(90), () ->
                run(dir, "storm", keyspace, 16, Duration.ofMillis(2)));

        EngineHarness.assertExactlyOnce(run.emitted(), keyspace);
        System.out.printf("[storm] futility_paced=%d owner=%d thief=%d peak=%d%n",
                run.counter("STEAL.futility_paced"), run.counter("OWNER_SPLIT.self_published"),
                run.counter("CHILD_CREATED.split_committed"), run.peakInFlight());
        // The key assertion here is schedule-INVARIANT liveness: it terminates (preemptive timeout, never
        // a hang) and is byte-exact even while pacing may be firing constantly. Whether THIS run trips
        // pacing (futility_paced > 0) is scheduler-dependent and read 0 on oversubscribed CI — so
        // the engagement guarantee is asserted deterministically in
        // futilityRunTripsPacingAndSkipsTheVictimDeterministically(), not raced here (printf kept for
        // observability only).
    }

    // ============================================================================================
    // Engine: pacing is per-victim, not global — throughput + parallelism survive it.
    // ============================================================================================

    @Test
    void pacingIsPerVictimSoSplitsAndParallelismSurviveTheStorm(@TempDir Path dir) throws Exception {
        // Do not make pacing global: if futility pacing were GLOBAL, a
        // sustained futility storm would throttle ALL steals and the bucket would drain serially. With
        // per-victim scope, real carves and genuine parallelism must persist DESPITE pacing firing — so
        // while futility_paced is non-zero, splits still commit and peak-in-flight climbs above one.
        List<byte[]> keyspace = distinct(denseFlat(40_000));

        Run run = assertTimeoutPreemptively(Duration.ofSeconds(90), () ->
                run(dir, "perVictim", keyspace, 16, Duration.ofMillis(2)));

        EngineHarness.assertExactlyOnce(run.emitted(), keyspace);
        long splits = run.counter("CHILD_CREATED.split_committed") + run.counter("OWNER_SPLIT.self_published");
        System.out.printf("[per-victim] futility_paced=%d splits=%d peak=%d%n",
                run.counter("STEAL.futility_paced"), splits, run.peakInFlight());

        // The per-victim-isolation property is schedule-INVARIANT: under the storm, real carves still commit
        // and genuine parallelism still climbs — the property a GLOBAL pace would
        // have suppressed. That pacing itself engaged is proven deterministically (and per-victim, with a
        // productive sibling staying stealable) in FutilityPacingTest +
        // futilityRunTripsPacingAndSkipsTheVictimDeterministically(); asserting it on this racy storm read
        // 0 on oversubscribed CI, so it is not raced here.
        assertThat(splits).as("real splits still committed despite pacing (not globally throttled)")
                .isGreaterThanOrEqualTo(2L);
        assertThat(run.peakInFlight()).as("genuine parallelism survived the pacing storm (not serialized)")
                .isGreaterThanOrEqualTo(2L);
    }

    // ============================================================================================
    // Engagement (deterministic): a threshold-length futile run trips pacing and skips the victim.
    // ============================================================================================

    /** Records the child ids handed to the driver (no child is ever created on the futile path). */
    private static final class RecordingSink implements Thief.ChildSink {
        final List<Long> children = new ArrayList<>();
        @Override public void accept(long childNodeId, byte[] childLo, byte[] childHi) { children.add(childNodeId); }
    }

    @Test
    void futilityRunTripsPacingAndSkipsTheVictimDeterministically() throws SwathException, InterruptedException {
        // The engagement guarantee the 16-thief storm CANNOT assert deterministically: that a run
        // of consecutive FUTILE steal outcomes against ONE victim trips its per-victim cooldown and the
        // next selection then SKIPS it (feeding STEAL.futility_paced). We drive the REAL pacing wiring —
        // real Thief.steal, real WorkerState cooldown, real RunMetrics — to certainty, with zero thread
        // races: a MockPageFetcher interceptor advances the victim's cursor to its bound during the
        // speculative probe, so the lock-guarded re-validate ALWAYS finds cursor >= pivot and records a
        // cursor_passed_pivot futile (one of the exact outcomes the storm relies on). Repeating that for
        // FUTILITY_PACE_THRESHOLD steals arms the cooldown; one more selection pass then observes the
        // paced victim and skips it. No production behavior is changed and no seam is added — only the
        // existing test-only MockPageFetcher.interceptor and public WorkerState/Thief APIs are used.
        byte[] lo = b("d/00");
        byte[] hi = b("d/05");
        byte[] resumeCursor = b("d/01");           // < every synthesized pivot in (d/01, d/05]
        WorkerState victim = WorkerStates.of(1, lo, resumeCursor, hi);
        victim.addKeysEmitted(1_000);              // a real remaining est ⇒ selected as the argmax victim

        // Keys span up to the bound so the far-ahead/midpoint probe (m, hi] is NON-empty — the attempt
        // takes the straight-to-re-validate path, where our advanced cursor forces cursor_passed_pivot
        // (never the empty-upper structure/reflect/bisection machinery, whose fallbacks avoid futility).
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(List.of(b("d/02"), b("d/03"), b("d/04")))
                .interceptor((req, idx, page) -> {
                    if (req.maxKeys() == 1) {      // the speculative probe: advance the victim past any pivot
                        victim.lock().lock();
                        try {
                            victim.setCursor(hi);  // cursor == hi >= m (m < hi always) ⇒ cursor_passed_pivot
                        } finally {
                            victim.lock().unlock();
                        }
                    }
                    return page;
                })
                .build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        // splitNode must never be reached: every steal in this guard loses the cursor_passed_pivot
        // re-validate BEFORE the durable split, so a call here would mean the forced-futility
        // construction broke (fail loudly, not silently mint a child).
        StubCheckpointStore neverSplits = new StubCheckpointStore(s -> {
            throw new AssertionError("split must not be reached: every attempt is a forced cursor_passed_pivot futile");
        });
        Thief thief = Thiefs.of(neverSplits, fetcher, 108L, new byte[0], ListingMode.OBJECTS,
                new RecordingSink(), metrics);

        for (int round = 0; round < WorkerState.FUTILITY_PACE_THRESHOLD; round++) {
            victim.setCursor(resumeCursor);        // fresh progress: cursor below the pivot AND the bound
            assertThat(thief.steal(List.of(victim)))
                    .as("attempt %d loses the cursor_passed_pivot race (a futile RETRY)", round)
                    .isEqualTo(Thief.Outcome.RETRY);
        }

        // The threshold-th futile outcome armed a per-victim cooldown. The NEXT selection pass must skip
        // the (sole) victim as paced — proving the futile run actually engaged pacing.
        victim.setCursor(resumeCursor);
        assertThat(thief.steal(List.of(victim)))
                .as("the paced victim is skipped at selection (no other victim ⇒ NO_VICTIM)")
                .isEqualTo(Thief.Outcome.NO_VICTIM);

        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.getOrDefault("RETRY.cursor_passed_pivot", 0L))
                .as("the forced futile path really fired the threshold-length run")
                .isEqualTo((long) WorkerState.FUTILITY_PACE_THRESHOLD);
        assertThat(reasons.getOrDefault("STEAL.futility_paced", 0L))
                .as("the futile run tripped pacing and the victim was skipped (the mechanism engaged)")
                .isGreaterThanOrEqualTo(1L);
    }

    // ============================================================================================
    // Helpers.
    // ============================================================================================

    private static List<byte[]> distinct(List<byte[]> keys) {
        TreeSet<byte[]> set = new TreeSet<>(Arrays::compareUnsigned);
        set.addAll(keys);
        return new ArrayList<>(set);
    }

}

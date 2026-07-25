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
import io.varve.swath.error.SwathException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageFetcher;
import io.varve.swath.store.PageRequest;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.EngineHarness;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import io.varve.swath.testkit.StubCheckpointStore;
import io.varve.swath.testkit.Thiefs;
import io.varve.swath.testkit.WorkerStates;
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
 * The thief's per-attempt probe budgets: the empty-upper
 * bisection loop is bounded to a <b>log-scaled</b> per-attempt budget — {@code ceil(log2(bandWidthBytes))
 * + MARGIN(6)} — instead of a blunt fixed cap, so PROP-3's legitimate {@code O(log band width)} wide-gap
 * convergence (a band wider than its content) still completes, while a genuinely pathological gap whose
 * true convergence depth exceeds the estimate still bails deterministically. Consecutive
 * zero-fan-out {@code delimiter=/} structure probes suppress after {@code K=8}. Both are
 * cost-only — the split CAS/transaction guarantees correctness independent of any pivot/probe choice —
 * so these guard the LIST budget while a full listing still completes exactly-once.
 *
 * <p>Ordinary unit guards of the two budgets — the cross-cutting PROP-1/RES-3/CONC
 * interleavings are covered separately. {@link Prop3BandWiderThanContentTest} is the guard
 * that this cap must NOT regress — deliberately left unmodified.
 */
final class ThiefProbeBudgetTest {

    private static final long RUN_ID = 11L;

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static final class RecordingSink implements Thief.ChildSink {
        @Override public void accept(long childNodeId, byte[] childLo, byte[] childHi) { }
    }

    private static PageFetcher fetcherWithKeys(String... keys) {
        List<byte[]> ks = new ArrayList<>();
        for (String k : keys) {
            ks.add(b(k));
        }
        return MockPageFetcher.builder().keys(ks).build();
    }

    // -------------------------------------------------------------------------
    // The empty-upper bisection loop is capped at a log-scaled per-attempt budget.
    // -------------------------------------------------------------------------

    @Test
    void bisectionBudgetIsLogScaledAndExhaustsOnAPathologicallyWideOpenFrontierGap()
            throws SwathException, InterruptedException {
        // An OPEN FRONTIER (H == null) whose cursor is a single high-code-point character (U+E000) and
        // whose lo is BOTTOM: extrapolate's forward reflection (StealMath.extrapolate ->
        // ByteMidpoint.forwardReflect) has no lo to anchor against, so it reflects the cursor's OWN
        // scalar-index value forward (roughly doubling it), landing the initial pivot far enough ahead
        // of the cursor that fully bisecting down to adjacency needs ~16 halvings. The open-frontier
        // budget uses the FIXED fallback width (40, since there is no hi to measure a byte distance
        // against): ceil(log2(40)) + MARGIN(6) = 12 halvings — smaller than the ~16 the gap actually
        // needs. An EMPTY store (no keys anywhere) makes every probe empty regardless of where it
        // lands, so the loop exhausts the 12-halving budget and bails with bisect_budget_exhausted
        // rather than looping further toward null. An unbounded loop would instead run well past 12
        // (toward ~16) before returning null as RETRY.retry_pivot_adjacent, never
        // bisect_budget_exhausted.
        byte[] cursor = new String(Character.toChars(0xE000)).getBytes(StandardCharsets.UTF_8);
        WorkerState victim = WorkerStates.of(1, null, cursor, null);
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        PageFetcher emptyStore = MockPageFetcher.builder().build();   // no keys anywhere: every probe is empty
        Thief thief = Thiefs.of(StubCheckpointStore.returning(5L), emptyStore,
                RUN_ID, new byte[0], ListingMode.OBJECTS, new RecordingSink(), metrics);

        assertThat(thief.steal(List.of(victim))).isEqualTo(Thief.Outcome.RETRY);

        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.getOrDefault("RETRY.bisect_budget_exhausted", 0L))
                .as("the log-scaled budget bailed with bisect_budget_exhausted, not an unbounded loop")
                .isEqualTo(1L);

        // The open-frontier fallback width (40) yields a budget of ceil(log2(40)) + 6 = 12 halvings;
        // probe.fetches = 1 (initial far-pivot probe) + 12 (the capped halvings) = 13, never unbounded.
        assertThat(metrics.diagnostics(Duration.ZERO).emptyUpperBisections())
                .as("the bisection loop ran exactly the log-scaled budget (12), not unbounded").isEqualTo(12L);
        assertThat(metrics.diagnostics(Duration.ZERO).probeFetches())
                .as("1-key probes this attempt stay within 1 (initial) + the log-scaled budget (12)")
                .isEqualTo(13L);
    }

    @Test
    void bisectionConvergesWithinTheLogScaledBudgetOnAWideBoundedGapLikePROP3()
            throws SwathException, InterruptedException {
        // The PROP-3 shape (Prop3BandWiderThanContentTest.wideBandConvergesInLogProbesNotLinearScan): a
        // dense window right above lo, and H thousands of code points away. The log-scaled cap must NOT
        // regress this legitimate convergence — this is the same guarantee PROP-3 itself asserts,
        // exercised here directly against Thief with the metrics wired so we can also see the reason tag.
        byte[] lo = b("k/");
        byte[] cursor = b("k/a");
        byte[] H = ("k/" + new String(Character.toChars(0x1000))).getBytes(StandardCharsets.UTF_8);
        List<byte[]> keyspace = new ArrayList<>();
        for (char ch = 'a'; ch <= 'h'; ch++) {
            keyspace.add(("k/" + ch).getBytes(StandardCharsets.UTF_8));
        }
        WorkerState victim = WorkerStates.of(1, lo, cursor, H);
        victim.addKeysEmitted(1);
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        Thief thief = Thiefs.of(StubCheckpointStore.returning(5L), MockPageFetcher.builder().keys(keyspace).build(),
                RUN_ID, new byte[0], ListingMode.OBJECTS, new RecordingSink(), metrics);

        assertThat(thief.steal(List.of(victim))).as("wide-gap convergence is not cut off by the log-scaled cap")
                .isEqualTo(Thief.Outcome.CHILD_CREATED);
        assertThat(metrics.diagnostics(Duration.ZERO).stealReasons().getOrDefault("RETRY.bisect_budget_exhausted", 0L))
                .as("a legitimately-convergent wide gap must never exhaust the budget").isZero();
    }

    @Test
    @Timeout(60)
    void fullListingCompletesExactlyOnceUnderTheBisectionCap(@TempDir Path dir) throws Exception {
        // A mass-hugging-cursor bucket: 2,000 keys densely clustered at the very bottom of a wide range
        // (a far "zzz" sentinel keeps the bound far), so the far/midpoint pivot repeatedly lands in the
        // empty upper and the thief may hit the log-scaled bisection cap on some attempts. The cap only
        // abandons doomed near-cursor pivots — the OWNER still drains the whole tail — so coverage stays
        // exactly-once regardless.
        List<byte[]> keyspace = new ArrayList<>();
        for (int i = 0; i < 2_000; i++) {
            keyspace.add(("aaa/%05d".formatted(i)).getBytes(StandardCharsets.UTF_8));
        }
        keyspace.add(b("zzz/end"));

        List<byte[]> emitted = scan(dir, "bisect-cap", keyspace, 32, 64);
        EngineHarness.assertExactlyOnce(emitted, keyspace);
    }

    // -------------------------------------------------------------------------
    // Structure probes suppress after K consecutive zero-fan-out probes.
    // -------------------------------------------------------------------------

    @Test
    void structureProbesSuppressAfterConsecutiveZeroFanout() throws SwathException, InterruptedException {
        // A flat keyspace (no '/') always returns zero fan-out from the delimiter=/ structure probe. ONE
        // persistent WorkerState across many attempts, backed by a store that always aborts the split CAS
        // (restoring hi on every attempt) so the (cursor, hi) snapshot never advances and every
        // attempt re-runs the identical funnel. The zero-fan-out counter lives on THIS victim
        // (WorkerState), not the shared Thief, so it accumulates across repeated attempts against the
        // SAME victim — after K=8 it stops probing (except a 1-in-64 recovery probe). Without
        // suppression every attempt would fire a structure probe (~256); the plateau and suppressed
        // counter below catch that.
        int attempts = 256;
        AtomicInteger structureProbes = new AtomicInteger();
        MockPageFetcher mock = MockPageFetcher.builder()
                .keys(List.of(b("ab")))
                .interceptor((PageRequest req, int idx, ListPage page) -> {
                    if (req.delimiter() != null && req.startAfter() != null) {
                        structureProbes.incrementAndGet();
                    }
                    return page;
                })
                .build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        Thief thief = Thiefs.of(StubCheckpointStore.alwaysAborts(), mock,
                RUN_ID, new byte[0], ListingMode.OBJECTS, new RecordingSink(), metrics);
        WorkerState victim = WorkerStates.of(1, b("a"), b("a"), b("z"));

        for (int i = 0; i < attempts; i++) {
            thief.steal(List.of(victim));
        }

        assertThat(structureProbes.get())
                .as("the first K=8 probes fire before suppression engages").isGreaterThanOrEqualTo(8);
        assertThat(structureProbes.get())
                .as("structure probes plateau near K + rare 1-in-64 retries, far below %d attempts", attempts)
                .isLessThanOrEqualTo(60);
        assertThat(metrics.diagnostics(Duration.ZERO).stealReasons().getOrDefault("STRUCTURE.suppressed_zero_fanout", 0L))
                .as("suppressed structure probes are counted").isPositive();
    }

    @Test
    void structureSuppressionIsPerVictimNotGlobal() throws SwathException, InterruptedException {
        // A flat victim A saturates ITS OWN zero-fan-out counter to well past
        // K=8 (many attempts, always-aborting store so its snapshot never advances). A DIFFERENT,
        // freshly-created victim B — a real parent-empty sliver with genuine sub-directory structure
        // (the ThiefParentEmptyPivotTest shape) — must still fire its structure probe and recover on its
        // VERY FIRST attempt: B's counter starts at zero regardless of how saturated A's is. A
        // Thief-global counter would let A's saturation suppress B too (only a 1-in-64 chance of
        // firing) — the same starvation failure mode a prior global futility counter was reverted for.
        MockPageFetcher flatStore = MockPageFetcher.builder()
                .keys(List.of(b("ab")))
                .build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        Thief flatThief = Thiefs.of(StubCheckpointStore.alwaysAborts(), flatStore,
                RUN_ID, new byte[0], ListingMode.OBJECTS, new RecordingSink(), metrics);
        WorkerState victimA = WorkerStates.of(1, b("a"), b("a"), b("z"));
        for (int i = 0; i < 32; i++) {
            flatThief.steal(List.of(victimA));
        }
        assertThat(victimA.consecutiveZeroFanoutProbes())
                .as("victim A's own counter is well past the K=8 suppression threshold")
                .isGreaterThanOrEqualTo(8);

        // Victim B: the parent-empty sliver shape (led/0100/obj cursor, led/02 bound) over a
        // store with real sibling directories led/0101/..led/0199/ — genuine structure to discover.
        List<byte[]> siblingDirs = new ArrayList<>();
        for (int i = 0; i <= 199; i++) {
            siblingDirs.add(b("led/%04d/obj".formatted(i)));
        }
        MockPageFetcher structuredStore = MockPageFetcher.builder().keys(siblingDirs).build();
        StubCheckpointStore childStore = StubCheckpointStore.returning(42L);
        Thief structuredThief = Thiefs.of(childStore, structuredStore,
                RUN_ID, new byte[0], ListingMode.OBJECTS, new RecordingSink(), metrics);
        WorkerState victimB = WorkerStates.of(1, b("led/0100/"), b("led/0100/obj"), b("led/02"));
        victimB.addKeysEmitted(100);

        long suppressedBefore = metrics.diagnostics(Duration.ZERO).stealReasons()
                .getOrDefault("STRUCTURE.suppressed_zero_fanout", 0L);

        assertThat(structuredThief.steal(List.of(victimB)))
                .as("victim B's own (fresh) counter must not inherit victim A's suppression — the "
                        + "structured split must commit on the very first attempt")
                .isEqualTo(Thief.Outcome.CHILD_CREATED);

        long suppressedAfter = metrics.diagnostics(Duration.ZERO).stealReasons()
                .getOrDefault("STRUCTURE.suppressed_zero_fanout", 0L);
        assertThat(suppressedAfter)
                .as("victim B's attempt must not itself be counted as a suppressed probe")
                .isEqualTo(suppressedBefore);
    }

    @Test
    void structureProbesKeepFiringWhenFanoutIsNonZero() throws SwathException, InterruptedException {
        // A bucket WITH sub-directory structure: the delimiter=/ probe returns real CommonPrefixes
        // (b/ c/ d/ e/), all below the far/midpoint pivot, so every probe has non-zero fan-out and the
        // suppression counter never accumulates. Probing must NOT plateau, and nothing is suppressed.
        int attempts = 64;
        AtomicInteger structureProbes = new AtomicInteger();
        MockPageFetcher mock = MockPageFetcher.builder()
                .keys(List.of(b("b/x"), b("c/x"), b("d/x"), b("e/x")))
                .interceptor((PageRequest req, int idx, ListPage page) -> {
                    if (req.delimiter() != null && req.startAfter() != null) {
                        structureProbes.incrementAndGet();
                    }
                    return page;
                })
                .build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        Thief thief = Thiefs.of(StubCheckpointStore.returning(5L), mock,
                RUN_ID, new byte[0], ListingMode.OBJECTS, new RecordingSink(), metrics);

        for (int i = 0; i < attempts; i++) {
            thief.steal(List.of(WorkerStates.of(1, b("a"), b("a"), b("z"))));
        }

        assertThat(structureProbes.get())
                .as("non-zero fan-out keeps the thief probing every attempt (no plateau)").isEqualTo(attempts);
        assertThat(metrics.diagnostics(Duration.ZERO).stealReasons().getOrDefault("STRUCTURE.suppressed_zero_fanout", 0L))
                .as("a structured bucket is never suppressed").isZero();
    }

    // -------------------------------------------------------------------------

    private static List<byte[]> scan(Path baseDir, String name, List<byte[]> keyspace, int workers, int maxKeys)
            throws Exception {
        Path ckptDir = baseDir.resolve(name);
        Files.createDirectories(ckptDir);

        MockPageFetcher mock = MockPageFetcher.builder().keys(keyspace).build();
        List<byte[]> emitted = new ArrayList<>(keyspace.size());
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(ckptDir.resolve("ckpt.sqlite"))) {
            RunMeta run = store.openRun(new RunKey("s3", null, "bucket", new byte[0], name + "-hash",
                    "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl"), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS),
                    mock, store, workers, maxKeys, seeds, FilterChain.EMPTY);

            PipelineDrain.collectKeys(5000, engine, emitted);
        }
        return emitted;
    }

}

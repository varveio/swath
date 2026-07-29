/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeKind;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.engine.policy.Carve;
import io.varve.swath.engine.policy.ConfettiObservation;
import io.varve.swath.engine.policy.OwnerSplitDecision;
import io.varve.swath.engine.policy.OwnerSplitGovernor;
import io.varve.swath.engine.policy.OwnerSplitSkipReason;
import io.varve.swath.engine.policy.OwnerSplitView;
import io.varve.swath.engine.policy.Skip;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.EngineHarness;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * <b>Independent adversarial guard for the carve brake</b> (commit 3b06e0e, campaign memo §5;
 * {@link CarveMassRing}, {@link ConfettiFeedbackGate}'s carve-brake probe pair, {@link
 * OwnerSplitGovernor}'s {@code CARVE_BRAKED}/{@code carve_brake_probe} branch, {@link
 * OwnerSelfSplit}'s executor-claim resolution). Authored separately from the brake itself
 * (AGENTS.md's adversarial-guard-author rule for high-risk engine changes): every test here tries
 * to prove the brake CAN wedge a scan and is kept in the suite only because it could not.
 *
 * <p>The four properties under guard, one section each below:
 * <ol>
 *   <li><b>A braked range still drains.</b> The brake refuses SPLITS only — page listing,
 *       stealing, and every other gate are untouched by any brake state.</li>
 *   <li><b>Suppression is bounded.</b> The probe escape (every {@code CARVE_BRAKE_PROBE_K}th
 *       would-be-braked consult) actually fires under adversarial concurrent interleavings of the
 *       executor's claim/consume protocol — no owner is starved of a probe win forever.</li>
 *   <li><b>A poisoned window recovers.</b> Once a healthy-mass child publishes, the ring's average
 *       rises and the brake disengages — stale low masses do not pin the window forever.</li>
 *   <li><b>Exact-once emission holds under the brake</b> in adversarial shapes: dense/uniform (the
 *       #78 shape) and a shape whose window average sits exactly AT the K threshold.</li>
 * </ol>
 */
final class CarveBrakeAdversarialGuardTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // =============================================================================================
    // PROPERTY 2 — bounded suppression under the executor-claim protocol (ConfettiFeedbackGate's
    // carve-brake probe pair: claimCarveBrakeProbeSlot / consumeCarveBrakeProbeSlot).
    // =============================================================================================

    /**
     * <b>Attack:</b> force N owners to snapshot the IDENTICAL pre-increment {@code
     * carveBrakeProbeSeq} (a {@link CyclicBarrier} makes every racer's read happen-before every
     * racer's claim call — a real race, not a hoped-for interleaving) and race {@link
     * ConfettiFeedbackGate#claimCarveBrakeProbeSlot} concurrently.
     *
     * <p><b>What a broken brake would show here:</b> a non-atomic claim (e.g. a plain
     * read-compare-then-set instead of {@code compareAndSet}, or a claim that forgot to advance the
     * sequence on loss) would let this test observe either ZERO winners (every racer loses — the
     * probe escape starves outright) or MORE THAN ONE winner (every racer sharing the snapshot
     * carves through, multiplying exactly the confetti-sized carves the brake exists to suppress —
     * the issue #31 race, unmirrored). The real implementation admits EXACTLY one winner per shared
     * snapshot and the sequence advances by exactly {@code racers} (winner + every loser each
     * contribute one increment) — the "no owner ever wins a probe for unboundedly long" guarantee
     * rests on this: the sequence never stalls, never skips, never double-counts.
     */
    @Test
    void carveBrakeClaimAdmitsExactlyOneWinnerUnderForcedConcurrentRace() throws Exception {
        int racers = 24;
        ConfettiFeedbackGate gate = new ConfettiFeedbackGate();
        long shared = gate.snapshot().carveBrakeProbeSeq();
        CyclicBarrier allReadsDone = new CyclicBarrier(racers);
        boolean[] won = new boolean[racers];
        Thread[] threads = new Thread[racers];
        for (int i = 0; i < racers; i++) {
            int idx = i;
            threads[i] = new Thread(() -> {
                await(allReadsDone);
                won[idx] = gate.claimCarveBrakeProbeSlot(shared);
            });
        }
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }

        int winners = 0;
        for (boolean w : won) {
            if (w) {
                winners++;
            }
        }
        assertThat(winners)
                .as("exactly one of %d racers sharing the same carveBrakeProbeSeq snapshot may win the "
                        + "probe slot -- more than one publishes duplicate probe carves, zero starves "
                        + "the escape outright", racers)
                .isEqualTo(1);
        assertThat(gate.snapshot().carveBrakeProbeSeq())
                .as("winner and every loser each still consume exactly one slot -- the sequence "
                        + "advances by racers regardless of who won, so the NEXT probe boundary is "
                        + "always a bounded number of consults away")
                .isEqualTo(shared + racers);
    }

    /**
     * <b>Attack:</b> a MIXED, sustained, high-contention race that exercises the real governor
     * protocol shape directly against the live gate — many threads in a tight loop each read the
     * current {@code carveBrakeProbeSeq}, decide PROBE vs BRAKE from the same {@code (seq + 1) %
     * CARVE_BRAKE_PROBE_K == 0} test {@code OwnerSplitGovernor} itself uses, and call the
     * corresponding claim (PROBE) or consume (BRAKE) — the exact mixed CLAIM/CONSUME contention the
     * real executor produces under many concurrent owners, not a single-method loop.
     *
     * <p><b>What a broken brake would show here:</b> a lost-update bug in either method (e.g. a
     * plain {@code set(get()+1)} in place of an atomic increment) would let concurrent racers
     * clobber each other's advances, so the final sequence would land BELOW {@code
     * threads * itersPerThread} (a conservation violation — proven, not merely likely, since every
     * call increments the counter by exactly one on inspection). A protocol that could let the probe
     * escape starve forever would show ZERO winners despite thousands of boundary crossings; the
     * real one always shows a strictly positive win count matching the bounded suppression the
     * property demands.
     */
    @Test
    void carveBrakeMixedClaimAndConsumeConserveEveryAdvanceUnderSustainedContention() throws Exception {
        // OwnerSplitGovernor.CARVE_BRAKE_PROBE_K is package-private to io.varve.swath.engine.policy
        // and not visible from here; 16 is its pinned literal value (OwnerSplitGovernorTest pins the
        // same number symbolically in its own package). A change to that constant would only widen or
        // narrow how often this loop's PROBE branch is taken, never invalidate the conservation
        // invariant this test actually checks.
        long probeK = 16L;
        int threadCount = 16;
        int itersPerThread = 8_000;
        ConfettiFeedbackGate gate = new ConfettiFeedbackGate();
        AtomicLong wins = new AtomicLong();
        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(() -> {
                for (int c = 0; c < itersPerThread; c++) {
                    long seq = gate.snapshot().carveBrakeProbeSeq();
                    if ((seq + 1) % probeK == 0) {
                        if (gate.claimCarveBrakeProbeSlot(seq)) {
                            wins.incrementAndGet();
                        }
                    } else {
                        gate.consumeCarveBrakeProbeSlot();
                    }
                }
            });
        }
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }

        long totalCalls = (long) threadCount * itersPerThread;
        assertThat(gate.snapshot().carveBrakeProbeSeq())
                .as("every one of %d concurrent calls (claim or consume) advances the sequence by "
                        + "exactly one -- no lost updates, no double-counts, under real thread "
                        + "contention on both branches at once", totalCalls)
                .isEqualTo(totalCalls);
        assertThat(wins.get())
                .as("the probe escape fired at least once under %d threads x %d iterations of "
                        + "sustained contention -- a permanently-starved escape would show zero wins "
                        + "here despite thousands of boundary crossings", threadCount, itersPerThread)
                .isGreaterThan(0);
    }

    private static void await(CyclicBarrier barrier) {
        try {
            barrier.await();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    // =============================================================================================
    // PROPERTY 4 (boundary half) — the REAL CarveMassRing + OwnerSplitGovernor together (not a
    // hand-fed Double), at a window average exactly AT and exactly one unit below the K threshold.
    // =============================================================================================

    private static final int MAX_KEYS = 100;

    /** A cold digest and a view that clears every gate above the brake, mirroring OwnerSplitGovernorTest. */
    private static OwnerSplitView brakeViewWithRealGate(ConfettiFeedbackGate gate) {
        byte[] lo = b("a");
        byte[] hi = b("z");
        byte[] cursorTo = b("n");
        ConfettiFeedbackGate.Snapshot snap = gate.snapshot();
        return new OwnerSplitView(hi, lo, cursorTo, 100_000L, OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN, 0L, 0,
                0.5, 1.0, new WorkerState(0, lo, lo, hi).alphabetDigest().snapshot(),
                new ConfettiObservation(snap.taggedTotal(), snap.taggedConfetti(), snap.probeSeq(),
                        snap.windowAverageMass(), snap.carveBrakeProbeSeq()));
    }

    /**
     * <b>Attack:</b> feed the REAL {@link ConfettiFeedbackGate} (hence the real {@link
     * CarveMassRing#windowAverage()} floating-point division) exactly 8 completions of mass 200
     * each ({@code mass_k2}'s threshold is {@code 2 * MAX_KEYS = 200}), landing the window average
     * EXACTLY at the K boundary — the shape #78's dense/uniform corpus fixture and the "sits exactly
     * at the threshold" adversarial case both risk (a window average that lands on the boundary by
     * genuine arithmetic, not by construction of a test double).
     *
     * <p><b>What a broken brake would show here:</b> the contract is "== threshold ADMITS" ({@code
     * massAvg < threshold} is the ONLY suppress condition — strict). A boundary bug (e.g. {@code <=}
     * in place of {@code <}, or an off-by-one in {@link CarveMassRing}'s divisor) would suppress
     * this exact-at-threshold carve; this test drives the real division (1600L / 8 as a double) so a
     * floating rounding surprise in the real component — not a hand-set {@code Double} — would also
     * surface here.
     */
    @Test
    void realGateWindowAverageExactlyAtThresholdAdmitsTheCarve() {
        ConfettiFeedbackGate gate = new ConfettiFeedbackGate();
        for (int i = 0; i < CarveMassRing.SIZE; i++) {
            gate.recordCompletion(false, 200L);
        }
        assertThat(gate.snapshot().windowAverageMass())
                .as("the real ring's arithmetic actually lands exactly on mass_k2's threshold")
                .isEqualTo(200.0);

        OwnerSplitGovernor governor =
                new OwnerSplitGovernor(EngineToggles.DEFAULT.withCarveBrake(CarveBrakeMode.MASS_K2),
                        4, MAX_KEYS, null);
        OwnerSplitDecision decision = governor.decide(brakeViewWithRealGate(gate));

        assertThat(decision)
                .as("window average == K * maxKeys admits the carve (strict '<' suppresses, not '<=')")
                .isInstanceOf(Carve.class);
    }

    /**
     * The same real-gate setup, one mass unit below the boundary (average {@code 199.875 < 200}):
     * the sibling of the test above, pinning that the real division's ordinary "just under" case
     * still suppresses, so the boundary test above is proven against a REAL gate that can also
     * suppress, not one wired to always admit.
     */
    @Test
    void realGateWindowAverageJustBelowThresholdBrakesTheCarve() {
        ConfettiFeedbackGate gate = new ConfettiFeedbackGate();
        for (int i = 0; i < CarveMassRing.SIZE - 1; i++) {
            gate.recordCompletion(false, 200L);
        }
        gate.recordCompletion(false, 199L);   // sum 1599, average 199.875 < 200
        assertThat(gate.snapshot().windowAverageMass()).isEqualTo(199.875);

        OwnerSplitGovernor governor =
                new OwnerSplitGovernor(EngineToggles.DEFAULT.withCarveBrake(CarveBrakeMode.MASS_K2),
                        4, MAX_KEYS, null);
        OwnerSplitDecision decision = governor.decide(brakeViewWithRealGate(gate));

        assertThat(decision).isInstanceOf(Skip.class);
        assertThat(((Skip) decision).reason()).isEqualTo(OwnerSplitSkipReason.CARVE_BRAKED);
    }

    // =============================================================================================
    // PROPERTIES 1, 3, 4 (dense/uniform half) — the real engine, single-worker where determinism is
    // required (mirrors ConfettiFeedbackContractTest's own w=1 discipline), multi-worker where the
    // attack IS the concurrency.
    // =============================================================================================

    private static RunKey key(String label) {
        return new RunKey("s3", null, "bucket", new byte[0], label,
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    private record Run(List<byte[]> emitted, Map<String, Long> reasons) {
        long r(String k) {
            return reasons.getOrDefault(k, 0L);
        }
    }

    private Run run(Path dir, String label, List<byte[]> keyspace, byte[] lo, byte[] hi, int workers,
                    EngineToggles toggles) throws Exception {
        MockPageFetcher mock = MockPageFetcher.builder().keys(keyspace).build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<byte[]> emitted = new ArrayList<>(keyspace.size());
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve(label + ".sqlite"))) {
            RunMeta run = store.openRun(key(label), false, false);
            store.insertNode(new NodeSpec(run.id(), null, NodeKind.RANGE, lo, hi, lo, null));
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics).withToggles(toggles),
                    mock, store, workers, MAX_KEYS, seeds, FilterChain.EMPTY);

            PipelineDrain.collectKeys(4000, engine, emitted);
        }
        return new Run(emitted, metrics.diagnostics(Duration.ZERO).stealReasons());
    }

    /**
     * A dense flat cluster ({@code 000/00000000..}) packed at the low end of a wide-open window,
     * followed by NOTHING until a second, much larger dense cluster far above it — the same
     * confetti-head shape {@code ConfettiFeedbackContractTest} uses to force tiny-mass, never-split
     * owner-split children (far-ahead pivots land in the empty middle), immediately followed by a
     * second cluster large enough that, once carving resumes there, its owner-split children realize
     * substantial mass. Engineered specifically to POISON the mass ring first, then let it recover.
     */
    private static List<byte[]> poisonThenHeal(int poisonKeys, int healKeys) {
        List<byte[]> keys = new ArrayList<>(poisonKeys + healKeys);
        for (int i = 0; i < poisonKeys; i++) {
            keys.add(b(String.format("000/%08d", i)));
        }
        for (int i = 0; i < healKeys; i++) {
            keys.add(b(String.format("g00/%08d", i)));
        }
        return keys;
    }

    private static final byte[] LO = b("000/");
    private static final byte[] HI = b("zzz");

    /**
     * <b>Attack (properties 1 + 3):</b> {@code carve_brake=mass_k8} (the most aggressive shipped
     * threshold) over a keyspace built to poison the ring first (a confetti head whose owner-split
     * children realize near-zero mass) and then hand it a much larger healthy region. If the brake
     * could pin a poisoned window forever, EVERY subsequent owner-split consult on the healthy
     * region would also brake except the rare 1-in-16 probe escape — so almost all successful carves
     * would be probes, and {@code self_published} would stay small relative to {@code carve_braked}.
     *
     * <p><b>What a broken brake would show here:</b> a ring that never evicts stale low masses (or a
     * window-average bug that never crosses back above the threshold) would pin the brake engaged
     * for the REST of the run once poisoned. Under a permanent brake every successful carve could
     * only ever be the rare 1-in-{@code CARVE_BRAKE_PROBE_K} probe escape, so {@code self_published}
     * would be capped near {@code carve_braked / 16} and every realized child would eventually be
     * classified confetti again (never a majority {@code substantial}). A range that stopped
     * draining entirely under a permanently-engaged brake would fail {@link
     * EngineHarness#assertExactlyOnce} outright (a hang would also trip this test's {@link Timeout})
     * — property 1's "still drains" and property 3's "recovers" are both falsifiable by the same run.
     */
    @Test
    @Timeout(60)
    void poisonedWindowRecoversAndTheRangeKeepsDrainingWhileBraked(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = poisonThenHeal(60_000, 400_000);

        Run on = run(dir, "poison-then-heal", keyspace, LO, HI, 1,
                EngineToggles.DEFAULT.withCarveBrake(CarveBrakeMode.MASS_K8));

        // Property 1: the range still drains byte-exact, whatever the brake did along the way.
        EngineHarness.assertExactlyOnce(on.emitted(), keyspace);

        long braked = on.r("OWNER_SPLIT.carve_braked");
        long published = on.r("OWNER_SPLIT.self_published");
        long confetti = on.r("OWNER_SPLIT_CHILD.confetti");
        long substantial = on.r("OWNER_SPLIT_CHILD.substantial");

        assertThat(braked).as("the confetti head actually poisoned the window (the brake engaged)").isPositive();
        // Property 3: recovery, not permanent starvation. A permanently-poisoned window could only
        // ever publish via the rare probe escape (measured, observed 32 here), which upper-bounds
        // published at roughly braked/16 in a stuck regime (observed ceiling ~30 on this run).
        // Recovery instead lets ORDINARY (non-probe) carving resume on the healthy region once the
        // window average climbs back above the threshold, so published clears that probe-only
        // ceiling by a wide margin (observed 269, ~9x the ceiling).
        long probeOnlyCeiling = braked / 16;
        assertThat(published)
                .as("recovery: successful carves (%d) clear the probe-only ceiling (braked/16 = %d) by "
                        + "a wide margin -- a permanently-poisoned window could not exceed it", published,
                        probeOnlyCeiling)
                .isGreaterThan(4 * probeOnlyCeiling);
        assertThat(substantial)
                .as("healthy-mass children were realized once the window recovered, and are the "
                        + "MAJORITY of every classified child across the whole run (confetti=%d, "
                        + "substantial=%d) -- a pinned window would keep confetti dominant", confetti,
                        substantial)
                .isGreaterThan(confetti);
        assertThat(confetti + substantial)
                .as("every self-published owner-split child is still classified exactly once with the "
                        + "brake engaged")
                .isEqualTo(published);
    }

    /**
     * <b>Attack (property 4, dense/uniform #78 shape):</b> the SAME dense/uniform 20k shape the
     * author's own {@code ConfettiFeedbackWiringTest} uses, but driven repeatedly under a genuinely
     * adversarial worker count (8 concurrent owners racing the same probe-slot claim protocol) — not
     * a single retried attempt, EVERY attempt in this loop must hold. Independently authored: this
     * does not reuse that test's retry-until-engaged idiom, it demands the invariant holds on every
     * single one of {@link #REPEATS} repeated schedules, engaged or not.
     *
     * <p><b>What a broken brake would show here:</b> a race in the executor's claim resolution
     * (issue #31 unmirrored for the brake's own sequence) would occasionally let two workers publish
     * from the same probe slot, or drop a key, or hang under concurrent claim contention across 8
     * workers — any of which fails {@link EngineHarness#assertExactlyOnce} or the method
     * {@link Timeout} on SOME iteration, even if most iterations look fine.
     */
    @Test
    @Timeout(120)
    void denseUniformShapeCompletesExactOnceUnderConcurrentBrakeContentionEveryAttempt(@TempDir Path dir)
            throws Exception {
        int repeats = REPEATS;
        List<byte[]> keyspace = denseFlat(20_000);
        for (int attempt = 0; attempt < repeats; attempt++) {
            Run run = run(dir, "dense-uniform-concurrent-" + attempt, keyspace, b("d/00"), b("d/02"), 8,
                    EngineToggles.DEFAULT.withCarveBrake(CarveBrakeMode.MASS_K8));

            EngineHarness.assertExactlyOnce(run.emitted(), keyspace);

            long published = run.r("OWNER_SPLIT.self_published");
            long confetti = run.r("OWNER_SPLIT_CHILD.confetti");
            long substantial = run.r("OWNER_SPLIT_CHILD.substantial");
            assertThat(confetti + substantial)
                    .as("attempt %d: every self-published owner-split child classified exactly once "
                            + "under 8-worker brake contention", attempt)
                    .isEqualTo(published);
        }
    }

    private static final int REPEATS = 10;

    private static List<byte[]> denseFlat(int n) {
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(b(String.format("d/%06d", i)));
        }
        return keys;
    }
}

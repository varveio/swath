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
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Adversarial regression guard for the two confetti mechanisms wired into
 * {@link OwnerSelfSplit#maybeOwnerSelfSplit}: the realized-mass feedback gate
 * ({@link ConfettiFeedbackGate}, toggle {@code confetti_feedback}) and the
 * REFLECT-LIFT (toggle {@code reflect_lift}, counter {@code OWNER_SPLIT.pivot_reflect_lifted}).
 *
 * <p><b>The pathology, in its two field shapes.</b> The observed-mass child-tail floor
 * and the demand gate both reason from UPSTREAM estimates ({@code est}/{@code densityRatio}). On a
 * skewed keyspace — a dense same-prefix cluster whose code-point tail is almost empty — those
 * estimates are structurally blind: {@code densityRatio ≈ 1} (the EWMA is fed only in-cluster keys)
 * and {@link StealMath#estRemaining} is inflated (in-cluster density extrapolated across the
 * mostly-empty code-point tail), so the floor passes carve after carve. The realized damage comes in
 * two geometries:
 * <ul>
 *   <li><b>Terminal confetti</b> (the rarer shape, cases 1-3 here): the carved children realize
 *       {@code <= 2*maxKeys} keys and never split further — ~unbounded confetti fission. Fixed by
 *       the feedback gate: each tagged child's REALIZED mass folds into a run-level confetti rate
 *       (ground truth); past {@code MIN_SAMPLE} samples over {@code SUPPRESS_THRESHOLD}, carves are
 *       suppressed, except every {@code PROBE_K}th as a probe so a densifying keyspace recovers.</li>
 *   <li><b>Recursive relay chain</b> (the DOMINANT field shape; case 5 here): on a range whose
 *       divergent byte has ADJACENT scalars, {@link ByteMidpoint}'s no-room fallback ignores the
 *       fraction and returns pivot {@code m = cursor⧺0x20} — the owner keeps an empty sliver, pays
 *       exactly ONE zero-key proof-fetch, and the child inherits everything and repeats. Fixed by the
 *       reflect-lift: when the owner's kept share is sub-one-page (measured in est's own
 *       {@code [ws.lo(), H]} frame), the pivot is lifted UP to {@link StealMath#extrapolate} so the
 *       owner keeps ~a page of real mass — the carve still happens (never suppressed), only the
 *       zero-page waste is removed.</li>
 * </ul>
 *
 * <p><b>Why this bites.</b> Every fixture was tuned until the pathology actually manifests
 * under ablation: the dense-head fixture publishes {@code 131} owner-splits with ZERO suppression
 * once BOTH mechanisms are off (case&nbsp;2 — each mechanism independently fixes this shape, so the
 * true baseline needs both ablated), and the relay fixture wastes ~1 zero page per carve with
 * {@code reflect_lift=off} (data-page ratio ≈ 1.8-2.0x ideal vs ≈ 1.04x with the lift). Every case
 * drives the REAL {@link WorkStealingScan} engine over an in-memory {@link MockPageFetcher} (no
 * mocks of internals) and asserts byte-exact completeness alongside the counter behavior, disjoint
 * from {@code ConfettiFeedbackGateTest} (pure bookkeeping) and {@code ConfettiFeedbackWiringTest}
 * (classification wiring smoke).
 *
 * <p><b>Determinism.</b> Every case runs single-worker ({@code w=1}): such runs are bit-stable
 * (verified identical across repeated runs, including under load), whereas multi-worker schedules
 * were measured to admit runs where a mechanism under test simply does not engage (thieves bound the
 * relay chain first; the lift never fires) — which no assertion slack can absorb. Cross-arm claims
 * are additionally asserted in PAIRED relative form (both arms on the same machine), and byte-exact
 * set equality is schedule-invariant regardless.
 */
final class ConfettiFeedbackContractTest {

    private static final int MAX_KEYS = 100;

    /**
     * The half-open seed range {@code (lo, hi]} for the dense-head fixtures. {@code lo} sits snugly
     * just below the dense head's first key (so the estRemaining window is not spread over empty low
     * space and the in-cluster density estimate does not collapse — same discipline as
     * {@code OwnerSelfSplitContractTest}), and {@code hi} is a far, unpopulated code-point ceiling:
     * EVERY fixture key starts with an ASCII digit ({@code 0x30..0x39}), so an owner-split pivot
     * synthesized at far-ahead fraction {@code f} toward {@code 'z'} lands in the empty
     * {@code (0x39, 0x7a]} tail — the confetti carve the gate must learn to suppress.
     */
    private static final byte[] LO = b("000/");
    private static final byte[] HI = b("zzz");

    /**
     * Ceiling on {@code OWNER_SPLIT.self_published} for the dense-head fixture whenever at least one
     * of the two confetti mechanisms is active (observed, deterministic at w=1: {@code 14} at default
     * toggles, {@code 71} with only the feedback gate, {@code 29} with only the lift), and the value
     * the both-off ablation control must clear by a wide margin (observed {@code 131}) to prove the
     * ablation reproduces the pathology.
     */
    private static final long GATED_PUBLISHED_CEILING = 100;

    /** {@code confetti_feedback=off} AND {@code reflect_lift=off}: the true both-off baseline. */
    private static final EngineToggles BOTH_CONFETTI_MECHANISMS_OFF =
            EngineToggles.DEFAULT.withConfettiFeedback(false).withReflectLift(false);
    /** {@code confetti_feedback=off}, {@code reflect_lift=on}: isolates the lift's independent effect. */
    private static final EngineToggles LIFT_ONLY =
            EngineToggles.DEFAULT.withConfettiFeedback(false);
    /** {@code reflect_lift=off}, {@code confetti_feedback=on}: the relay case's in-test control arm. */
    private static final EngineToggles LIFT_OFF =
            EngineToggles.DEFAULT.withReflectLift(false);

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static RunKey key(String label) {
        return new RunKey("s3", null, "bucket", new byte[0], label,
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    /**
     * One engine run's observable outcome: the emitted keys, the metrics steal-reason counters, and
     * the number of DATA pages fetched ({@code maxKeys > 1, delimiter == null} — excludes the thief's
     * 1-key speculative probes and {@code delimiter=/} structure probes), so the page-efficiency
     * ratio measures exactly the waste the relay-chain pathology causes: zero-key proof-fetches.
     */
    private record Run(List<byte[]> emitted, Map<String, Long> reasons, long dataPages) {
        long r(String k) {
            return reasons.getOrDefault(k, 0L);
        }
    }

    /**
     * A dense flat head {@code 000/00000000..} of {@code headKeys} uniform keys packed into a tiny
     * byte span at the LOW end of {@code (LO, HI]}, with the entire rest of the window empty. The
     * owner drains the head (in-cluster density high ⇒ inflated est, densityRatio ≈ 1) and every
     * far-ahead pivot it synthesizes lands in the empty tail ⇒ a realized-empty child that never
     * splits ⇒ confetti. This is the diagnosed failure geometry (children realize ≤ 2*maxKeys and
     * never split), reproduced through the real engine.
     */
    private static List<byte[]> denseHeadEmptyTail(int headKeys) {
        List<byte[]> keys = new ArrayList<>(headKeys);
        for (int i = 0; i < headKeys; i++) {
            keys.add(b(String.format("000/%08d", i)));
        }
        return keys;
    }

    /**
     * The probe-escape fixture: the {@link #denseHeadEmptyTail} confetti head PLUS a second dense
     * uniform region at first-byte {@code 'g'} — exactly where the far-ahead pivots land. Once the
     * gate has engaged (suppressing on the confetti head), a {@code PROBE_K}th probe carve that lands
     * in the {@code g00/..} region hits REAL mass ⇒ a substantial child ⇒ the observed rate falls and
     * owner-split recovers, instead of the gate starving itself permanently.
     */
    private static List<byte[]> confettiHeadThenDenseRegion(int headKeys, int denseKeys) {
        List<byte[]> keys = new ArrayList<>(headKeys + denseKeys);
        for (int i = 0; i < headKeys; i++) {
            keys.add(b(String.format("000/%08d", i)));
        }
        for (int i = 0; i < denseKeys; i++) {
            keys.add(b(String.format("g00/%08d", i)));
        }
        return keys;
    }

    /** {@link #run(Path, String, List, byte[], byte[], int, EngineToggles)} seeded with {@code (LO, HI]}. */
    private Run run(Path dir, String label, List<byte[]> keyspace, int workers, EngineToggles toggles)
            throws Exception {
        return run(dir, label, keyspace, LO, HI, workers, toggles);
    }

    /**
     * Drive the full {@link WorkStealingScan} over {@code keyspace} from a single {@code (lo, hi]}
     * seed with the given toggles, collecting the emitted keys, the run's steal-reason counters, and
     * the data-page fetch count. {@code workers == 1} keeps the recorded counters deterministic —
     * every case in this class runs single-worker for exactly that reason (verified bit-identical
     * across repeated runs, including under load; multi-worker schedules were measured to admit runs
     * where a mechanism under test simply does not engage, which no assertion slack can absorb).
     */
    private Run run(Path dir, String label, List<byte[]> keyspace, byte[] lo, byte[] hi, int workers,
                    EngineToggles toggles) throws Exception {
        AtomicLong dataPages = new AtomicLong();
        MockPageFetcher mock = MockPageFetcher.builder()
                .keys(keyspace)
                .interceptor((req, idx, page) -> {
                    if (req.maxKeys() > 1 && req.delimiter() == null) {
                        dataPages.incrementAndGet();
                    }
                    return page;
                })
                .build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<byte[]> emitted = new ArrayList<>(keyspace.size());
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve(label + ".sqlite"))) {
            RunMeta run = store.openRun(key(label), false, false);
            // A fresh bounded seed resumes from its lower bound (cursor == lo), exactly like a split child.
            store.insertNode(new NodeSpec(run.id(), null, NodeKind.RANGE, lo, hi, lo, null));
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics).withToggles(toggles),
                    mock, store, workers, MAX_KEYS, seeds, FilterChain.EMPTY);

            PipelineDrain.collectKeys(4000, engine, emitted);
        }
        return new Run(emitted, metrics.diagnostics(Duration.ZERO).stealReasons(), dataPages.get());
    }

    // -------------------------------------------------------------------------
    // (1) THE KEY CASE: on the skewed keyspace the gate suppresses confetti carving, self_published is
    //     bounded well below the un-gated carve opportunities, and completion is byte-exact.
    // -------------------------------------------------------------------------

    @Test
    @Timeout(60)
    void confettiCarvingIsSuppressedAndBoundedWhileStayingByteExact(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = denseHeadEmptyTail(60_000);

        Run on = run(dir, "gate-on", keyspace, 1, EngineToggles.DEFAULT);

        // Correctness is non-negotiable regardless of how aggressively the gate suppresses.
        EngineHarness.assertExactlyOnce(on.emitted(), keyspace);

        long published = on.r("OWNER_SPLIT.self_published");
        long suppressed = on.r("OWNER_SPLIT.confetti_suppressed");
        long confetti = on.r("OWNER_SPLIT_CHILD.confetti");
        long substantial = on.r("OWNER_SPLIT_CHILD.substantial");

        // The gate engaged: it suppressed real confetti carves (observed, deterministic: 75;
        // ≥ MIN_SAMPLE is a robust floor well above zero).
        assertThat(suppressed)
                .as("the confetti feedback gate suppressed carves on the skewed keyspace")
                .isGreaterThanOrEqualTo(ConfettiFeedbackGate.MIN_SAMPLE);

        // ...and owner-split publication is held well below the un-gated carve opportunities
        // (case 2's both-off ablation publishes 131; here, observed 14 — bounded to ≤ 100).
        assertThat(published)
                .as("gated self_published (%d) stays well below the un-gated carve opportunities", published)
                .isLessThanOrEqualTo(GATED_PUBLISHED_CEILING);

        // Every published owner-split child is classified exactly once (the tagging/completion wiring
        // is intact under real suppression, not just on the wiring smoke's dense-uniform shape).
        assertThat(confetti + substantial)
                .as("every self-published owner-split child classified exactly once")
                .isEqualTo(published);
        // This skewed shape genuinely produces confetti children (the pathology is present, not absent).
        assertThat(confetti).as("confetti children were realized on the skewed keyspace").isPositive();
    }

    // -------------------------------------------------------------------------
    // (2) ABLATION CONTROL: with BOTH confetti mechanisms off (confetti_feedback AND reflect_lift —
    //     each independently fixes this shape, so the true both-off baseline needs both ablated) the
    //     SAME keyspace publishes many more owner-splits with zero suppression and zero lifts —
    //     proving the case-1 reduction is the mechanisms' doing, not the fixture's.
    // -------------------------------------------------------------------------

    @Test
    @Timeout(60)
    void withBothMechanismsOffTheSameKeyspacePublishesFarMoreCarvesWithNoSuppression(@TempDir Path dir)
            throws Exception {
        List<byte[]> keyspace = denseHeadEmptyTail(60_000);

        // PAIRED arms on the same machine (platform-robust): the both-off ablation vs the shipped
        // defaults, over the identical fixture.
        Run off = run(dir, "gate-off", keyspace, 1, BOTH_CONFETTI_MECHANISMS_OFF);
        Run on = run(dir, "gate-off-ctrl", keyspace, 1, EngineToggles.DEFAULT);

        EngineHarness.assertExactlyOnce(off.emitted(), keyspace);
        EngineHarness.assertExactlyOnce(on.emitted(), keyspace);

        // Toggles off ⇒ both mechanisms are bit-for-bit bypassed: no suppression, no probes, no
        // tagging, no lifts — and the once-per-scan ablation marks fire so post-analysis never has
        // to infer the ablation from absence.
        assertThat(off.r("OWNER_SPLIT.confetti_suppressed")).as("no suppression with the gate off").isZero();
        assertThat(off.r("OWNER_SPLIT.confetti_probe")).as("no probes with the gate off").isZero();
        assertThat(off.r("OWNER_SPLIT_CHILD.confetti")).as("no tagging with the gate off").isZero();
        assertThat(off.r("OWNER_SPLIT_CHILD.substantial")).as("no tagging with the gate off").isZero();
        assertThat(off.r("OWNER_SPLIT.pivot_reflect_lifted")).as("no lifts with the lift off").isZero();
        assertThat(off.r("TOGGLE.confetti_feedback_off")).as("the gate ablation mark fired once").isEqualTo(1L);
        assertThat(off.r("TOGGLE.reflect_lift_off")).as("the lift ablation mark fired once").isEqualTo(1L);

        long offPub = off.r("OWNER_SPLIT.self_published");
        long onPub = on.r("OWNER_SPLIT.self_published");
        // The both-off arm publishes many more owner-splits. PRIMARY (relative, paired — immune
        // to platform scheduling since both arms share the machine): the ablated arm out-carves the
        // default arm by at least 4x (observed, deterministic at w=1: 131 vs 14 ≈ 9.4x).
        assertThat(offPub)
                .as("both-off self_published (%d) exceeds the default arm (%d) by a wide relative margin",
                        offPub, onPub)
                .isGreaterThanOrEqualTo(4L * onPub);
        // Loose absolute sanity: the ablated count also clears the case-1 ceiling outright.
        assertThat(offPub)
                .as("both-off self_published exceeds the gated ceiling (%d)", GATED_PUBLISHED_CEILING)
                .isGreaterThan(GATED_PUBLISHED_CEILING);
    }

    // -------------------------------------------------------------------------
    // (2b) MECHANISM INDEPENDENCE: reflect_lift ALONE (confetti_feedback=off) also collapses the
    //      carve count on this shape — WITHOUT any suppression counters. This pins that the two
    //      mechanisms fix the same pathology independently (and is exactly why case 2's control must
    //      ablate both to reproduce the both-off baseline).
    // -------------------------------------------------------------------------

    @Test
    @Timeout(60)
    void reflectLiftAloneCollapsesConfettiCarvingWithoutSuppression(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = denseHeadEmptyTail(60_000);

        Run liftOnly = run(dir, "lift-only", keyspace, 1, LIFT_ONLY);

        EngineHarness.assertExactlyOnce(liftOnly.emitted(), keyspace);

        // The lift engaged (observed, deterministic: 21 lifts)...
        assertThat(liftOnly.r("OWNER_SPLIT.pivot_reflect_lifted"))
                .as("the reflect-lift engaged on the skewed keyspace").isPositive();
        // ...and by itself holds publication under the ceiling (observed 29 vs the both-off 131) —
        // lifted owners keep ~a page of real mass and re-engage the 32-page self-split progress gate.
        assertThat(liftOnly.r("OWNER_SPLIT.self_published"))
                .as("lift-only self_published collapses well below the both-off baseline")
                .isLessThanOrEqualTo(GATED_PUBLISHED_CEILING);
        // The feedback gate is off: zero suppression/probe/tagging — the collapse is the lift's alone.
        assertThat(liftOnly.r("OWNER_SPLIT.confetti_suppressed")).as("gate off: no suppression").isZero();
        assertThat(liftOnly.r("OWNER_SPLIT.confetti_probe")).as("gate off: no probes").isZero();
        assertThat(liftOnly.r("OWNER_SPLIT_CHILD.confetti")).as("gate off: no tagging").isZero();
        assertThat(liftOnly.r("OWNER_SPLIT_CHILD.substantial")).as("gate off: no tagging").isZero();
    }

    // -------------------------------------------------------------------------
    // (3) PROBE ESCAPE / RECOVERY: a keyspace that starts confetti-shaped then turns dense drives a
    //     PROBE carve; suppression does not permanently starve owner-split — substantial carving resumes.
    // -------------------------------------------------------------------------

    @Test
    @Timeout(60)
    void probeEscapeLetsOwnerSplitRecoverOnADensifyingKeyspace(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = confettiHeadThenDenseRegion(60_000, 60_000);

        Run on = run(dir, "probe", keyspace, 1, EngineToggles.DEFAULT);

        EngineHarness.assertExactlyOnce(on.emitted(), keyspace);

        long suppressed = on.r("OWNER_SPLIT.confetti_suppressed");
        long probe = on.r("OWNER_SPLIT.confetti_probe");
        long confetti = on.r("OWNER_SPLIT_CHILD.confetti");
        long substantial = on.r("OWNER_SPLIT_CHILD.substantial");

        // The gate engaged (confetti head) AND a probe escaped the suppression (observed,
        // deterministic: supp=60, probe=4).
        assertThat(suppressed).as("the gate engaged on the confetti head").isPositive();
        assertThat(probe).as("a PROBE_K-th carve escaped suppression as a probe").isPositive();

        // Recovery: owner-split was NOT permanently starved — carving continued past the gate's
        // engagement and the dense region realized SUBSTANTIAL children (observed: pub=15, conf=7,
        // subst=8 — asserted loosely: substantial completions exist at all, which is impossible if
        // suppression latched permanently on this all-confetti-head start).
        assertThat(on.r("OWNER_SPLIT.self_published"))
                .as("owner-split kept publishing (not permanently starved)").isPositive();
        assertThat(substantial)
                .as("substantial owner-split children were realized once the keyspace densified "
                        + "(confetti=%d, substantial=%d) — the gate recovered", confetti, substantial)
                .isPositive();
    }

    // -------------------------------------------------------------------------
    // (4) RECURSIVE RELAY CHAIN (the dominant field pathology) + REFLECT-LIFT. Adjacent scalars at
    //     the divergent byte of the seed range degenerate every interpolated pivot to cursor⧺0x20
    //     (ByteMidpoint's no-room fallback ignores the fraction): with reflect_lift off, the owner
    //     keeps an empty sliver and pays exactly ONE zero-key proof-fetch per carve while the child
    //     inherits everything and repeats. The lift moves the pivot UP to the density-reflected
    //     extrapolation so the owner keeps ~a page of real mass — the waste disappears WITHOUT
    //     suppressing carves.
    // -------------------------------------------------------------------------

    /**
     * The relay-chain fixture: {@code relayKeys} dense flat keys {@code a/1-00000..} under ONE narrow
     * prefix, scanned as seed range {@code ("a/1", "a/2"]}. The divergent byte of the range bounds is
     * {@code '1'} vs {@code '2'} — ADJACENT scalars — so {@code safeBetween} finds nothing strictly
     * between and every owner-split pivot degenerates to {@code cursor⧺0x20} regardless of the
     * far-ahead fraction: the relay-chain geometry, reproduced through the real engine.
     */
    private static List<byte[]> relayChain(int relayKeys) {
        List<byte[]> keys = new ArrayList<>(relayKeys);
        for (int i = 0; i < relayKeys; i++) {
            keys.add(b(String.format("a/1-%05d", i)));
        }
        return keys;
    }

    @Test
    @Timeout(60)
    void reflectLiftRemovesTheRelayChainZeroPageWasteWithoutSuppressingCarves(@TempDir Path dir)
            throws Exception {
        List<byte[]> keyspace = relayChain(5_000);
        byte[] lo = b("a/1");
        byte[] hi = b("a/2");
        long idealPages = (keyspace.size() + MAX_KEYS - 1) / MAX_KEYS;   // 50

        // Single worker on BOTH arms: the relay pathology is owner-side pivot geometry (no thief
        // involved), and w=1 pins every asserted quantity bit-stable (measured identical across 10
        // reps under load: ON pub=2/lift=1/pages=52, OFF pub=48/pages=98). Multi-worker schedules
        // were measured to admit runs where the mechanisms simply do not engage — w=2 showed an OFF
        // arm where thieves bound the chain before it degenerated (pub=5, ratio 1.10) and ON arms
        // where the lift never fired at all — which no assertion slack can absorb.
        Run on = run(dir, "relay-on", keyspace, lo, hi, 1, EngineToggles.DEFAULT);
        Run off = run(dir, "relay-off", keyspace, lo, hi, 1, LIFT_OFF);

        // (c) byte-exact coverage in both arms, whatever the pivot placement did.
        EngineHarness.assertExactlyOnce(on.emitted(), keyspace);
        EngineHarness.assertExactlyOnce(off.emitted(), keyspace);

        // (a) the lift engaged on the degenerate-pivot geometry (observed: exactly 1 — the first
        // carve is lifted, the lifted owner then re-engages the 32-page self-split progress gate).
        assertThat(on.r("OWNER_SPLIT.pivot_reflect_lifted"))
                .as("the reflect-lift fired on the adjacent-scalar relay geometry").isPositive();

        // (d) the chain lives: carves were NOT suppressed, the waste was fixed by lifting, not gating
        // (observed ON: self_published=2; and the confetti gate stayed out of it in both arms).
        assertThat(on.r("OWNER_SPLIT.self_published"))
                .as("owner-split still carves with the lift (fixed the waste WITHOUT suppression)")
                .isPositive();
        assertThat(on.r("OWNER_SPLIT.confetti_suppressed"))
                .as("the relay fix is the lift's, not the feedback gate's").isZero();

        // (b) API efficiency — PRIMARY assertion is RELATIVE (paired arms on the same machine, so it
        // is immune to platform scheduling): the un-lifted control fetches at least 1.3x the lifted
        // arm's data pages (observed, deterministic: 98 vs 52 = 1.885x). Each relay carve costs ~one
        // wasted final page; the lift collapses the carve chain (2 vs 48 carves), so the waste
        // collapses with it.
        double pairedRatio = off.dataPages() / (double) on.dataPages();
        assertThat(pairedRatio)
                .as("lift OFF fetches measurably more data pages than ON (OFF %d vs ON %d = %.3fx)",
                        off.dataPages(), on.dataPages(), pairedRatio)
                .isGreaterThanOrEqualTo(1.3);
        // Loose absolute sanity on the control arm alone: the pathology genuinely manifests — the
        // ratio to the ideal page count degrades past 1.5x (observed 98/50 = 1.96x). No ON-arm
        // absolute ceiling: the relative gap + the counters above carry that (an absolute pair of
        // 1.5/1.5 bounds could collide on a slow runner).
        double offRatio = off.dataPages() / (double) idealPages;
        assertThat(offRatio)
                .as("lift OFF: the relay chain wastes ~1 page per carve (%d/%d = %.3fx ideal)",
                        off.dataPages(), idealPages, offRatio)
                .isGreaterThanOrEqualTo(1.5);
        // The OFF arm's waste tracks its (un-lifted) carve count: many more carves, zero lifts.
        assertThat(off.r("OWNER_SPLIT.pivot_reflect_lifted")).as("no lifts with the lift off").isZero();
        assertThat(off.r("OWNER_SPLIT.self_published"))
                .as("the un-lifted relay chain carves many times (observed, deterministic: 48)")
                .isGreaterThan(10L);
        assertThat(off.r("TOGGLE.reflect_lift_off")).as("the lift ablation mark fired once").isEqualTo(1L);
    }
}

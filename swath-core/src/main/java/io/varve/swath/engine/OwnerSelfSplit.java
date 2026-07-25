/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import io.varve.swath.checkpoint.CheckpointStore;
import io.varve.swath.checkpoint.SplitSpec;
import io.varve.swath.error.SwathException;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.TraceSink;
import io.varve.swath.runtime.RunContext;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The owner-side proactive self-split policy. A draining worker, at page-commit and holding its own
 * {@link WorkerState#lock()}, carves its OWN far-ahead tail into a new child node instead of waiting
 * for a reactive {@link Thief} probe near its moving cursor. Because the owner picks the pivot
 * {@code m > cursorTo} under its lock, the split CAS holds by construction, so a split that a thief
 * would routinely lose to the advancing {@code cursor_passed_pivot} race here always commits. The
 * split transaction itself is the thief's UNCHANGED primitive ({@link WorkerState#narrowHi} +
 * {@link CheckpointStore#splitNode} + the child hand-off), so the no-gap/no-overlap tiling (I2/I3),
 * the I4 CAS, the I1 commit ordering, and the {@code outstanding} quiescence accounting are all
 * untouched; only <i>who initiates</i> a split changes, and it adds zero API calls (INT-8). See
 * {@code docs/internals/walkthroughs.md} §4 and {@code docs/internals/algorithms.md} §3.3, §4.3.
 *
 * <p><b>Tagged-child lifecycle.</b> When the confetti-feedback loop is engaged the policy owns a
 * run-scoped ledger of the children it has carved: {@link #maybeOwnerSelfSplit} <b>tags</b> a child
 * before publishing it, and {@link #onNodeCompleted} — invoked once whichever worker eventually
 * drains that child finishes — <b>classifies</b> the completed child's realized mass as confetti or
 * substantial and folds that ground truth into the {@link ConfettiFeedbackGate} the next carve
 * reads. See {@link #ownerSplitTaggedChildren} for the claim/drain/remove-before-tag ordering that
 * makes each tag consumed exactly once (even under stealing), and {@code
 * docs/internals/metrics-internals.md} §5 for the feedback rationale.
 *
 * <p>One instance per {@link WorkStealingScan} (run-scoped). The gate and the tagged-child set are
 * both concurrency-safe; every method may be called by any worker. Package-private engine collaborator.
 */
final class OwnerSelfSplit {

    private static final Logger log = LoggerFactory.getLogger(OwnerSelfSplit.class);

    /**
     * Owner-side proactive self-split. A draining worker carves its OWN range at
     * page-commit only when its estimated remaining work exceeds this many pages (× {@code maxKeys}
     * keys) — i.e. a large dense drain, not a range about to finish. Small so a genuine mega-day
     * (~110 pages) qualifies while ordinary short ranges never do.
     */
    static final long SELF_SPLIT_MIN_REMAINING_PAGES = 4;
    /**
     * ...and at most once per this many committed non-empty pages, so a fast tail sheds <b>O(1)</b>
     * self-splits per drain (each child recursively re-drains and self-splits again — an O(log) ramp)
     * rather than shattering into a child per page ("confetti"). Paired with the progress-gate
     * ({@link WorkerState#markStolen()}) which already bounds carves to ≤1 per emitted page.
     */
    static final long SELF_SPLIT_MIN_PAGES_BETWEEN = 32;

    private final long runId;
    private final int workerCount;
    private final int maxKeys;
    private final CheckpointStore store;
    private final EngineToggles toggles;
    private final RunMetrics metrics;
    private final TraceSink trace;
    /**
     * The live-node demand-gate count ({@code outstanding.get()}), read AT USE TIME inside the
     * demand gate — never pre-evaluated at construction — so the gate observes the count at the
     * instant the carve is considered.
     */
    private final LongSupplier outstanding;
    /**
     * The concurrency gauge's current target ({@code gauge.effectiveT()}), read AT USE TIME for the
     * demand-gated visibility record — never pre-evaluated at construction.
     */
    private final IntSupplier effectiveT;
    /**
     * The child hand-off: the identical ready-queue + {@code outstanding} accounting a {@link Thief}
     * uses, invoked while the owner still holds its {@link WorkerState#lock()} (§4.4).
     */
    private final Thief.ChildSink enqueueChild;
    /**
     * The realized-child-mass feedback gate for owner self-split (see {@link ConfettiFeedbackGate})
     * and the node ids of owner-split children it is currently waiting to classify. A child id is
     * added in {@link #maybeOwnerSelfSplit} <b>BEFORE</b> {@link #enqueueChild} publishes it:
     * publishing makes the child immediately claimable, so tagging after publish would let a fast
     * worker claim, drain, and reach the completion site's removal before the tag exists — silently
     * dropping the classification and leaking the id into this set forever. Tagging first is safe
     * because {@code childId} is already validated non-aborted and {@code enqueueChild} cannot fail.
     * The id is removed and classified exactly once per process run at {@link #onNodeCompleted}, by
     * whichever worker drains it, so double-classification is impossible even under stealing. Only
     * populated when {@code toggles.confettiFeedback()} is on.
     *
     * <p><b>Process-local, never durable.</b> Like {@link WorkerState#hasSplit()} and the gate's own
     * counters, this set lives only in heap and is never checkpointed. On resume a child tagged
     * before a crash reopens as an ordinary node and completes UNTAGGED, contributing <i>no</i>
     * classification rather than a wrong one (no double-count) — the intended, decided behavior, not
     * a durability gap to close. See {@code docs/internals/metrics-internals.md} §5.
     */
    private final ConfettiFeedbackGate confettiFeedback = new ConfettiFeedbackGate();
    private final Set<Long> ownerSplitTaggedChildren = ConcurrentHashMap.newKeySet();

    OwnerSelfSplit(long runId, int workerCount, int maxKeys, CheckpointStore store, EngineToggles toggles,
                   RunMetrics metrics, TraceSink trace, LongSupplier outstanding, IntSupplier effectiveT,
                   Thief.ChildSink enqueueChild) {
        this.runId = runId;
        this.workerCount = workerCount;
        this.maxKeys = maxKeys;
        this.store = store;
        this.toggles = toggles;
        this.metrics = metrics;
        this.trace = trace;
        this.outstanding = outstanding;
        this.effectiveT = effectiveT;
        this.enqueueChild = enqueueChild;
    }

    /**
     * Owner-side proactive self-split at page-commit. Called <b>under
     * {@link WorkerState#lock()}</b> right after the worker advanced its cursor to {@code cursorTo}
     * and enqueued the page commit. This is the race-killer: the reactive {@link Thief} probes near a
     * drainer's moving cursor and can lose the {@code cursor_passed_pivot} race (the drainer advances
     * past the pivot before the split CAS commits). Here the <i>draining worker itself</i> chooses a
     * far-ahead pivot {@code m > cursorTo} from its own density (no probe) while holding its lock,
     * so the split CAS guard {@code cursor < pivot AND range_end IS oldHi AND status <>
     * COMPLETED} holds <b>by construction</b>: the owner picked {@code m > cursorTo}, {@code hi} is
     * unchanged (no thief can interleave — the owner holds the lock), and the node is IN_PROGRESS
     * ({@code completed} is false at the only call site).
     *
     * <p>The split transaction itself is the thief's UNCHANGED primitive: {@link WorkerState#narrowHi}
     * + {@link CheckpointStore#splitNode} (same {@link SplitSpec}, same I4 CAS + {@code outstanding}
     * accounting) + {@link #enqueueChild} (the identical ready-queue/{@code outstanding} hand-off a
     * thief uses). The owner does <b>not</b> run the child {@code (m, H]} — it continues listing its
     * now-narrowed {@code (cursorTo, m]} (RangeScanner re-reads {@code hi} per key and stops at the new
     * bound); an idle worker/stealer claims the PENDING child. Adds <b>zero</b> API calls (INT-8).
     *
     * <p>No double-emit: the in-flight page was already trimmed to {@code cursorTo <= m}, so every key
     * about to be emitted is {@code <= cursorTo < m} (owner's half); the child owns {@code (m, H]}.
     * Boundary {@code m} belongs LEFT (owner keeps {@code (lo, m]}) — I2/I3, exactly as a thief split.
     * {@link WorkerState#markStolen()} after a publish reuses the progress-gate so this worker is not
     * also carved by a thief in the same page window (≤1 carve per emitted page).
     */
    OwnerSplitTrace maybeOwnerSelfSplit(long nodeId, WorkerState ws, byte[] cursorTo, long[] selfSplit)
            throws SwathException, InterruptedException {
        byte[] H = ws.hiSupplier().get();
        if (H == null) {
            return null;   // open frontier keeps its extrapolation path — never self-split the frontier
        }
        long committed = ++selfSplit[0];
        // Plain code-point estRemaining (NOT the rank-space variant): an owner-split is zero-probe
        // (interpolate is pure math, no LIST), so under-firing here trades cheap owner-splits for
        // costlier thief structure-probes on the un-split tail — strictly worse. The rank-space
        // deflation belongs only to the pivot synthesis below (interpolate(..., alphabetDigest())),
        // which needs no estRemaining change to land on a populated value.
        double est = StealMath.estRemaining(cursorTo, ws.lo(), H, ws.keysEmitted());
        if (est <= (double) SELF_SPLIT_MIN_REMAINING_PAGES * maxKeys) {
            return null;   // remaining work too small to be worth a proactive carve
        }
        if (committed - selfSplit[1] < SELF_SPLIT_MIN_PAGES_BETWEEN) {
            return null;   // rate-limit: O(1) self-splits per drain, not one per page
        }
        // Owner-split DEMAND GATE. On a SATURATED bucket the ready queue already holds enough live
        // nodes to keep every worker busy, so an extra child buys ZERO parallelism and only costs a
        // wasted page (its bounded final page is fetched full, then trimmed per key). Suppress the
        // carve once live nodes reach T; below T (during ramp) the gate stays open so the engine
        // still ramps to T busy workers. {@code outstanding} is the existing lock-free AtomicLong —
        // one relaxed read, no new shared state. {@code workerCount > 1} guards a T=1 run: with no
        // thief at all, "buys zero parallelism" is moot, and gating would only shrink the durable
        // checkpoint granularity for the lone worker without ever saving an S3 call — it drains the
        // un-split range at the identical cost either way. See docs/internals/metrics-internals.md §5.
        if (workerCount > 1 && outstanding.getAsLong() >= (long) workerCount) {
            metrics.recordStealReason("OWNER_SPLIT", "demand_gated");
            ws.recordDemandGated();   // per-range tally for the slow-range dump
            // Record T vs Tmax at the instant the gate fired, so a shrunken-T gate closure is
            // readable from one artifact instead of correlating the swath.workers.active gauge's
            // history against this event's log timestamp.
            metrics.recordDemandGatedConcurrency(effectiveT.getAsInt(), workerCount);
            return null;
        }
        // Far-ahead pivot fraction from the worker's own zero-cost density (>= 0.5 ⇒ >= byteMidpoint).
        // The child owns the far tail — so floor that tail above two pages: even below the demand gate an
        // owner-split must never fission into a ~1-page "confetti" child whose bounded final page is
        // fetched full then trimmed per key. The floor measures the tail in OBSERVED-density terms —
        // the plain span share (1-f)*est over-states a thinning tail on skewed keyspaces (see
        // StealMath.childTailBelowObservedMassFloor's math).
        double f = toggles.farAheadFraction(ws);
        double densityRatio = toggles.observedDensityRatio(ws);
        if (StealMath.childTailBelowObservedMassFloor(est, f, densityRatio, maxKeys)) {
            metrics.recordStealReason("OWNER_SPLIT", "floor_reflected_blocked");
            return null;   // child tail below two pages of observed mass — not worth a proactive carve
        }
        // CONFETTI FEEDBACK GATE. The gates above reason from upstream estimates (est/densityRatio);
        // on a keyspace whose tail thins out over most of the observed span those estimates still
        // pass a carve whose REALIZED mass turns out confetti-sized. Once the run has accumulated
        // enough tagged-child evidence (MIN_SAMPLE), a high observed confetti rate suppresses further
        // carving directly from that ground truth — with a periodic probe so a keyspace that later
        // turns genuinely dense recovers on its own. See {@link ConfettiFeedbackGate} and
        // docs/internals/metrics-internals.md §5.
        if (toggles.confettiFeedback()) {
            ConfettiFeedbackGate.Decision decision = confettiFeedback.decide();
            if (decision == ConfettiFeedbackGate.Decision.SUPPRESSED) {
                metrics.recordStealReason("OWNER_SPLIT", "confetti_suppressed");
                return null;
            }
            if (decision == ConfettiFeedbackGate.Decision.PROBE) {
                metrics.recordStealReason("OWNER_SPLIT", "confetti_probe");
            }
        }
        // Synthesize the pivot at fraction f in the observed-alphabet rank space so it lands on a
        // populated value.
        byte[] m = toggles.interpolate(cursorTo, H, f, ws.alphabetDigest());
        if (m == null
                || KeyBytes.compareUnsigned(cursorTo, m) >= 0
                || KeyBytes.compareUnsigned(m, H) > 0) {
            return null;   // unsplittable, or pivot not strictly in (cursorTo, H] — skip this page
        }
        // Engagement (§5): did the observed-alphabet chooser land the owner-split pivot on a
        // populated value (differs from the plain code-point interpolate at the same fraction)? Recorded
        // about the INTERPOLATED pivot (the alphabet chooser's own output), before the reflection clamp below.
        byte[] plainPivot = StealMath.interpolate(cursorTo, H, f);
        metrics.recordStealReason("ALPHABET",
                !Arrays.equals(m, plainPivot) ? "alphabet_chosen" : "alphabet_fallback");
        // Owner-split reflection clamp (gated by `reflect`). On a skewed keyspace the
        // f-interpolated pivot can overshoot the observed mass into vacuum (a near-empty child); the
        // density-reflected pivot m_r = extrapolate(lo, cursor, H) marks where the drained mass reflects
        // to. Clamp the pivot DOWN to m_r ONLY when m_r is strictly below the interpolated pivot (the
        // interpolate overshot) AND the clamped child tail (m_r, H] still clears the observed-mass
        // floor — so the split lands inside the mass instead of carving vacuum. On a uniform keyspace
        // m_r >= m (reflection reaches at least as far as f), so the clamp never engages (f's skew is
        // load-bearing there — don't clamp blind). Tiling is preserved: extrapolate guarantees
        // cursor <_u m_r <_u H, and the CAS below re-validates under the lock.
        if (toggles.reflect()) {
            byte[] mReflect = StealMath.extrapolate(ws.lo(), cursorTo, H);
            if (StealMath.shouldClampToReflected(cursorTo, m, mReflect, ws.lo(), H, est, densityRatio, maxKeys)) {
                m = mReflect;
                metrics.recordStealReason("OWNER_SPLIT", "pivot_reflect_clamped");
            }
        }
        // REFLECT-LIFT. When the FINAL post-clamp pivot would leave the owner a sub-one-page kept
        // share (measured in est's OWN [ws.lo(), H] frame — fKeptLo below, never a re-scoped span),
        // LIFT m to the density-reflected pivot instead of carving at cursorTo's degenerate
        // successor: the owner then keeps ~one page of REAL mass (its final page partial-trims
        // instead of coming back empty) and the child still gets the far tail. Lift only UP (the
        // clamp above owns the down direction) and only if the lifted child tail still clears the
        // observed-mass floor; any condition failing falls through to the unchanged carve, with the
        // confetti feedback gate above as the realized-mass backstop. (Why an owner-kept MASS floor
        // is NOT reintroduced here, and why relay carves are never suppressed outright:
        // docs/internals/metrics-internals.md §5.)
        //
        // Gate on BOTH toggles: the lift is a density-reflection application — the SAME
        // StealMath.extrapolate the thief's reflection and the clamp above use, into the SAME
        // reflected-pivot family — so `reflect=off` disables the thief's reflection, this method's
        // clamp, and the lift together (full reflection ablation, matching docs/usage.md's
        // "reflect=off restores exact pre-reflection placement"); `reflect_lift=off` alone disables
        // only this lift.
        if (toggles.reflect() && toggles.reflectLift()) {
            byte[] mReflect = StealMath.extrapolate(ws.lo(), cursorTo, H);
            if (StealMath.shouldLiftToReflected(cursorTo, m, mReflect, ws.lo(), H, est, densityRatio, maxKeys)) {
                m = mReflect;
                metrics.recordStealReason("OWNER_SPLIT", "pivot_reflect_lifted");
            }
        }
        // Publish via the UNCHANGED thief split transaction: volatile narrow, then the CAS-guarded
        // durable split (synchronous, held across the lock exactly as the thief holds victim.lock).
        ws.narrowHi(m);
        long childId = store.splitNode(new SplitSpec(runId, nodeId, m, H));
        if (childId == CheckpointStore.SPLIT_ABORTED) {
            // Cannot happen by construction (cursor==cursorTo<m, range_end==H, IN_PROGRESS), but if the
            // CAS ever rejects, restore the bound we validated under the lock and skip — never break
            // tiling. This mirrors the thief's late-loser restore.
            ws.restoreHi(H);
            metrics.recordStealReason("OWNER_SPLIT", "self_aborted");
            return null;
        }
        if (toggles.confettiFeedback()) {
            // Tag this child BEFORE enqueueChild publishes it — see ownerSplitTaggedChildren's
            // javadoc for why tagging first (rather than after publish) is safe here.
            ownerSplitTaggedChildren.add(childId);
        }
        enqueueChild.accept(childId, m, H);   // same ready-queue + outstanding hand-off a thief uses
        ws.markStolen();               // progress-gate: not also thief-carved this page
        selfSplit[1] = committed;
        metrics.recordStealReason("OWNER_SPLIT", "self_published");
        metrics.recordPivotByteRegion(m, cursorTo);   // dead-zone diagnostic (§5)
        if (log.isDebugEnabled()) {
            log.debug("owner_self_split run_id={} worker_id={} node_id={} cursor={} m={} hi={} child_node={}",
                    runId, RunContext.workerIdOrNone(), nodeId, StealMath.describe(cursorTo), StealMath.describe(m),
                    StealMath.describe(H), childId);
        }
        // Do NOT emit trace.ownerSplit here — the caller holds ws.lock across this whole method, and a
        // lock-holder must never wait on the trace sink's global-synchronized flush. Return the
        // captured event so the caller emits it AFTER releasing ws.lock. null ⇒ trace disabled or
        // no split (correctness is unchanged either way — trace is a pure side-channel).
        return trace.enabled() ? new OwnerSplitTrace(nodeId, childId, m, H) : null;
    }

    /**
     * Classify a completed TAGGED owner-split child and fold its realized mass into the run-level
     * confetti-rate feedback the next carve reads (ground truth, not the upstream estimate). Called
     * from the engine's node-completion site once, by whichever worker eventually claims and drains
     * the child — so classification happens exactly once per tagged child even under stealing. A
     * non-tagged node (a seed, a thief child, or any node when confetti feedback is off) is a no-op.
     */
    void onNodeCompleted(long nodeId, WorkerState ws) {
        // Classify TAGGED children only; remove-once makes classification exactly-once even under
        // stealing. isConfettiChild carries the test (a small own tally AND ws.hasSplit() false) and
        // its javadoc the rationale for why the never-split condition is load-bearing.
        if (toggles.confettiFeedback() && ownerSplitTaggedChildren.remove(nodeId)) {
            boolean confetti = isConfettiChild(ws.keysEmitted(), ws.hasSplit(), maxKeys);
            confettiFeedback.recordCompletion(confetti);
            metrics.recordStealReason("OWNER_SPLIT_CHILD", confetti ? "confetti" : "substantial");
        }
    }

    /**
     * The owner-split trace event captured under {@link WorkerState#lock()} in {@link #maybeOwnerSelfSplit}
     * and emitted by the caller AFTER the lock is released — a lock-holder must never wait on
     * {@code JsonlTraceSink.writeEvent}'s global-synchronized disk flush. {@code null} return ⇒ no
     * split (or trace disabled); a pure side-channel, so it never affects split correctness or quiescence.
     */
    record OwnerSplitTrace(long nodeId, long childId, byte[] pivot, byte[] hi) {
    }

    /**
     * Whether a completed TAGGED owner-split child is CONFETTI ({@code true}) or
     * SUBSTANTIAL ({@code false}). Both conditions must hold for confetti: a small own tally
     * ({@code keysEmitted <= 2*maxKeys}, the same threshold the floor uses) AND {@code !hasSplit}
     * (it never itself split — no owner self-split, no successful thief steal, ever carved a child
     * off it in turn). {@code hasSplit} is load-bearing, not an edge case: on a dense/uniform range
     * owner self-split recurses deliberately deep, so a healthy intermediate node that DID split routinely finishes
     * with a small own tally purely because it shed its own further tail(s) onward — that is proof
     * the carve was worthwhile, never confetti, regardless of how small its own tally turns out.
     * Package-private static (pure predicate, no engine state) so the exact boundary/never-split
     * interaction is unit-testable without driving the whole engine to a precise combination of
     * realized mass and recursive splitting. See {@link ConfettiFeedbackGate}'s javadoc for the full
     * rationale and the pathology this targets.
     */
    static boolean isConfettiChild(long keysEmitted, boolean hasSplit, int maxKeys) {
        return keysEmitted <= 2L * (long) maxKeys && !hasSplit;
    }
}

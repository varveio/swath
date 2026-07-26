/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import io.varve.swath.checkpoint.CheckpointStore;
import io.varve.swath.checkpoint.SplitSpec;
import io.varve.swath.engine.policy.Carve;
import io.varve.swath.engine.policy.ConfettiObservation;
import io.varve.swath.engine.policy.Engagement;
import io.varve.swath.engine.policy.OwnerSplitDecision;
import io.varve.swath.engine.policy.OwnerSplitGovernor;
import io.varve.swath.engine.policy.OwnerSplitMutation;
import io.varve.swath.engine.policy.OwnerSplitPolicy;
import io.varve.swath.engine.policy.OwnerSplitSkipReason;
import io.varve.swath.engine.policy.OwnerSplitView;
import io.varve.swath.engine.policy.Skip;
import io.varve.swath.error.SwathException;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.TraceSink;
import io.varve.swath.runtime.RunContext;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The owner-side proactive self-split executor. A draining worker, at page-commit and holding its own
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
 * <p><b>The gate chain itself is {@link OwnerSplitGovernor}</b> (the {@code io.varve.swath.engine.policy}
 * seam, swath-notes' 2026-07-26 simulator campaign): a pure {@code view -> Skip(reason) | Carve(pivot)}
 * decision over one page-commit's {@link OwnerSplitView}, with no lock/clock/RPC of its own. This
 * class is the executor: it translates {@link WorkerState} into the view, issues every RPC/mutation
 * the decision implies (metrics engagements, {@code splitNode}, the child hand-off), and owns every
 * invariant carrying live lock/CAS/quiescence state that the policy never sees.
 *
 * <p><b>Tagged-child lifecycle.</b> When the confetti-feedback loop is engaged this class owns a
 * run-scoped ledger of the children it has carved: {@link #maybeOwnerSelfSplit} <b>tags</b> a child
 * before publishing it, and {@link #onNodeCompleted} — invoked once whichever worker eventually
 * drains that child finishes — <b>classifies</b> the completed child's realized mass as confetti or
 * substantial and folds that ground truth into the {@link ConfettiFeedbackGate}, snapshotted into
 * every subsequent view the governor's next carve considers. See {@link #ownerSplitTaggedChildren}
 * for the claim/drain/remove-before-tag ordering
 * that makes each tag consumed exactly once (even under stealing), and {@code
 * docs/internals/metrics-internals.md} §5 for the feedback rationale.
 *
 * <p>One instance per {@link WorkStealingScan} (run-scoped). The gate and the tagged-child set are
 * both concurrency-safe; every method may be called by any worker. Package-private engine collaborator.
 */
final class OwnerSelfSplit {

    private static final Logger log = LoggerFactory.getLogger(OwnerSelfSplit.class);

    private final long runId;
    private final int workerCount;
    private final int maxKeys;
    private final CheckpointStore store;
    private final EngineToggles toggles;
    private final RunMetrics metrics;
    private final TraceSink trace;
    /**
     * The live-node demand-gate count ({@code outstanding.get()}), read AT USE TIME inside {@link
     * #maybeOwnerSelfSplit} — never pre-evaluated at construction. Read at {@link OwnerSplitView}
     * construction, before the governor's decision runs (ahead of the remaining-est floor and the
     * rate limit, not "at the instant" the demand gate itself is reached) — a heuristic input, so
     * this earlier-by-nanoseconds read never affects correctness, only which page-commit's snapshot
     * the demand gate happens to see.
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
     * populated when {@code toggles.confettiFeedback()} is on. This class snapshots the gate's
     * counters into every {@link OwnerSplitView} it builds (issue #22: {@link #governor} never
     * holds a live reference to it) and applies {@link #governor}'s {@code
     * CONSUME_CONFETTI_PROBE_SLOT} mutation back onto it — see that class's javadoc.
     *
     * <p><b>Process-local, never durable.</b> Like {@link WorkerState#hasSplit()} and the gate's own
     * counters, this set lives only in heap and is never checkpointed. On resume a child tagged
     * before a crash reopens as an ordinary node and completes UNTAGGED, contributing <i>no</i>
     * classification rather than a wrong one (no double-count) — the intended, decided behavior, not
     * a durability gap to close. See {@code docs/internals/metrics-internals.md} §5.
     */
    private final ConfettiFeedbackGate confettiFeedback = new ConfettiFeedbackGate();
    private final Set<Long> ownerSplitTaggedChildren = ConcurrentHashMap.newKeySet();
    /** The gate chain itself (algorithms.md §3.3) — see this class's javadoc for the executor/policy split. */
    private final OwnerSplitPolicy governor;

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
        this.governor = new OwnerSplitGovernor(toggles, workerCount, maxKeys);
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
        // selfSplit[0] (committed non-empty pages so far) only ever advances when the range is
        // bounded — an open frontier has no rate-limit window to track (OwnerSplitSkipReason#OPEN_FRONTIER
        // is structural, not a suppression, so it never consumes a page count). Reading the
        // would-be-incremented value here, rather than incrementing unconditionally, preserves that
        // exactly while still letting the governor's decide() be the sole gate-chain decision point.
        long committed = (H == null) ? selfSplit[0] : ++selfSplit[0];
        ConfettiFeedbackGate.Snapshot confettiSnapshot = confettiFeedback.snapshot();
        OwnerSplitView view = new OwnerSplitView(H, ws.lo(), cursorTo, ws.keysEmitted(), committed,
                selfSplit[1], outstanding.getAsLong(), ws.densityFraction(), ws.observedDensityRatio(),
                ws.alphabetDigest(), new ConfettiObservation(confettiSnapshot.taggedTotal(),
                        confettiSnapshot.taggedConfetti(), confettiSnapshot.probeSeq()));
        OwnerSplitDecision decision = governor.decide(view);
        applyEngagements(decision.engagements());
        applyMutations(decision.mutations());
        if (decision instanceof Skip skip) {
            if (skip.reason() == OwnerSplitSkipReason.DEMAND_GATED) {
                ws.recordDemandGated();   // per-range tally for the slow-range dump
                // Record T vs Tmax at the instant the gate fired, so a shrunken-T gate closure is
                // readable from one artifact instead of correlating the swath.workers.active gauge's
                // history against this event's log timestamp.
                metrics.recordDemandGatedConcurrency(effectiveT.getAsInt(), workerCount);
            }
            return null;
        }
        byte[] m = ((Carve) decision).pivot();
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

    /** Record every {@link Engagement} the governor returned — the executor's own metrics emission (§5). */
    private void applyEngagements(List<Engagement> engagements) {
        for (Engagement e : engagements) {
            metrics.recordStealReason(e.category(), e.reason());
        }
    }

    /**
     * Apply every {@link OwnerSplitMutation} the governor returned to the real collaborator it
     * names — the policy never touches {@link ConfettiFeedbackGate} directly (issue #22).
     */
    private void applyMutations(List<OwnerSplitMutation> mutations) {
        for (OwnerSplitMutation m : mutations) {
            if (m == OwnerSplitMutation.CONSUME_CONFETTI_PROBE_SLOT) {
                confettiFeedback.consumeProbeSlot();
            }
        }
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

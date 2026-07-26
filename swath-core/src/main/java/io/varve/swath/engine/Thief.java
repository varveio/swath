/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import io.varve.swath.checkpoint.CheckpointStore;
import io.varve.swath.checkpoint.SplitSpec;
import io.varve.swath.engine.policy.Commit;
import io.varve.swath.engine.policy.Engagement;
import io.varve.swath.engine.policy.FloorProbeOutcome;
import io.varve.swath.engine.policy.KeyProbeOutcome;
import io.varve.swath.engine.policy.KeyProbePhase;
import io.varve.swath.engine.policy.MarkUnsplittable;
import io.varve.swath.engine.policy.NoVictim;
import io.varve.swath.engine.policy.NoVictimReason;
import io.varve.swath.engine.policy.RequestFloorProbe;
import io.varve.swath.engine.policy.RequestKeyProbe;
import io.varve.swath.engine.policy.RequestStructureProbe;
import io.varve.swath.engine.policy.Retry;
import io.varve.swath.engine.policy.Selected;
import io.varve.swath.engine.policy.Selection;
import io.varve.swath.engine.policy.StealAction;
import io.varve.swath.engine.policy.StealAttempt;
import io.varve.swath.engine.policy.StealAttemptView;
import io.varve.swath.engine.policy.StealPolicy;
import io.varve.swath.engine.policy.StructureProbeOutcome;
import io.varve.swath.engine.policy.ThiefPolicy;
import io.varve.swath.engine.policy.VictimMutation;
import io.varve.swath.engine.policy.VictimView;
import io.varve.swath.error.SwathException;
import io.varve.swath.error.ThrottleException;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.TraceSink;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageFetcher;
import io.varve.swath.store.PageRequest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The thief side of demand-driven range stealing (algorithms.md §3, I4): executor mechanics only.
 * One {@link #steal} call performs a <b>single</b> steal attempt against the live worker pool —
 * victim selection and the full pivot cascade are the {@link ThiefPolicy} (a {@link StealPolicy}
 * decided by observed events alone, no lock/clock/RPC); this class drives that decision to
 * completion by issuing the RPCs it requests, then under the victim's lock re-validates and hands
 * off the upper half {@code (m, H]} as a new durable child node. The engine driver calls this in
 * its idle loop.
 *
 * <p>The pre-lock coherent snapshot {@code (c, H)} and every probe are <b>speculative</b>;
 * everything that mutates the victim is re-validated under {@link WorkerState#lock()}, so two
 * thieves that pick the same victim are serialized by that lock. The two distinct loser paths
 * differ by exactly one thing — whether they touch {@code victim.hi} — and the proof of why only
 * the late loser restores it lives at the hand-off in {@link #commit} (algorithms.md §3).
 *
 * <p>All key comparisons are unsigned byte-exact via {@link KeyBytes#compareUnsigned} (I10) —
 * never {@code String.compareTo}.
 */
public final class Thief {

    private static final Logger log = LoggerFactory.getLogger(Thief.class);

    /** The result of a single {@link #steal} attempt. */
    public enum Outcome {
        /** A child {@code (m, H]} was durably created; the driver enqueued it. */
        CHILD_CREATED,
        /** No splittable victim in the pool (every worker idle/unsplittable/empty) → maybe done. */
        NO_VICTIM,
        /** The chosen victim is genuinely too small to split (cached on the victim). */
        UNSPLITTABLE,
        /** A stale snapshot lost a race (bound moved, victim advanced/completed) → re-steal later. */
        RETRY
    }

    /**
     * Called with the new child's node id <b>and its range</b> {@code (childLo, childHi]}
     * <b>while the thief still holds the victim's lock</b> (algorithms.md §4.4): the
     * driver enqueues the child {@code (m, H]} (so {@code childLo == m}, {@code childHi == H},
     * and the child's resume cursor is {@code m}) and bumps {@code outstanding}
     * <b>before</b> the lock is released, so quiescence can never observe a moment where
     * the victim has handed off its tail yet the child is uncounted.
     *
     * <p>Carrying the range here lets the driver build the child's worker state from
     * the pivot it already knows — no extra checkpoint round-trip to re-read the freshly
     * inserted row, and identical to how a seeded {@code Node} becomes a claim.
     */
    @FunctionalInterface
    public interface ChildSink {
        void accept(long childNodeId, byte[] childLo, byte[] childHi);
    }

    private final CheckpointStore store;
    private final PageFetcher fetcher;
    private final long runId;
    private final byte[] prefix;
    private final ListingMode mode;
    private final ChildSink childSink;
    private final RunMetrics metrics;
    private final EngineToggles toggles;
    private final StealPolicy policy;
    /**
     * The {@code --trace <file>} flight recorder seam: {@link TraceSink#NONE} unless
     * {@link WorkStealingScan} threaded a real sink through the full constructor.
     */
    private final TraceSink trace;

    /**
     * The single construction path. {@code metrics} may be {@code null} — the seam unit tests use to
     * drive a {@link Thief} directly with no metrics sink, in which case every {@code if (metrics !=
     * null)} instrumentation call below is a no-op. {@code toggles} is the {@code --engine-toggle}
     * ablation namespace — {@code structure_probes}/{@code far_ahead}/{@code density_ewma}/{@code
     * alphabet_pivots} all bear on the policy's steal-attempt pivot synthesis (see {@link
     * EngineToggles}'s javadoc for the exact mechanical effect of each) — and null-defaults to {@link
     * EngineToggles#DEFAULT}. {@code trace} is the opt-in {@code --trace} JSONL flight recorder seam,
     * threaded from {@link WorkStealingScan} (see {@link TraceSink}), and null-defaults to {@link
     * TraceSink#NONE}. A testkit object-mother supplies these same defaults explicitly for the many
     * unit tests that don't care about them.
     */
    public Thief(CheckpointStore store, PageFetcher fetcher, long runId, byte[] prefix,
                 ListingMode mode, ChildSink childSink, RunMetrics metrics, EngineToggles toggles,
                 TraceSink trace) {
        this.store = store;
        this.fetcher = fetcher;
        this.runId = runId;
        this.prefix = prefix;
        this.mode = mode;
        this.childSink = childSink;
        this.metrics = metrics;
        this.toggles = toggles == null ? EngineToggles.DEFAULT : toggles;
        this.trace = trace == null ? TraceSink.NONE : trace;
        this.policy = new ThiefPolicy(this.toggles, prefix);
        recordDisabledToggleMarks(this.toggles, this.metrics);
    }

    /**
     * One {@code TOGGLE.<name>_off} engagement mark (§5 discipline) per disabled
     * toggle this {@link Thief} was constructed with, fired exactly once (constructor time — "scan
     * start") — {@link WorkStealingScan} constructs exactly one {@link Thief} per engine instance, so
     * this fires once per run without any extra bookkeeping. {@code owner_split} is {@link
     * WorkStealingScan}'s own mechanism (marked there instead — it has no bearing on this unit);
     * {@code radix_bands} is {@link SeedStep}'s own toggle. The toggle-name → mark-string derivation
     * itself is single-sourced in {@link EngineToggles#recordOffMarks} — this method only lists which
     * of the ten ablation toggles this unit owns.
     */
    private static void recordDisabledToggleMarks(EngineToggles toggles, RunMetrics metrics) {
        if (metrics == null) {
            return;   // the metrics-less constructor (unit tests driving Thief directly): no-op
        }
        toggles.recordOffMarks(metrics, "density_ewma", "structure_probes", "far_ahead", "alphabet_pivots",
                "reflect");
    }

    /**
     * Perform one steal attempt over the live worker pool (algorithms.md §3): select a victim, drive
     * the {@link ThiefPolicy}'s pivot cascade to a terminal decision (issuing every RPC it requests
     * along the way), then commit under the victim's lock.
     *
     * @param pool the currently-live workers (the driver passes a live snapshot)
     * @return the outcome of this single attempt (never blocks beyond the probes +
     *         the synchronous durable split)
     */
    public Outcome steal(Iterable<WorkerState> pool) throws SwathException, InterruptedException {
        Map<Long, WorkerState> byId = new LinkedHashMap<>();
        List<VictimView> views = new ArrayList<>();
        for (WorkerState w : pool) {
            byId.put(w.nodeId(), w);
            views.add(new VictimView(w.nodeId(), w.lo(), w.cursor(), w.hi(), w.keysEmitted(),
                    w.unsplittable(), w.pacingSkipAvailable()));
        }

        Selection selection = policy.selectVictim(views);
        applyEngagements(selection.engagements());
        applyMutations(byId, selection.mutations(), null);
        if (selection instanceof NoVictim noVictim) {
            // The discriminator (exactly ONE of the five specific reasons) fires alongside the
            // aggregate `no_splittable_victim` record() below emits for every NO_VICTIM outcome, so
            // the two counter series stay reconcilable (sum(discriminators) == no_splittable_victim).
            if (metrics != null) {
                metrics.recordStealReason("NO_VICTIM", noVictim.reason().code());
            }
            log.debug("steal_failed run_id={} worker_id={} outcome={} reason={}",
                    runId, RunContext.workerIdOrNone(), Outcome.NO_VICTIM, noVictim.reason().code());
            return record(Outcome.NO_VICTIM, NoVictimReason.NO_SPLITTABLE_VICTIM.code());
        }
        WorkerState victim = byId.get(((Selected) selection).victimNodeId());

        // A probe fetch (GaugedFetcher slotGated=false) fails fast on its own small cap
        // (PROBE_TRANSIENT_RETRY_CAP) rather than riding out a storm — it throws the
        // ThrottleException back up through every probe call site below instead of resolving a page.
        // Catch it here and fold it into the SAME non-productive-steal outcome an ordinary RETRY
        // takes (no victim/lock state was touched above this point — the probing below is entirely
        // speculative, so there is nothing to unwind), freeing the sole IdleStealBackoff in-flight
        // slot immediately. This is the ONLY source of a ThrottleException reaching this method in
        // production (the token-less degenerate path never applies to the thief's shared fetcher — a
        // real CancellationToken is always wired before any worker can steal).
        try {
            WorkerState.Snapshot snap = victim.snapshot();
            StealAttemptView attemptView = new StealAttemptView(
                    victim.nodeId(), victim.lo(), snap.cursor(), snap.hi(), victim.keysEmitted(),
                    victim.densityFraction(), victim.alphabetDigest(),
                    victim.unchangedSinceNonProductiveSteal(snap),
                    victim.consecutiveZeroFanoutProbes(), victim.consecutiveTimedOutStructureProbes());

            StealAttempt attempt = policy.beginAttempt(attemptView);
            StealAction action = attempt.start();
            while (true) {
                applyEngagements(action.engagements());
                applyMutations(byId, action.mutations(), snap);
                if (action instanceof RequestKeyProbe rkp) {
                    boolean nonEmpty = probeNonEmpty(rkp.pivot(), rkp.hi());
                    if (rkp.phase() == KeyProbePhase.BISECT) {
                        recordEmptyUpperBisection();
                    }
                    action = attempt.onProbeResult(new KeyProbeOutcome(nonEmpty));
                } else if (action instanceof RequestStructureProbe rsp) {
                    action = attempt.onProbeResult(issueStructureProbe(victim, rsp));
                } else if (action instanceof RequestFloorProbe rfp) {
                    ListPage page = fetcher.fetchPage(
                            new PageRequest(mode, 1, rfp.leafPrefix(), null, null, null, null, null, null, 0));
                    action = attempt.onProbeResult(new FloorProbeOutcome(firstEntryKey(page)));
                } else if (action instanceof Commit commit) {
                    return commit(victim, snap, commit);
                } else if (action instanceof Retry retry) {
                    log.debug("steal_failed run_id={} worker_id={} outcome={} victim_node={} reason={}",
                            runId, RunContext.workerIdOrNone(), Outcome.RETRY, victim.nodeId(), retry.reason().code());
                    return record(Outcome.RETRY, retry.reason().code());
                } else if (action instanceof MarkUnsplittable unsplittable) {
                    if (log.isDebugEnabled()) {
                        log.debug("unsplittable_cached run_id={} worker_id={} victim_node={} cursor={} hi={}",
                                runId, RunContext.workerIdOrNone(), victim.nodeId(),
                                StealMath.describe(snap.cursor()), StealMath.describe(snap.hi()));
                    }
                    return record(Outcome.UNSPLITTABLE, unsplittable.reason().code());
                } else {
                    throw new IllegalStateException("unhandled StealAction: " + action);
                }
            }
        } catch (ThrottleException te) {
            // The probe's retry cap (GaugedFetcher.PROBE_TRANSIENT_RETRY_CAP) exhausted — see the
            // try-block header above: fail this ONE attempt into the ordinary non-productive RETRY
            // (victim rotation / IdleStealBackoff pacing) rather than camp on it.
            log.debug("steal_failed run_id={} worker_id={} outcome={} victim_node={} reason={}",
                    runId, RunContext.workerIdOrNone(), Outcome.RETRY, victim.nodeId(),
                    "probe_retry_cap_failfast");
            return record(Outcome.RETRY, "probe_retry_cap_failfast");
        }
    }

    /**
     * Lock-guarded RE-VALIDATE-and-narrow hand-off (algorithms.md §3, §4.3). The policy's
     * {@link Commit} is speculative — its pivot and every probe that placed it were taken against
     * the pre-lock snapshot {@code snap}; re-check BOTH the bound and the cursor under the lock
     * before publishing the narrow and running the durable split CAS.
     */
    private Outcome commit(WorkerState victim, WorkerState.Snapshot snap, Commit commit)
            throws SwathException, InterruptedException {
        byte[] m = commit.pivot();
        byte[] hi = snap.hi();

        // The RETRY outcomes below are captured into pendingOutcome/pendingReason rather than
        // returned directly: record(...) emits a trace event (a lock-holder must never wait on
        // JsonlTraceSink's global synchronized writer), so it is called AFTER the finally/unlock.
        // traceChildId keeps its dummy initializer because it is only read on the success path,
        // which is the sole branch that assigns it.
        long traceChildId = -1;
        Outcome pendingOutcome = null;
        String pendingReason = null;
        victim.lock().lock();
        try {
            if (!Arrays.equals(victim.hi(), hi)) {
                // EARLY loser: another thief already narrowed the bound, so our m/probes are stale.
                // Re-snapshot on the next attempt — and DO NOT touch victim.hi: restoring hi here
                // would clobber the winner's narrow and re-open the overlap.
                victim.recordFutileSteal();   // per-victim futility (this racing victim)
                log.debug("steal_failed run_id={} worker_id={} outcome={} victim_node={} reason={}",
                        runId, RunContext.workerIdOrNone(), Outcome.RETRY, victim.nodeId(), "bound_moved");
                pendingOutcome = Outcome.RETRY;
                pendingReason = "bound_moved";
            } else if (victim.cursor() != null && KeyBytes.compareUnsigned(victim.cursor(), m) >= 0) {
                // Victim already advanced to/past the pivot — leave hi untouched and re-steal.
                victim.recordFutileSteal();   // per-victim futility (this racing drainer)
                victim.recordCursorPassedPivot();   // per-range tally for the slow-range dump
                log.debug("steal_failed run_id={} worker_id={} outcome={} victim_node={} reason={}",
                        runId, RunContext.workerIdOrNone(), Outcome.RETRY, victim.nodeId(), "cursor_passed_pivot");
                pendingOutcome = Outcome.RETRY;
                pendingReason = "cursor_passed_pivot";
            } else {
                // Volatile narrow to (lo, m]: publishes the new bound to the scanner's per-key reader,
                // which stops the victim at the first key > m. Boundary m stays LEFT (m <= victim's
                // bound), I3.
                victim.narrowHi(m);

                // Synchronous durable-before-list split (algorithms.md §3, §4.3): returns only after
                // the child is committed PENDING. Holding victim.lock across this await is correct for
                // the thief — it gates only this victim's next commit-enqueue.
                long childId = store.splitNode(new SplitSpec(runId, victim.nodeId(), m, hi));
                if (childId == CheckpointStore.SPLIT_ABORTED) {
                    // LATE loser: we narrowed, but the durable CAS rejected us (a 2nd thief's stale
                    // split, or the victim completed via a terminal page). Restore the bound we
                    // validated under the lock and retry; the victim keeps owning the whole (lo, H].
                    victim.restoreHi(hi);
                    log.debug("steal_failed run_id={} worker_id={} outcome={} victim_node={} reason={}",
                            runId, RunContext.workerIdOrNone(), Outcome.RETRY, victim.nodeId(), "split_aborted");
                    pendingOutcome = Outcome.RETRY;
                    pendingReason = "split_aborted";
                } else {
                    // Success: hand off (m, H] to the driver. The bump of `outstanding` happens inside
                    // accept(), BEFORE we release the lock, so quiescence can't miss the new child (§4.4).
                    childSink.accept(childId, m, hi);
                    // Clear the victim's emit-progress flag: it is now ineligible as a steal victim
                    // until it commits another non-empty page (progress-gated stealing — paces
                    // re-splits to ≤1 per emitted page so the fetch wins each round, closing the
                    // latency livelock). The driver curates the eligible victim pool; this is the
                    // matching "consume" of that progress.
                    victim.markStolen();
                    if (metrics != null) {
                        metrics.recordStealReason("PIVOT", commit.mechanism().code());   // winning-mechanism engagement (§5)
                        metrics.recordPivotByteRegion(m, snap.cursor());                  // dead-zone diagnostic (§5)
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("split_committed run_id={} worker_id={} victim_node={} child_node={} lo={} m={} hi={}",
                                runId, RunContext.workerIdOrNone(), victim.nodeId(), childId,
                                StealMath.describe(victim.lo()), StealMath.describe(m), StealMath.describe(hi));
                    }
                    traceChildId = childId;
                }
            }
        } finally {
            victim.lock().unlock();
        }
        if (pendingOutcome != null) {
            // Emit OUTSIDE victim.lock: see the comment above the lock block.
            return record(pendingOutcome, pendingReason);
        }
        // Emit the trace split OUTSIDE victim.lock: JsonlTraceSink.writeEvent is a global synchronized
        // whose flush can block on disk I/O — a lock-holder must never wait on it. The fields were
        // captured under the lock above; correctness is unchanged (trace is a pure side-channel), and
        // quiescence accounting already completed inside childSink.accept().
        if (trace.enabled()) {
            trace.split(RunContext.workerIdOrNone(), victim.nodeId(), traceChildId, commit.mechanism().code(), m, hi);
        }
        return record(Outcome.CHILD_CREATED, "split_committed");
    }

    /**
     * Issue the {@code delimiter=/} structure probe {@link RequestStructureProbe} describes and
     * distill the response — the mechanical counters ({@code recordDelimiterFanout},
     * {@code recordStructureProbeFetch}) that never affect the goldens (§5's {@code STRUCTURE.*}
     * reason counters are the policy's job, via the returned {@link Engagement}s) are recorded here,
     * directly from the real page, before it is ever handed to the policy.
     */
    private StructureProbeOutcome issueStructureProbe(WorkerState victim, RequestStructureProbe request)
            throws SwathException, InterruptedException {
        ListPage page = probeStructure(victim, new PageRequest(mode, ThiefPolicy.STRUCTURE_PROBE_MAX_KEYS,
                request.probePrefix(), SeedStep.DELIMITER, request.startAfter(), null, null, null, null, 0));
        if (metrics != null) {
            metrics.recordDelimiterFanout(page.commonPrefixes().size());
            metrics.recordStructureProbeFetch();
        }
        List<byte[]> commonPrefixes = new ArrayList<>(page.commonPrefixes().size());
        for (KeyBytes cp : page.commonPrefixes()) {
            commonPrefixes.add(cp.rawUnsafe());
        }
        return new StructureProbeOutcome(commonPrefixes, page.truncated());
    }

    /**
     * The one speculative call: {@code ListObjectsV2(prefix=P, start_after=m, max_keys=1)}.
     * "Empty" iff it returns no key {@code <= H} — either no key at all past {@code m}, or
     * the first key already exceeds the (snapshotted) bound {@code H}. {@code H == null}
     * (open frontier) accepts any returned key.
     */
    private boolean probeNonEmpty(byte[] m, byte[] H) throws SwathException, InterruptedException {
        if (metrics != null) {
            metrics.recordProbeFetch();
        }
        ListPage page = fetcher.fetchPage(
                new PageRequest(mode, 1, prefix, null, m, null, null, null, null, 0));
        byte[] k = firstEntryKey(page);
        return k != null && (H == null || KeyBytes.compareUnsigned(k, H) <= 0);
    }

    /** The first key of {@code page}'s entries, or {@code null} if it has none. */
    private static byte[] firstEntryKey(ListPage page) {
        for (ListEntry e : page.entries()) {
            return e.key().rawUnsafe();
        }
        return null;
    }

    /**
     * Issue one {@code delimiter=/} structure probe, attributing an ATTEMPT-TIMEOUT outcome to this
     * victim before rethrowing.
     *
     * <p><b>Why this exists.</b> The policy's per-victim structure-probe suppression is fed by what a
     * probe REPORTS. A probe that times out reports nothing — it throws past the fan-out accounting
     * below — so before this, a timing-out probe left the suppression counters untouched: <em>the
     * timeout destroyed the very evidence that would have stopped the next probe</em>, and the thief
     * kept re-probing a region that had already proved it could not answer, paying the full escalated
     * budget every time. Recording the streak here closes that loop.
     *
     * <p>Only {@link ThrottleException.Kind#ATTEMPT_TIMEOUT} counts. A 503/5xx is store backpressure —
     * a statement about the store's load, not about this keyspace's shape — and suppressing structure
     * discovery on it would misattribute a transient service condition to the bucket. Every fault,
     * counted or not, still propagates unchanged: this observes, it never swallows.
     *
     * <p>This whole method is executor mechanics (RPC issuing + the timeout streak it feeds), not
     * policy: the timeout aborts the ENTIRE attempt (the {@link ThrottleException} propagates up to
     * {@link #steal}'s own catch), so there is no cascade decision left to make once it fires.
     */
    private ListPage probeStructure(WorkerState victim, PageRequest req)
            throws SwathException, InterruptedException {
        try {
            ListPage page = fetcher.fetchPage(req);
            // The probe ANSWERED (whatever its fan-out) -- this victim is reachable, so the streak of
            // consecutive timeouts is broken. Mirrors the zero-fan-out counter's own reset discipline.
            victim.resetTimedOutStructureProbes();
            return page;
        } catch (ThrottleException te) {
            if (te.kind() == ThrottleException.Kind.ATTEMPT_TIMEOUT) {
                victim.recordTimedOutStructureProbe();
                if (metrics != null) {
                    // §5: post-hoc analysis must be able to see this path engage, and to tell a
                    // timeout-suppressed victim from a flat (zero-fan-out) one.
                    metrics.recordStealReason("STRUCTURE", "probe_timed_out");
                }
            }
            throw te;
        }
    }

    private Outcome record(Outcome outcome, String reason) {
        if (metrics != null) {
            metrics.recordStealReason(outcome.name(), reason);
        }
        if (trace.enabled()) {
            trace.stealAttempt(RunContext.workerIdOrNone(), outcome.name(), reason);
        }
        return outcome;
    }

    private void recordEmptyUpperBisection() {
        if (metrics != null) {
            metrics.recordEmptyUpperBisection();
        }
    }

    /** Record every {@link Engagement} the policy returned — the executor's own metrics-presence gate. */
    private void applyEngagements(List<Engagement> engagements) {
        if (metrics == null) {
            return;
        }
        for (Engagement e : engagements) {
            metrics.recordStealReason(e.category(), e.reason());
        }
    }

    /**
     * Apply every {@link VictimMutation} the policy returned to the real {@link WorkerState} it
     * names. {@code snap} is the current attempt's coherent snapshot, needed only by
     * {@code MARK_NON_PRODUCTIVE} — {@code null} when applying {@link Selection}-scoped mutations
     * (selection never returns that kind; only the per-attempt cascade, against its own single
     * victim, does).
     */
    private void applyMutations(Map<Long, WorkerState> byId, List<VictimMutation> mutations,
                                WorkerState.Snapshot snap) {
        for (VictimMutation mutation : mutations) {
            WorkerState target = byId.get(mutation.victimNodeId());
            switch (mutation.kind()) {
                case CONSUME_PACING_SKIP -> target.consumePacingSkip();
                case MARK_NON_PRODUCTIVE -> target.markNonProductiveSteal(snap);
                case RECORD_FUTILE_STEAL -> target.recordFutileSteal();
                case SET_UNSPLITTABLE -> target.setUnsplittable(true);
                case RECORD_NO_PIVOT_TALLY -> target.recordNoPivot();
                case RECORD_ZERO_FANOUT_STRUCTURE_PROBE -> target.recordZeroFanoutStructureProbe();
                case RESET_ZERO_FANOUT_STRUCTURE_PROBES -> target.resetZeroFanoutStructureProbes();
                // Preserves the original cascade's coupling of this ONE mutation to metrics presence
                // (it lived inside the same `if (metrics != null)` block as its accompanying
                // recordStealReason calls) — see ThiefPolicy.Attempt#addStructureSuppressionMarks.
                // recordStructureSuppressed() is a slow-range-dump-only tally, never consulted by any
                // split/steal decision, so this cannot affect the goldens either way.
                case RECORD_STRUCTURE_SUPPRESSED_TALLY -> {
                    if (metrics != null) {
                        target.recordStructureSuppressed();
                    }
                }
            }
        }
    }

}

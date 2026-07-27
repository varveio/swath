/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import io.varve.swath.engine.ConfettiFeedbackGate;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.engine.StealMath;
import io.varve.swath.engine.WorkerState;
import io.varve.swath.engine.policy.Carve;
import io.varve.swath.engine.policy.Commit;
import io.varve.swath.engine.policy.ConfettiObservation;
import io.varve.swath.engine.policy.DecisionClock;
import io.varve.swath.engine.policy.DecisionRng;
import io.varve.swath.engine.policy.Engagement;
import io.varve.swath.engine.policy.FloorProbeOutcome;
import io.varve.swath.engine.policy.HybridSeedPlanner;
import io.varve.swath.engine.policy.IdleStealPacingDecision;
import io.varve.swath.engine.policy.IdleStealPacingPolicy;
import io.varve.swath.engine.policy.IdleStealPacingState;
import io.varve.swath.engine.policy.KeyProbeOutcome;
import io.varve.swath.engine.policy.KeyProbePhase;
import io.varve.swath.engine.policy.MarkUnsplittable;
import io.varve.swath.engine.policy.NoVictim;
import io.varve.swath.engine.policy.OwnerSplitDecision;
import io.varve.swath.engine.policy.OwnerSplitGovernor;
import io.varve.swath.engine.policy.OwnerSplitMutation;
import io.varve.swath.engine.policy.OwnerSplitPolicy;
import io.varve.swath.engine.policy.OwnerSplitSkipReason;
import io.varve.swath.engine.policy.OwnerSplitView;
import io.varve.swath.engine.policy.RequestFloorProbe;
import io.varve.swath.engine.policy.RequestKeyProbe;
import io.varve.swath.engine.policy.RequestSeedProbe;
import io.varve.swath.engine.policy.RequestStructureProbe;
import io.varve.swath.engine.policy.Retry;
import io.varve.swath.engine.policy.SeedAction;
import io.varve.swath.engine.policy.SeedDescent;
import io.varve.swath.engine.policy.SeedPlan;
import io.varve.swath.engine.policy.SeedPlanner;
import io.varve.swath.engine.policy.SeedProbeOutcome;
import io.varve.swath.engine.policy.Selected;
import io.varve.swath.engine.policy.Selection;
import io.varve.swath.engine.policy.Skip;
import io.varve.swath.engine.policy.StealAction;
import io.varve.swath.engine.policy.StealAttempt;
import io.varve.swath.engine.policy.StealAttemptView;
import io.varve.swath.engine.policy.StealPolicy;
import io.varve.swath.engine.policy.StructureProbeOutcome;
import io.varve.swath.engine.policy.ThiefPolicy;
import io.varve.swath.engine.policy.VictimMutation;
import io.varve.swath.engine.policy.VictimView;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.replay.store.ListingStore;
import io.varve.swath.sim.kernel.FifoServer;
import io.varve.swath.sim.kernel.SimContext;
import io.varve.swath.sim.kernel.SimEventLog;
import io.varve.swath.sim.kernel.SimKernel;
import io.varve.swath.sim.kernel.SimRng;
import io.varve.swath.sim.kernel.SimRngStream;
import io.varve.swath.sim.kernel.SimRunResult;
import io.varve.swath.sim.model.CallClass;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Runs swath's <b>real</b> listing policies — the seed planner, the owner-side split governor, the
 * thief's victim selection and pivot cascade, the idle-steal pacing arithmetic — against a ground-truth
 * store, in virtual time.
 *
 * <p>The policies are consumed exactly as the engine consumes them: as decisions over views, returning
 * actions and mutations. This class is the other half of that seam — the executor. It builds each view
 * from the state it owns, issues whatever the decision asks for, applies the mutations the decision
 * returns, and owns everything the policies deliberately never see: the clock, the concurrency target,
 * the ranges' bookkeeping, and the compare-and-set that decides whether a proposed split survives.
 *
 * <h2>What a lock is here</h2>
 * The kernel never interleaves two event bodies, so a region other actors cannot observe half-finished
 * <em>is</em> one event body. The page commit — trimming the batch to the current bound, advancing the
 * cursor, folding the page into the density digest, and running the owner-side split decision — is one
 * body, which is exactly the region the engine holds a worker's lock across.
 *
 * <p>The converse is what makes a simulated race real. A thief reads its victim's cursor and bound in
 * one body, issues probes that resolve in later ones, and only then proposes a split. Everything the
 * victim does in between happens in bodies of its own, so by the time the proposal is checked the
 * cursor may have passed the pivot or another thief may have narrowed the bound — and the check
 * legitimately fails, without a lock, a thread, or a retry loop anywhere in this file.
 *
 * <h2>Timing, and the two timing widenings this reproduces</h2>
 * A call's duration is drawn when the request is issued; the store is read when the response arrives.
 * A modelled attempt timeout is decided at issue too, wherever the completion instant is known then:
 * the executor schedules the response <em>or</em> the timeout, never both, so a timeout costs no extra
 * event. It cannot be decided at issue when a queueing store is modelled, and there the executor arms
 * both and retires the loser by attempt generation — a dead event that is counted
 * ({@code events.stale}) rather than absorbed.
 *
 * <p>Three disclosed read-window widenings from the policy extraction are reproduced as the current
 * engine behaves, not as it behaved before the seam existed — modelling the narrower pre-seam versions
 * would make the simulator disagree with the engine that actually ships:
 *
 * <ul>
 *   <li><b>The per-victim futility cooldown is read and consumed as two steps.</b> Whether a candidate
 *       has a cooldown skip left is read while the pool is being scanned, and the skip is consumed
 *       afterwards, for exactly the candidates the policy reports it skipped — where the pre-seam code
 *       checked and decremented in one call. A cooldown can therefore end a call or two later than it
 *       used to.</li>
 *   <li><b>A victim's structure-probe suppression streaks are read once, at view construction.</b> A
 *       streak that changes mid-cascade is not observed until the next attempt.</li>
 *   <li><b>The zero-fan-out streak is applied one step late.</b> The policy returns it as a mutation
 *       alongside the action it decided, so it lands after that step rather than during it.</li>
 * </ul>
 *
 * <p>The fleet-wide idle-steal pacing window is <em>not</em> one of them: its arithmetic moved behind
 * the seam unchanged, still consulted under the same monitor, so this executor's single check-then-act
 * at the top of an attempt is the engine's own shape rather than a widening of it.
 *
 * <h2>Instrumenting the position sensor</h2>
 * Victim choice, pivot mass floors, the owner's self-split and the density feedback all steer on one
 * quantity: {@link StealMath#estRemaining}, a local density times a remaining span, both measured in
 * the window-relative fraction {@link StealMath#fracIn} defines. Whether that sensor can actually see a
 * given keyspace is a property of the keys, not of the policies, so the executor measures it where the
 * policies read it — at every bounded page commit, and at every victim scanned — using the same public
 * arithmetic the decisions use. Nothing in swath-core changes to produce these counters; they are a
 * second reader of a function the engine already exports.
 *
 * <h2>Determinism</h2>
 * One scenario at one seed against one store reproduces itself exactly, including its event trace. That
 * is a claim about the simulator only. It is not a claim that a seeded live run would produce the same
 * decisions: a real fleet's assignment of ranges to threads is scheduling-dependent, and no seed
 * removes that.
 */
public final class SimExecutor {

    /** Store calls issued (counted at request time, so a truncated run overstates by those in flight). */
    public static final String STORE_CALLS_COUNTER = "store.calls";
    /** Keys returned to workers across every page, counted on arrival. */
    public static final String KEYS_LISTED_COUNTER = "store.keys";
    /** Keys committed to the output — the page's in-range share, which is what a real run emits. */
    public static final String KEYS_EMITTED_COUNTER = "emit.keys";
    /** Pages committed (non-empty and empty alike). */
    public static final String PAGES_COUNTER = "emit.pages";
    /** Ranges claimed off the ready queue. */
    public static final String RANGES_CLAIMED_COUNTER = "ranges.claimed";
    /** Ranges drained to completion. */
    public static final String RANGES_COMPLETED_COUNTER = "ranges.completed";
    /** Steal attempts made (the denominator every steal outcome is read against). */
    public static final String STEAL_ATTEMPTS_COUNTER = "steal.attempts";
    /** Worker page attempts that hit their declared timeout. */
    public static final String PAGE_TIMEOUTS_COUNTER = "page.timeouts";
    /** Probe attempts that hit their declared timeout. */
    public static final String PROBE_TIMEOUTS_COUNTER = "probe.timeouts";
    /**
     * Events that were dispatched and had no effect because a faster event had already retired what
     * they referred to. The kernel has no cancellation, so this is what a cancelled timer costs, and it
     * is counted rather than absorbed: these events are charged against the run's event budget like any
     * other, so a budget has to be sized including them.
     */
    public static final String STALE_EVENTS_COUNTER = "events.stale";
    /** Seed-descent probes issued. */
    public static final String SEED_PROBES_COUNTER = "seed.probes";
    /** Children published by the owner-side proactive split. */
    public static final String OWNER_SPLIT_COUNTER = "owner_split.published";
    /** Children published by a thief's steal. */
    public static final String THIEF_SPLIT_COUNTER = "steal.children";
    /** Page commits on a bounded range — the denominator the two position-sensor counters below read against. */
    public static final String SENSOR_BOUNDED_COMMITS_COUNTER = "sensor.bounded_page_commits";
    /**
     * Bounded page commits whose cursor advance did not move {@link io.varve.swath.engine.StealMath#fracIn}
     * at all: real keys came out and the position the policies measure stayed where it was.
     */
    public static final String SENSOR_INVISIBLE_ADVANCE_COUNTER = "sensor.cursor_advance_invisible";
    /** Victims scanned by victim selection, summed over attempts — the denominator of the two below. */
    public static final String SENSOR_VICTIMS_SCANNED_COUNTER = "sensor.victims_scanned";
    /**
     * Scanned victims whose {@code estRemaining} read zero, so selection skipped them as having no
     * remaining span — the reading behind a {@code NO_VICTIM.all_no_remaining_span}.
     */
    public static final String SENSOR_EST_ZERO_COUNTER = "sensor.victim_est_zero";
    /**
     * Scanned victims whose consumed span read zero, so {@code estRemaining} returned the raw remaining
     * span and <b>ignored {@code keysEmitted}</b>: a worker that has emitted a million keys scores
     * identically to one that has emitted none.
     */
    public static final String SENSOR_EST_IGNORES_KEYS_COUNTER = "sensor.victim_est_ignores_keys";

    private static final HexFormat HEX = HexFormat.of();

    private final PolicyScenario scenario;
    private final String storeLabel;
    private final SimListingView view;
    private final SimNodeLedger ledger = new SimNodeLedger();
    private final Map<Long, WorkerState> livePool = new LinkedHashMap<>();
    private final Set<Long> ownerSplitTaggedChildren = new HashSet<>();
    private final ConfettiFeedbackGate confettiFeedback = new ConfettiFeedbackGate();
    private final PolicyRunTimeline.Recorder timeline = new PolicyRunTimeline.Recorder();
    private final OwnerSplitPolicy governor;
    private final SeedPlanner seedPlanner;
    private final SimConcurrencyPolicy gauge;
    private final IdleStealPacingPolicy idlePacing;
    private final FifoServer storeServer;
    private final Worker[] workers;
    private final List<Worker> slotWaiters = new ArrayList<>();
    private final List<Worker> parked = new ArrayList<>();

    private SimEventLog log = SimEventLog.disabled();
    private IdleStealPacingState pacingState = IdleStealPacingState.INITIAL;
    private boolean stealAttemptInFlight;
    private int slotsHeld;
    private int callsInFlight;
    private long parkGeneration;
    private boolean runStuck;

    private SimExecutor(PolicyScenario scenario, ListingStore store, String storeLabel) {
        this.scenario = scenario;
        this.storeLabel = storeLabel;
        this.view = new SimListingView(store, scenario.scanPrefix());
        this.governor = new OwnerSplitGovernor(scenario.toggles(), scenario.workerCount(), scenario.pageSize());
        this.seedPlanner = new HybridSeedPlanner(scenario.scanPrefix(), scenario.workerCount(),
                scenario.toggles());
        // The controller is one instrument for the whole fleet, so its jitter is drawn on the reserved
        // fleet actor rather than through any worker's context -- see SimKernel#FLEET_ACTOR. Holding the
        // stream here (rather than adding a context accessor for another actor's tape) keeps the merged
        // kernel's surface unchanged, and nothing else can reach this (actor, stream) key.
        this.gauge = new SimConcurrencyPolicy(scenario.workerCount(), scenario.budgets(),
                SimRng.forStream(scenario.seed(), SimKernel.FLEET_ACTOR, SimRngStream.AIMD_JITTER));
        this.idlePacing = new IdleStealPacingPolicy(scenario.budgets().idleStealBaseParkNanos(),
                scenario.budgets().idleStealBackoffCapNanos());
        this.storeServer = scenario.storeServerCapacity() > 0
                ? new FifoServer("store.server", scenario.storeServerCapacity())
                : null;
        this.workers = new Worker[scenario.workerCount()];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Worker(i);
        }
    }

    /**
     * Runs {@code scenario} against an already-open {@code store}.
     *
     * @param store      the caller's handle; used, never opened, never closed. Opening one over a large
     *                   fixture costs more than most runs do, so it is opened once and reused across a
     *                   whole sweep; this signature makes that the only possibility
     * @param storeLabel what served this run (the resolved backend, or a test fixture's own name) — part
     *                   of the run record, because a result is not readable without it
     */
    public static PolicyRunResult run(PolicyScenario scenario, ListingStore store, String storeLabel) {
        if (store == null) {
            throw new IllegalArgumentException("the run API takes an already-open store handle");
        }
        if (storeLabel == null || storeLabel.isBlank()) {
            throw new IllegalArgumentException("a run record must say which store served it");
        }
        scenario.clientCost().requireReadyForNewRun();
        SimExecutor executor = new SimExecutor(scenario, store, storeLabel);
        return executor.execute();
    }

    private PolicyRunResult execute() {
        log = scenario.recordEventLog() ? SimEventLog.recording() : SimEventLog.disabled();
        SimKernel kernel = new SimKernel(scenario.seed(), scenario.budgets(), log, scenario.maxEvents());
        kernel.scheduleBootstrap(0, 0, "seed.start", this::startSeedPhase);
        SimRunResult result = kernel.run();
        return PolicyRunResult.of(result, scenario, storeLabel, gauge.counters(), gauge.effectiveT(),
                ledger.nodesCreated(), ledger.splitsAborted(), view.storeReads(), runStuck,
                timeline.finish(result.wallNanos()));
    }

    // ---- seed phase ---------------------------------------------------------------------

    /**
     * The descent runs before any worker exists, and it costs real time: every probe it asks for is a
     * store call with its own latency, charged on the timeline like any other. A run that seeds nothing
     * skips straight to one range over the whole keyspace.
     */
    private void startSeedPhase(SimContext ctx) {
        if (scenario.seedMode() == PolicyScenario.SimSeedMode.NONE) {
            ctx.record("seed.mode", "none");
            seedRanges(ctx, List.of());
            return;
        }
        ctx.record("seed.mode", "shallow");
        ctx.count("seed.probe_budget", seedPlanner.probeBudget());
        SeedDescent descent = seedPlanner.beginDescent();
        driveSeed(ctx, descent, descent.start());
    }

    private void driveSeed(SimContext ctx, SeedDescent descent, SeedAction action) {
        recordEngagements(ctx, action.engagements());
        if (action instanceof SeedPlan plan) {
            ctx.count(SEED_PROBES_COUNTER, plan.probes());
            ctx.count("seed.cuts", plan.cuts().size());
            ctx.count("seed.synthesized_cuts", plan.synthesizedCuts());
            for (var decision : plan.decisions()) {
                ctx.count("seed.level." + decision.classification(), 1);
            }
            seedRanges(ctx, plan.cuts());
            return;
        }
        RequestSeedProbe request = (RequestSeedProbe) action;
        issueCall(ctx, CallClass.SEED_PROBE, scenario.budgets().probeAttemptTimeoutNanos(), 0,
                new CallOutcome() {
                    @Override
                    public void onResponse(SimContext arrived) {
                        SimListingView.Rollup rollup = view.rollup(request.probePrefix(),
                                request.startAfter(), HybridSeedPlanner.PROBE_PAGE);
                        SeedProbeOutcome outcome = new SeedProbeOutcome(rollup.commonPrefixes(),
                                rollup.capped(), rollup.objectCount(), rollup.lastKey());
                        driveSeed(arrived, descent, descent.onProbeResult(outcome));
                    }

                    @Override
                    public void onTimeout(SimContext at, int attempt) {
                        // A seed probe carries no store-backpressure signal, exactly like a thief's.
                        gauge.onTransientTimeout(at.nowNanos(), false);
                        at.count(PROBE_TIMEOUTS_COUNTER, 1);
                        if (attempt > scenario.budgets().probeAttemptRetryCap()) {
                            // The descent cannot proceed without an answer, so a probe that will not
                            // answer ends the seed phase at whatever it has: the engine's own bounded
                            // behaviour, and better than an unbounded retry that never seeds at all.
                            at.record("seed.abandoned", "probe_retry_cap");
                            seedRanges(at, List.of());
                            return;
                        }
                        retryCall(at, this, CallClass.SEED_PROBE,
                                scenario.budgets().probeAttemptTimeoutNanos(), attempt);
                    }
                });
    }

    /** Tiles the descent's cut set into seed ranges and starts the fleet. */
    private void seedRanges(SimContext ctx, List<byte[]> cuts) {
        byte[] lo = null;
        for (byte[] cut : cuts) {
            ledger.addSeed(lo, cut);
            lo = cut;
        }
        ledger.addSeed(lo, null);
        ctx.count("seed.ranges", cuts.size() + 1);
        timeline.seedCompleted(ctx.nowNanos());
        for (Worker worker : workers) {
            ctx.scheduleFor(worker.id, 0, "worker.start", worker::idle);
        }
    }

    // ---- the worker: claim, page, commit, split -------------------------------------------

    /**
     * One simulated worker. It is a claimant, a drainer and — when there is nothing to claim — a thief;
     * the engine gives one thread all three roles for the same reason, so that idleness is what pays for
     * discovering more work.
     */
    private final class Worker {

        private final int id;
        private final StealPolicy thief;
        private SimContext ctx;
        /** The attempt number a worker waiting for a slot will resume on. */
        private int pendingAttempt;

        // The claim in progress.
        private long nodeId = -1L;
        private WorkerState state;
        private long committedPages;
        private long lastSelfSplitPage;

        // The steal attempt in progress.
        private WorkerState victim;
        private byte[] snapshotCursor;
        private byte[] snapshotHi;
        private StealAttempt attempt;

        private Worker(int id) {
            this.id = id;
            // The thief brain is per-worker and bound to that worker's own decision tape: a variant that
            // changes how often ONE worker consults its escape hatch must not re-tape another's draws.
            // The engine shares one instance fleet-wide because it has one Thief; there, the same draw
            // source is shared under a lock, and which worker consumes which value is already a function
            // of the interleaving -- the property a comparison between two simulated variants needs to
            // not have.
            DecisionRng rng = bound -> ctx.rng(SimRngStream.STEAL_DECISION).nextInt(bound);
            this.thief = new ThiefPolicy(scenario.toggles(), scenario.scanPrefix(), rng);
        }

        /**
         * Binds this worker to the context of the body it is running in. The kernel reuses one context
         * and re-points it before each dispatch, so it may be held only for the duration of a body —
         * this is set at the top of every one of this worker's own bodies and read nowhere else.
         */
        private Worker enter(SimContext current) {
            this.ctx = current;
            return this;
        }

        /** The idle policy: claim a range, retire at quiescence, or become a thief. */
        private void idle(SimContext current) {
            enter(current);
            if (runStuck) {
                ctx.record("worker.retire", "stuck");
                return;
            }
            SimNodeLedger.Claim claim = ledger.poll();
            if (claim != null) {
                pacingState = idlePacing.onReset();   // claimed work resets the fleet's pacing ladder
                startClaim(claim);
                return;
            }
            if (ledger.quiescent()) {
                ctx.record("worker.retire", "quiescent");
                return;
            }
            tryStealOrPark();
        }

        private void startClaim(SimNodeLedger.Claim claim) {
            nodeId = claim.nodeId();
            state = new WorkerState(claim.nodeId(), claim.lo(), claim.cursor(), claim.hi());
            committedPages = 0L;
            // Seeded so the first qualifying page may carve immediately, then rate-limited.
            lastSelfSplitPage = -OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN;
            livePool.put(nodeId, state);
            timeline.occupancyChanged(ctx.nowNanos(), livePool.size());
            ctx.count(RANGES_CLAIMED_COUNTER, 1);
            ctx.record("range.claim", "node=" + nodeId);
            requestPage(0);
        }

        /**
         * A worker page fetch is slot-gated: it may not go out unless the adaptive controller's current
         * target has room for it. A worker denied a slot waits for one to be released rather than
         * spinning, which is what a permit does in the engine.
         */
        private void requestPage(int attemptNumber) {
            if (slotsHeld >= gauge.effectiveT()) {
                pendingAttempt = attemptNumber;
                slotWaiters.add(this);
                ctx.record("slot.wait", "worker=" + id);
                return;
            }
            slotsHeld++;
            issuePage(attemptNumber);
        }

        private void issuePage(int attemptNumber) {
            long issuedAt = ctx.nowNanos();
            issueCall(ctx, CallClass.WORKER_PAGE, scenario.budgets().workerAttemptTimeoutNanos(),
                    attemptNumber, new CallOutcome() {
                        @Override
                        public void onResponse(SimContext arrived) {
                            enter(arrived);
                            releaseSlot(arrived);
                            gauge.onSuccess(arrived.nowNanos());
                            gauge.onAttemptLatency(arrived.nowNanos(), arrived.nowNanos() - issuedAt);
                            // The success may have grown the target; the slots that growth released are
                            // handed out now rather than at whatever later instant a slot happens to be
                            // returned.
                            grantSlots(arrived);
                            onPage(arrived);
                        }

                        @Override
                        public void onTimeout(SimContext at, int attempt) {
                            enter(at);
                            releaseSlot(at);
                            at.count(PAGE_TIMEOUTS_COUNTER, 1);
                            // A worker-class timeout, which is the only kind that feeds the growth
                            // freeze and the shed gate. The latency of a timed-out attempt is never
                            // sampled: it is censored at the budget, and feeding it would poison the
                            // baseline the freeze reads.
                            gauge.onTransientTimeout(at.nowNanos(), true);
                            if (scenario.faultDisposition() == PolicyScenario.FaultDisposition.BOUNDED
                                    && attempt > scenario.budgets().workerAttemptRetryCap()) {
                                // The bounded disposition: the retry ceiling ends the run. Under the
                                // shipped default a watchdog owns storm death instead, so the fetch keeps
                                // retrying and the run ends on its declared ceilings, not here.
                                runStuck = true;
                                at.record("run.stuck", "worker_attempt_retry_cap");
                                return;
                            }
                            at.schedule(scenario.budgets().transientRetryBackoffNanos(), "page.retry",
                                    retry -> enter(retry).requestPage(attempt));
                        }
                    });
        }

        /**
         * The page commit — one body, which is one lock hold. The batch is re-validated against the
         * bound as it stands right now (a thief may have narrowed it while the call was in flight), the
         * cursor advances, the page folds into the density digest, and the owner-side split decision
         * runs against a view of all of it, before any other actor can observe a half-finished commit.
         */
        private void onPage(SimContext arrived) {
            byte[] cursorFrom = state.cursor();
            SimListingView.Page page = view.page(cursorFrom, scenario.pageSize());
            arrived.count(KEYS_LISTED_COUNTER, page.keys().size());
            byte[] hi = state.hi();
            List<byte[]> inRange = new ArrayList<>(page.keys().size());
            for (byte[] key : page.keys()) {
                if (hi != null && KeyBytes.compareUnsigned(key, hi) > 0) {
                    break;
                }
                inRange.add(key);
            }
            boolean trimmed = inRange.size() < page.keys().size();
            boolean completed = trimmed || !page.truncated();
            byte[] cursorTo = inRange.isEmpty() ? null : inRange.getLast();
            if (!inRange.isEmpty()) {
                state.setCursor(cursorTo);
                state.addKeysEmitted(inRange.size());
                state.recordPage(inRange.getFirst(), cursorTo, inRange.size());
            }
            ledger.commitPage(nodeId, cursorTo, completed);
            arrived.count(PAGES_COUNTER, 1);
            arrived.count(KEYS_EMITTED_COUNTER, inRange.size());
            timeline.keysCommitted(inRange.size());
            recordSensorReading(arrived, state.lo(), cursorFrom, cursorTo, hi);
            if (log.isRecording()) {
                // The page's own emitted interval goes into the trace, not just its size: a total tells a
                // reader that the right NUMBER of keys came out, which a gap and an overlap of equal size
                // would also satisfy. The interval makes both visible.
                //
                // Guarded, unlike every other trace site here, because this is the only one that formats
                // key bytes: a sweep leg runs with the trace off, and hex-encoding two keys per page for
                // a string nothing retains is the one piece of trace work worth not doing. The bytes are
                // identical when the trace is on.
                arrived.record("page.commit", "node=" + nodeId + "|keys=" + inRange.size()
                        + "|from=" + (inRange.isEmpty() ? "" : HEX.formatHex(inRange.getFirst()))
                        + "|to=" + (cursorTo == null ? "" : HEX.formatHex(cursorTo))
                        + (completed ? "|completed" : ""));
            }
            if (scenario.toggles().ownerSplit() && !inRange.isEmpty() && !completed) {
                maybeOwnerSelfSplit(arrived, cursorTo);
            }
            if (!inRange.isEmpty()) {
                signalStealableProgress(arrived);
            }
            // Everything after this point is the page's client-side cost: the worker's own conversion
            // work, then the durability commit it waits for before emitting, then the consumer stage.
            scenario.clientCost().chargePage(arrived, inRange.size(), charged -> {
                enter(charged);
                if (completed) {
                    completeClaim(charged);
                } else {
                    requestPage(0);
                }
            });
        }

        private void completeClaim(SimContext at) {
            livePool.remove(nodeId);
            timeline.occupancyChanged(at.nowNanos(), livePool.size());
            classifyOwnerSplitChild(at);
            at.count(RANGES_COMPLETED_COUNTER, 1);
            at.record("range.complete", "node=" + nodeId + "|keys=" + state.keysEmitted());
            nodeId = -1L;
            state = null;
            long remaining = ledger.decrement();
            // A decrement is a quiescence signal in the engine's ledger; here it wakes every parked
            // worker so none of them sits out the end of the run on a stale park timer.
            wakeParked(at);
            if (remaining == 0L) {
                timeline.quiesced(at.nowNanos());
                at.record("run.quiescent", "");
            }
            idle(at);
        }

        // ---- owner-side proactive split ---------------------------------------------------

        private void maybeOwnerSelfSplit(SimContext at, byte[] cursorTo) {
            byte[] hi = state.hi();
            // The rate-limit's page counter advances only where the rate limit applies: an open frontier
            // has no far tail to carve, so it is not a suppressed carve and never consumes a page.
            long committed = hi == null ? committedPages : ++committedPages;
            ConfettiFeedbackGate.Snapshot confetti = confettiFeedback.snapshot();
            OwnerSplitView splitView = new OwnerSplitView(hi, state.lo(), cursorTo, state.keysEmitted(),
                    committed, lastSelfSplitPage, ledger.outstanding(), state.densityFraction(),
                    rawObservedDensityRatio(state),
                    state.alphabetDigest().snapshot(),
                    new ConfettiObservation(confetti.taggedTotal(), confetti.taggedConfetti(),
                            confetti.probeSeq()));
            OwnerSplitDecision decision = governor.decide(splitView);
            // A claimed probe slot resolves BEFORE any engagement is recorded: every owner that
            // snapshotted the same sequence decides to probe, and the run-scoped gate admits exactly
            // one, so recording first would credit carves that never happened.
            boolean probeSlotClaimed = false;
            if (decision instanceof Carve
                    && decision.mutations().contains(OwnerSplitMutation.CLAIM_CONFETTI_PROBE_SLOT)) {
                if (confettiFeedback.claimProbeSlot(confetti.probeSeq())) {
                    probeSlotClaimed = true;
                } else {
                    at.count("OWNER_SPLIT." + OwnerSplitSkipReason.CONFETTI_SUPPRESSED.code(), 1);
                    return;
                }
            }
            recordEngagements(at, decision.engagements());
            for (OwnerSplitMutation mutation : decision.mutations()) {
                if (mutation == OwnerSplitMutation.CONSUME_CONFETTI_PROBE_SLOT
                        || (mutation == OwnerSplitMutation.CLAIM_CONFETTI_PROBE_SLOT && !probeSlotClaimed)) {
                    confettiFeedback.consumeProbeSlot();
                }
            }
            if (decision instanceof Skip) {
                return;
            }
            byte[] pivot = ((Carve) decision).pivot();
            // The owner picks a pivot ahead of its own cursor while nothing else can observe it, so this
            // split cannot lose the race a thief's can -- the guard holds by construction.
            state.narrowHi(pivot);
            long childId = ledger.splitNode(nodeId, pivot, hi);
            if (childId == SimNodeLedger.SPLIT_ABORTED) {
                state.restoreHi(hi);
                at.count("OWNER_SPLIT.self_aborted", 1);
                return;
            }
            if (scenario.toggles().confettiFeedback()) {
                ownerSplitTaggedChildren.add(childId);
            }
            ledger.enqueueChild(childId, pivot, hi);
            state.markStolen();
            lastSelfSplitPage = committed;
            timeline.splitPublished(at.nowNanos());
            at.count(OWNER_SPLIT_COUNTER, 1);
            at.count("OWNER_SPLIT.self_published", 1);
            at.record("owner_split", "node=" + nodeId + "|child=" + childId);
            wakeParked(at);
        }

        /**
         * Folds a completed owner-split child's realized mass into the feedback the next carve reads.
         * The classification mirrors the engine's own predicate: a small tally is only confetti if the
         * child never itself split — a node that shed its own tail finished small because the carve
         * worked, which is the opposite of the pathology this measures.
         */
        private void classifyOwnerSplitChild(SimContext at) {
            if (!scenario.toggles().confettiFeedback() || !ownerSplitTaggedChildren.remove(nodeId)) {
                return;
            }
            boolean confetti = state.keysEmitted() <= 2L * scenario.pageSize() && !state.hasSplit();
            confettiFeedback.recordCompletion(confetti);
            at.count("OWNER_SPLIT_CHILD." + (confetti ? "confetti" : "substantial"), 1);
        }

        // ---- the thief ---------------------------------------------------------------------

        private void tryStealOrPark() {
            if (!gauge.isStealingAllowed()) {
                park(scenario.budgets().idleStealBaseParkNanos());
                return;
            }
            if (stealAttemptInFlight) {
                // The fleet allows one steal attempt at a time; the others wait on the slot rather than
                // multiplying probes against the same pool.
                ctx.count("IDLE_SLOT.in_flight", 1);
                park(scenario.budgets().idleStealAttemptParkNanos());
                return;
            }
            long now = clock(ctx).nanoTime();
            if (idlePacing.decide(pacingState, now) == IdleStealPacingDecision.PACED) {
                // Checked and acted on as one step, which is the engine's own shape: this arithmetic
                // moved behind the seam unchanged and is still consulted under one monitor there, so
                // unlike the per-victim cooldown it is not one of the extraction's widened windows.
                ctx.count("IDLE_SLOT.paced", 1);
                park(idlePacing.parkNanos(pacingState, now));
                return;
            }
            stealAttemptInFlight = true;
            ctx.count(STEAL_ATTEMPTS_COUNTER, 1);
            beginSteal();
        }

        private void beginSteal() {
            List<VictimView> pool = new ArrayList<>();
            for (WorkerState candidate : livePool.values()) {
                if (candidate.stealEligible()) {
                    pool.add(new VictimView(candidate.nodeId(), candidate.lo(), candidate.cursor(),
                            candidate.hi(), candidate.keysEmitted(), candidate.unsplittable(),
                            candidate.pacingSkipAvailable()));
                }
            }
            recordVictimSensorReadings(ctx, pool);
            Selection selection = thief.selectVictim(pool);
            recordEngagements(ctx, selection.engagements());
            applyVictimMutations(selection.mutations(), null);
            if (selection instanceof NoVictim noVictim) {
                finishSteal("NO_VICTIM", noVictim.reason().code(), false);
                return;
            }
            victim = livePool.get(((Selected) selection).victimNodeId());
            // The coherent snapshot: read here, in this body, and used to propose a split several
            // bodies later. Everything the victim does in between is exactly the race the proposal has
            // to survive.
            snapshotCursor = victim.cursor();
            snapshotHi = victim.hi();
            StealAttemptView attemptView = new StealAttemptView(victim.nodeId(), victim.lo(),
                    snapshotCursor, snapshotHi, victim.keysEmitted(), victim.densityFraction(),
                    victim.alphabetDigest().snapshot(),
                    victim.unchangedSinceNonProductiveSteal(new WorkerState.Snapshot(snapshotCursor, snapshotHi)),
                    victim.consecutiveZeroFanoutProbes(), victim.consecutiveTimedOutStructureProbes());
            attempt = thief.beginAttempt(attemptView);
            driveSteal(attempt.start());
        }

        private void driveSteal(StealAction action) {
            recordEngagements(ctx, action.engagements());
            applyVictimMutations(action.mutations(), new WorkerState.Snapshot(snapshotCursor, snapshotHi));
            switch (action) {
                case RequestKeyProbe keyProbe -> probe(CallClass.PIVOT_PROBE, arrived -> {
                    if (keyProbe.phase() == KeyProbePhase.BISECT) {
                        arrived.count("STEAL.empty_upper_bisection", 1);
                    }
                    boolean nonEmpty = view.probeNonEmpty(keyProbe.pivot(), keyProbe.hi());
                    driveSteal(attempt.onProbeResult(new KeyProbeOutcome(nonEmpty)));
                }, true);
                case RequestStructureProbe structureProbe -> probe(CallClass.STRUCTURE_PROBE, arrived -> {
                    SimListingView.Rollup rollup = view.rollup(structureProbe.probePrefix(),
                            structureProbe.startAfter(), ThiefPolicy.STRUCTURE_PROBE_MAX_KEYS);
                    // The probe answered, whatever its fan-out, so this victim's consecutive-timeout
                    // streak is broken -- the mirror of the zero-fan-out streak's own reset.
                    victim.resetTimedOutStructureProbes();
                    arrived.count("STRUCTURE.fanout", rollup.commonPrefixes().size());
                    driveSteal(attempt.onProbeResult(
                            new StructureProbeOutcome(rollup.commonPrefixes(), rollup.capped())));
                }, false);
                case RequestFloorProbe floorProbe -> probe(CallClass.PIVOT_PROBE, arrived -> {
                    byte[] firstKey = view.firstKeyUnder(floorProbe.leafPrefix());
                    driveSteal(attempt.onProbeResult(new FloorProbeOutcome(firstKey)));
                }, true);
                case Commit commit -> commitSteal(commit);
                case Retry retry -> finishSteal("RETRY", retry.reason().code(), false);
                case MarkUnsplittable unsplittable ->
                        finishSteal("UNSPLITTABLE", unsplittable.reason().code(), false);
            }
        }

        /**
         * Issues one probe. Probes are not slot-gated — they are rare one-key calls, and pausing steals
         * is the separate lever that holds them back — and a probe that times out fails the whole
         * attempt fast rather than riding out a storm.
         */
        private void probe(CallClass callClass, ProbeResponse onArrival, boolean keyProbe) {
            issueCall(ctx, callClass, scenario.budgets().probeAttemptTimeoutNanos(), 0, new CallOutcome() {
                @Override
                public void onResponse(SimContext arrived) {
                    enter(arrived);
                    onArrival.accept(arrived);
                }

                @Override
                public void onTimeout(SimContext at, int attemptNumber) {
                    enter(at);
                    at.count(PROBE_TIMEOUTS_COUNTER, 1);
                    // A probe timeout is deliberately not a store-backpressure signal: it never feeds
                    // the growth freeze or the shed gate, only the attribution split.
                    gauge.onTransientTimeout(at.nowNanos(), false);
                    if (!keyProbe) {
                        // A structure probe that times out reported nothing, so without this the timeout
                        // would destroy the very evidence that would stop the next one.
                        victim.recordTimedOutStructureProbe();
                        at.count("STRUCTURE.probe_timed_out", 1);
                    }
                    if (attemptNumber > scenario.budgets().probeAttemptRetryCap()) {
                        finishSteal("RETRY", "probe_retry_cap_failfast", false);
                        return;
                    }
                    at.schedule(scenario.budgets().transientRetryBackoffNanos(), "probe.retry",
                            retry -> enter(retry).probeRetry(callClass, onArrival, keyProbe, attemptNumber));
                }
            });
        }

        private void probeRetry(CallClass callClass, ProbeResponse onArrival, boolean keyProbe, int attemptNumber) {
            issueCall(ctx, callClass, scenario.budgets().probeAttemptTimeoutNanos(), attemptNumber,
                    new CallOutcome() {
                        @Override
                        public void onResponse(SimContext arrived) {
                            enter(arrived);
                            onArrival.accept(arrived);
                        }

                        @Override
                        public void onTimeout(SimContext at, int attempt) {
                            enter(at);
                            at.count(PROBE_TIMEOUTS_COUNTER, 1);
                            gauge.onTransientTimeout(at.nowNanos(), false);
                            if (!keyProbe) {
                                victim.recordTimedOutStructureProbe();
                            }
                            finishSteal("RETRY", "probe_retry_cap_failfast", false);
                        }
                    });
        }

        /**
         * The proposal, re-validated. Both checks are against the victim as it stands <em>now</em>,
         * not as it was when the snapshot was taken: if the bound moved, another thief already narrowed
         * it and this pivot was placed against a range that no longer exists; if the cursor reached the
         * pivot, the victim drained past it while the probes were in flight. Either way the attempt is
         * futile — recorded as such against that victim, which is what eventually paces attempts
         * against a drainer nobody can catch.
         */
        private void commitSteal(Commit commit) {
            byte[] pivot = commit.pivot();
            if (!Arrays.equals(victim.hi(), snapshotHi)) {
                victim.recordFutileSteal();
                finishSteal("RETRY", "bound_moved", false);
                return;
            }
            if (victim.cursor() != null && KeyBytes.compareUnsigned(victim.cursor(), pivot) >= 0) {
                victim.recordFutileSteal();
                victim.recordCursorPassedPivot();
                finishSteal("RETRY", "cursor_passed_pivot", false);
                return;
            }
            victim.narrowHi(pivot);
            long childId = ledger.splitNode(victim.nodeId(), pivot, snapshotHi);
            if (childId == SimNodeLedger.SPLIT_ABORTED) {
                // The durable guard rejected what the in-memory checks above allowed: restore the bound
                // this attempt validated and re-steal later.
                victim.restoreHi(snapshotHi);
                victim.recordFutileSteal();
                finishSteal("RETRY", "split_aborted", false);
                return;
            }
            ledger.enqueueChild(childId, pivot, snapshotHi);
            victim.markStolen();
            timeline.splitPublished(ctx.nowNanos());
            ctx.count(THIEF_SPLIT_COUNTER, 1);
            ctx.count("PIVOT." + commit.mechanism().code(), 1);
            ctx.record("steal.split", "victim=" + victim.nodeId() + "|child=" + childId
                    + "|mechanism=" + commit.mechanism().code());
            finishSteal("CHILD_CREATED", "split_committed", true);
        }

        private void finishSteal(String outcome, String reason, boolean productive) {
            ctx.count("steal.outcome." + outcome, 1);
            ctx.count(outcome + "." + reason, 1);
            victim = null;
            attempt = null;
            snapshotCursor = null;
            snapshotHi = null;
            stealAttemptInFlight = false;
            if (productive) {
                pacingState = idlePacing.onReset();
            } else {
                pacingState = idlePacing.onNonProductive(pacingState, clock(ctx).nanoTime());
            }
            // Whatever the outcome, the attempt slot is free again, so anyone parked behind it should
            // re-evaluate rather than sit out its full backstop.
            wakeParked(ctx);
            idle(ctx);
        }

        // ---- parking ------------------------------------------------------------------------

        private void park(long nanos) {
            long generation = parkGeneration;
            parked.add(this);
            ctx.schedule(Math.max(1L, nanos), "worker.park", woken -> {
                if (generation != parkGeneration) {
                    // Something happened while this worker was parked and it was already woken: this
                    // timer is the loser of that race. The kernel cannot cancel it, so it is retired
                    // here, and counted, because it still cost an event.
                    woken.count(STALE_EVENTS_COUNTER, 1);
                    woken.count("events.stale.park", 1);
                    return;
                }
                parked.remove(this);
                enter(woken).idle(woken);
            });
        }

        private void applyVictimMutations(List<VictimMutation> mutations, WorkerState.Snapshot snapshot) {
            for (VictimMutation mutation : mutations) {
                WorkerState target = livePool.get(mutation.victimNodeId());
                if (target == null) {
                    continue;   // the victim finished while this attempt was in flight
                }
                switch (mutation.kind()) {
                    case CONSUME_PACING_SKIP -> target.consumePacingSkip();
                    case MARK_NON_PRODUCTIVE -> target.markNonProductiveSteal(snapshot);
                    case RECORD_FUTILE_STEAL -> target.recordFutileSteal();
                    case SET_UNSPLITTABLE -> target.setUnsplittable(true);
                    case RECORD_NO_PIVOT_TALLY -> target.recordNoPivot();
                    case RECORD_ZERO_FANOUT_STRUCTURE_PROBE -> target.recordZeroFanoutStructureProbe();
                    case RESET_ZERO_FANOUT_STRUCTURE_PROBES -> target.resetZeroFanoutStructureProbes();
                    case RECORD_STRUCTURE_SUPPRESSED_TALLY -> target.recordStructureSuppressed();
                }
            }
        }
    }

    // ---- shared executor mechanics ----------------------------------------------------------

    /** What a simulated call resolves to: an answer, or the budget running out first. */
    private interface CallOutcome {
        void onResponse(SimContext arrived);

        void onTimeout(SimContext at, int attemptNumber);
    }

    /** A probe's arrival handler — the store read happens here, at the instant the answer lands. */
    private interface ProbeResponse {
        void accept(SimContext arrived);
    }

    /**
     * Issues one store call and schedules its outcome.
     *
     * <p><b>Where the completion instant is known at issue</b> — the ordinary case, a store that
     * answers independently — exactly one event is scheduled: the response if the drawn duration fits
     * the attempt budget, the timeout if it does not. Nothing has to be cancelled, because nothing
     * competing was ever armed.
     *
     * <p><b>Where it is not</b> — a modelled store with a queue, whose answer depends on what else is
     * in flight — both are armed and the loser is retired when it fires, by comparing the call's
     * generation against the one that resolved first. That is what a discrete-event kernel without
     * cancellation costs: one extra dispatched event per timed-out call, counted as
     * {@link #STALE_EVENTS_COUNTER} rather than quietly absorbed into the run's event budget.
     */
    private void issueCall(SimContext ctx, CallClass callClass, long timeoutNanos, int attemptNumber,
                           CallOutcome outcome) {
        ctx.count(STORE_CALLS_COUNTER, 1);
        ctx.count("store.calls." + callClass.name().toLowerCase(Locale.ROOT), 1);
        int attempt = attemptNumber + 1;
        long serviceNanos = scenario.latency().drawNanos(callClass, ctx.rng(SimRngStream.LATENCY),
                callsInFlight);
        callsInFlight++;
        if (storeServer == null) {
            if (serviceNanos > timeoutNanos) {
                ctx.schedule(timeoutNanos, "call.timeout", at -> {
                    callsInFlight--;
                    outcome.onTimeout(at, attempt);
                });
            } else {
                ctx.schedule(serviceNanos, "call.response", at -> {
                    callsInFlight--;
                    outcome.onResponse(at);
                });
            }
            return;
        }
        boolean[] resolved = {false};
        storeServer.submit(ctx, serviceNanos, at -> {
            // Occupancy is retired HERE, on the store's own completion, whether or not the caller gave
            // up first: a call the client has timed out on is still work the store is doing, and it is
            // still crowding out the next one. Retiring it at the timeout instead would understate
            // occupancy by exactly the calls a struggling store is struggling with -- the one regime
            // where an occupancy-sensitive latency model has anything to say.
            callsInFlight--;
            if (resolved[0]) {
                at.count(STALE_EVENTS_COUNTER, 1);
                at.count("events.stale.store_response", 1);
                return;
            }
            resolved[0] = true;
            outcome.onResponse(at);
        });
        ctx.schedule(timeoutNanos, "call.timeout", at -> {
            if (resolved[0]) {
                at.count(STALE_EVENTS_COUNTER, 1);
                at.count("events.stale.timeout", 1);
                return;
            }
            resolved[0] = true;
            outcome.onTimeout(at, attempt);
        });
    }

    /** Re-issues a call after its transient backoff, keeping the attempt count. */
    private void retryCall(SimContext ctx, CallOutcome outcome, CallClass callClass, long timeoutNanos,
                           int attemptNumber) {
        ctx.schedule(scenario.budgets().transientRetryBackoffNanos(), "call.retry",
                at -> issueCall(at, callClass, timeoutNanos, attemptNumber, outcome));
    }

    /** Returns a page-fetch slot; whoever the target now has room for is handed one. */
    private void releaseSlot(SimContext ctx) {
        slotsHeld--;
        grantSlots(ctx);
    }

    /**
     * Hands out every slot the current target has room for, in the order workers began waiting.
     *
     * <p>Called both when a slot is returned and after the controller has had a chance to move the
     * target, because a growth step releases as many slots as it grew by: waking one worker per
     * completion would let the fleet lag its own target indefinitely on a run where completions are rare.
     * The slot is reserved here and the waiter is woken in its own event body, so the two workers never
     * hold one between them.
     */
    private void grantSlots(SimContext ctx) {
        while (!slotWaiters.isEmpty() && slotsHeld < gauge.effectiveT()) {
            Worker next = slotWaiters.removeFirst();
            slotsHeld++;
            int attempt = next.pendingAttempt;
            ctx.scheduleFor(next.id, 0, "slot.granted", granted -> next.enter(granted).issuePage(attempt));
        }
    }

    /**
     * Wakes every parked worker. Bumping the generation is what retires their outstanding park timers:
     * the timer still fires, sees a generation it does not recognise, and returns.
     */
    private void wakeParked(SimContext ctx) {
        if (parked.isEmpty()) {
            return;
        }
        parkGeneration++;
        List<Worker> woken = List.copyOf(parked);
        parked.clear();
        for (Worker worker : woken) {
            ctx.scheduleFor(worker.id, 0, "worker.wake", at -> worker.enter(at).idle(at));
        }
    }

    /**
     * A non-empty page commit is the fleet's signal that there is something worth stealing again: it
     * resets the idle-steal pacing ladder and wakes whoever was parked behind it.
     */
    private void signalStealableProgress(SimContext ctx) {
        pacingState = idlePacing.onReset();
        wakeParked(ctx);
    }

    /**
     * Reads the position sensor across one page commit: did the keys that just came out move the range's
     * cursor <em>in the fraction the policies measure</em>?
     *
     * <p>Only bounded ranges are read. An open frontier has no {@code hi} to define a window and always
     * scores {@code +∞} in selection, so a fraction over it would be measuring nothing.
     */
    private static void recordSensorReading(SimContext ctx, byte[] lo, byte[] cursorFrom, byte[] cursorTo,
                                            byte[] hi) {
        if (hi == null || cursorTo == null) {
            return;
        }
        ctx.count(SENSOR_BOUNDED_COMMITS_COUNTER, 1);
        if (StealMath.fracIn(cursorTo, lo, hi) - StealMath.fracIn(cursorFrom, lo, hi) <= 0.0) {
            ctx.count(SENSOR_INVISIBLE_ADVANCE_COUNTER, 1);
        }
    }

    /**
     * Reads {@code estRemaining} over the pool victim selection is about to rank, recording the two ways
     * it degenerates: a zero score, which takes a candidate out of the running entirely, and a zero
     * consumed span, which leaves the score a raw width with the candidate's emitted keys discarded.
     *
     * <p>This duplicates the arithmetic the policy is about to do rather than asking it what it saw:
     * the policy's contract is a {@link Selection}, and widening it to report its own intermediate
     * readings would put a diagnostic in the engine's decision seam. The inputs are the same view the
     * policy gets and the function is the same public one it calls.
     */
    private static void recordVictimSensorReadings(SimContext ctx, List<VictimView> pool) {
        for (VictimView candidate : pool) {
            if (candidate.hi() == null) {
                continue;
            }
            ctx.count(SENSOR_VICTIMS_SCANNED_COUNTER, 1);
            if (StealMath.estRemaining(candidate.cursor(), candidate.lo(), candidate.hi(),
                    candidate.keysEmitted()) <= 0.0) {
                ctx.count(SENSOR_EST_ZERO_COUNTER, 1);
            }
            if (StealMath.spanIn(candidate.lo(), candidate.cursor(), candidate.lo(), candidate.hi())
                    <= 0.0) {
                ctx.count(SENSOR_EST_IGNORES_KEYS_COUNTER, 1);
            }
        }
    }

    /** Records every engagement a policy fired, under the same category and reason the engine uses. */
    private static void recordEngagements(SimContext ctx, List<Engagement> engagements) {
        for (Engagement engagement : engagements) {
            ctx.count(engagement.category() + "." + engagement.reason(), 1);
        }
    }

    /**
     * The worker's <b>raw, pre-toggle</b> observed density ratio — what the view's contract asks for, and
     * what the engine passes it.
     *
     * <p>The accessor itself is package-private to the engine, so it is read through the one public
     * overload that returns it untouched: the all-on toggle namespace applies no substitution, so this is
     * the identical value, obtained without widening anything in swath-core. The scenario's own toggles
     * are deliberately not applied here — the governor applies them itself, and pre-applying them would
     * hand it a value its own contract says is raw.
     */
    private static double rawObservedDensityRatio(WorkerState state) {
        return EngineToggles.DEFAULT.observedDensityRatio(state);
    }

    /**
     * The engine's own clock seam, bound to the virtual clock. The pacing arithmetic takes its instant
     * as an argument, so this is the whole of what "injecting a clock" means here — and it is the only
     * clock anything in this file can reach.
     */
    private static DecisionClock clock(SimContext ctx) {
        return ctx::nowNanos;
    }
}

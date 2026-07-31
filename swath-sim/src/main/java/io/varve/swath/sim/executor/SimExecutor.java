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
 * Runs swath's shared seed, owner-split, thief and pacing policies against a ground-truth store in
 * virtual time. This executor owns scheduling, policy views, application of returned mutations, the
 * range ledger, and the simulator-side concurrency port.
 *
 * <p>One event body is one atomic region. In particular, page trim, cursor/density update, ledger
 * commit, sensor reading and owner decision share a body; thief probes deliberately separate its
 * snapshot from revalidation. See {@code docs/executor-ordering.md} for the full ordering, timing and
 * documented divergence contract.
 *
 * <p>A fixed scenario and seed against the same store state determine simulated event order. They do
 * not make live thread scheduling deterministic.
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
    /** Dispatched losers of uncancellable event races; included in the run's event budget. */
    public static final String STALE_EVENTS_COUNTER = "events.stale";
    /** Events that drain timed-out nonqueued calls; queued timeout losers count as stale instead. */
    public static final String OCCUPANCY_DRAIN_EVENTS_COUNTER = "events.timeout_occupancy_drain";
    /** Seed-descent probes issued. */
    public static final String SEED_PROBES_COUNTER = "seed.probes";
    /** Children published by the owner-side proactive split. */
    public static final String OWNER_SPLIT_COUNTER = "owner_split.published";
    /** Children published by a thief's steal. */
    public static final String THIEF_SPLIT_COUNTER = "steal.children";
    /** Engine category for owner-split engagements and refusals. */
    public static final String OWNER_SPLIT_CATEGORY = "OWNER_SPLIT";
    /** Completed owner-split children whose realized mass came back confetti-sized. */
    public static final String OWNER_SPLIT_CHILD_CONFETTI_COUNTER = "OWNER_SPLIT_CHILD.confetti";
    /** Completed owner-split children that came back substantial — the denominator's other half. */
    public static final String OWNER_SPLIT_CHILD_SUBSTANTIAL_COUNTER = "OWNER_SPLIT_CHILD.substantial";
    /** The prefix a steal attempt's terminal outcome is counted under; the outcome's name follows. */
    public static final String STEAL_OUTCOME_PREFIX = "steal.outcome.";
    /** The outcome of an attempt whose victim selection found nothing eligible to steal from. */
    public static final String NO_VICTIM_OUTCOME = "NO_VICTIM";
    /** Installed sensing routes, counted once per policy per run. */
    public static final String SENSING_ROUTE_CATEGORY = "SENSING_ROUTE";
    /** Stable schema name for the unsteered governor route, which uses legacy WINDOW sensing. */
    public static final String OWNER_SPLIT_ROUTE_SHIPPED = "owner_split_shipped";
    /** It ran on the sensor a variant installed through that seam. */
    public static final String OWNER_SPLIT_ROUTE_ESTIMATOR = "owner_split_estimator";
    /** Stable schema name for the unsteered thief route, which uses legacy WINDOW sensing. */
    public static final String THIEF_ROUTE_SHIPPED = "thief_shipped";
    /** It scored them on the sensor a variant installed through that seam. */
    public static final String THIEF_ROUTE_ESTIMATOR = "thief_estimator";
    /** Non-empty bounded page commits; denominator for invisible cursor advances. */
    public static final String SENSOR_BOUNDED_COMMITS_COUNTER = "sensor.bounded_page_commits";
    /** Non-empty bounded commits invisible to the installed estimator's position measure. */
    public static final String SENSOR_INVISIBLE_ADVANCE_COUNTER = "sensor.cursor_advance_invisible";
    /** Victims actually scored, after pre-score skips, summed over attempts. */
    public static final String SENSOR_VICTIMS_SCANNED_COUNTER = "sensor.victims_scanned";
    /** Scored victims with a bound; denominator for the victim degeneracy counters. */
    public static final String SENSOR_VICTIMS_BOUNDED_COUNTER = "sensor.victims_scanned_bounded";
    /** Scored bounded victims whose installed-estimator remaining-work score is zero. */
    public static final String SENSOR_EST_ZERO_COUNTER = "sensor.victim_est_zero";
    /** Scored bounded victims whose installed-estimator score ignores their emitted-key count. */
    public static final String SENSOR_EST_IGNORES_KEYS_COUNTER = "sensor.victim_est_ignores_keys";

    /**
     * The event-kind prefix a wake is traced under; the signal that caused it follows. Carried in the
     * kind rather than a detail string so a trace can be read for wake sources without parsing details.
     */
    public static final String WAKE_EVENT_PREFIX = "worker.wake.";
    /** A parked worker was woken because a split child was published and is claimable. */
    public static final String WAKE_CHILD_PUBLISHED = "child_published";
    /** A parked worker was woken because a range completed — a quiescence signal in the engine's ledger. */
    public static final String WAKE_RANGE_COMPLETED = "range_completed";
    /** A parked worker was woken because the fleet's single steal-attempt slot was released. */
    public static final String WAKE_STEAL_ATTEMPT_FINISHED = "steal_attempt_finished";
    /** A parked worker was woken by a non-empty page commit. */
    public static final String WAKE_PAGE_COMMITTED = "page_committed";

    private static final HexFormat HEX = HexFormat.of();

    /** Per-call-class counter names, precomputed for the issue path. */
    private static final String[] CALL_CLASS_COUNTERS = callClassCounters();

    private final PolicyScenario scenario;
    private final SensingVariant sensing;
    private final RemainingWorkEstimator estimator;
    /** Variant estimator, or null for the simulator's ordinary legacy-WINDOW route. */
    private final RemainingWorkEstimator installedEstimator;
    private final String storeLabel;
    private final SimListingView view;
    private final SimNodeLedger ledger = new SimNodeLedger();
    private final Map<Long, WorkerState> livePool = new LinkedHashMap<>();
    private final Set<Long> ownerSplitTaggedChildren = new HashSet<>();
    private final ConfettiFeedbackGate confettiFeedback = new ConfettiFeedbackGate();
    private final PolicyRunTimeline.Recorder timeline = new PolicyRunTimeline.Recorder();
    /**
     * Opt-in decision dump; null avoids formatting work when disabled. Always closed by
     * {@link #execute()}.
     */
    private final SimGateDump gateDump = SimGateDump.fromSystemProperties();
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

    private SimExecutor(PolicyScenario scenario, ListingStore store, String storeLabel,
                        SensingVariant sensing) {
        this.scenario = scenario;
        this.sensing = sensing;
        this.estimator = sensing.estimator(scenario.pageSize());
        // CURRENT leaves the seam unsteered, selecting the simulator's legacy WINDOW control.
        this.installedEstimator = sensing == SensingVariant.CURRENT ? null : estimator;
        this.storeLabel = storeLabel;
        this.view = new SimListingView(store, scenario.scanPrefix());
        this.governor = new OwnerSplitGovernor(scenario.toggles(), scenario.workerCount(),
                scenario.pageSize(), installedEstimator);
        this.seedPlanner = new HybridSeedPlanner(scenario.scanPrefix(), scenario.workerCount(),
                scenario.toggles());
        // AIMD jitter belongs to the fleet's reserved stream, isolated from every worker's draws.
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
     * Runs {@code scenario} on the simulator's ordinary {@link SensingVariant#CURRENT} arm.
     *
     * @param store      caller-owned handle; borrowed, never opened or closed here
     * @param storeLabel resolved backend or fixture label recorded with the result
     */
    public static PolicyRunResult run(PolicyScenario scenario, ListingStore store, String storeLabel) {
        return run(scenario, store, storeLabel, SensingVariant.CURRENT);
    }

    /**
     * Runs {@code scenario} on an explicit simulator sensing arm.
     *
     * @param sensing position sensor read by thief selection and owner-split gates
     * @see #run(PolicyScenario, ListingStore, String)
     */
    public static PolicyRunResult run(PolicyScenario scenario, ListingStore store, String storeLabel,
                                      SensingVariant sensing) {
        if (store == null) {
            throw new IllegalArgumentException("the run API takes an already-open store handle");
        }
        if (storeLabel == null || storeLabel.isBlank()) {
            throw new IllegalArgumentException("a run record must say which store served it");
        }
        if (sensing == null) {
            throw new IllegalArgumentException("a run must say which position sensor it steers on");
        }
        scenario.clientCost().requireReadyForNewRun();
        SimExecutor executor = new SimExecutor(scenario, store, storeLabel, sensing);
        return executor.execute();
    }

    private PolicyRunResult execute() {
        log = scenario.recordEventLog() ? SimEventLog.recording() : SimEventLog.disabled();
        SimKernel kernel = new SimKernel(scenario.seed(), scenario.budgets(), log, scenario.maxEvents());
        kernel.scheduleBootstrap(0, 0, "seed.start", this::startSeedPhase);
        SimRunResult result;
        try {
            result = kernel.run();
        } finally {
            // Preserve an opt-in dump even when execution fails.
            if (gateDump != null) {
                gateDump.close();
            }
        }
        return PolicyRunResult.of(result, scenario, storeLabel, gauge.counters(), gauge.effectiveT(),
                ledger.nodesCreated(), ledger.splitsAborted(), view.storeReads(), runStuck,
                timeline.finish(result.wallNanos()), sensing);
    }

    // ---- seed phase ---------------------------------------------------------------------

    /** Runs timed seed probes before workers; abandonment falls back to the whole keyspace. */
    private void startSeedPhase(SimContext ctx) {
        recordSensingRoutes(ctx);
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
                        // Probe-class transients affect attribution, not AIMD backpressure.
                        gauge.onTransientTimeout(at.nowNanos(), false);
                        at.count(PROBE_TIMEOUTS_COUNTER, 1);
                        if (attempt > scenario.budgets().probeAttemptRetryCap()) {
                            // A partial descent cannot tile safely; start one whole-keyspace range.
                            at.record("seed.abandoned", "probe_retry_cap");
                            seedRanges(at, List.of());
                            return;
                        }
                        retryCall(at, this, CallClass.SEED_PROBE,
                                scenario.budgets().probeAttemptTimeoutNanos(), attempt);
                    }
                });
    }

    /** Tiles the cuts into seed ranges, then starts the fleet. */
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

    /** Simulated claimant, drainer and idle thief. */
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
            // Each worker owns its decision RNG so one actor's path cannot shift another's draws.
            DecisionRng rng = bound -> ctx.rng(SimRngStream.STEAL_DECISION).nextInt(bound);
            this.thief = new ThiefPolicy(scenario.toggles(), scenario.scanPrefix(), rng,
                    installedEstimator);
        }

        /** Binds this worker to the current event body's reused context. */
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

        /** Issues a worker page only after acquiring an adaptive-concurrency slot. */
        private void requestPage(int attemptNumber) {
            if (slotsHeld >= gauge.effectiveT()) {
                pendingAttempt = attemptNumber;
                slotWaiters.add(this);
                if (log.isRecording()) {
                    // Avoid trace formatting on the frequent denied-slot path.
                    ctx.record("slot.wait", "worker=" + id);
                }
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
                            // A target increase makes slots available immediately.
                            grantSlots(arrived);
                            onPage(arrived);
                        }

                        @Override
                        public void onTimeout(SimContext at, int attempt) {
                            enter(at);
                            releaseSlot(at);
                            at.count(PAGE_TIMEOUTS_COUNTER, 1);
                            // Worker timeouts feed AIMD, but their censored durations are not samples.
                            gauge.onTransientTimeout(at.nowNanos(), true);
                            if (scenario.faultDisposition() == PolicyScenario.FaultDisposition.BOUNDED
                                    && attempt > scenario.budgets().workerAttemptRetryCap()) {
                                // BOUNDED stops here; RIDE_OUT ignores the worker threshold.
                                runStuck = true;
                                at.record("run.stuck", "worker_attempt_retry_cap");
                                return;
                            }
                            at.schedule(scenario.budgets().transientRetryBackoffNanos(), "page.retry",
                                    retry -> enter(retry).requestPage(attempt));
                        }
                    });
        }

        /** Commits page trim, ledger state, sensor state and owner decision in one event body. */
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
                // Intervals expose gaps/overlaps; guard their key formatting when tracing is off.
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
            // Charge modelled client stages only after the atomic commit body.
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
            // Completion wakes parked workers so they can observe work or quiescence.
            wakeParked(at, WAKE_RANGE_COMPLETED);
            if (remaining == 0L) {
                timeline.quiesced(at.nowNanos());
                at.record("run.quiescent", "");
            }
            idle(at);
        }

        // ---- owner-side proactive split ---------------------------------------------------

        private void maybeOwnerSelfSplit(SimContext at, byte[] cursorTo) {
            byte[] hi = state.hi();
            // Open frontiers have no carveable far tail and do not spend the rate-limit counter.
            long committed = hi == null ? committedPages : ++committedPages;
            ConfettiFeedbackGate.Snapshot confetti = confettiFeedback.snapshot();
            OwnerSplitView splitView = new OwnerSplitView(hi, state.lo(), cursorTo, state.keysEmitted(),
                    committed, lastSelfSplitPage, ledger.outstanding(), state.densityFraction(),
                    rawObservedDensityRatio(state),
                    state.alphabetDigest().snapshot(),
                    new ConfettiObservation(confetti.taggedTotal(), confetti.taggedConfetti(),
                            confetti.probeSeq()));
            OwnerSplitDecision decision = governor.decide(splitView);
            if (gateDump != null) {
                gateDump.ownerDecision(at.nowNanos(), nodeId, decision.gateInputs(), state.lo(),
                        cursorTo, hi);
            }
            // Claim the run-scoped probe slot before crediting the decision's engagements.
            boolean probeSlotClaimed = false;
            if (decision instanceof Carve
                    && decision.mutations().contains(OwnerSplitMutation.CLAIM_CONFETTI_PROBE_SLOT)) {
                if (confettiFeedback.claimProbeSlot(confetti.probeSeq())) {
                    probeSlotClaimed = true;
                } else {
                    at.count(OWNER_SPLIT_CATEGORY + "." + OwnerSplitSkipReason.CONFETTI_SUPPRESSED.code(), 1);
                    recordOwnerSplitSkip(at, OwnerSplitSkipReason.CONFETTI_SUPPRESSED);
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
            if (decision instanceof Skip skipped) {
                recordOwnerSplitSkip(at, skipped.reason());
                return;
            }
            byte[] pivot = ((Carve) decision).pivot();
            // The owner chooses and publishes while its cursor/bound state is atomic.
            state.narrowHi(pivot);
            long childId = ledger.splitNode(nodeId, pivot, hi);
            if (childId == SimNodeLedger.SPLIT_ABORTED) {
                state.restoreHi(hi);
                at.count(OWNER_SPLIT_CATEGORY + ".self_aborted", 1);
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
            at.count(OWNER_SPLIT_CATEGORY + ".self_published", 1);
            at.record("owner_split", "node=" + nodeId + "|child=" + childId);
            // Publication makes the child claimable before the wake is scheduled.
            wakeParked(at, WAKE_CHILD_PUBLISHED);
        }

        /** Traces an owner refusal against its range for per-range attribution. */
        private void recordOwnerSplitSkip(SimContext at, OwnerSplitSkipReason reason) {
            if (log.isRecording()) {
                at.record("owner_split.skip", "node=" + nodeId + "|reason=" + reason.code());
            }
        }

        /** Classifies an unsplit, small owner child as confetti for the next carve's feedback. */
        private void classifyOwnerSplitChild(SimContext at) {
            if (!scenario.toggles().confettiFeedback() || !ownerSplitTaggedChildren.remove(nodeId)) {
                return;
            }
            boolean confetti = state.keysEmitted() <= 2L * scenario.pageSize() && !state.hasSplit();
            confettiFeedback.recordCompletion(confetti);
            at.count(confetti ? OWNER_SPLIT_CHILD_CONFETTI_COUNTER
                    : OWNER_SPLIT_CHILD_SUBSTANTIAL_COUNTER, 1);
        }

        // ---- the thief ---------------------------------------------------------------------

        private void tryStealOrPark() {
            if (!gauge.isStealingAllowed()) {
                park(scenario.budgets().idleStealBaseParkNanos());
                return;
            }
            if (stealAttemptInFlight) {
                // The fleet permits one probe cascade at a time.
                ctx.count("IDLE_SLOT.in_flight", 1);
                park(scenario.budgets().idleStealAttemptParkNanos());
                return;
            }
            long now = clock(ctx).nanoTime();
            if (idlePacing.decide(pacingState, now) == IdleStealPacingDecision.PACED) {
                // Pacing check-and-act is one executor step, matching the policy seam.
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
                if (gateDump != null) {
                    gateDump.victimScan(ctx.nowNanos(), selection.scan(), SimGateDump.NO_CHOSEN_VICTIM,
                            noVictim.reason().code(), null, null, null);
                }
                finishSteal(NO_VICTIM_OUTCOME, noVictim.reason().code(), false);
                return;
            }
            victim = livePool.get(((Selected) selection).victimNodeId());
            if (gateDump != null) {
                gateDump.victimScan(ctx.nowNanos(), selection.scan(), victim.nodeId(), null,
                        victim.lo(), victim.cursor(), victim.hi());
            }
            // Snapshot cursor and bound coherently; later bodies must revalidate both.
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
                    // Any structure response ends this victim's consecutive-timeout streak.
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
         * Issues a probe outside worker-page slot gating. Point probes and bounded delimiter scans alike
         * obey the declared probe retry cap.
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
                    // Probe-class transients affect attribution, not AIMD backpressure.
                    gauge.onTransientTimeout(at.nowNanos(), false);
                    if (!keyProbe) {
                        // Preserve timeout evidence used by structure-probe suppression.
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

        /** Reissues a probe with the chain's attempt number and declared retry cap. */
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
                                // Every timed-out attempt contributes suppression evidence.
                                victim.recordTimedOutStructureProbe();
                                at.count("STRUCTURE.probe_timed_out", 1);
                            }
                            if (attempt > scenario.budgets().probeAttemptRetryCap()) {
                                finishSteal("RETRY", "probe_retry_cap_failfast", false);
                                return;
                            }
                            at.schedule(scenario.budgets().transientRetryBackoffNanos(), "probe.retry",
                                    retry -> enter(retry).probeRetry(callClass, onArrival, keyProbe, attempt));
                        }
                    });
        }

        /** Revalidates the snapshotted bound and pivot against the victim's current state. */
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
                // Restore after the ledger guard. Futility attribution differs from production; see
                // docs/executor-ordering.md.
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
            // Attribute wakes to claimable-child publication before releasing the attempt slot.
            wakeParked(ctx, WAKE_CHILD_PUBLISHED);
            finishSteal("CHILD_CREATED", "split_committed", true);
        }

        private void finishSteal(String outcome, String reason, boolean productive) {
            ctx.count(STEAL_OUTCOME_PREFIX + outcome, 1);
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
            // Release the attempt slot before waking its waiters.
            wakeParked(ctx, WAKE_STEAL_ATTEMPT_FINISHED);
            idle(ctx);
        }

        // ---- parking ------------------------------------------------------------------------

        private void park(long nanos) {
            long generation = parkGeneration;
            parked.add(this);
            ctx.schedule(Math.max(1L, nanos), "worker.park", woken -> {
                if (generation != parkGeneration) {
                    // A wake retires its uncancellable timer by advancing the park generation.
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
                    continue;   // victim completed while the attempt was in flight
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

    /** A simulated call resolves to either an answer or its attempt timeout. */
    private interface CallOutcome {
        void onResponse(SimContext arrived);

        void onTimeout(SimContext at, int attemptNumber);
    }

    /** Probe arrival handler; the store is read when the answer lands. */
    private interface ProbeResponse {
        void accept(SimContext arrived);
    }

    /**
     * Issues one store call. Independent calls schedule one client outcome plus any later occupancy
     * drain; queued calls arm response and timeout and count the loser stale. Occupancy always ends at
     * store completion. See {@code docs/executor-ordering.md}.
     */
    private void issueCall(SimContext ctx, CallClass callClass, long timeoutNanos, int attemptNumber,
                           CallOutcome outcome) {
        ctx.count(STORE_CALLS_COUNTER, 1);
        ctx.count(CALL_CLASS_COUNTERS[callClass.ordinal()], 1);
        int attempt = attemptNumber + 1;
        // Draw against existing occupancy; this call is excluded from its own input.
        long serviceNanos = scenario.latency().drawNanos(callClass, ctx.rng(SimRngStream.LATENCY),
                callsInFlight);
        callsInFlight++;
        if (storeServer == null) {
            if (serviceNanos > timeoutNanos) {
                ctx.schedule(timeoutNanos, "call.timeout", at -> outcome.onTimeout(at, attempt));
                // Client timeout does not end the store's occupancy.
                ctx.schedule(serviceNanos, "call.service_completed", at -> {
                    at.count(OCCUPANCY_DRAIN_EVENTS_COUNTER, 1);
                    callsInFlight--;
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
            // Queued work retires occupancy at store completion even after a client timeout.
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

    /** Reserves every available slot FIFO before scheduling its waiter, including target growth. */
    private void grantSlots(SimContext ctx) {
        while (!slotWaiters.isEmpty() && slotsHeld < gauge.effectiveT()) {
            Worker next = slotWaiters.removeFirst();
            slotsHeld++;
            int attempt = next.pendingAttempt;
            ctx.scheduleFor(next.id, 0, "slot.granted", granted -> next.enter(granted).issuePage(attempt));
        }
    }

    /** Wakes parked workers, retiring their timers; {@code reason} is encoded in the event kind. */
    private void wakeParked(SimContext ctx, String reason) {
        if (parked.isEmpty()) {
            return;
        }
        parkGeneration++;
        List<Worker> woken = List.copyOf(parked);
        parked.clear();
        for (Worker worker : woken) {
            ctx.scheduleFor(worker.id, 0, WAKE_EVENT_PREFIX + reason, at -> worker.enter(at).idle(at));
        }
    }

    /** Resets fleet pacing and wakes thieves after a non-empty commit. */
    private void signalStealableProgress(SimContext ctx) {
        pacingState = idlePacing.onReset();
        wakeParked(ctx, WAKE_PAGE_COMMITTED);
    }

    /** Records whether a non-empty bounded commit advances the installed estimator's position. */
    private void recordSensorReading(SimContext ctx, byte[] lo, byte[] cursorFrom, byte[] cursorTo,
                                     byte[] hi) {
        if (hi == null || cursorTo == null) {
            return;
        }
        ctx.count(SENSOR_BOUNDED_COMMITS_COUNTER, 1);
        if (!estimator.advanceVisible(lo, cursorFrom, cursorTo, hi)) {
            ctx.count(SENSOR_INVISIBLE_ADVANCE_COUNTER, 1);
        }
    }

    /**
     * Mirrors the policy's pre-score skips without consuming returned mutations, then asks the
     * installed estimator for the diagnostics selection is about to read.
     */
    private void recordVictimSensorReadings(SimContext ctx, List<VictimView> pool) {
        for (VictimView candidate : pool) {
            if (candidate.unsplittable() || candidate.pacingSkipAvailable()) {
                continue;
            }
            ctx.count(SENSOR_VICTIMS_SCANNED_COUNTER, 1);
            if (candidate.hi() == null) {
                continue;
            }
            ctx.count(SENSOR_VICTIMS_BOUNDED_COUNTER, 1);
            if (estimator.estRemaining(candidate.cursor(), candidate.lo(), candidate.hi(),
                    candidate.keysEmitted()) <= 0.0) {
                ctx.count(SENSOR_EST_ZERO_COUNTER, 1);
            }
            if (estimator.ignoresEmittedKeys(candidate.cursor(), candidate.lo(), candidate.hi())) {
                ctx.count(SENSOR_EST_IGNORES_KEYS_COUNTER, 1);
            }
        }
    }

    private static String[] callClassCounters() {
        CallClass[] classes = CallClass.values();
        String[] names = new String[classes.length];
        for (int i = 0; i < classes.length; i++) {
            names[i] = "store.calls." + classes[i].name().toLowerCase(Locale.ROOT);
        }
        return names;
    }

    /**
     * Counts the installed owner and thief sensing routes once per run. The stable {@code *_SHIPPED}
     * schema names denote this simulator's unsteered legacy-WINDOW route, not production's 0.2.0
     * {@code RATE_ANCHORED_FLOOR_QUARTER} default.
     */
    private void recordSensingRoutes(SimContext ctx) {
        boolean steered = installedEstimator != null;
        ctx.count(SENSING_ROUTE_CATEGORY + "."
                + (steered ? OWNER_SPLIT_ROUTE_ESTIMATOR : OWNER_SPLIT_ROUTE_SHIPPED), 1);
        ctx.count(SENSING_ROUTE_CATEGORY + "."
                + (steered ? THIEF_ROUTE_ESTIMATOR : THIEF_ROUTE_SHIPPED), 1);
    }

    /** Records every engagement a policy fired, under the same category and reason the engine uses. */
    private static void recordEngagements(SimContext ctx, List<Engagement> engagements) {
        for (Engagement engagement : engagements) {
            ctx.count(engagement.category() + "." + engagement.reason(), 1);
        }
    }

    /** Returns the owner view's raw, pre-toggle observed density ratio. */
    private static double rawObservedDensityRatio(WorkerState state) {
        return EngineToggles.DEFAULT.observedDensityRatio(state);
    }

    /** Binds the policy clock seam to virtual time. */
    private static DecisionClock clock(SimContext ctx) {
        return ctx::nowNanos;
    }
}

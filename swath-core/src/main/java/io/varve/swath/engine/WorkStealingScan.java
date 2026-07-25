/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import io.micrometer.core.instrument.Timer;
import io.varve.swath.checkpoint.CheckpointStore;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.PageCommit;
import io.varve.swath.concurrent.Scope;
import io.varve.swath.error.CancelledException;
import io.varve.swath.error.SwathException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ListingMode;
import io.varve.swath.model.PackedPage;
import io.varve.swath.model.PageBatch;
import io.varve.swath.model.PagePacker;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.TraceSink;
import io.varve.swath.pipeline.Channel;
import io.varve.swath.pipeline.Item;
import io.varve.swath.pipeline.Pipeline;
import io.varve.swath.runtime.CancellationToken;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.store.PageFetcher;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code WorkStealingScan} engine driver (algorithms.md §2/§3/§4.4).
 * Ties the committed units — {@link RangeScanner}, {@link WorkerState},
 * the {@link CheckpointStore} writer, and the {@link Thief} — into one
 * running parallel scan, and returns <b>only at quiescence</b> so the
 * {@link Pipeline} can append the single {@code End}.
 *
 * <p><b>Worklist, not fan-out (I7/CONC-2).</b> A fixed pool of {@code workerCount}
 * (= the configured {@code T}) virtual-thread workers <i>is</i> the concurrency
 * bound. A worker never holds its slot while waiting on child work — it claims the
 * next node from an in-memory ready queue, or (when none is claimable) becomes a
 * {@link Thief} that splits the busiest live worker. This dissolves the
 * permit-held-across-{@code join()} deadlock class.
 *
 * <p><b>Per-page ordering (algorithms.md §4.1, I1).</b> For each page the worker,
 * <i>under {@link WorkerState#lock()}</i>, advances the leading {@code cursor} and
 * <i>enqueues</i> the durable commit; it then <b>releases the lock</b> and only
 * <i>then</i> awaits durability ({@code future.get()}) outside the lock before
 * pushing the page downstream. Holding the lock across the await would serialize a
 * victim's commits and gate every thief.
 *
 * <p><b>Quiescence (algorithms.md §4.4).</b> The {@link Worklist} ledger's {@code outstanding}
 * counts live nodes: incremented when a node is created (seed at
 * seed time; a split child <b>inside the victim lock, before it is released</b>, via
 * {@link #enqueueChild}), decremented when a node is COMPLETED. The run ends when
 * {@code outstanding == 0}. An empty ready queue alone is <b>not</b> termination — an
 * idle worker that can neither claim nor steal parks on the ledger's condition, which is
 * signalled on <b>every enqueue and every decrement</b>; the decrement that reaches
 * zero broadcasts, releasing all parked workers to terminate. The child increment
 * happens under the victim lock before the victim can complete-and-decrement, so
 * quiescence never observes a false zero.
 */
public final class WorkStealingScan implements Pipeline.Producer<PageBatch> {

    private static final Logger log = LoggerFactory.getLogger(WorkStealingScan.class);

    /** Idle-poll backstop so a worker re-attempts a steal once a busy victim has advanced
     *  into a splittable shape (no enqueue/decrement signal fires for that case). */
    private static final long PARK_NANOS = TimeUnit.MILLISECONDS.toNanos(5);
    /** Cap for shared idle-steal backoff after consecutive non-productive steal attempts. */
    private static final long IDLE_STEAL_BACKOFF_CAP_NANOS = TimeUnit.MILLISECONDS.toNanos(50);
    /**
     * Park backstop for a worker denied the sole in-flight steal slot. Sized to the seconds-scale a
     * real probe can take, not to {@link #PARK_NANOS}: the slot's release broadcasts on the ledger,
     * so this bounds the wait for an attempt that outlives it rather than ending the ordinary wait,
     * while polling at the 5 ms base produced ~11k denials/sec against a multi-second probe.
     * Quiescence is unaffected — enqueue/decrement/progress all broadcast.
     */
    private static final long IDLE_STEAL_ATTEMPT_PARK_NANOS = TimeUnit.SECONDS.toNanos(1);

    private final PageFetcher fetcher;
    private final CheckpointStore store;
    private final long runId;
    private final byte[] prefix;
    private final ListingMode mode;
    private final int workerCount;
    private final int maxKeys;
    private final List<Node> seeds;
    private final FilterChain filters;
    private final RunMetrics metrics;
    /**
     * The {@code --engine-toggle} ablation namespace ({@link EngineToggles}). {@code
     * ownerSplit} is the same master switch the old {@code --no-owner-split} kill-switch controlled
     * (that flag is now a documented alias for {@code owner_split=off} — single source of truth
     * here); {@code density_ewma}/{@code far_ahead}/{@code alphabet_pivots} additionally bear on
     * {@link OwnerSelfSplit#maybeOwnerSelfSplit}'s pivot synthesis, and {@code structure_probes} is threaded into
     * {@link #thief}.
     */
    private final EngineToggles toggles;
    /**
     * The {@code --trace <file>} flight recorder seam: {@link TraceSink#NONE} unless the CLI wired a
     * real sink onto the {@link EngineContext}, mirroring how {@link #toggles} is threaded.
     */
    private final TraceSink trace;
    /**
     * The transient-retry policy + injectable backoff sleep the {@link GaugedFetcher}s use.
     * The CLI threads an EXPLICIT config (in {@code ListCommand.retryConfig()}): {@link
     * RetryPolicy#RIDE_OUT} when a real watchdog is armed (it owns storm death) else {@link
     * RetryPolicy#BOUNDED} when both watchdog windows are disabled, and a test threads a no-op
     * sleeper, both onto the {@link EngineContext} like {@link #trace}. The implicit {@link
     * RetryConfig#DEFAULT} (used only when no config is threaded) is {@code BOUNDED}: never an
     * owner-less unbounded ride-out.
     */
    private final RetryConfig retryConfig;

    private final Thief thief;
    private final ConcurrencyGauge gauge;
    private final IdleStealBackoff idleStealBackoff;
    /**
     * The DEDICATED, fail-soft, off-gauge
     * fetcher every worker's {@link RangeScanner}/{@link SpeculativeReadahead} uses for guess-ahead
     * fetches — constructed exactly like {@link #thief}'s own probe fetcher (see its javadoc, and
     * {@link SpeculativeReadahead}'s fetcher-isolation class-javadoc paragraph). Shared across every worker
     * (like {@link #thief} itself) since {@link GaugedFetcher} is already safe for concurrent callers.
     */
    private final PageFetcher speculativeFetcher;

    // --- worklist state -------------------------------------------------------
    /** The quiescence ledger: the ready queue + {@code outstanding} counter and their transitions. */
    private final Worklist worklist = new Worklist();
    /** The thief's victim pool: workers currently running a range (weakly-consistent iteration). */
    private final Set<WorkerState> livePool = ConcurrentHashMap.newKeySet();
    /**
     * Multi-worker seed-consumption signal (deterministic, structure-level — NOT a timing/
     * in-flight fraction). {@link #seedNodeIds} is the id set of the SEED (initial worklist) nodes;
     * {@link #distinctSeedClaimWorkers} is the set of distinct worker ids that have claimed at least
     * one of them. The first time a NEW worker claims a seed node, the run fires
     * {@code SEED_SCHED.distinct_seed_worker} exactly once, so that counter's value is the COUNT of
     * distinct workers that consumed a seed range: a forced-serial run ({@code workerCount == 1})
     * reads 1; a concurrent run that actually spreads a banded heavy-tail's seed ranges across the
     * pool reads many. This is the offline, deterministic replacement for a CI-flaky
     * {@code avg_in_flight} timing proxy. Pure observation — both sets
     * are thread-safe and this has ZERO effect on scheduling/stealing. Excludes
     * thief/owner-split children (only initial seed ids are tracked), so a collapsed single-heavy-seed
     * shape that only parallelizes via later child splits does NOT inflate the distinct-seed-worker
     * count.
     */
    private final Set<Long> seedNodeIds = ConcurrentHashMap.newKeySet();
    private final Set<Long> distinctSeedClaimWorkers = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean receiverGone = new AtomicBoolean();
    /**
     * The owner-side proactive self-split policy ({@link OwnerSelfSplit}) — the far-ahead
     * page-commit carve, its demand/floor/confetti gates, and the tagged-child realized-mass
     * feedback loop. Constructed once per run below; invoked from {@link #runClaim}'s page-commit
     * callback and, for tagged-child classification, from its node-completion site.
     */
    private final OwnerSelfSplit ownerSelfSplit;
    /**
     * The run's cancellation token, bound at {@link #produce} entry so the throttle-retry loop in
     * {@link GaugedFetcher} can abort promptly on {@code --max-duration}/SIGTERM. Read via a
     * supplier by both the worker and thief fetchers; {@code null} only before the first
     * {@code produce} call (no fetch happens then).
     */
    private volatile CancellationToken cancellation;

    /**
     * The {@code --sort} page packer, or {@code null} for every non-sort run.
     * The page-build site below feeds the text/parquet-direct pipelines too, so it packs on
     * the fetch worker ONLY when this is non-null (set once by the sort wiring via {@link #enableSortPacking}
     * before {@link #produce} forks any worker, so the thread-start happens-before edge publishes it to the
     * workers; {@code volatile} is belt-and-suspenders). When null, pages carry a raw {@link ListEntry} list
     * exactly as before — the non-sort path is byte-for-byte unchanged.
     */
    private volatile PagePacker pagePacker;

    /**
     * Enable pack-on-fetch for a {@code --sort} run — the fetch worker packs each built page into
     * a {@link PackedPage} (parallelizing packing across workers and shrinking in-flight memory) instead of
     * shipping a raw entry list to the single sort drain thread. Called by the sort wiring before
     * {@link #produce}; a no-arg-equivalent {@code null} leaves the non-sort behaviour untouched.
     */
    public void enableSortPacking(PagePacker packer) {
        this.pagePacker = packer;
    }

    /**
     * The single public construction path: the run-scoped {@link EngineContext} (run identity, metrics,
     * and the toggle/trace/retry policy seams, each already null-defaulted to its canonical value there)
     * plus this run's unit-specific collaborators — the fetcher, store, worker count, key budget, seeds,
     * and filters. The cancellation token is not a parameter — it is bound late at {@link #produce} and
     * read here through the {@code () -> cancellation} supplier the fetchers close over.
     */
    public WorkStealingScan(EngineContext context, PageFetcher fetcher, CheckpointStore store,
                            int workerCount, int maxKeys, List<Node> seeds, FilterChain filters) {
        this(context, fetcher, store, workerCount, maxKeys, seeds, filters, null);
    }

    /**
     * The test-only construction seam: as {@link #WorkStealingScan(EngineContext, PageFetcher,
     * CheckpointStore, int, int, List, FilterChain)}, but threads a {@link GaugeClock} into the
     * {@link ConcurrencyGauge} so a regime/composition test can COMPRESS the gauge's real wall-clock
     * adaptive windows (shed/clean/growth-freeze/pace/latency-decay) to the ~100&nbsp;ms scale a fast
     * offline run can span. A {@code null} {@code gaugeClock} means production (the gauge builds its own
     * {@code System::nanoTime} + jittered-window defaults, zero behavior change) — the public path
     * above passes {@code null}. Package-private because the only callers are in-package engine tests.
     */
    WorkStealingScan(EngineContext context, PageFetcher fetcher, CheckpointStore store,
                     int workerCount, int maxKeys, List<Node> seeds, FilterChain filters,
                     GaugeClock gaugeClock) {
        this.fetcher = fetcher;
        this.store = store;
        this.runId = context.runId();
        this.prefix = context.prefix();
        this.mode = context.mode();
        this.workerCount = Math.max(1, workerCount);
        this.maxKeys = maxKeys;
        this.seeds = List.copyOf(seeds);
        this.filters = filters;
        this.metrics = context.metrics();
        this.toggles = context.toggles();
        this.trace = context.trace();
        this.retryConfig = context.retryConfig();
        recordDisabledToggleMarks(this.toggles, this.metrics);
        this.metrics.setStrategy("WORK_STEALING");
        this.metrics.setPrefix(this.prefix);
        // Wire the live-worklist snapshot source for the terminal/mid-run slow_ranges[] dump —
        // observation only, read only when a summary is built (never on the hot path).
        this.metrics.registerRangeSnapshotSource(this::snapshotLiveRanges);
        this.idleStealBackoff = new IdleStealBackoff(
                PARK_NANOS, IDLE_STEAL_BACKOFF_CAP_NANOS, IDLE_STEAL_ATTEMPT_PARK_NANOS, this.metrics);
        // Production (gaugeClock == null) uses the gauge's own System::nanoTime + jittered-window
        // defaults, byte-for-byte the plain path; a regime test injects a compressed clock/window.
        this.gauge = gaugeClock == null
                ? new ConcurrencyGauge(this.workerCount, this.metrics)
                : new ConcurrencyGauge(this.workerCount, this.metrics, gaugeClock.nanoClock(),
                        gaugeClock.shedWindowNanosSupplier(), ConcurrencyGauge.defaultInitialT(this.workerCount));
        // The thief probes through a throttle-aware (but NOT slot-gated) wrapper: a 503 reaching a
        // probe must drive AIMD + retry the same probe (the probe is not lost), yet probes stay off
        // the concurrency gate (rare 1-key calls; stealing is paused separately via the gauge flag).
        this.thief = new Thief(store, new GaugedFetcher(fetcher, gauge, false, false, this.metrics,
                () -> cancellation, this.retryConfig.sleeper(), this.retryConfig.policy()), this.runId, this.prefix,
                this.mode, this::enqueueChild, this.metrics, this.toggles, this.trace);
        // Speculative readahead's guess-ahead
        // fetches get their OWN off-gauge, fail-soft fetcher — the IDENTICAL construction as the
        // Thief's probe fetcher just above (slotGated=false, reportSuccess=false): a disposable guess
        // must never consume a concurrency-gauge slot, vote AIMD on the happy path, or — under
        // RetryPolicy.BOUNDED — exhaust the transient-retry cap and cancel the WHOLE RUN as a side
        // effect (that give-up path is reachable only past MAX_TRANSIENT_RETRIES, which slotGated=false
        // fetches never reach — they fail fast on their own small PROBE_TRANSIENT_RETRY_CAP first). See
        // SpeculativeReadahead's class javadoc for the full rationale.
        this.speculativeFetcher = new GaugedFetcher(fetcher, gauge, false, false, this.metrics,
                () -> cancellation, this.retryConfig.sleeper(), this.retryConfig.policy());
        // The owner-side self-split policy shares this engine's live state through use-time
        // suppliers (never pre-evaluated): the outstanding demand-gate count and the gauge's
        // effective T are read at the instant a carve is considered, and the child hand-off is the
        // identical ready-queue/outstanding path a thief uses.
        this.ownerSelfSplit = new OwnerSelfSplit(this.runId, this.workerCount, this.maxKeys, this.store,
                this.toggles, this.metrics, this.trace, worklist::outstanding, gauge::effectiveT, this::enqueueChild);
    }

    /**
     * The {@code TOGGLE.owner_split_off}/{@code
     * TOGGLE.confetti_feedback_off}/{@code TOGGLE.reflect_lift_off} engagement marks (§5 discipline), fired
     * exactly once (constructor time — "scan start") when disabled, so post-hoc analysis never has
     * to infer the ablation from the absence of {@code OWNER_SPLIT.*} alone (zero could otherwise
     * mean either "toggled off" or "never fired"). All three are this engine's OWN mechanisms (never
     * {@link Thief}'s) — {@code confetti_feedback} and {@code reflect_lift} bear only on
     * {@link OwnerSelfSplit#maybeOwnerSelfSplit}'s mechanisms, so they are marked here alongside
     * {@code owner_split} rather than in {@link Thief}'s constructor — unlike the other five toggles
     * ({@code density_ewma}/{@code structure_probes}/{@code far_ahead}/{@code alphabet_pivots}/
     * {@code reflect}), which are marked once from {@link Thief}'s constructor instead —
     * {@link Thief} is constructed exactly once per engine instance, so that still fires exactly once
     * per run without duplicating the mark here. {@code radix_bands} is {@link SeedStep}'s own toggle
     * (fired there — it has no bearing on this engine at all). The toggle-name → mark-string
     * derivation itself is single-sourced in {@link EngineToggles#recordOffMarks} — this method only
     * lists which of the ten ablation toggles this engine owns.
     */
    private static void recordDisabledToggleMarks(EngineToggles toggles, RunMetrics metrics) {
        toggles.recordOffMarks(metrics, "owner_split", "confetti_feedback", "reflect_lift");
        // Readahead is opt-in (default OFF), so — inverted from the *_off ablation marks above —
        // record an engagement mark when it is turned ON, so post-hoc analysis sees the run requested
        // speculative readahead even on a bucket where no range ever collapsed enough to engage it.
        if (toggles.readahead()) {
            metrics.recordStealReason("TOGGLE", "readahead_on");
        }
    }

    /**
     * A point-in-time snapshot of the live worklist ({@link #livePool}) for the terminal/
     * mid-run {@code slow_ranges[]} dump — {@link RunMetrics#registerRangeSnapshotSource} wires this
     * in at construction. Weakly-consistent iteration (same as {@link #livePool}'s own javadoc): a
     * range that completes mid-iteration may or may not appear, which is fine for a diagnostic dump.
     * {@link WorkerState#snapshot()} takes that worker's own tiny lock briefly (the same lock a
     * thief already takes to steal from it) — cheap, and never held across this whole iteration.
     */
    private List<RunMetrics.RangeSnapshot> snapshotLiveRanges() {
        long nowNanos = System.nanoTime();
        List<RunMetrics.RangeSnapshot> out = new ArrayList<>();
        for (WorkerState ws : livePool) {
            WorkerState.Snapshot snap = ws.snapshot();
            double est = StealMath.estRemaining(snap.cursor(), ws.lo(), snap.hi(), ws.keysEmitted());
            long elapsedNanos = Math.max(1L, nowNanos - ws.createdAtNanos());
            double drainRate = ws.keysEmitted() / (elapsedNanos / 1_000_000_000.0);
            out.add(new RunMetrics.RangeSnapshot(ws.lo(), snap.hi(), snap.cursor(), est, drainRate,
                    ws.cursorPassedPivotTally(), ws.noPivotTally(), ws.structureSuppressedTally(),
                    ws.demandGatedTally()));
        }
        return out;
    }

    /** Stops a worker cleanly when the downstream receiver is gone (broken pipe). */
    private static final class ReceiverGone extends RuntimeException {
        ReceiverGone() {
            super(null, null, false, false);
        }
    }

    @Override
    public void produce(RunContext ctx, Channel<PageBatch> channel) throws Exception {
        metrics.setRunId(runId);
        // Publish the run's cancellation token so the GaugedFetcher throttle-retry loop can
        // observe cancellation and abort a persistently-throttled fetch instead of spinning forever.
        this.cancellation = ctx.cancellation();
        if (RunContext.runIdOrNone() != runId) {
            ctx.runWhereBound(runId, () -> {
                produce(ctx, channel);
                return null;
            });
            return;
        }
        log.info("work_stealing_scan_start run_id={} worker_count={} seed_count={} max_keys={}",
                runId, workerCount, seeds.size(), maxKeys);
        // Seed the ready queue from the loaded nodes; outstanding = #non-COMPLETED seeds (§4.4).
        for (Node n : seeds) {
            worklist.addSeed(new Worklist.Claim(n.id(), n.rangeStart(), n.cursor(), n.rangeEnd()));
            seedNodeIds.add(n.id());   // Mark the initial worklist so seed claims are attributable
            if (trace.enabled()) {
                trace.seeded(n.id(), n.rangeStart(), n.rangeEnd());
            }
        }
        worklist.initOutstanding(seeds.size());
        if (seeds.isEmpty()) {
            log.info("quiescence_reached run_id={} outstanding={}", runId, 0);
            return;   // nothing outstanding ⇒ immediate quiescence
        }

        // Captured inside the try-with-resources body below (its first statement) so the
        // RejectedExecutionException catch — outside the resource's own scope per JLS 14.20.3 —
        // can still reach it to recover the real recorded failure.
        Scope[] scopeCapture = new Scope[1];
        try (Scope scope = new Scope()) {
            scopeCapture[0] = scope;
            // Pre-cancelled run: surface CancelledException deterministically without forking (so
            // apiCalls stays 0 and the run is not marked COMPLETED). Otherwise an early worker can
            // fail-fast and shut the scope down mid-loop, making a later fork() throw
            // RejectedExecutionException instead — the flaky case handled in the catch below.
            if (ctx.cancellation().isCancelled()) {
                throwCancellationOrReceiverGone(ctx);
            }
            for (int i = 0; i < workerCount; i++) {
                long workerId = i;
                scope.fork(() -> ctx.runWhereBound(runId, () -> {
                    return RunContext.runWorkerWhereBound(workerId, () -> {
                        runWorker(ctx, channel);
                        return null;
                    });
                }));
            }
            scope.fork(() -> ctx.runWhereBound(() -> {
                watchReceiver(ctx, channel);
                return null;
            }));
            scope.joinAllOrThrow();   // returns only when every worker has reached quiescence
        } catch (ReceiverGone cleanStop) {
            // Downstream closed (broken pipe / cancellation): stop cleanly. The last
            // committed page may not have been emitted (at-most-once for the default sink).
        } catch (CancelledException e) {
            if (!isReceiverGoneTeardown(e, receiverGone.get())) {
                throw e;
            }
        } catch (InterruptedException e) {
            if (isReceiverGoneTeardown(e, receiverGone.get())) {
                // Clean receiver-gone (broken-pipe) teardown: swallow, stop cleanly.
            } else if (ctx.cancellation().isCancelled()) {
                // Every worker was stuck in the GaugedFetcher throttle-retry loop when the
                // run was cancelled (--max-duration / signal), so the abort surfaced as an
                // InterruptedException rather than a loop-top CancelledException. Convert it to the
                // contractual CancelledException so the unwind writes the terminal summary + commits
                // the checkpoint and the exit-code path attributes the stop (max_duration vs signal).
                throwCancellationOrReceiverGone(ctx);
            } else {
                throw e;
            }
        } catch (RejectedExecutionException e) {
            // Cancellation (or a receiver-gone teardown) can shut the Scope down between forks, so a
            // later fork() is rejected. Under cancellation that IS the cancel path — surface the
            // contractual CancelledException (exit 130), not a leaked RejectedExecutionException.
            if (ctx.cancellation().isCancelled() || receiverGone.get()) {
                throwCancellationOrReceiverGone(ctx);
            }
            // Otherwise this REE is a symptom, not the cause: a sibling task's real failure
            // already triggered shutdownNow() and raced this fork() call. Surface that recorded
            // failure — the run's actual typed cause — instead of the untyped REE. If nothing was
            // recorded (a genuine, unexplained rejection), fall through and let the REE stand.
            if (scopeCapture[0] != null) {
                scopeCapture[0].rethrowFirstFailure();
            }
            throw e;
        } finally {
            log.info("work_stealing_scan_exit run_id={} outstanding={}", runId, worklist.outstanding());
        }
    }

    /**
     * The pipeline drops the receiver as soon as output returns (including broken pipe).
     * Workers blocked in fetch/probe/commit may not reach {@link Channel#send} promptly,
     * so this watcher converts receiver-drop into the scope's shutdown-on-failure path.
     */
    private void watchReceiver(RunContext ctx, Channel<PageBatch> channel) throws InterruptedException {
        while (true) {
            if (channel.isReceiverDropped()) {
                stopForReceiverGone(ctx);
            }
            if (ctx.cancellation().isCancelled() || worklist.outstanding() == 0L) {
                return;
            }
            TimeUnit.NANOSECONDS.sleep(PARK_NANOS);
        }
    }

    private void stopForReceiverGone(RunContext ctx) {
        receiverGone.set(true);
        ctx.cancellation().cancel();
        worklist.signalAll();
        throw new ReceiverGone();
    }

    /** A single worker: claim → run → repeat, until quiescence or cancellation (§2). */
    private void runWorker(RunContext ctx, Channel<PageBatch> channel) throws Exception {
        // Each worker uses a gauge-wrapped fetcher: acquires a permit before the HTTP call,
        // releases it immediately after, and feeds the status into the AIMD policy (§5).
        // The Thief uses a throttle-aware GaugedFetcher without slot gating: probes are rare
        // 1-key calls, stealing is paused separately via isStealingAllowed(), and thrown
        // throttles still reduce T and retry the same probe.
        RangeScanner scanner = new RangeScanner(
                new GaugedFetcher(fetcher, gauge, true, true, metrics, ctx::cancellation,
                        retryConfig.sleeper(), retryConfig.policy()),
                maxKeys, metrics,
                toggles.readahead() ? ReadaheadConfig.on() : ReadaheadConfig.off(),
                speculativeFetcher, gauge::isStealingAllowed);
        log.debug("worker_start run_id={} worker_id={} worker_count={}",
                runId, RunContext.workerIdOrNone(), workerCount);
        Worklist.Claim claim;
        try {
            while ((claim = nextClaim(ctx)) != null) {
                Worklist.Claim boundClaim = claim;
                // The first time THIS worker claims a SEED (initial worklist) node, tick the
                // distinct-seed-worker engagement counter once. The counter's total is then the number
                // of distinct workers that consumed a seed range — a deterministic multi-worker-
                // consumption signal (serial run => 1, concurrent spread of a banded heavy tail =>
                // many). Pure observation; no scheduling effect.
                if (seedNodeIds.contains(boundClaim.nodeId())
                        && distinctSeedClaimWorkers.add(RunContext.workerIdOrNone())) {
                    metrics.recordStealReason("SEED_SCHED", "distinct_seed_worker");
                }
                if (trace.enabled()) {
                    trace.claimed(RunContext.workerIdOrNone(), boundClaim.nodeId(),
                            boundClaim.lo(), boundClaim.cursor(), boundClaim.hi());
                }
                RunContext.runNodeWhereBound(boundClaim.nodeId(), boundClaim.lo(), boundClaim.hi(), () -> {
                    runClaim(ctx, channel, scanner, boundClaim);
                    return null;
                });
            }
        } finally {
            log.debug("worker_exit run_id={} worker_id={}", runId, RunContext.workerIdOrNone());
        }
    }

    /**
     * The idle policy: claim a node, terminate at quiescence, or become a thief. Returns
     * {@code null} to terminate the worker at quiescence. The claim/quiescence/park transitions over
     * the {@link Worklist} ledger are its gated primitives; the steal itself runs <b>outside</b> the
     * ledger lock (it takes victim locks — never gate-then-victim).
     */
    private Worklist.Claim nextClaim(RunContext ctx) throws Exception {
        while (true) {
            if (ctx.cancellation().isCancelled()) {
                throwCancellationOrReceiverGone(ctx);
            }
            Worklist.Poll poll = worklist.poll();
            if (poll.claim() != null) {
                idleStealBackoff.reset();
                return poll.claim();
            }
            if (poll.quiescent()) {
                return null;
            }

            // No claim, but work is still outstanding ⇒ try to steal a victim's tail,
            // but only when the gauge permits it (paused during sustained 503 throttling —
            // creating new nodes adds more S3 load when the store is already backing off).
            if (gauge.isStealingAllowed()) {
                if (idleStealBackoff.tryAcquireAttemptSlot()) {
                    // Everything done while holding the slot lives in this try: an escape from ANY
                    // of it — metrics, logging, eligibleVictims(), the thief's child enqueue — would
                    // otherwise strand the slot and disable stealing for the rest of the run.
                    try {
                        log.debug("steal_attempted run_id={} worker_id={} live_workers={} outstanding={}",
                                runId, RunContext.workerIdOrNone(), livePool.size(), worklist.outstanding());
                        metrics.recordStealAttempt();   // attempt denominator vs successes (§5)
                        Thief.Outcome outcome = thief.steal(eligibleVictims());
                        metrics.recordSteal(outcome.name());
                        log.debug("steal_finished run_id={} worker_id={} outcome={} live_workers={} outstanding={}",
                                runId, RunContext.workerIdOrNone(), outcome, livePool.size(), worklist.outstanding());
                        if (outcome == Thief.Outcome.CHILD_CREATED) {
                            idleStealBackoff.reset();
                            continue;   // a child was enqueued + counted; loop to claim it
                        }
                        idleStealBackoff.recordNonProductive();
                    } finally {
                        // Release, then wake the workers parked on the in-flight backstop. The
                        // signal is issued here — outside the backoff monitor — because
                        // Worklist.park holds its gate across parkNanos(), so gate→backoff is the
                        // only safe order (see IdleStealBackoff#releaseSlot).
                        idleStealBackoff.releaseSlot();
                        worklist.signalAll();
                    }
                }
                // A denial (the backoff guard, not the AIMD gauge — distinct from a slow/throttled
                // store, §5) is counted and attributed inside tryAcquireAttemptSlot, which is the
                // only place that knows WHICH of the two regimes refused: in_flight vs paced.
            }
            // Nothing claimable right now: park until an enqueue/decrement signals (or the
            // poll backstop fires), then re-evaluate. The ledger re-checks under its lock to avoid a
            // lost wakeup against a concurrent enqueue/decrement.
            if (ctx.cancellation().isCancelled()) {
                throwCancellationOrReceiverGone(ctx);
            }
            worklist.park(idleStealBackoff::parkNanos, metrics);
        }
    }

    /**
     * Whether the sole fleet-wide steal-attempt slot is currently owned. The observation point for
     * the CONC guard on the release: after every worker has exited, a slot still held means some
     * path out of the acquired region above skipped its {@code finally}.
     */
    boolean stealAttemptInFlight() {
        return idleStealBackoff.attemptInFlight();
    }

    /**
     * The steal-eligible subset of the live worker pool (algorithms.md §3 — progress-gated
     * stealing). A worker is a victim only once it has committed a non-empty page since it was
     * created or last stolen from ({@link WorkerState#stealEligible()}); curating the pool here
     * is what bounds re-splitting to ≤1 per emitted page and closes the high-concurrency latency
     * livelock (idle thieves out-racing the page fetch and carving every owner's tail into the
     * empty gap above its cursor). The {@link Thief} then picks {@code argmax estRemaining} among
     * these — so a freshly-created child is never carved before it lists its first page. A small
     * per-attempt snapshot on the idle path (no claim to run); the empty case yields
     * {@link Thief.Outcome#NO_VICTIM} and the worker parks, exactly as when nothing is splittable.
     */
    private List<WorkerState> eligibleVictims() {
        List<WorkerState> eligible = new ArrayList<>(livePool.size());
        for (WorkerState w : livePool) {
            if (w.stealEligible()) {
                eligible.add(w);
            }
        }
        return eligible;
    }

    private void throwCancellationOrReceiverGone(RunContext ctx) throws CancelledException {
        if (receiverGone.get()) {
            throw new ReceiverGone();
        }
        ctx.cancellation().throwIfCancelled();
    }

    /** Run one claimed range to completion, then decrement {@code outstanding} and signal. */
    private void runClaim(RunContext ctx, Channel<PageBatch> channel, RangeScanner scanner, Worklist.Claim claim)
            throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("claim_start run_id={} worker_id={} node_id={} lo={} hi={}",
                    runId, RunContext.workerIdOrNone(), claim.nodeId(), StealMath.describe(claim.lo()),
                    StealMath.describe(claim.hi()));
        }
        WorkerState ws = new WorkerState(claim.nodeId(), claim.lo(), claim.cursor(), claim.hi(), metrics);
        AtomicLong pageSeq = new AtomicLong();
        // Owner-side self-split rate-limit state, mutated only by this worker's own (serial)
        // page-commit callback: [0] = committed non-empty pages so far, [1] = the page index of the
        // last self-split. Seeded so the first qualifying page may self-split immediately (ramp),
        // then gated to one per SELF_SPLIT_MIN_PAGES_BETWEEN pages.
        long[] selfSplit = {0L, -OwnerSelfSplit.SELF_SPLIT_MIN_PAGES_BETWEEN};
        livePool.add(ws);
        try {
            try {
                scanner.runRange(prefix, mode, claim.cursor(), ws.hiSupplier(), ctx.cancellation(),
                        (batch, lastKey, completed) -> {
                            // §4.1 ordering: under ws.lock advance the leading cursor + ENQUEUE the
                            // commit; release the lock; await durability OUTSIDE it; then emit (I1).
                            CompletableFuture<Void> commit;
                            List<ListEntry> inRange;
                            byte[] cursorTo;
                            boolean madeProgress;
                            OwnerSelfSplit.OwnerSplitTrace ownerSplitTrace = null;
                            ws.lock().lock();
                            try {
                                // Re-validate the batch against the volatile hi UNDER ws.lock (§4.2,
                                // edge-case 7). RangeScanner built `batch` reading hi per key WITHOUT the
                                // lock, so a thief that narrowed hi to m AFTER a key was batched but BEFORE
                                // this commit (the narrow serializes on ws.lock) would otherwise let the
                                // victim emit a key now owned by the child (m, H] — a double-emit. Dropping
                                // the trailing keys now above hi here closes that window; the dropped keys
                                // are exactly the child's, and the next page (start_after past them) finds
                                // the bound and completes the node.
                                inRange = inRange(batch, ws.hiSupplier().get());
                                cursorTo = inRange.isEmpty() ? null : inRange.getLast().key().rawUnsafe();
                                if (!inRange.isEmpty()) {
                                    ws.setCursor(cursorTo);
                                    ws.addKeysEmitted(inRange.size());
                                    ws.recordPage(inRange.getFirst().key().rawUnsafe(),
                                            cursorTo, inRange.size());   // density digest
                                    metrics.setCursor(cursorTo);
                                }
                                madeProgress = !inRange.isEmpty();
                                commit = store.commitPageAsync(new PageCommit(
                                        claim.nodeId(), cursorTo, completed));
                                // Owner-side proactive split: still under ws.lock, with the cursor
                                // just advanced to the exact `cursorTo`, carve this worker's OWN
                                // far-ahead tail. Zero-race by construction (see maybeOwnerSelfSplit).
                                if (toggles.ownerSplit() && madeProgress && !completed) {
                                    ownerSplitTrace = ownerSelfSplit.maybeOwnerSelfSplit(claim.nodeId(), ws, cursorTo, selfSplit);
                                }
                            } finally {
                                ws.lock().unlock();
                            }
                            // Emit the owner-split trace OUTSIDE ws.lock, before pageCommitted
                            // to preserve the original event order (the split happened during this commit).
                            if (ownerSplitTrace != null) {
                                trace.ownerSplit(RunContext.workerIdOrNone(), ownerSplitTrace.nodeId(),
                                        ownerSplitTrace.childId(), "self_published",
                                        ownerSplitTrace.pivot(), ownerSplitTrace.hi());
                            }
                            if (madeProgress) {
                                signalStealableProgress();
                            }
                            awaitCommit(commit);   // durable before emit (I1); the await never holds ws.lock
                            if (trace.enabled()) {
                                trace.pageCommitted(RunContext.workerIdOrNone(), claim.nodeId(),
                                        inRange.size(), cursorTo, completed);
                            }
                            if (inRange.isEmpty()) {
                                return;
                            }
                            List<ListEntry> kept = filters.apply(inRange);
                            if (kept.isEmpty()) {
                                return;
                            }
                            // Pack on THIS fetch worker only in --sort mode (pagePacker
                            // non-null). Pack AFTER filters.apply so filtered-out entries are never packed;
                            // the non-sort path (pagePacker == null) still ships the raw List byte-for-byte.
                            PagePacker packer = pagePacker;
                            PageBatch pageBatch;
                            if (packer != null) {
                                PackedPage packed = packer.pack(kept);
                                metrics.recordStealReason("SORT", "pack_on_fetch");
                                pageBatch = PageBatch.ofPacked(claim.nodeId(), pageSeq.getAndIncrement(), packed);
                            } else {
                                pageBatch = new PageBatch(claim.nodeId(), pageSeq.getAndIncrement(), kept);
                            }
                            Timer.Sample queueWait = metrics.startQueueWaitTimer();
                            boolean sent = channel.send(new Item<>(pageBatch));
                            metrics.recordQueueWait(queueWait);
                            if (!sent) {
                                stopForReceiverGone(ctx);
                            }
                            metrics.recordPage();
                        });
            } catch (CancelledException e) {
                if (isReceiverGoneTeardown(e, receiverGone.get())) {
                    throw new ReceiverGone();
                }
                throw e;
            } catch (InterruptedException e) {
                if (isReceiverGoneTeardown(e, receiverGone.get())) {
                    throw new ReceiverGone();
                }
                throw e;
            } finally {
                livePool.remove(ws);   // deregister from the victim pool on completion OR cancel/broken-pipe
            }
        } catch (Exception e) {
            // trace: a node-level `failed` event —
            // ReceiverGone is an expected clean-stop teardown (broken pipe / cancellation unwind), not
            // a genuine failure, so it is excluded. Pure side effect: rethrows the SAME exception
            // unchanged, so behavior is identical to before this trace hook existed.
            if (trace.enabled() && !(e instanceof ReceiverGone)) {
                trace.failed(RunContext.workerIdOrNone(), claim.nodeId(), e.getClass().getSimpleName());
            }
            throw e;
        }
        // The range completed (runRange returns only on DONE; a cancel/broken-pipe throws and
        // skips this). If this completed node is a TAGGED owner-split child
        // (removed here so it is classified exactly once, whichever worker drains it), fold its
        // realized mass into the run-level confetti-rate feedback the NEXT owner-split carve reads
        // (ground truth, not the upstream estimate) — before the generic CHILD_MASS bucketing below,
        // which every completed node (seed, thief child, owner-split child) already gets.
        ownerSelfSplit.onNodeCompleted(claim.nodeId(), ws);
        // Bucket the per-node emitted mass (§5 dead-zone diagnostic: bimodal empty/large ⇒
        // zero-transfer splits) before the outstanding decrement.
        metrics.recordChildMass(ws.keysEmitted());
        // Fold this completed node's observed per-position alphabet into the run-level
        // cardinality aggregate (the owner thread is the digest's sole writer and no thief reads it
        // after livePool.remove above, so the read is race-free; once per node, never per key).
        metrics.recordAlphabetObservation(ws.alphabetDigest().maskWords());
        if (trace.enabled()) {
            trace.completed(RunContext.workerIdOrNone(), claim.nodeId());
        }
        // Decrement + signal (under the ledger gate) so a parked worker re-checks quiescence.
        long remaining = worklist.decrement();
        if (remaining == 0L) {
            log.info("quiescence_reached run_id={} outstanding={}", runId, remaining);
        }
    }

    /**
     * A non-empty page commit makes its worker steal-eligible again. This is the progress event
     * the fixed 5 ms poll backstop used to discover indirectly; signalling it keeps legitimate
     * skew rebalancing prompt while unchanged/non-productive probes remain paced.
     */
    private void signalStealableProgress() {
        // Reset immediately before the broadcast, so workers woken by this signal observe the cleared
        // backoff PACING state and not a stale next-attempt instant (otherwise they would re-park for
        // a full backoff window — defeating the prompt-rebalance intent of this signal). It clears
        // pacing only: an attempt already in flight keeps its slot until its owner releases, so this
        // signal can never admit a second concurrent attempt. The reset touches only the
        // self-synchronized IdleStealBackoff — never the ledger — so it needs no gate; the broadcast
        // takes the ledger gate inside signalAll, so the woken parkers still cannot run until it is
        // released.
        idleStealBackoff.reset();
        worklist.signalAll();
    }

    /**
     * The prefix of {@code batch} whose keys are {@code <= hi} (the in-range head). {@code batch}
     * is ascending and bounded already by {@link RangeScanner}'s per-key check; this re-trims it
     * against a {@code hi} that may have been narrowed since, under {@link WorkerState#lock()}.
     * Returns {@code batch} unchanged when nothing trails the bound (the common case).
     */
    private static List<ListEntry> inRange(List<ListEntry> batch, byte[] hi) {
        if (hi == null || batch.isEmpty()) {
            return batch;
        }
        int n = batch.size();
        while (n > 0 && KeyBytes.compareUnsigned(batch.get(n - 1).key().rawUnsafe(), hi) > 0) {
            n--;
        }
        return n == batch.size() ? batch : batch.subList(0, n);
    }

    static boolean isReceiverGoneTeardown(Throwable failure, boolean receiverGone) {
        return receiverGone && (failure instanceof CancelledException || failure instanceof InterruptedException);
    }

    private static void awaitCommit(CompletableFuture<Void> commit) throws SwathException, InterruptedException {
        try {
            commit.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            switch (e.getCause()) {
                case SwathException se -> throw se;
                case InterruptedException ie -> {
                    Thread.currentThread().interrupt();
                    throw ie;
                }
                case RuntimeException re -> throw re;
                case Error err -> throw err;
                case null -> throw new RuntimeException("checkpoint commit failed");
                default -> throw new RuntimeException(e.getCause());
            }
        }
    }

    // ---- package-private test seam -------------------------------------------------

    /**
     * Returns the live {@link ConcurrencyGauge} for this scan. Read-only accessor — no
     * behavior change. Package-private: THR-1 reads {@link ConcurrencyGauge#effectiveT()}
     * after the run to verify that the real {@link GaugedFetcher} path (not a test helper)
     * actually fed 503 statuses into the gauge, collapsing {@code T} to the floor.
     */
    ConcurrencyGauge gauge() {
        return gauge;
    }

    /**
     * Returns the wired {@link TraceSink} — {@link TraceSink#NONE} unless the caller passed a real
     * one. Read-only accessor (no behavior change); lets a test assert the zero-cost default
     * actually IS the singleton no-op instance, not merely behaviorally inert.
     */
    TraceSink trace() {
        return trace;
    }

    /**
     * The thief's {@link Thief.ChildSink}: enqueue the child {@code (m, H]} (cursor = {@code m})
     * and bump {@code outstanding} <b>while the thief still holds the victim lock</b> (§4.4). The
     * ledger's {@link Worklist#enqueue} does the counted, signalled hand-off (victim→gate nesting,
     * the increment landing under the victim lock so quiescence never sees a false zero); the split
     * counter and trace record it afterward off the ledger gate.
     */
    private void enqueueChild(long childNodeId, byte[] childLo, byte[] childHi) {
        long value = worklist.enqueue(new Worklist.Claim(childNodeId, childLo, childLo, childHi));
        metrics.recordSplit();
        if (log.isDebugEnabled()) {
            log.debug("child_enqueued run_id={} worker_id={} node_id={} lo={} hi={} outstanding={}",
                    runId, RunContext.workerIdOrNone(), childNodeId, StealMath.describe(childLo),
                    StealMath.describe(childHi), value);
        }
    }

}

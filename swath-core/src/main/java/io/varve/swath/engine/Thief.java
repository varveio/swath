/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import io.varve.swath.checkpoint.CheckpointStore;
import io.varve.swath.checkpoint.SplitSpec;
import io.varve.swath.error.SwathException;
import io.varve.swath.error.ThrottleException;
import io.varve.swath.model.ByteMidpoint;
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
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The thief side of demand-driven range stealing (algorithms.md §3, I4). One
 * {@link #steal} call performs a <b>single</b> steal attempt against the live
 * worker pool: pick the busiest victim, compute a pivot, probe it, then under the
 * victim's lock re-validate and hand off the upper half {@code (m, H]} as a new
 * durable child node. The engine driver calls this in its idle loop.
 *
 * <p>The pre-lock snapshot {@code (c, H)} and the probe are <b>speculative</b>;
 * everything that mutates the victim is re-validated under {@link WorkerState#lock()},
 * so two thieves that pick the same victim are serialized by that lock. The two
 * distinct loser paths differ by exactly one thing — whether they touch
 * {@code victim.hi} — and the proof of why only the late loser restores it lives at
 * the hand-off in {@link #steal} (algorithms.md §3).
 *
 * <p>All key comparisons are unsigned byte-exact via {@link KeyBytes#compareUnsigned}
 * (I10) — never {@code String.compareTo}.
 */
public final class Thief {

    private static final Logger log = LoggerFactory.getLogger(Thief.class);

    /** The ⊥ sentinel (cursor at range start) as bytes for {@link ByteMidpoint}. */
    private static final byte[] BOTTOM = new byte[0];

    /**
     * The {@code max_keys} of the demand-driven {@code delimiter=/} structure probe: one bounded
     * page (never paginated, like {@link SeedStep}'s seed probe), from which the median qualifying
     * {@code CommonPrefix} is taken. Caps the cost of the single speculative structure call per
     * probe. See walkthroughs.md §2 for how the probe ladder keeps total LIST at O(pages +
     * structure), never O(directories) (INT-8).
     *
     * <p>Sized against the S3 store's probe attempt timeout, because a
     * {@code delimiter=/} listing costs roughly linearly in the {@code CommonPrefixes} it returns —
     * the store has to walk the directory to find them. Measured on a dense commoncrawl prefix with
     * 121 children (`projects/headers-testing/`): 121 prefixes 4.2 s, 64 prefixes 2.0 s, 48
     * prefixes 1.5 s, <b>32 prefixes 1.0 s</b>, 16 prefixes 0.5 s. A cap of 1000 asked for every
     * child and so paid the full 4.2 s (6.9 s mean against real S3), which <b>exceeds the probe
     * budget</b> — and a timed-out probe yields no cut point at all, so the range never splits and
     * the scan serializes on it. That is the failure this constant caused on `s3://commoncrawl`.
     *
     * <p>The probe consumes exactly ONE of the boundaries it asks for (the median, below), so
     * requesting a full page bought detail the caller cannot use. A cap sized to complete well
     * inside the budget trades pivot balance — the median of a truncated sample sits nearer the
     * cursor than the true median — for a pivot that actually arrives. An unbalanced split that
     * commits beats a balanced one that times out, and the thief re-probes continuously, so the
     * carve repeats. On a truncated page the pivot switches from the sample median to the furthest
     * sampled boundary (see {@link #structurePivot}) — the largest carve the probe can prove.
     * {@code STRUCTURE.fanout_capped} counts the truncated probes; {@code PIVOT.structure_capped}
     * counts the splits that actually committed on one.
     */
    private static final int STRUCTURE_PROBE_MAX_KEYS = 32;

    /**
     * The cap on extra {@code delimiter=/} probes the parent-empty sliver's adaptive back-out
     * ({@link #adaptiveStructurePivot}) issues past the first (coarsest) level — so one steal
     * attempt fires at most {@code MAX_STRUCTURE_BACKOUT_LEVELS + 1} structure probes: the
     * coarse→fine search descends at most this many {@code /}-segments from the cursor∧H divergence
     * directory toward the cursor before falling back to the byte-exact sliver. A small O(1) cap
     * (INT-8).
     */
    private static final int MAX_STRUCTURE_BACKOUT_LEVELS = 3;

    /**
     * Safety margin, in halvings, added to the log-scaled band-width estimate that bounds the
     * empty-upper bisection loop ({@link #emptyUpperBisectionBudget}). The budget scales with the
     * gap's own byte width — {@code B = ceil(log2(bandWidthBytes)) + MARGIN} — rather than using a
     * fixed cap, which would truncate the {@code O(log band width)} convergence PROP-3 contracts: a
     * band wider than its content needs multiple halvings to reach its dense sub-window.
     *
     * <p>{@code MARGIN=6} gives 64× slack above the estimate. Convergence is bounded by
     * {@link ByteMidpoint}'s code-point-space bisection, itself bounded by the ~21-bit Unicode scalar
     * range, so it always completes inside the budget — making INT-8's "never unbounded LISTs per
     * attempt" a closed-form ceiling.
     */
    private static final int EMPTY_UPPER_BISECTION_MARGIN = 6;

    /**
     * The number of bytes (starting at the {@code (cursor, hi)} divergence position) used to estimate
     * the gap's byte width for {@link #emptyUpperBisectionBudget}. 2 bytes (a 16-bit window) gives
     * {@code 2^16 · 2^MARGIN = 2^22} — comfortably above the ~21-bit (0x10FFFF) Unicode scalar range, so
     * the resulting cap always dominates the true code-point-space bisection depth for any valid-UTF-8
     * bound pair, however deep the shared prefix.
     */
    private static final int BAND_WIDTH_SUFFIX_BYTES = 2;

    /**
     * The fixed fallback gap width (bytes) used to size {@link #emptyUpperBisectionBudget} for the open
     * frontier ({@code H == null}) — there is no upper bound to measure a byte distance against, so we
     * use a small, generous constant rather than special-casing the open frontier as unbounded.
     */
    private static final long OPEN_FRONTIER_BAND_WIDTH_FALLBACK = 40L;

    /**
     * After this many <b>consecutive</b> zero-fan-out
     * {@code delimiter=/} structure probes (a bucket with no sub-directory structure at the probed
     * level returns zero fan-out every time), the thief stops issuing them
     * and falls straight to the byte-midpoint/sliver fallback (a missed
     * boundary costs balance, never correctness). Reset to zero on any non-zero fan-out.
     */
    private static final int STRUCTURE_ZERO_FANOUT_SUPPRESS_THRESHOLD = 8;
    /**
     * Consecutive TIMED-OUT structure probes on one victim before probing is suppressed for it —
     * far lower than {@link #STRUCTURE_ZERO_FANOUT_SUPPRESS_THRESHOLD} because the two streaks cost
     * differently by an order of magnitude. A zero-fan-out probe answered cheaply; a timed-out probe
     * answered nothing after burning its whole escalated budget and aborting a connection.
     *
     * <p>Sized at 2 so a single blip (a transient network fault, an S3 hiccup) is never enough — but
     * a second consecutive timeout on the SAME victim is evidence about the keyspace, not the
     * network, and further probes there are throwing good time after bad. This is the flat-directory
     * case a budget cannot fix: see {@code docs/internals/probe-budgets.md} §5.
     */
    private static final int STRUCTURE_TIMEOUT_SUPPRESS_THRESHOLD = 2;

    /**
     * While suppressed, still issue 1-in-{@code N} structure probes so a bucket whose
     * sub-directory structure appears only far below the seed level still recovers (re-enabling on the
     * first non-zero fan-out).
     */
    private static final int STRUCTURE_SUPPRESS_RETRY_DIVISOR = 64;

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
     * alphabet_pivots} all bear on this unit's steal-attempt pivot synthesis (see {@link
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
     * Perform one steal attempt over the live worker pool (algorithms.md §3).
     *
     * @param pool the currently-live workers (the driver passes a live snapshot)
     * @return the outcome of this single attempt (never blocks beyond the probe +
     *         the synchronous durable split)
     */
    public Outcome steal(Iterable<WorkerState> pool) throws SwathException, InterruptedException {
        // 1. Victim selection: argmax estRemaining over the live pool. Skip workers cached
        //    unsplittable and workers with no remaining span (est <= 0). Speculative — the
        //    cursor/hi reads here are not a coherent snapshot; we re-validate under the lock.
        WorkerState victim = null;
        double best = Double.NEGATIVE_INFINITY;
        // Why each candidate was rejected. NO_VICTIM.no_splittable_victim is by far the most
        // common steal outcome on a collapsed run (4,000 of 4,221 attempts = 95% on
        // s3://wis2globalcache/, 2026-07-25) but it folds FOUR structurally different situations
        // into one number, and they call for opposite responses: an empty eligible pool means the
        // fleet is waiting on page commits (nothing is wrong), all-unsplittable means the ranges
        // are genuinely atomic, all-paced means the thieves are in per-victim futility cooldown,
        // and no-remaining-span means estRemaining read zero on every candidate — which on a deep
        // shared prefix can be a MEASUREMENT artefact rather than a fact about the keyspace.
        // Post-hoc analysis cannot tell those apart from the aggregate, so the tail's actual
        // refusal branch has never been identified. Counted here, reported below.
        int seen = 0;
        int skippedUnsplittable = 0;
        int skippedPaced = 0;
        int skippedNoSpan = 0;
        for (WorkerState w : pool) {
            seen++;
            if (w.unsplittable()) {
                skippedUnsplittable++;
                continue;
            }
            // A victim that has racked up consecutive futile steal
            // outcomes (cursor_passed_pivot / bound_moved / bisect_budget_exhausted) is in a per-victim
            // cooldown — skip it this attempt (consuming one skip) so idle thieves stop hammering the
            // specific racing drainer, while a productive sibling below stays fully stealable. Do NOT
            // make this pacing GLOBAL: a global cooldown starves a productive sibling's splits on a
            // skewed workload.
            if (w.stealPaced()) {
                skippedPaced++;
                if (metrics != null) {
                    metrics.recordStealReason("STEAL", "futility_paced");
                }
                continue;
            }
            // Victim selection ranks the busiest worker first, so it must compare workers on one
            // absolute scale: the plain code-point `estRemaining` (localDensity × remaining span). Do
            // not switch this to a rank-space (alphabet-aware) variant: it deflates a genuinely-large
            // victim whose observed alphabet is sparse (hex/UUID/digit tails), making it look *less*
            // loaded than a smaller victim on a denser alphabet — so idle thieves stop stealing from the
            // true hotspot and one worker dominates the drain. The pivot *synthesis* below
            // (`interpolate(..., alphabetDigest())`) is unaffected — it never depended on rank-space sizing.
            double est = StealMath.estRemaining(w.cursor(), w.lo(), w.hi(), w.keysEmitted());
            if (est <= 0.0) {
                skippedNoSpan++;
                continue;   // no remaining span — nothing to steal here
            }
            if (est > best) {
                best = est;
                victim = w;
            }
        }
        if (victim == null) {
            // Exactly ONE discriminator fires alongside the aggregate, so the two series stay
            // reconcilable: sum(discriminators) == no_splittable_victim. Literal arguments at each
            // call site — check-instrumentation-drift.py resolves only literals.
            if (metrics != null) {
                if (seen == 0) {
                    metrics.recordStealReason("NO_VICTIM", "pool_empty");
                } else if (skippedNoSpan == seen) {
                    metrics.recordStealReason("NO_VICTIM", "all_no_remaining_span");
                } else if (skippedPaced == seen) {
                    metrics.recordStealReason("NO_VICTIM", "all_futility_paced");
                } else if (skippedUnsplittable == seen) {
                    metrics.recordStealReason("NO_VICTIM", "all_unsplittable");
                } else {
                    metrics.recordStealReason("NO_VICTIM", "mixed_skips");
                }
            }
            log.debug("steal_failed run_id={} worker_id={} outcome={} reason={} "
                            + "seen={} unsplittable={} paced={} no_span={}",
                    runId, RunContext.workerIdOrNone(), Outcome.NO_VICTIM, "no_splittable_victim",
                    seen, skippedUnsplittable, skippedPaced, skippedNoSpan);
            return record(Outcome.NO_VICTIM, "no_splittable_victim");
        }

        // A probe fetch (GaugedFetcher slotGated=false) fails
        // fast on its own small cap (PROBE_TRANSIENT_RETRY_CAP) rather than riding out a storm — it
        // throws the ThrottleException back up through every probe call site below (probeNonEmpty /
        // structurePivot / adaptiveStructurePivot / flatLeafDensityPivot) instead of resolving a
        // page. Catch it here and fold it into the SAME non-productive-steal outcome an ordinary
        // RETRY takes (no victim/lock state was touched above this point — the probing below is
        // entirely speculative, so there is nothing to unwind), freeing the sole IdleStealBackoff
        // in-flight slot immediately. This is the ONLY source of a ThrottleException reaching this
        // method in production (the token-less degenerate path never applies to the thief's shared
        // fetcher — a real CancellationToken is always wired before any worker can steal).
        try {
        // 2. Coherent snapshot (c, H) — one lock-guarded read of the (cursor, hi) pair.
        WorkerState.Snapshot snap = victim.snapshot();
        byte[] c = snap.cursor();
        byte[] H = snap.hi();
        byte[] cForPivot = (c == null) ? BOTTOM : c;   // null cursor = ⊥
        if (victim.unchangedSinceNonProductiveSteal(snap)) {
            log.debug("steal_failed run_id={} worker_id={} outcome={} victim_node={} reason={}",
                    runId, RunContext.workerIdOrNone(), Outcome.RETRY, victim.nodeId(),
                    "unchanged_nonproductive_snapshot");
            return record(Outcome.RETRY, "unchanged_nonproductive_snapshot");
        }

        // 3. Pivot m. Bounded range → byte-midpoint between c and H. Open frontier
        //    (H == null) → density extrapolation toward the prefix ceiling. Victim selection
        //    reads (cursor, hi) speculatively (not a coherent snapshot), so a bounded victim may
        //    have raced its cursor to/past H by the time we take the coherent snapshot here —
        //    there is then no room to split (and byteMidpoint's precondition would reject it), so
        //    re-steal elsewhere next cycle.
        if (H != null && KeyBytes.compareUnsigned(cForPivot, H) >= 0) {
            victim.markNonProductiveSteal(snap);
            log.debug("steal_failed run_id={} worker_id={} outcome={} victim_node={} reason={}",
                    runId, RunContext.workerIdOrNone(), Outcome.RETRY, victim.nodeId(), "cursor_at_or_past_hi");
            return record(Outcome.RETRY, "cursor_at_or_past_hi");
        }
        // Far-ahead placement: for a bounded range place the pivot at
        // fraction `farAheadFraction` of (c, H] in code-point space (StealMath.interpolate) rather
        // than at the plain midpoint — so a fast drainer cannot advance its cursor past the pivot
        // before the split CAS commits (`RETRY.cursor_passed_pivot`). The fraction comes from the
        // victim's zero-cost density digest and is >= 0.5, so interpolate at 0.5 is byte-for-byte
        // byteMidpoint. The open frontier is unchanged.
        double farAheadFraction = (H == null) ? 0.5 : toggles.farAheadFraction(victim);
        byte[] m = (H == null)
                ? StealMath.extrapolate(victim.lo(), c, StealMath.prefixCeil(prefix))
                : toggles.interpolate(cForPivot, H, farAheadFraction, victim.alphabetDigest());
        // Instrumentation (§5): which pivot MECHANISM produced the committed split, recorded at
        // the CHILD_CREATED hand-off below. Starts as the far-ahead interpolated pivot (or a plain
        // midpoint when density is flat / an open-frontier extrapolation); later reassignments below
        // (step-back, structure, bisection, flat-leaf) update it so the winning mechanism is counted.
        String pivotMechanism = (H == null) ? "extrapolate" : (farAheadFraction > 0.5 ? "far_ahead" : "midpoint");
        if (m == null) {
            // extrapolate / byteMidpoint produced no pivot — but there are TWO distinct nulls
            // (algorithms.md §3.1), and only one is terminal:
            //   * TRANSIENT — an un-started open frontier (H == null, cursor still at lo / ⊥, no
            //     consumed span to reflect): the owner simply hasn't committed page 1 yet. RETRY
            //     (the idle loop parks ~PARK_NANOS and re-polls); once the owner advances, a
            //     consumed span exists and the next extrapolate yields a real pivot. We must NOT
            //     cache unsplittable here — doing so permanently excludes the seed victim (which
            //     owns the whole keyspace) from every future steal, collapsing the scan to one
            //     worker (INT-7). Never a blind-gallop: the OWNER makes the first progress, the
            //     thief just waits.
            //   * GENUINE — bounded bounds UTF-8-adjacent, or an open frontier whose cursor has
            //     reached / is adjacent to the ceiling: no key can exist strictly between, so the
            //     range really is unsplittable. Cache it so repeated steals don't re-probe a dead
            //     range every cycle (algorithms.md §3); the owner finishes it and quiescence holds.
            if (H == null && StealMath.isUnstartedFrontier(victim.lo(), c)) {
                victim.markNonProductiveSteal(snap);
                log.debug("steal_failed run_id={} worker_id={} outcome={} victim_node={} reason={}",
                        runId, RunContext.workerIdOrNone(), Outcome.RETRY, victim.nodeId(), "unstarted_frontier");
                return record(Outcome.RETRY, "unstarted_frontier");
            }
            victim.setUnsplittable(true);
            victim.recordNoPivot();   // per-range tally for the slow-range dump
            // (no markNonProductiveSteal: an unsplittable victim is skipped at selection, so the
            // unchanged-snapshot suppression never consults it.)
            if (log.isDebugEnabled()) {
                log.debug("unsplittable_cached run_id={} worker_id={} victim_node={} cursor={} hi={}",
                        runId, RunContext.workerIdOrNone(), victim.nodeId(), StealMath.describe(c), StealMath.describe(H));
            }
            return record(Outcome.UNSPLITTABLE, "no_pivot");
        }
        // Engagement (§5): did the observed-alphabet chooser land the pivot on a populated value
        // (differs from the plain code-point interpolate at the same fraction), or fall back? Bounded
        // ranges only — the open frontier uses extrapolate, not the alphabet chooser.
        recordAlphabetEngagement(m, cForPivot, H, farAheadFraction);

        // 4. Probe (m, H] with a single key. If empty (no key <= H), the upper half is empty — the
        //    range's mass is clustered BELOW m (the funnel / deep-nest signal). Rather than sliver
        //    at the cursor (baton-passing, never parallelism), first attempt ONE bounded delimiter=/
        //    structure probe for a REAL populated sub-directory boundary strictly in (c, H) so victim
        //    and child run in DIFFERENT regions in parallel; a truly flat region yields nothing and
        //    falls back UNCHANGED to bisect-toward-cursor. See walkthroughs.md §2.
        boolean upperEmpty = !probeNonEmpty(m, H);
        // Far-ahead step-back: the far-ahead pivot (farAheadFraction > 0.5)
        // may overshoot into an empty upper on a range whose keys don't extend as far as the fraction.
        // Step the fraction back to the plain code-point midpoint (== byteMidpoint) ONCE and re-probe
        // before falling through to the costlier structure machinery — so the far-ahead placement is
        // never worse than byte-midpoint, at O(1) extra probes per attempt (INT-8 budget untouched).
        if (upperEmpty && H != null && farAheadFraction > 0.5) {
            // Step back to the TRUE byte-midpoint (no alphabet digest): a digest-drawn 0.5 pivot can
            // select the same populated scalar as the far-ahead pivot (sparse alphabet, few observed
            // values), making the step-back a no-op. The plain code-point midpoint is byte-for-byte the
            // historical byteMidpoint, so f=0.5 without the digest is the real fallback.
            byte[] midpoint = StealMath.interpolate(cForPivot, H, 0.5);
            if (midpoint != null && !Arrays.equals(midpoint, m)) {
                if (metrics != null) {
                    metrics.recordStealReason("PIVOT", "step_back");   // step-back fired (§5)
                }
                if (log.isDebugEnabled()) {
                    log.debug("steal_far_ahead_stepback run_id={} worker_id={} victim_node={} far={} midpoint={} hi={}",
                            runId, RunContext.workerIdOrNone(), victim.nodeId(), StealMath.describe(m), StealMath.describe(midpoint), StealMath.describe(H));
                }
                m = midpoint;
                pivotMechanism = "midpoint";   // stepped back to the plain code-point midpoint
                upperEmpty = !probeNonEmpty(m, H);
            }
        }
        // The PARENT-EMPTY degenerate pivot — the symmetric partner of the empty-upper funnel. Over
        // a DEEP cursor and a SHORT bound diverging at ADJACENT code points (e.g. cursor `2022/03/…`
        // vs hi `2022/04/`), byte-midpoint can only return the cursor's immediate MIN_SAFE successor
        // (cursor·0x20): byte-exact (tiling holds) but USELESS — the parent (cursor, m] is empty and
        // the child inherits the ENTIRE tail, so one worker drains serially. The empty-upper signal
        // never fires here (the upper IS non-empty), so detect the cursor-adjacent sliver
        // STRUCTURALLY (isCursorAdjacentSliver — why not a span==0 test is proved there) and route it
        // through the SAME structure-probe machinery below.
        boolean parentEmptySliver = H != null && !upperEmpty && c != null
                && isCursorAdjacentSliver(m, cForPivot);
        if (upperEmpty || parentEmptySliver) {
            if (log.isDebugEnabled()) {
                log.debug("steal_probe_{} run_id={} worker_id={} victim_node={} pivot={} hi={}",
                        upperEmpty ? "empty" : "parent_empty",
                        runId, RunContext.workerIdOrNone(), victim.nodeId(), StealMath.describe(m), StealMath.describe(H));
            }
            // Demand-driven structure discovery applies to BOUNDED ranges only: it is over a finite
            // far H that byte-midpoint can only sliver. The open frontier (H == null) is balanced by
            // extrapolate's density reflection, so it needs no structure probe and keeps
            // bisect-toward-cursor unchanged.
            //
            // The probe prefix Q differs by pathology. Empty-upper probes at the lo∧cursor level
            // (structureProbePrefix). The parent-empty sliver must NOT — after repeated splits
            // lo∧cursor lands at the cursor's own leaf directory (only files, probe wasted) — and
            // instead searches adaptively for the COARSEST level yielding a FAR-ahead boundary in
            // (c, H) (adaptiveStructurePivot, whose javadoc carries the keyspace-dependence).
            StructuralPivot structural = null;
            if (H != null) {
                if (structureProbesEnabled(victim)) {
                    structural = parentEmptySliver
                            ? adaptiveStructurePivot(victim, cForPivot, c, H)
                            : structurePivot(victim, structureProbePrefix(victim.lo(), cForPivot), cForPivot, c, H);
                } else if (metrics != null) {
                    if (!toggles.structureProbes()) {
                        // The toggle disabled structure probing outright, distinct from the
                        // per-victim zero-fan-out suppression below — kept as its own tag so post-hoc
                        // analysis never confuses an ablation run with a genuinely flat/suppressed victim.
                        metrics.recordStealReason("STRUCTURE", "toggle_disabled");
                    } else {
                        // This VICTIM has returned zero fan-out on its
                        // last K structure probes (scoped per-victim) — skip the probe (falling to the
                        // byte-midpoint/sliver fallback below) and count it (§5). A different victim's
                        // counter is unaffected, so a suppressed flat region can never starve a mixed
                        // bucket's structured victims.
                        // Distinguish the evidence: a victim proven FLAT (cheap zero-fan-out answers)
                        // vs one that could not answer at all (timeout streak). Same decision, very
                        // different diagnosis -- see STRUCTURE_TIMEOUT_SUPPRESS_THRESHOLD.
                        metrics.recordStealReason("STRUCTURE",
                                victim.consecutiveTimedOutStructureProbes() >= STRUCTURE_TIMEOUT_SUPPRESS_THRESHOLD
                                        ? "suppressed_probe_timeout" : "suppressed_zero_fanout");
                        victim.recordStructureSuppressed();   // per-range tally for the slow-range dump
                    }
                }
            }
            if (structural != null) {
                // A CommonPrefix is returned ONLY when ≥1 object shares it, so the upper half
                // (structural, H] is *almost always* non-empty without a second 1-key probe — the one
                // exception is benign: if H falls inside that prefix's interval (its objects all sort
                // above H), the child (structural, H] is empty and simply completes at once. That
                // wastes a split but cannot gap/overlap/dup (tiling holds either way), so we skip the
                // extra probe (INT-8 keeps structure cost at ~1 RPC). It is a real prefix's bytes
                // (XML-safe, a valid start-after) and, by construction below, c < structural < H — so
                // it tiles exactly like a byte-midpoint split (I2/I3); the lock-guarded CAS hand-off
                // is unchanged.
                if (log.isDebugEnabled()) {
                    log.debug("steal_structure_pivot run_id={} worker_id={} victim_node={} pivot={} hi={}",
                            runId, RunContext.workerIdOrNone(), victim.nodeId(),
                            StealMath.describe(structural.pivot()), StealMath.describe(H));
                }
                m = structural.pivot();
                // The parent-empty sliver's coarse->fine adaptive
                // back-out (adaptiveStructurePivot) and the plain empty-upper structure probe
                // (structurePivot) carry distinct tags so post-hoc analysis can tell which of the two
                // durability mechanisms actually carried the split (§5).
                // A capped pivot carries a _capped variant of either tag: it is the furthest boundary
                // a TRUNCATED probe could prove, not the directory's median, so post-hoc analysis can
                // tell how many committed splits the max_keys cap shaped — on both mechanisms — and
                // whether raising the cap would buy balance.
                pivotMechanism = parentEmptySliver
                        ? (structural.capped() ? "adaptive_structure_capped" : "adaptive_structure")
                        : (structural.capped() ? "structure_capped" : "structure_probe");
            } else if (upperEmpty) {
                // Density-reflected empty-upper pivot. The far-ahead probe and the
                // step-back midpoint both came back empty and structure discovery found nothing —
                // this range's real mass sits in a thin low sliver while the uniform-midpoint pivot
                // overshot into vacuum. Before the blind bisection walks back
                // toward the cursor one halving at a time, reflect the consumed span (lo, c] FORWARD under
                // observed density (StealMath.extrapolate — zero extra I/O, the same skew-immune estimator
                // the open frontier uses) to a pivot m_r that lands at/inside the mass, and probe it ONCE:
                //   * hit  → commit at m_r (mechanism "reflect"), skipping the bisection entirely;
                //   * miss → seed the EXISTING budgeted bisection at the SHORTER interval (c, m_r] instead
                //            of (c, m) — same budget function, a materially closer start.
                // Probe cost: +1 constant probe per empty-upper attempt, ON TOP of the
                // 1 + emptyUpperBisectionBudget the bisection already spends (worst case placed +
                // step-back + reflect + budget) — the budget itself is unchanged, only its start.
                // Bounded ranges only (H != null): the open frontier already synthesized m via extrapolate,
                // so re-reflecting there is a no-op. Toggle `reflect` off ⇒ this block is skipped and the
                // path is byte-for-byte the plain uniform-midpoint bisection.
                boolean reflectCommitted = false;
                if (toggles.reflect() && H != null) {
                    byte[] mReflect = StealMath.extrapolate(victim.lo(), cForPivot, H);
                    if (mReflect != null
                            && KeyBytes.compareUnsigned(cForPivot, mReflect) < 0
                            && KeyBytes.compareUnsigned(mReflect, m) < 0) {
                        if (probeNonEmpty(mReflect, H)) {
                            if (metrics != null) {
                                metrics.recordStealReason("PIVOT", "reflect_hit");   // probe non-empty (§5)
                            }
                            if (log.isDebugEnabled()) {
                                log.debug("steal_reflect_hit run_id={} worker_id={} victim_node={} reflect={} hi={}",
                                        runId, RunContext.workerIdOrNone(), victim.nodeId(), StealMath.describe(mReflect), StealMath.describe(H));
                            }
                            m = mReflect;
                            pivotMechanism = "reflect";
                            reflectCommitted = true;
                        } else {
                            if (metrics != null) {
                                metrics.recordStealReason("PIVOT", "reflect_empty");   // probe empty; bisect (c, m_r] (§5)
                            }
                            if (log.isDebugEnabled()) {
                                log.debug("steal_reflect_empty run_id={} worker_id={} victim_node={} reflect={} hi={}",
                                        runId, RunContext.workerIdOrNone(), victim.nodeId(), StealMath.describe(mReflect), StealMath.describe(H));
                            }
                            m = mReflect;   // shorten the bisection interval to (c, m_r]
                        }
                    }
                }
                if (!reflectCommitted) {
                    // No sub-directory structure here (a truly flat dense region, or no boundary in
                    // (c, H)): preserve the existing empty-upper behavior — retry NEARER the cursor by
                    // bisecting (c, m]. Do NOT cache unsplittable on a mere empty probe. The halving
                    // is capped at a LOG-SCALED per-attempt budget (emptyUpperBisectionBudget) —
                    // large enough that PROP-3's legitimate O(log band width) wide-gap convergence always
                    // completes, but still a closed-form, finite ceiling (never literally unbounded).
                    int bisectionBudget = emptyUpperBisectionBudget(cForPivot, H);
                    boolean bisected = false;
                    for (int budget = 0; budget < bisectionBudget; budget++) {
                        recordEmptyUpperBisection();
                        m = ByteMidpoint.between(cForPivot, m);
                        if (m == null) {
                            victim.markNonProductiveSteal(snap);
                            log.debug("steal_failed run_id={} worker_id={} outcome={} victim_node={} reason={}",
                                    runId, RunContext.workerIdOrNone(), Outcome.RETRY, victim.nodeId(),
                                    "retry_pivot_adjacent");
                            return record(Outcome.RETRY, "retry_pivot_adjacent");   // c and the live head are adjacent now; re-steal later
                        }
                        if (probeNonEmpty(m, H)) {
                            bisected = true;
                            break;
                        }
                    }
                    if (!bisected) {
                        // Budget exhausted — every capped halving still probed an empty upper, so the
                        // only pivots left are near-cursor slivers that lose the cursor_passed_pivot race. Give
                        // up this attempt (the owner drains the tail; a genuinely slow victim wins next time)
                        // rather than commit a doomed pivot.
                        victim.markNonProductiveSteal(snap);
                        victim.recordFutileSteal();   // per-victim futility (exhausted bisection chain)
                        log.debug("steal_failed run_id={} worker_id={} outcome={} victim_node={} reason={}",
                                runId, RunContext.workerIdOrNone(), Outcome.RETRY, victim.nodeId(),
                                "bisect_budget_exhausted");
                        return record(Outcome.RETRY, "bisect_budget_exhausted");
                    }
                    pivotMechanism = "bisect";
                }
            } else {
                // A parent-empty sliver with NO discoverable sub-directory structure — a FLAT LEAF
                // (one high-entropy directory of flat FILES). Only density extrapolation can bisect
                // it; attempt a flat-leaf density-reflected pivot BEFORE committing the byte-exact
                // sliver, falling through UNCHANGED on reject (flatLeafDensityPivot).
                byte[] flatLeaf = flatLeafDensityPivot(victim, cForPivot, H);
                if (flatLeaf != null) {
                    if (log.isDebugEnabled()) {
                        log.debug("steal_flat_leaf_pivot run_id={} worker_id={} victim_node={} pivot={} hi={}",
                                runId, RunContext.workerIdOrNone(), victim.nodeId(), StealMath.describe(flatLeaf), StealMath.describe(H));
                    }
                    m = flatLeaf;
                    pivotMechanism = "flat_leaf";
                }
            }
        }

        // 5. Lock-guarded RE-VALIDATE-and-narrow hand-off. The snapshot and probe were
        //    speculative; re-check BOTH the bound and the cursor under the lock.
        //
        // The RETRY outcomes below are captured into pendingOutcome/pendingReason rather than
        // returned directly: record(...) emits a trace event (a lock-holder must
        // never wait on JsonlTraceSink's global synchronized writer), so it is called AFTER the
        // finally/unlock, mirroring the split_committed hoist above. traceVictimNodeId/
        // traceChildId keep their dummy initializers because they are only read on the success
        // path, which is the sole branch that assigns them.
        long traceVictimNodeId = -1;
        long traceChildId = -1;
        Outcome pendingOutcome = null;
        String pendingReason = null;
        victim.lock().lock();
        try {
            if (!Arrays.equals(victim.hi(), H)) {
                // EARLY loser: another thief already narrowed the bound, so our m/probe
                // are stale. Re-snapshot on the next attempt — and DO NOT touch victim.hi:
                // restoring H here would clobber the winner's narrow and re-open the overlap.
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
                // Volatile narrow to (lo, m]: publishes the new bound to the scanner's per-key reader, which
                // stops the victim at the first key > m. Boundary m stays LEFT (m <= victim's bound), I3.
                victim.narrowHi(m);

                // Synchronous durable-before-list split (algorithms.md §3, §4.3): returns only after the
                // child is committed PENDING. Holding victim.lock across this await is correct for the
                // thief — it gates only this victim's next commit-enqueue.
                long childId = store.splitNode(new SplitSpec(runId, victim.nodeId(), m, H));
                if (childId == CheckpointStore.SPLIT_ABORTED) {
                    // LATE loser: we narrowed, but the durable CAS rejected us (a 2nd thief's
                    // stale split, or the victim completed via a terminal page). Restore the bound we
                    // validated under the lock and retry; the victim keeps owning the whole (lo, H].
                    victim.restoreHi(H);
                    log.debug("steal_failed run_id={} worker_id={} outcome={} victim_node={} reason={}",
                            runId, RunContext.workerIdOrNone(), Outcome.RETRY, victim.nodeId(), "split_aborted");
                    pendingOutcome = Outcome.RETRY;
                    pendingReason = "split_aborted";
                } else {
                    // Success: hand off (m, H] to the driver. The bump of `outstanding` happens inside
                    // accept(), BEFORE we release the lock, so quiescence can't miss the new child (§4.4).
                    childSink.accept(childId, m, H);
                    // Clear the victim's emit-progress flag: it is now ineligible as a steal victim until it
                    // commits another non-empty page (progress-gated stealing — paces re-splits to ≤1 per
                    // emitted page so the fetch wins each round, closing the latency livelock). The driver
                    // curates the eligible victim pool; this is the matching "consume" of that progress.
                    victim.markStolen();
                    if (metrics != null) {
                        metrics.recordStealReason("PIVOT", pivotMechanism);   // winning-mechanism engagement (§5)
                        metrics.recordPivotByteRegion(m, cForPivot);          // dead-zone diagnostic (§5)
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("split_committed run_id={} worker_id={} victim_node={} child_node={} lo={} m={} hi={}",
                                runId, RunContext.workerIdOrNone(), victim.nodeId(), childId,
                                StealMath.describe(victim.lo()), StealMath.describe(m), StealMath.describe(H));
                    }
                    traceVictimNodeId = victim.nodeId();
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
        // Emit the trace split OUTSIDE victim.lock: JsonlTraceSink.writeEvent is a
        // global synchronized whose flush can block on disk I/O — a lock-holder must never wait on it.
        // The fields were captured under the lock above; correctness is unchanged (trace is a pure
        // side-channel), and quiescence accounting already completed inside childSink.accept().
        if (trace.enabled()) {
            trace.split(RunContext.workerIdOrNone(), traceVictimNodeId, traceChildId, pivotMechanism, m, H);
        }
        return record(Outcome.CHILD_CREATED, "split_committed");
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
        for (ListEntry e : page.entries()) {
            byte[] k = e.key().rawUnsafe();
            return H == null || KeyBytes.compareUnsigned(k, H) <= 0;
        }
        return false;   // no key past m
    }

    /**
     * The demand-driven {@code delimiter=/} structure probe. Issues ONE bounded
     * {@code ListObjectsV2(prefix=Q, delimiter=/, start_after=c,
     * max_keys=}{@link #STRUCTURE_PROBE_MAX_KEYS}{@code )} where {@code Q} is the caller-supplied
     * directory prefix at/above the cursor ({@link #structureProbePrefix} for the empty-upper
     * funnel, {@link #adaptiveStructurePivot} for the parent-empty sliver), and returns a
     * {@code CommonPrefix} boundary {@code b} with {@code c < b < H} from the one page — a real,
     * populated, XML-safe boundary to split on: the <b>median</b> when the page is complete, the
     * <b>furthest proved boundary</b> when it is truncated (the two-regime choice below). Returns
     * {@code null} when the region has no qualifying sub-directory boundary (a flat dense region,
     * or every boundary is at/past {@code H}); the caller then falls back to byte-midpoint
     * bisection unchanged.
     *
     * <p><b>Median, not the first sibling.</b> Under progress-gated stealing the victim has already
     * emitted its eligible page, so the next sibling directory leaves it near-nothing
     * (baton-passing), while the median hands victim and child each ~half the clustered region —
     * both run in parallel and each half recursively bisects (the {@code O(log)} ramp). ANY populated
     * boundary strictly in {@code (c, H)} tiles exactly (I2/I3), so the choice is pure balance; one
     * probe per attempt keeps it O(pages), never O(directories) (INT-8). On a truncated page the
     * true median is unaffordable (see {@link #STRUCTURE_PROBE_MAX_KEYS}) and the furthest sampled
     * boundary is the largest carve the probe can prove. See walkthroughs.md §2.
     */
    /**
     * Issue one {@code delimiter=/} structure probe, attributing an ATTEMPT-TIMEOUT outcome to this
     * victim before rethrowing.
     *
     * <p><b>Why this exists.</b> The per-victim suppression in {@link #structureProbesEnabled} is fed
     * by what a probe REPORTS. A probe that times out reports nothing — it throws past the fan-out
     * accounting in {@link #structurePivot} — so before this, a timing-out probe left the suppression
     * counters untouched: <em>the timeout destroyed the very evidence that would have stopped the next
     * probe</em>, and the thief kept re-probing a region that had already proved it could not answer,
     * paying the full escalated budget every time. Recording the streak here closes that loop.
     *
     * <p>Only {@link ThrottleException.Kind#ATTEMPT_TIMEOUT} counts. A 503/5xx is store backpressure —
     * a statement about the store's load, not about this keyspace's shape — and suppressing structure
     * discovery on it would misattribute a transient service condition to the bucket. Every fault,
     * counted or not, still propagates unchanged: this observes, it never swallows.
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

    private StructuralPivot structurePivot(WorkerState victim, byte[] q, byte[] cForPivot, byte[] startAfter,
                                           byte[] H) throws SwathException, InterruptedException {
        ListPage page = probeStructure(victim, new PageRequest(
                mode, STRUCTURE_PROBE_MAX_KEYS, q, SeedStep.DELIMITER, startAfter, null, null, null, null, 0));
        int fanout = page.commonPrefixes().size();
        // The delimiter=/ fan-out this runtime structure probe observed (§5 classification).
        // Structure-probe fetches are their own probe-I/O class (swath.probe.structure_fetches).
        // Fan-out drives THIS VICTIM's consecutive zero-fan-out
        // suppression counter (per-victim, not shared across the pool) — zero fan-out means no
        // sub-directory structure here; any non-zero fan-out re-enables probing on this victim.
        if (metrics != null) {
            metrics.recordDelimiterFanout(fanout);
            metrics.recordStructureProbeFetch();
            if (page.truncated()) {
                // The probe did not reach the end of the directory, so its CommonPrefixes are a
                // PREFIX of the children and the median below would be the sample's, not the range's.
                // Counted so post-hoc analysis can attribute small carves to the cap rather than to
                // the keyspace, and can tell whether raising it would buy balance (§5).
                metrics.recordStealReason("STRUCTURE", "fanout_capped");
            }
        }
        if (fanout == 0) {
            victim.recordZeroFanoutStructureProbe();
        } else {
            victim.resetZeroFanoutStructureProbes();
        }
        List<byte[]> boundaries = new ArrayList<>();
        for (KeyBytes cp : page.commonPrefixes()) {
            byte[] b = cp.rawUnsafe();
            if (KeyBytes.compareUnsigned(b, cForPivot) <= 0) {
                continue;                                  // not strictly past the cursor
            }
            if (H != null && KeyBytes.compareUnsigned(b, H) >= 0) {
                continue;                                  // not strictly below the bound
            }
            boundaries.add(b);
        }
        if (boundaries.isEmpty()) {
            return null;                                   // flat dense region — caller bisects
        }
        boundaries.sort(KeyBytes::compareUnsigned);        // S3 sorts, but mock/other stores may not
        if (!page.truncated()) {
            // Whole directory seen — the true median.
            return new StructuralPivot(boundaries.get(boundaries.size() / 2), false);
        }
        // Truncated: these are the FIRST children only, so the sample's median sits near the cursor,
        // not near the middle of the range — a balanced split is not available from this probe at any
        // cost we can afford (see STRUCTURE_PROBE_MAX_KEYS). Take the furthest boundary instead: the
        // deepest cut this probe can PROVE exists, and so the largest carve on offer — which is also
        // the most race-durable, since a pivot far from the cursor is the one a fast drainer cannot
        // overrun before the CAS commits. Any boundary strictly in (c, H) tiles exactly (I2/I3), so
        // this is a balance choice only. `truncated` is the authoritative signal: max_keys is shared
        // between Contents and CommonPrefixes, so a low fan-out does NOT imply a complete directory.
        return new StructuralPivot(boundaries.get(boundaries.size() - 1), true);
    }

    /**
     * A structure-probe pivot plus whether the probe page was truncated — i.e. whether this is the
     * directory's true median or the furthest boundary the probe could prove. Carried to the commit
     * site so the {@code PIVOT} mechanism tag distinguishes the two (§5).
     */
    private record StructuralPivot(byte[] pivot, boolean capped) {
    }

    /**
     * The probe prefix {@code Q} for {@link #structurePivot}: the deepest {@code /}-terminated
     * prefix shared by the range's {@code lo} and the cursor (so a {@code delimiter=/} listing at
     * {@code Q} enumerates the cursor's sibling sub-directories), never shorter than the scan
     * prefix {@code P}. Operates on raw bytes (I10) — {@code /} is a single ASCII byte that never
     * occurs inside a multi-byte UTF-8 sequence, so this is byte-exact.
     */
    private byte[] structureProbePrefix(byte[] lo, byte[] cForPivot) {
        byte[] loBytes = (lo == null) ? BOTTOM : lo;
        int common = commonPrefixLen(loBytes, cForPivot);
        int len = 0;
        for (int i = common - 1; i >= 0; i--) {
            if (cForPivot[i] == '/') {
                len = i + 1;                               // include the trailing '/'
                break;
            }
        }
        int floor = (prefix == null) ? 0 : prefix.length;
        if (len < floor) {
            len = floor;                                   // never probe above the scan scope
        }
        return Arrays.copyOf(cForPivot, len);
    }

    /**
     * The durable adaptive probe for the <b>parent-empty sliver</b>. A single FIXED back-out level is
     * keyspace-dependent: for {@code YYYY/MM/DD/uuid} keys the cursor's sibling level
     * ({@code 2022/03/05/}) lands WITHIN the current day, whose median sits so close ahead that the
     * fast drainer races past it before the CAS commits ({@code cursor_passed_pivot}); for
     * {@code .../ledgers/<hash>} keys that SAME level is the durable hash-sibling level. So probe
     * COARSE→FINE from the directory where the cursor and {@code H} first diverge and return the
     * FIRST (coarsest) level whose {@code delimiter=/} listing yields a boundary strictly in
     * {@code (c, H)}: there the siblings span toward {@code H}, so the median lands FAR ahead of the
     * cursor (durable). Bounded to {@code MAX_STRUCTURE_BACKOUT_LEVELS + 1} probes per attempt (INT-8).
     * Returns {@code null} when no level yields a boundary (a genuinely flat funnel); the caller then
     * commits the byte-exact sliver unchanged. See walkthroughs.md §2.
     */
    private StructuralPivot adaptiveStructurePivot(WorkerState victim, byte[] cForPivot, byte[] startAfter,
                                                  byte[] H) throws SwathException, InterruptedException {
        int floor = (prefix == null) ? 0 : prefix.length;
        int len = divergenceFlooredPrefixLen(cForPivot, H, floor);
        for (int level = 0; level <= MAX_STRUCTURE_BACKOUT_LEVELS; level++) {
            StructuralPivot pivot = structurePivot(victim, Arrays.copyOf(cForPivot, len), cForPivot, startAfter, H);
            if (pivot != null) {
                return pivot;                               // coarsest level with a real boundary — most durable
            }
            int next = nextSegmentEnd(cForPivot, len);
            if (next <= len) {
                break;                                      // no finer '/'-segment within the cursor
            }
            len = next;                                     // descend one directory level and re-probe
        }
        return null;
    }

    /**
     * The {@code /}-terminated directory prefix where {@code cursor} and {@code H} first diverge — the
     * COARSEST directory whose {@code delimiter=/} children can straddle the cursor and the bound (a
     * coarser prefix contains both; a finer one only the cursor's own side). Trims the raw common byte
     * prefix back to its last {@code /} and floors at the scan prefix {@code P}. Byte-exact ({@code /}
     * is a single ASCII byte that never occurs inside a multi-byte UTF-8 sequence — I10).
     */
    private static int divergenceFlooredPrefixLen(byte[] cursor, byte[] H, int floor) {
        int common = commonPrefixLen(cursor, H);
        int len = 0;
        for (int i = common - 1; i >= 0; i--) {
            if (cursor[i] == '/') {
                len = i + 1;                                // include the divergence directory's trailing '/'
                break;
            }
        }
        return Math.max(len, floor);
    }

    /**
     * The end index (just past the next {@code /}) of the cursor's directory segment beginning at
     * {@code from}, or {@code from} when no further {@code /} exists — one step FINER in the coarse→fine
     * adaptive back-out.
     */
    private static int nextSegmentEnd(byte[] cursor, int from) {
        for (int i = from; i < cursor.length; i++) {
            if (cursor[i] == '/') {
                return i + 1;
            }
        }
        return from;
    }

    /**
     * The flat-leaf density-reflected pivot. When a parent-empty sliver has no discoverable
     * sub-directory structure — one high-entropy directory of flat FILES — neither byte-midpoint nor
     * the {@code delimiter=/} probe can do better than sliver at the cursor; only DENSITY
     * EXTRAPOLATION bisects it. Reflect the consumed span forward via the open-frontier
     * {@link StealMath#extrapolate} (pure math, no probe) toward a ceiling clamped INSIDE the leaf.
     *
     * <p>The reflection reference is the child's own {@code lo} when it is a real deep key inside the leaf
     * (ZERO probes); otherwise — a bootstrap worker whose {@code lo} is the leaf directory itself, not a
     * real key — ONE bounded {@code maxKeys=1} floor probe reads the leaf's true minimum key to seed the
     * reflection. The ceiling is clamped to {@code min(H, prefixCeil(leafDir))} so the pivot stays
     * strictly inside the leaf and can NEVER cross into the next directory. Returns {@code null} (caller
     * commits the byte-exact sliver unchanged) when the leaf is empty, there is no consumed span below
     * the cursor, or the reflected pivot is not a strictly-interior non-sliver boundary. See
     * walkthroughs.md §2.
     */
    private byte[] flatLeafDensityPivot(WorkerState victim, byte[] cForPivot, byte[] H)
            throws SwathException, InterruptedException {
        int floor = (prefix == null) ? 0 : prefix.length;
        byte[] leafDir = Arrays.copyOf(cForPivot, leafDirPrefixLen(cForPivot, floor));
        byte[] leafCeil = StealMath.prefixCeil(leafDir);
        byte[] ceil = (leafCeil == null) ? H : minUnsigned(H, leafCeil);

        byte[] lo = victim.lo();
        byte[] ref;
        if (lo != null && lo.length > leafDir.length && commonPrefixLen(lo, leafDir) == leafDir.length) {
            ref = lo;   // a real deep key inside the leaf — reflect the child's own consumed span, no probe
        } else {
            // Bootstrap worker: lo IS the leaf directory (not a real key). ONE maxKeys=1 floor probe reads
            // the leaf's true minimum key to seed the reflection — no delimiter, matching the probe arity
            // used elsewhere in Thief. Bounded by progress-gating + the unproductive-steal suppression.
            ListPage page = fetcher.fetchPage(
                    new PageRequest(mode, 1, leafDir, null, null, null, null, null, null, 0));
            byte[] kFirst = null;
            for (ListEntry e : page.entries()) {
                kFirst = e.key().rawUnsafe();
                break;
            }
            if (kFirst == null || KeyBytes.compareUnsigned(kFirst, cForPivot) >= 0) {
                return null;   // empty leaf, or no consumed span below the cursor — commit the sliver
            }
            ref = kFirst;
        }

        byte[] m2 = StealMath.extrapolate(ref, cForPivot, ceil);
        // Accept only a strictly-interior, non-sliver boundary. The c < m2 < H check is LOAD-BEARING,
        // not merely defensive: when ceil (= min(H, prefixCeil(leafDir))) is not valid UTF-8 — reachable
        // when leafDir == P and P doesn't end in '/', so prefixCeil(P) increments into a lone
        // continuation byte — extrapolate's effectiveCeiling falls back to MAX_UTF8_KEY and can return
        // m2 >= H. Do NOT delete this check. Byte-exactness (I1/I2/I3) is THE gate; on any
        // failure fall through to the byte-exact sliver.
        if (m2 != null
                && !isCursorAdjacentSliver(m2, cForPivot)
                && KeyBytes.compareUnsigned(cForPivot, m2) < 0
                && KeyBytes.compareUnsigned(m2, H) < 0) {
            return m2;
        }
        return null;
    }

    /**
     * The cursor's deepest {@code /}-terminated directory prefix (the LEAF directory) length, floored at
     * the scan prefix {@code P}. Mirrors the byte-exact {@code /}-handling of {@link #structureProbePrefix}
     * ({@code /} = 0x2F is a single ASCII byte that never occurs inside a multi-byte UTF-8 sequence — I10).
     */
    private static int leafDirPrefixLen(byte[] cForPivot, int floor) {
        int len = 0;
        for (int i = cForPivot.length - 1; i >= 0; i--) {
            if (cForPivot[i] == '/') {
                len = i + 1;   // include the trailing '/'
                break;
            }
        }
        return Math.max(len, floor);
    }

    /** The unsigned byte-lex minimum of {@code a} and {@code b}. */
    private static byte[] minUnsigned(byte[] a, byte[] b) {
        return KeyBytes.compareUnsigned(a, b) <= 0 ? a : b;
    }

    /**
     * Whether {@code m} is the <b>cursor-adjacent MIN_SAFE sliver</b>: {@code cForPivot}
     * with exactly one {@code 0x20} (U+0020, {@code ByteMidpoint.MIN_SAFE}) byte appended — the
     * cursor's immediate successor that {@link ByteMidpoint#between} is forced to return when the
     * bounds diverge at adjacent code points. This is the byte-exact, deterministic signature of the
     * parent-empty degenerate pivot (a {@code fracIn} span test would miss it: the appended byte
     * leaves a sub-epsilon residue inside the {@code K}-byte window, not a hard zero). A genuine
     * divergent midpoint does NOT carry the full cursor as a prefix, so it is never flagged.
     */
    private static boolean isCursorAdjacentSliver(byte[] m, byte[] cForPivot) {
        return m.length == cForPivot.length + 1
                && m[m.length - 1] == (byte) 0x20
                && Arrays.equals(m, 0, cForPivot.length, cForPivot, 0, cForPivot.length);
    }

    private static int commonPrefixLen(byte[] a, byte[] b) {
        int n = Math.min(a.length, b.length);
        int i = 0;
        while (i < n && a[i] == b[i]) {
            i++;
        }
        return i;
    }

    /**
     * The log-scaled empty-upper bisection budget — {@code ceil(log2(bandWidthBytes))
     * + MARGIN} halvings — for one steal attempt's retry-nearer-cursor loop. {@code H == null} (open
     * frontier) has no upper bound to measure a byte distance against, so it uses the fixed
     * {@link #OPEN_FRONTIER_BAND_WIDTH_FALLBACK}.
     */
    private static int emptyUpperBisectionBudget(byte[] cForPivot, byte[] H) {
        long width = (H == null) ? OPEN_FRONTIER_BAND_WIDTH_FALLBACK : bandWidthBytes(cForPivot, H);
        int log2Width = 64 - Long.numberOfLeadingZeros(Math.max(1L, width));
        return log2Width + EMPTY_UPPER_BISECTION_MARGIN;
    }

    /**
     * A coarse byte-distance estimate of the {@code (c, H]} gap: reads {@link #BAND_WIDTH_SUFFIX_BYTES}
     * bytes starting at the first divergence position of {@code c} and {@code H} as unsigned big-endian
     * numbers (missing trailing bytes — a shorter suffix — read as 0) and returns their absolute
     * difference, floored at 1. Never a precise code-point-space measurement (that lives inside {@link
     * ByteMidpoint}) — just cheap and generous enough to size {@link #emptyUpperBisectionBudget}.
     */
    private static long bandWidthBytes(byte[] c, byte[] H) {
        int i = commonPrefixLen(c, H);
        long a = suffixValue(c, i);
        long b = suffixValue(H, i);
        return Math.max(1L, Math.abs(b - a));
    }

    /** {@code arr}'s {@link #BAND_WIDTH_SUFFIX_BYTES}-byte suffix starting at {@code from}, as an unsigned
     * big-endian integer (missing bytes past {@code arr}'s end read as 0). */
    private static long suffixValue(byte[] arr, int from) {
        long v = 0L;
        for (int k = 0; k < BAND_WIDTH_SUFFIX_BYTES; k++) {
            int idx = from + k;
            v = (v << 8) | ((idx < arr.length) ? (arr[idx] & 0xFFL) : 0L);
        }
        return v;
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

    /**
     * (Scoped per-victim): whether a {@code delimiter=/} structure probe
     * should fire this attempt against {@code victim}. Enabled until {@code victim} has racked up
     * {@link #STRUCTURE_ZERO_FANOUT_SUPPRESS_THRESHOLD} consecutive zero-fan-out probes proving THIS
     * victim's region has no sub-directory structure at the probed level, OR
     * {@link #STRUCTURE_TIMEOUT_SUPPRESS_THRESHOLD} consecutive probes that TIMED OUT rather than
     * answering (see {@link #probeStructure}); once suppressed for this victim by EITHER streak, only
     * a 1-in-{@link #STRUCTURE_SUPPRESS_RETRY_DIVISOR} recovery probe fires so late-appearing
     * structure still re-enables probing (the next non-zero fan-out resets the zero-fan-out counter
     * in {@link #structurePivot}; the next successful answer resets the timeout counter in
     * {@link #probeStructure}). A DIFFERENT victim's counters are entirely independent, so one
     * flat/suppressed victim can never starve structure probing on another (the same
     * global-vs-per-victim scoping discipline as the futility pacing in {@link #steal} above).
     */
    private boolean structureProbesEnabled(WorkerState victim) {
        // The structure_probes toggle off disables demand-driven delimiter=/ structure
        // probing entirely — both callers below fall straight through to their non-structure
        // fallback (bisection / flat-leaf), and the per-victim zero-fan-out suppression counter
        // simply never gets consulted or mutated (moot, not NPE-prone).
        if (!toggles.structureProbes()) {
            return false;
        }
        if (victim.consecutiveZeroFanoutProbes() < STRUCTURE_ZERO_FANOUT_SUPPRESS_THRESHOLD
                && victim.consecutiveTimedOutStructureProbes() < STRUCTURE_TIMEOUT_SUPPRESS_THRESHOLD) {
            return true;
        }
        // EITHER streak suppresses; the same random re-probe escape applies, so a victim is never
        // locked out permanently if the keyspace or the network changes underneath it.
        return ThreadLocalRandom.current().nextInt(STRUCTURE_SUPPRESS_RETRY_DIVISOR) == 0;
    }

    /**
     * Engagement counter (§5): the alphabet-aware chooser {@code selected} a populated scalar when
     * the digest-drawn pivot {@code m} differs from the plain code-point interpolate at the same
     * fraction; otherwise it {@code fell back}. Bounded ranges only ({@code H != null}) — the open
     * frontier synthesizes its pivot via {@code extrapolate}, not the alphabet chooser. The extra
     * interpolate is pure math on split (never per-key), so it costs nothing on the hot path.
     */
    private void recordAlphabetEngagement(byte[] m, byte[] lo, byte[] hi, double f) {
        if (metrics == null || hi == null) {
            return;
        }
        byte[] plain = StealMath.interpolate(lo, hi, f);
        metrics.recordStealReason("ALPHABET",
                !Arrays.equals(m, plain) ? "alphabet_chosen" : "alphabet_fallback");
    }

}

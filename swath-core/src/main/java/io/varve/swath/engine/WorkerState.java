/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import io.varve.swath.observability.RunMetrics;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * The published per-worker state a thief reads; the worker scans its half-open range
 * {@code (lo, hi]} of one {@code listing_node}, and the thief and engine driver live elsewhere.
 * The concurrency contract — a leading cursor, a volatile bound, and the per-worker lock tying
 * them together — is specified in algorithms.md §1.2/§3; the per-field roles:
 *
 * <p><b>Leading in-memory cursor.</b> {@link #cursor} is advanced <b>under {@link #lock()} at the
 * instant the worker enqueues a page commit</b> — never on future resolution, never the DB cursor.
 * It thus leads the durable cursor, so a thief reading {@code cursor < m} under the lock reads the
 * leading edge — the reason the split CAS's {@code cursor < pivot} clause cannot fail within one
 * process (algorithms.md §3).
 *
 * <p><b>Volatile bound.</b> {@link #hi} is an {@link AtomicReference} exposed to
 * {@link RangeScanner} as {@code () -> hi.get()} ({@link #hiSupplier()}) so the scanner's per-key
 * re-read sees a thief's narrow: the volatile write carries the happens-before the lock-free reader
 * needs, and the thief writes it under {@link #lock()} so two thieves serialize.
 *
 * <p><b>Coordination.</b> {@link #lock()} is taken only at page-commit (worker: advance
 * {@code cursor} + enqueue) and at steal (thief: snapshot, re-validate, narrow {@code hi} across
 * the whole hand-off); {@link #snapshot()} reads a coherent {@code (cursor, hi)} pair under it.
 */
public final class WorkerState {

    private final long nodeId;
    private final byte[] lo;
    private final AtomicReference<byte[]> cursor;
    private final AtomicReference<byte[]> hi;
    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicLong keysEmitted = new AtomicLong();
    private volatile boolean unsplittable = false;
    private volatile Snapshot lastNonProductiveSteal;

    /**
     * Whether this worker has committed a non-empty page (advanced its {@link #cursor}) since it was
     * created or last stolen from — the progress gate that makes it a valid steal victim (see
     * {@link #stealEligible()}; algorithms.md §3.2). Set by {@link #setCursor} (worker, at page
     * commit), cleared by {@link #markStolen} (thief, at hand-off); both under {@link #lock()}.
     */
    private final AtomicBoolean emittedSinceSteal = new AtomicBoolean(false);

    /**
     * Whether this node has EVER split during its lifetime — a thief steal or an owner self-split
     * carved a child off it. Set by {@link #markStolen()}, the single choke point both split
     * mechanisms already call (see that method); sticky, never cleared. Consumed by
     * {@link ConfettiFeedbackGate}'s confetti classification: a node that split even once is never
     * confetti, no matter how small its own final {@link #keysEmitted()} tally — shedding a
     * productive subtree is the feedback signal the gate wants, not a thinning tail. See
     * docs/internals/metrics-internals.md §5. Read only after this node's range has completed (already
     * out of the live victim pool), so a plain volatile read races no writer.
     *
     * <p><b>Process-local, not durable.</b> Never persisted to the checkpoint: a resumed node gets a
     * fresh {@code WorkerState} with {@code hasSplit} starting {@code false}, the same run-scoped
     * treatment as the density digest fields above. Intentional (see {@link OwnerSelfSplit}'s
     * {@code ownerSplitTaggedChildren} field javadoc for the resume-semantics rationale), not a
     * durability gap to close.
     */
    private volatile boolean hasSplit = false;

    /**
     * Running count of consecutive zero-fan-out {@code delimiter=/} structure probes against
     * <b>this victim</b>. Scoped per-victim, never a shared {@link Thief}-global counter: a global
     * one lets a long zero-fan-out run on one flat region suppress structure probing for every other
     * victim too, starving a mixed flat-then-structured bucket's parent-empty slivers.
     * {@link Thief#structurePivot} increments it on zero fan-out and resets it on any non-zero
     * fan-out; {@link Thief#structureProbesEnabled} consults it before firing a probe on this
     * victim. Read/written without {@link #lock()} — a pure balance heuristic, never correctness,
     * so a benign race only shifts when suppression engages by a probe or two.
     */
    private final AtomicInteger consecutiveZeroFanoutProbes = new AtomicInteger();

    /**
     * Per-victim futility pacing. Running count of consecutive <b>futile</b> steal outcomes against
     * <b>this victim</b> ({@code cursor_passed_pivot} races, {@code bound_moved}, and
     * {@code bisect_budget_exhausted} chains) since it last yielded a child. After
     * {@link #FUTILITY_PACE_THRESHOLD} of them a per-victim cooldown ({@link #stealPacingSkips})
     * trips so idle thieves stop hammering this specific racing drainer — never a global pace, so a
     * productive sibling stays fully stealable. Reset on this victim's productive progress
     * ({@link #markStolen}). {@link #futilityTrips} grows the cooldown (bounded-exponential) for a
     * persistently-racing victim. All read/written without {@link #lock()} — a pure balance
     * heuristic, never correctness (a stale read only shifts when pacing engages by an attempt or two).
     */
    private final AtomicInteger consecutiveFutileSteals = new AtomicInteger();
    private final AtomicInteger futilityTrips = new AtomicInteger();
    private final AtomicInteger stealPacingSkips = new AtomicInteger();

    /** Consecutive futile outcomes against one victim that trip a cooldown. */
    static final int FUTILITY_PACE_THRESHOLD = 4;
    /** The cap on the bounded-exponential per-victim cooldown length, in steal-selection skips. */
    static final int FUTILITY_PACE_MAX_COOLDOWN = 64;

    /**
     * Density digest — a tiny per-worker signal recorded as the
     * worker commits pages, used only to pick the {@link Thief}'s far-ahead pivot fraction (§3). It
     * adds <b>zero</b> extra store/S3 calls and carries no velocity/timestamp state: just the last
     * committed page's first/last key + count ({@code lastPage}) and an EWMA of the trailing pages'
     * keys-per-code-point-span ({@code trailingDensityEwma}). Denser ahead ⇒ a larger fraction ⇒ the
     * split lands farther ahead of a fast drainer's cursor so the split CAS wins the race.
     */
    private record PageDigest(byte[] firstKey, byte[] lastKey, long count) {
    }

    private volatile PageDigest lastPage;
    private volatile double trailingDensityEwma;

    /**
     * Observed-alphabet digest — a zero-API companion to the density digest that learns the
     * per-position populated byte alphabet from keys already in hand (updated under {@link #lock()} in
     * {@link #recordPage}, read lock-free by the {@link Thief}). Fed to {@link StealMath#interpolate}
     * so a synthesized pivot lands on a real populated value (not a hex/UUID dead zone). Both
     * {@code estRemaining} call sites (owner-split gate, {@link Thief} victim selection) use the
     * plain code-point {@link StealMath#estRemaining(byte[], byte[], byte[], long)} estimate.
     */
    private final AlphabetDigest alphabet;

    /**
     * The pivot fraction with no density signal yet: the plain code-point midpoint, so
     * {@link StealMath#interpolate} degrades to {@code byteMidpoint} and the split is unchanged. The
     * far-ahead push (up to {@value #MAX_FAR_FRACTION}) is justified only once a page's local
     * density can be compared to the range average — see {@link #densityFraction()}.
     */
    private static final double DEFAULT_FAR_FRACTION = 0.5;
    /**
     * The maximum far-ahead fraction (a uniformly-dense drainer): place the split 3/4 toward
     * {@code hi}. Package-visible (not {@code private}) so {@link EngineToggles}'s {@code
     * density_ewma=off} substitute can reuse the same ceiling instead of duplicating the constant.
     */
    static final double MAX_FAR_FRACTION = 0.75;
    /** EWMA weight on the newest page's local density (the rest carried from the trailing history). */
    private static final double DENSITY_EWMA_ALPHA = 0.3;

    /**
     * Slow-range dump: this range's own wall-clock start (for the terminal summary's per-range
     * drain-rate estimate — {@code keysEmitted() / (now - createdAtNanos)}). Process-local only, same
     * treatment as {@link #hasSplit}/the density digest — never persisted, a resumed node gets a
     * fresh clock.
     */
    private final long createdAtNanos = System.nanoTime();

    /**
     * Slow-range dump: plain per-range steal-reason tallies, bumped alongside the existing
     * GLOBAL {@code swath.steal_reason} counters at the exact same 4 decision points ({@link Thief}'s
     * {@code cursor_passed_pivot}/{@code no_pivot}/{@code suppressed_zero_fanout} outcomes against
     * THIS victim, and {@link WorkStealingScan}'s {@code demand_gated} owner-split suppression of
     * THIS worker's own range) — never a new hot-path check, never a map. Read only by the terminal/
     * mid-run summary's {@code slow_ranges[]} dump; observation only, no bearing on any split/steal
     * decision.
     */
    private final AtomicLong cursorPassedPivotTally = new AtomicLong();
    private final AtomicLong noPivotTally = new AtomicLong();
    private final AtomicLong structureSuppressedTally = new AtomicLong();
    private final AtomicLong demandGatedTally = new AtomicLong();

    /**
     * The single construction path: wires {@code metrics} into the {@link AlphabetDigest} so its
     * per-consult {@code ALPHABET.*} fallback engagement counters fire. {@code metrics} may be
     * {@code null} — the seam a testkit object-mother uses for the many unit tests that drive a
     * {@link Thief} directly against a hand-built victim with no metrics sink.
     *
     * @param nodeId  the {@code listing_node} id this worker owns
     * @param lo      the immutable lower bound {@code A} of {@code (lo, hi]} (the node's
     *                {@code range_start}; used by the thief's frontier extrapolation, §3.1)
     * @param cursor  the initial leading cursor (the node's resume cursor; {@code null} = ⊥)
     * @param hi      the initial upper bound {@code B} ({@code null} = the open frontier)
     * @param metrics per-run metrics holder, or {@code null} for a metrics-less unit test
     */
    public WorkerState(long nodeId, byte[] lo, byte[] cursor, byte[] hi,
                       RunMetrics metrics) {
        this.nodeId = nodeId;
        this.lo = lo;
        this.cursor = new AtomicReference<>(cursor);
        this.hi = new AtomicReference<>(hi);
        this.alphabet = new AlphabetDigest(lo, hi, metrics);
        // A fresh node hasn't emitted yet (emittedSinceSteal = false), so it is not a steal
        // victim until it commits its first non-empty page (see stealEligible()).
    }

    /** A coherent {@code (cursor, hi)} pair read under {@link #lock()} (algorithms.md §3). */
    public record Snapshot(byte[] cursor, byte[] hi) {
    }

    public long nodeId() {
        return nodeId;
    }

    /** The immutable lower bound {@code A} of {@code (lo, hi]}. */
    public byte[] lo() {
        return lo;
    }

    /** The leading in-memory cursor (last emitted / about-to-commit key; {@code null} = ⊥). */
    public byte[] cursor() {
        return cursor.get();
    }

    /**
     * Advance the leading cursor. The worker calls this <b>while holding
     * {@link #lock()}</b>, at the moment it enqueues the page commit (§4.1), only for a non-empty
     * page (an empty page never advances the cursor, §2). Records emit progress so the worker
     * becomes a steal victim again (progress-gated stealing — see {@link #stealEligible()}).
     */
    public void setCursor(byte[] key) {
        cursor.set(key);
        emittedSinceSteal.set(true);
    }

    /**
     * Record the last committed non-empty page's shape into the density digest. The worker calls
     * this <b>while holding {@link #lock()}</b> at page commit (alongside {@link #setCursor}), so the
     * digest is coherent with the cursor advance. {@code firstKey}/{@code lastKey} are the first and
     * last in-range keys of the page and {@code count} its in-range size. No extra I/O — this is pure
     * arithmetic over keys already fetched.
     */
    public void recordPage(byte[] firstKey, byte[] lastKey, long count) {
        if (firstKey == null || lastKey == null || count <= 0) {
            return;
        }
        double span = StealMath.spanIn(firstKey, lastKey, lo, hi.get());
        if (span > 0.0) {
            double local = count / span;                 // keys per unit window-fraction over this page
            double prev = trailingDensityEwma;
            trailingDensityEwma = (prev <= 0.0)
                    ? local
                    : DENSITY_EWMA_ALPHA * local + (1.0 - DENSITY_EWMA_ALPHA) * prev;
        }
        lastPage = new PageDigest(firstKey, lastKey, count);
        // Fold this page's endpoints into the observed-alphabet digest (zero API — the keys are
        // already in hand). First/last of each ascending page sweep the leading-position alphabet as
        // the drain progresses, which is exactly the position radix pivots split on.
        alphabet.observe(firstKey);
        alphabet.observe(lastKey);
    }

    /**
     * The observed-alphabet digest. Consumed by {@link StealMath#interpolate(byte[], byte[], double,
     * AlphabetDigest)} (alphabet-aware pivot synthesis). Read lock-free by the {@link Thief}; a stale
     * read only shifts pivot balance within valid bounds (never correctness).
     */
    public AlphabetDigest alphabetDigest() {
        return alphabet;
    }

    /**
     * The {@link Thief}'s far-ahead pivot fraction {@code f ∈ [0.5, 0.75]}. Compares the
     * trailing pages' local key density to the range's overall density: when the region just listed
     * is at least as dense as the range average (a uniformly-dense drainer), return the forward
     * {@value #MAX_FAR_FRACTION} so the split lands far ahead of the
     * cursor; as the region ahead thins the fraction eases back toward {@code 0.5} (the plain
     * code-point midpoint, i.e. {@link StealMath#interpolate} degrades to {@code byteMidpoint}). With
     * no page recorded yet it returns {@value #DEFAULT_FAR_FRACTION} (the plain midpoint — a fresh
     * victim has no density basis, and the engine only steals from progress-gated victims that do).
     * Read <b>without</b> {@link #lock()} (like
     * the cursor/hi selection reads); a stale read only nudges load balance, never correctness — the
     * lock-guarded hand-off still re-validates the bound and cursor.
     */
    public double densityFraction() {
        double trailing = trailingDensityEwma;
        if (trailing <= 0.0) {
            return DEFAULT_FAR_FRACTION;
        }
        byte[] cur = cursor.get();
        byte[] hiNow = hi.get();
        if (cur == null || hiNow == null || lo == null) {
            return DEFAULT_FAR_FRACTION;
        }
        double consumed = StealMath.spanIn(lo, cur, lo, hiNow);
        long emitted = keysEmitted.get();
        if (consumed <= 0.0 || emitted <= 0) {
            return DEFAULT_FAR_FRACTION;
        }
        double overall = emitted / consumed;
        if (overall <= 0.0) {
            return DEFAULT_FAR_FRACTION;
        }
        double ratio = trailing / overall;               // >= 1 ⇒ region ahead at least as dense as avg
        double f = 0.5 + 0.25 * Math.min(1.0, ratio);
        return Math.max(0.5, Math.min(MAX_FAR_FRACTION, f));
    }

    /**
     * Observed local-vs-average density ratio ({@code trailingEwmaDensity / averageDensity}),
     * consumed by {@link WorkStealingScan}'s owner-split observed-mass child-tail floor. {@code >= 1}
     * means the region just listed is at least as dense as the drained average (uniform / still-dense);
     * {@code < 1} means the tail is thinning, so a code-point-span estimate over-states the far child
     * tail. Returns {@link Double#POSITIVE_INFINITY} when there is no usable density signal yet (fresh
     * victim / no consumed span), so the floor falls back to the plain span estimate (treats the region
     * as uniform — never blocks on an absent signal). Same zero-cost EWMA inputs as
     * {@link #densityFraction()}, read <b>without</b> {@link #lock()} (a stale read only nudges the
     * proactive-carve balance, never correctness — the lock-guarded hand-off still re-validates).
     */
    double observedDensityRatio() {
        double trailing = trailingDensityEwma;
        if (trailing <= 0.0) {
            return Double.POSITIVE_INFINITY;
        }
        byte[] cur = cursor.get();
        byte[] hiNow = hi.get();
        if (cur == null || hiNow == null || lo == null) {
            return Double.POSITIVE_INFINITY;
        }
        double consumed = StealMath.spanIn(lo, cur, lo, hiNow);
        long emitted = keysEmitted.get();
        if (consumed <= 0.0 || emitted <= 0) {
            return Double.POSITIVE_INFINITY;
        }
        double overall = emitted / consumed;
        if (overall <= 0.0) {
            return Double.POSITIVE_INFINITY;
        }
        return trailing / overall;
    }

    /** The current upper bound {@code B} ({@code null} = the open frontier). */
    public byte[] hi() {
        return hi.get();
    }

    /**
     * The per-key bound supplier wired into {@link RangeScanner} (algorithms.md §2): the
     * scanner re-reads it on every key so a thief's narrow takes effect mid-page.
     */
    public Supplier<byte[]> hiSupplier() {
        return hi::get;
    }

    /** The tiny per-worker lock (page-commit on the worker; steal on the thief). */
    public ReentrantLock lock() {
        return lock;
    }

    /**
     * Lower the bound to the pivot {@code m}. The thief calls this <b>while holding
     * {@link #lock()}</b>, after re-validating {@code hi == H} and {@code cursor < m};
     * the volatile write publishes the new bound to the lock-free per-key reader (§3).
     */
    public void narrowHi(byte[] m) {
        hi.set(m);
    }

    /**
     * Restore the bound to {@code oldHi} after an aborted split (thief, holding
     * {@link #lock()}) — {@code oldHi} was current under the lock, so this is
     * single-process safe (§3).
     */
    public void restoreHi(byte[] oldHi) {
        hi.set(oldHi);
    }

    /** A lock-guarded coherent snapshot of {@code (cursor, hi)} for the thief (§3). */
    public Snapshot snapshot() {
        lock.lock();
        try {
            return new Snapshot(cursor.get(), hi.get());
        } finally {
            lock.unlock();
        }
    }

    public long keysEmitted() {
        return keysEmitted.get();
    }

    public void addKeysEmitted(long n) {
        keysEmitted.addAndGet(n);
    }

    /** {@code true} once the range is known too small to split (§3 — cached so steals don't re-probe). */
    public boolean unsplittable() {
        return unsplittable;
    }

    public void setUnsplittable(boolean v) {
        unsplittable = v;
    }

    /**
     * Whether a previous steal attempt already proved this exact {@code (cursor, hi)}
     * snapshot non-productive. Repeating it would compute the same pivot/probe and burn
     * another 1-key store call without creating a child; once the owner advances the cursor
     * or another thief moves {@code hi}, the snapshot no longer matches and stealing resumes.
     */
    public boolean unchangedSinceNonProductiveSteal(Snapshot snap) {
        Snapshot prev = lastNonProductiveSteal;
        return prev != null
                && Arrays.equals(prev.cursor(), snap.cursor())
                && Arrays.equals(prev.hi(), snap.hi());
    }

    /** Remember a non-productive steal snapshot so idle thieves do not re-probe it unchanged. */
    public void markNonProductiveSteal(Snapshot snap) {
        lastNonProductiveSteal = snap;
    }

    /**
     * Whether this worker has made <b>emit progress</b> (committed a non-empty page) since it was
     * created or last stolen from — the progress gate that makes it a steal victim (algorithms.md
     * §3.2). The engine driver presents only steal-eligible workers to the {@link Thief}, which
     * paces re-splitting of one worker to <b>at most one split per emitted page</b>: the page fetch
     * always wins a round before the worker is carved again, and a freshly-created child is never
     * carved before it lists its first page. This closes the high-concurrency latency livelock —
     * idle thieves narrowing a victim's {@code hi} into the empty gap above its cursor faster than
     * the advancing page returns, spinning durable splits with the fetch starved — while a worker on
     * a genuinely large range still yields one split per page (each split halves the remaining span).
     * See algorithms.md §3.2.
     *
     * <p>Read without {@link #lock()} during victim curation; the lock-guarded hand-off still
     * re-validates the bound and cursor, so a stale read can at worst admit one extra (still-correct)
     * split — never a missed key.
     */
    public boolean stealEligible() {
        return emittedSinceSteal.get();
    }

    /**
     * Mark this worker stolen-from: clear the emit-progress flag so it is ineligible
     * ({@link #stealEligible()} → false) until it commits another non-empty page. The thief calls
     * this <b>while holding {@link #lock()}</b> at a successful hand-off.
     *
     * <p>A successful carve is this victim's productive progress, so it also clears any futility
     * pacing ({@link #recordFutileSteal}) — a victim that just yielded a child is not a phantom drainer.
     *
     * <p>Also sticks {@link #hasSplit} — the ONLY two call sites of this method are the
     * two ways a node can split (thief steal, owner self-split), so this is the single choke point for
     * "this node split at least once" regardless of which mechanism did it.
     */
    public void markStolen() {
        emittedSinceSteal.set(false);
        consecutiveFutileSteals.set(0);
        futilityTrips.set(0);
        stealPacingSkips.set(0);
        hasSplit = true;
    }

    /**
     * Whether this node has EVER split (thief steal or owner self-split) during
     * its lifetime. See {@link #hasSplit}'s javadoc for the confetti-classification rationale.
     */
    public boolean hasSplit() {
        return hasSplit;
    }

    /**
     * This victim's running count of consecutive zero-fan-out
     * {@code delimiter=/} structure probes. Consulted by {@link Thief#structureProbesEnabled}.
     */
    public int consecutiveZeroFanoutProbes() {
        return consecutiveZeroFanoutProbes.get();
    }

    /** Record a zero-fan-out structure probe against this victim (increments the counter). */
    public void recordZeroFanoutStructureProbe() {
        consecutiveZeroFanoutProbes.incrementAndGet();
    }

    /** Record a non-zero-fan-out structure probe against this victim (resets the counter). */
    public void resetZeroFanoutStructureProbes() {
        consecutiveZeroFanoutProbes.set(0);
    }

    /**
     * Record a <b>futile</b> steal outcome against this victim (a raced/exhausted attempt that
     * created no child — {@code cursor_passed_pivot}, {@code bound_moved}, {@code bisect_budget_exhausted}).
     * After {@link #FUTILITY_PACE_THRESHOLD} consecutive futile outcomes it trips a per-victim cooldown of
     * a bounded-exponential number of steal-selection skips ({@link #stealPaced}), so idle thieves pace
     * their attempts against THIS specific racing drainer instead of hammering it every cycle — while a
     * productive sibling stays fully stealable (per-victim scope, never global).
     */
    public void recordFutileSteal() {
        if (consecutiveFutileSteals.incrementAndGet() >= FUTILITY_PACE_THRESHOLD) {
            consecutiveFutileSteals.set(0);
            int trips = futilityTrips.incrementAndGet();
            int cooldown = (int) Math.min((long) FUTILITY_PACE_MAX_COOLDOWN, 1L << Math.min(trips, 20));
            stealPacingSkips.set(cooldown);
        }
    }

    /**
     * Whether this victim is currently <b>paced</b> (in a futility cooldown) and should be skipped
     * as a steal victim this attempt. Consumes one skip of the cooldown when it returns {@code true}, so
     * the cooldown decays one steal-selection pass at a time and the victim becomes eligible again once
     * it expires (or sooner, on productive progress via {@link #markStolen}). Read/mutated without
     * {@link #lock()} — a benign race only ends a cooldown an attempt or two early.
     */
    public boolean stealPaced() {
        if (stealPacingSkips.get() <= 0) {
            return false;
        }
        stealPacingSkips.decrementAndGet();
        return true;
    }

    // ---- Slow-range dump: per-range steal-reason tallies + drain-rate clock -------------

    /** This range's creation instant ({@link System#nanoTime()}), for the drain-rate estimate. */
    public long createdAtNanos() {
        return createdAtNanos;
    }

    /** Bump alongside {@code RETRY.cursor_passed_pivot} — see {@link Thief#steal}. */
    public void recordCursorPassedPivot() {
        cursorPassedPivotTally.incrementAndGet();
    }

    /** Bump alongside {@code UNSPLITTABLE.no_pivot} — see {@link Thief#steal}. */
    public void recordNoPivot() {
        noPivotTally.incrementAndGet();
    }

    /** Bump alongside {@code STRUCTURE.suppressed_zero_fanout} — see {@link Thief#steal}. */
    public void recordStructureSuppressed() {
        structureSuppressedTally.incrementAndGet();
    }

    /** Bump alongside {@code OWNER_SPLIT.demand_gated} — see {@code OwnerSelfSplit#maybeOwnerSelfSplit}. */
    public void recordDemandGated() {
        demandGatedTally.incrementAndGet();
    }

    public long cursorPassedPivotTally() {
        return cursorPassedPivotTally.get();
    }

    public long noPivotTally() {
        return noPivotTally.get();
    }

    public long structureSuppressedTally() {
        return structureSuppressedTally.get();
    }

    public long demandGatedTally() {
        return demandGatedTally.get();
    }
}

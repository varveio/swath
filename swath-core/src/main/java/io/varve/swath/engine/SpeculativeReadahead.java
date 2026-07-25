/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import io.varve.swath.error.ListingException;
import io.varve.swath.error.ProtocolViolationException;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageFetcher;
import io.varve.swath.store.PageRequest;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

/**
 * Intra-range speculative readahead.
 * Owned by a single {@link RangeScanner#runRange} invocation once that range has collapsed to a
 * serial owner drain (a dense opaque tail draining one page per RTT). While the scanner's cursor is
 * at {@code C}, this fires {@code K} concurrent {@code start-after} fetches at guessed offsets
 * {@code G1 < G2 < … < GK} ahead of {@code C}, holds the responses in a bounded adoption buffer,
 * and reconciles them back into the scanner's serial emission stream as the cursor advances.
 *
 * <p><b>Exactly-once linchpin.</b> This class NEVER emits or commits anything itself — it only
 * changes <i>how</i> {@link RangeScanner} obtains the entries for its next contiguous page (from a
 * pre-fetched buffered page instead of a fresh serial fetch). A buffered page fetched at guess
 * {@code G} is only ever handed back when {@code G <=ᵤ cursor} (unsigned), trimmed to the keys
 * {@code > cursor}: since a page returned by {@code start-after=G} is contiguous over
 * {@code (G, last]} and {@code cursor ∈ [G, last)}, the suffix {@code (cursor, last]} has no gap, so
 * the scanner's committed cursor still advances <b>only through contiguous, fetched pages</b> (I1
 * commit-before-emit and byte-exact in-order emission are preserved by construction). A guess that
 * lands ahead of the cursor ({@code G > cursor}) stays buffered (HOLD) until the cursor reaches it;
 * a fully-overlapped buffered page ({@code last <= cursor}) is discarded; a guess that a split
 * narrowed the range past ({@code G >= hi}) is discarded (cancelled). Never a jump-ahead adoption.
 *
 * <p><b>Split interaction.</b> The scanner re-reads the volatile {@code hi} per page and calls
 * {@link #onHiNarrowed} whenever a thief / owner-split lowered it; any in-flight or buffered guess
 * at/above the new bound is dropped (its keys are now the child's) and the future is cancelled. New
 * guesses are never placed at/above the current bound. So a split cleanly retargets/discards
 * speculation beyond the new {@code hi} with no leakage.
 *
 * <p><b>Bounded memory.</b> At most {@code K} speculative pages exist at any instant (in-flight +
 * buffered ≤ {@code K}); the buffer alone therefore holds ≤ {@code K} pages (~{@code K × maxKeys}
 * keys). Guesses that land far ahead simply stay in the {@code K}-slot budget as HOLD pages rather
 * than bloating; once the budget is full no new guess is launched (demand back-off).
 *
 * <p><b>Crash / resume.</b> All state here is process-local (buffer, futures, executor) and never
 * durable. A crash discards it harmlessly: on resume the node reopens at its durable last-emitted
 * cursor and re-lists forward, re-doing at most {@code K} un-adopted speculative fetches — no key is
 * lost or duplicated because nothing was ever emitted/committed out of contiguous cursor order.
 *
 * <p><b>Off-gauge by design — fetcher isolation.</b> The {@code fetcher} this class fetches through is deliberately NOT the worker's
 * own slot-gated {@code GaugedFetcher}: {@code RangeScanner} threads a DEDICATED
 * fetcher for speculation (mirroring the {@link Thief}'s own probe fetcher construction exactly —
 * {@code slotGated=false, reportSuccess=false}). This matters two ways: (1) a disposable guess never
 * acquires an AIMD concurrency-gauge slot or votes the gauge on the happy path, so {@code K} guesses
 * per engaged worker never compete with ordinary worker fetches for the same permit pool (the
 * {@code K+1}-per-worker pressure amplification a shared, slot-gated fetcher would cause during a 503
 * storm — exactly when the engine is trying to shed); and (2) a guess that exhausts its transient-
 * retry budget fails FAST on the probe fetcher's small, independent cap rather than ever reaching the
 * worker fetcher's {@code RetryPolicy.BOUNDED} give-up path, which cancels the WHOLE RUN as a side
 * effect ({@code CancellationToken#cancel(StopReason.STUCK, ...)}) — a disposable speculative fetch
 * must never be able to do that. {@code K} still bounds the RAW added HTTP concurrency (each engaged
 * worker issues up to {@code K} genuinely concurrent LIST calls, by design — that IS the mechanism);
 * what it does NOT do any more is count against the adaptive concurrency target {@code T} or amplify
 * AIMD pressure. As a crude additional safety valve, {@link #replenish} also consults a
 * {@code stealingAllowed} gate (the same {@code ConcurrencyGauge#isStealingAllowed()} the thief's idle
 * loop already reads) and stops LAUNCHING new guesses while it is false — already in-flight/buffered
 * guesses are still served normally.
 *
 * <p><b>Fail-soft, never run-cancelling — bar one fault.</b> A speculative fetch fault (a
 * throttle, a genuine {@link ListingException}, a cancellation) is absorbed here — recorded as
 * {@code READAHEAD.speculative_fault} and dropped — never propagated to the caller. The scanner then
 * falls back to an ordinary serial fetch for that page, exactly as if speculation had simply missed
 * (a {@code guess_gap}). This holds both for a completed-but-faulted background guess
 * ({@link #drainCompleted}) and for the PIPELINED continuation the scanner is actively waiting on
 * ({@link #tryNextPage}) — a guess that was about to be adopted must fail exactly as softly as one
 * discovered idle. The sole exception is a {@link ProtocolViolationException}, which propagates from
 * both paths: it condemns the ENDPOINT rather than the fetch, so absorbing it would let an endpoint
 * that over-serves only INTERMITTENTLY finish a run undetected — the serial refetch of the same
 * region can answer conformingly, and nothing would ever surface. It propagates from the DISCARD
 * paths too — budget expiry, split narrowing, a timed-out pipelined await, {@link #close} — because
 * those merely cancel a guess and drop it, and {@link java.util.concurrent.Future#cancel} silently
 * throws away the result of a future that already finished; see {@link #discard}.
 *
 * <p><b>Bounded even for a persistent voting throttle.</b> The isolated
 * fetcher's off-gauge isolation (above) does NOT change one thing: a genuine 503 {@code SlowDown}/5xx
 * (a {@code ThrottleException} whose {@code kind().votesAimdDown()} is true) retries INSIDE {@code
 * GaugedFetcher} UNBOUNDED, by design, identically regardless of {@code slotGated} — the
 * {@code PROBE_TRANSIENT_RETRY_CAP} fail-fast only ever applies to the NON-voting branch (attempt-
 * timeout/network faults). Left unguarded here, a speculative guess pinned by a persistent throttle
 * storm would never complete, and {@link #tryNextPage}'s pipelined await would then block the
 * OWNER'S OWN forward progress indefinitely — worse than no readahead at all (e.g. a guess that
 * happens to land in a hotter, differently-throttled sub-range than the cursor's own plain
 * continuation would have hit). So every speculative fetch — regardless of failure mode — is bounded
 * by a wall-clock {@code fetchBudgetMillis} ({@link ReadaheadConfig#fetchBudgetMillis()}), enforced
 * two ways: (1) {@link #tryNextPage}'s pipelined await uses a TIMED {@code Future#get}, converting an
 * expiry into the SAME fail-soft {@code speculative_fault} path (never an indefinite block); and (2)
 * {@link #drainCompleted} proactively cancels and reclaims any NOT-YET-DONE guess past budget (so a
 * storm cannot silently shrink the {@code K}-slot budget to zero for the rest of the drain — a stuck
 * future is reclaimed even when nobody is synchronously waiting on it). The {@link Thief}'s own probe
 * fetcher shares the identical unbounded-503 behavior and is NOT changed here — a stuck probe merely
 * idles the one thief thread that issued it, it never blocks an owner's drain, so the same urgency
 * does not apply there.
 *
 * <p>Not thread-safe: every method is called on the owning worker thread. The only cross-thread
 * objects are the fetch {@link Future}s (drained non-blocking, or awaited — with a bounded timeout —
 * when their guess is already at/behind the cursor — the pipelined continuation).
 */
final class SpeculativeReadahead implements AutoCloseable {

    private final PageFetcher fetcher;
    private final byte[] prefix;
    private final int maxKeys;
    private final RunMetrics metrics;
    private final int k;
    private final int maxBuffered;
    /**
     * The safety-valve gate: {@link #replenish} stops LAUNCHING new guesses
     * while this reports {@code false} (mirrors {@code ConcurrencyGauge#isStealingAllowed()}, the same
     * gate the thief's idle-steal loop already reads). Already in-flight/buffered guesses are still
     * served normally — this only pauses adding MORE off-gauge load.
     */
    private final BooleanSupplier stealingAllowed;
    /**
     * The wall-clock nanos budget (converted once from {@code
     * ReadaheadConfig#fetchBudgetMillis()}) a single speculative fetch — INCLUDING every internal
     * {@code GaugedFetcher} retry, voting-throttle or not — is allowed to run before it is treated as
     * a {@code speculative_fault} (see the class javadoc's "Bounded even for a persistent voting
     * throttle" paragraph).
     */
    private final long fetchBudgetNanos;

    private final ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor();
    /** Completed, non-empty speculative pages keyed by the guess (unsigned) they were fetched at. */
    private final TreeMap<byte[], ListPage> buffer = new TreeMap<>(KeyBytes::compareUnsigned);
    private final List<InFlight> inFlight = new ArrayList<>();

    /** The current (possibly narrowed) upper bound; guesses at/above it are discarded. */
    private byte[] hi;
    /**
     * The trailing edge of the guess-placement frontier: {@code [stepLo, stepHi]} is the last
     * one-page keyspace window, reflected forward one page at a time (representation-independent)
     * to place the next guess ahead of it — the proposal's density-reflected placement.
     */
    private byte[] stepLo;
    private byte[] stepHi;
    private boolean closed;

    /** {@code launchedAtNanos} is the budget clock's start ({@link System#nanoTime()} at {@link #launch}). */
    private record InFlight(byte[] guess, Future<ListPage> future, long launchedAtNanos) {
    }

    /** One page handed back to {@link RangeScanner}: entries already trimmed to {@code > cursor}. */
    record Adopted(List<ListEntry> entries, boolean truncated) {
    }

    /**
     * @param fetcher          the DEDICATED, fail-soft, off-gauge fetcher for guess-ahead fetches —
     *                         never the caller's own worker fetcher (see the class javadoc's
     *                         fetcher-isolation paragraph)
     * @param firstKey         first key of the observed engaging page (the one-page reflection window lo)
     * @param engageCursor     last key of the observed engaging page (= the cursor at engage; window hi)
     * @param hiAtEngage       the range's upper bound at engage
     * @param stealingAllowed  consulted by {@link #replenish} before launching new guesses (safety valve)
     * @param fetchBudgetMillis wall-clock budget per speculative fetch, ALL failure modes;
     *                         {@code <= 0} is clamped up to 1ns (never a zero/negative budget)
     */
    SpeculativeReadahead(PageFetcher fetcher, byte[] prefix, int maxKeys,
                         byte[] firstKey, byte[] engageCursor, byte[] hiAtEngage,
                         int k, int maxBuffered, BooleanSupplier stealingAllowed, long fetchBudgetMillis,
                         RunMetrics metrics) {
        this.fetcher = fetcher;
        this.prefix = prefix;
        this.maxKeys = maxKeys;
        this.metrics = metrics;
        this.hi = hiAtEngage;
        this.k = Math.max(1, k);
        this.maxBuffered = Math.max(1, maxBuffered);
        this.stealingAllowed = stealingAllowed == null ? () -> true : stealingAllowed;
        this.fetchBudgetNanos = Math.max(1L, TimeUnit.MILLISECONDS.toNanos(fetchBudgetMillis));
        this.stepLo = firstKey;
        this.stepHi = engageCursor;
        record("engaged");
    }

    /**
     * Back-compat overload, kept so the independent contract suite's existing direct-construction call
     * sites keep compiling unchanged: the {@code mode} param is accepted-but-unused (dead — {@link #launch}
     * always synthesizes a plain OBJECTS {@link PageRequest}, mirroring {@link RangeScanner}'s own
     * unused {@code mode} parameter), {@code stealingAllowed} defaults to always-allowed (no gauge to
     * isolate from in a bare direct-construction test), and {@code fetchBudgetMillis} defaults to
     * {@link ReadaheadConfig#DEFAULT_FETCH_BUDGET_MILLIS} (generous — none of that suite's fixtures
     * exercise a persistent-throttle scenario, so this is a no-op for them).
     */
    SpeculativeReadahead(PageFetcher fetcher, byte[] prefix, ListingMode mode, int maxKeys,
                         byte[] firstKey, byte[] engageCursor, byte[] hiAtEngage,
                         int k, int maxBuffered, RunMetrics metrics) {
        this(fetcher, prefix, maxKeys, firstKey, engageCursor, hiAtEngage, k, maxBuffered, () -> true,
                ReadaheadConfig.DEFAULT_FETCH_BUDGET_MILLIS, metrics);
    }

    /**
     * A thief / owner-split narrowed the range's upper bound. Drop every in-flight and buffered
     * guess at/above the new bound (their keys are now the child's) — counted {@code cancelled_split}
     * — and remember the new bound so no future guess is placed at/above it.
     *
     * <p>The pruning always runs to completion; a violation found on a discarded guess is raised
     * afterwards, so the bound and the guess set stay consistent either way.
     *
     * @throws ProtocolViolationException raised by a guess that had already come back over-serving
     *                                    when the narrow discarded it (see {@link #discard})
     */
    void onHiNarrowed(byte[] hiNow) throws ProtocolViolationException {
        if (hiNow == null || KeyBytes.compareUnsigned(hiNow, hi) >= 0) {
            return;   // widened or unchanged — never happens for a narrow, but guard defensively
        }
        this.hi = hiNow;
        ProtocolViolationException violation = null;
        for (Iterator<InFlight> it = inFlight.iterator(); it.hasNext(); ) {
            InFlight f = it.next();
            if (KeyBytes.compareUnsigned(f.guess(), hiNow) >= 0) {
                it.remove();
                try {
                    discard(f.future());
                } catch (ProtocolViolationException e) {
                    violation = violation == null ? e : violation;
                    continue;
                }
                record("cancelled_split");
            }
        }
        for (Iterator<byte[]> it = buffer.keySet().iterator(); it.hasNext(); ) {
            byte[] g = it.next();
            if (KeyBytes.compareUnsigned(g, hiNow) >= 0) {
                it.remove();
                record("cancelled_split");
            }
        }
        if (violation != null) {
            throw violation;
        }
    }

    /**
     * Try to satisfy the scanner's next page (continuing from {@code cursor}) from speculation.
     * Returns an {@link Adopted} page (entries already trimmed to {@code > cursor}) when a buffered —
     * or an about-to-complete in-flight — guess is a contiguous continuation of the cursor, else
     * {@code null} (the scanner then does an ordinary serial fetch — including when the pipelined
     * guess about to be adopted turns out to have FAULTED or EXPIRED, see below). Never returns a
     * page that would skip keys: only guesses with {@code guess <=ᵤ cursor} are adopted.
     *
     * @throws ProtocolViolationException the one speculative fault that is not absorbed (see the
     *                                    class javadoc's fail-soft paragraph)
     */
    Adopted tryNextPage(byte[] cursor) throws ProtocolViolationException {
        if (cursor == null) {
            return null;   // ⊥ cursor: nothing has been emitted, no guess can be at/behind it
        }
        drainCompleted();
        Adopted a = pollAdoptable(cursor);
        if (a != null) {
            return a;
        }
        // Nothing ready, but a guess already at/behind the cursor is the pipelined continuation —
        // await it (its siblings were fired concurrently and are completing in parallel). BOUNDED:
        // a persistent voting-throttle 503/5xx retries UNBOUNDED inside
        // GaugedFetcher regardless of slotGated, so an unconditional await here could block the
        // OWNER'S OWN forward progress indefinitely on a disposable guess — worse than no readahead.
        InFlight pipelined = takeInFlightAtOrBelow(cursor);
        if (pipelined == null) {
            return null;
        }
        ListPage page;
        try {
            page = awaitBounded(pipelined);
        } catch (ProtocolViolationException violation) {
            // Not absorbable: the response was one no conforming store may produce, so the verdict is
            // on the endpoint, not on this disposable fetch. Dropping it here would hide an endpoint
            // that over-serves only intermittently, since the serial refetch below can come back
            // conforming and the run would finish as if the violation never happened.
            throw violation;
        } catch (Exception e) {
            // Fail-soft: a guess the scanner was
            // about to ADOPT faulted OR ran out of its wall-clock budget — never propagate and abort
            // the whole range. Cancel it so a persistent-throttle guess stops consuming
            // resources/occupying its slot, drop it exactly like a background fault in drainCompleted
            // below, and let the caller fall back to an ordinary serial fetch. An EXPIRED wait leaves
            // the result unobserved, so the discard — not the wait above — is what catches a guess
            // that over-served just after the budget ran out.
            discard(pipelined.future());
            record("speculative_fault");
            return null;
        }
        absorb(pipelined.guess(), page);
        return pollAdoptable(cursor);
    }

    /**
     * Top the speculation budget back up to {@code K} in-flight/buffered guesses, placed one
     * (scaled) page apart ahead of {@code cursor} within the fixed engage window. Called after each
     * page the scanner emits. Launches NOTHING new while {@link #stealingAllowed} reports
     * {@code false} (the safety valve) — already in-flight/buffered guesses are
     * still drained/served normally either way.
     *
     * @throws ProtocolViolationException raised by a completed guess drained here (see the class
     *                                    javadoc's fail-soft paragraph)
     */
    void replenish(byte[] cursor) throws ProtocolViolationException {
        if (closed) {
            return;
        }
        drainCompleted();
        // If the cursor overtook the placement frontier (e.g. after a big serial catch-up), re-anchor
        // the one-page reflection window so the next guess is placed just ahead of the cursor.
        if (cursor != null && KeyBytes.compareUnsigned(stepHi, cursor) < 0) {
            stepLo = stepHi;
            stepHi = cursor;
        }
        if (!stealingAllowed.getAsBoolean()) {
            return;   // safety valve: don't add more off-gauge load during an active pause
        }
        while (inFlight.size() + buffer.size() < k && buffer.size() < maxBuffered) {
            byte[] g = nextGuess();
            if (g == null) {
                return;   // no forward room (frontier at/over the bound) — serial finishes the tail
            }
            stepLo = stepHi;
            stepHi = g;
            if (cursor != null && KeyBytes.compareUnsigned(g, cursor) <= 0) {
                continue;   // frontier still behind the cursor — advance the window without launching
            }
            launch(g);
        }
    }

    /**
     * The next guess one page ahead of the current frontier {@code [stepLo, stepHi]}: reflect the
     * observed one-page span forward (density-reflected placement, {@link StealMath#extrapolate}) —
     * representation-independent (it reflects the real observed one-page key span, so it works on
     * decimal, hex, UUID, and skewed keyspaces alike, degrading gracefully to bounded overlap/gap
     * waste on skew). The reflection lands just below the next page boundary, so the continuation is
     * usually an overlap-trim adoption rather than a gap that needs a serial fetch. {@code null} =
     * no forward room (frontier at/over the bound).
     */
    private byte[] nextGuess() {
        byte[] reflected = StealMath.extrapolate(stepLo, stepHi, hi);
        if (reflected == null || KeyBytes.compareUnsigned(reflected, stepHi) <= 0
                || KeyBytes.compareUnsigned(reflected, hi) >= 0) {
            return null;
        }
        return reflected;
    }

    private void launch(byte[] guess) {
        Future<ListPage> f = exec.submit(() ->
                fetcher.fetchPage(PageRequest.objects(prefix, guess, maxKeys)));
        inFlight.add(new InFlight(guess, f, System.nanoTime()));
        record("guess_placed");
    }

    /**
     * Move every completed in-flight future into the buffer (or discard it), without blocking. Also
     * reclaims any NOT-YET-DONE future that has been outstanding longer than
     * {@link #fetchBudgetNanos} — almost certainly stuck retrying an unbounded voting-throttle storm
     * inside the isolated fetcher — so a persistent storm cannot silently shrink the {@code K}-slot
     * speculation budget to zero for the rest of this range's drain even when nobody is synchronously
     * awaiting that particular guess ({@link #tryNextPage}'s OWN bounded wait handles the case where
     * someone IS waiting on it).
     */
    private void drainCompleted() throws ProtocolViolationException {
        long now = System.nanoTime();
        for (Iterator<InFlight> it = inFlight.iterator(); it.hasNext(); ) {
            InFlight f = it.next();
            if (!f.future().isDone()) {
                if (now - f.launchedAtNanos() >= fetchBudgetNanos) {
                    it.remove();
                    discard(f.future());   // it may have completed since the isDone() check above
                    record("speculative_fault");
                }
                continue;
            }
            it.remove();
            ListPage page;
            try {
                page = await(f.future());
            } catch (ProtocolViolationException violation) {
                throw violation;   // condemns the endpoint, not the guess — see tryNextPage's twin
            } catch (Exception e) {
                // A speculative fetch failed (throttle / cancelled / listing fault): drop it, fail-soft
                // (never propagate — this is a background guess, not the page the scanner is waiting
                // on right now). The serial path will fetch that region for real and surface any
                // genuine error there. Counted so a fail-soft event is visible in the metrics (§5).
                record("speculative_fault");
                continue;
            }
            absorb(f.guess(), page);
        }
    }

    /** File a completed speculative page into the buffer, or discard it if useless. */
    private void absorb(byte[] guess, ListPage page) {
        if (page == null || page.entries().isEmpty()) {
            return;   // empty ⇒ the listing ends before this guess; serial reaches the end first
        }
        if (KeyBytes.compareUnsigned(guess, hi) >= 0) {
            record("cancelled_split");   // a narrow beat this fetch home
            return;
        }
        buffer.put(guess, page);
    }

    /**
     * The buffered page that is a contiguous continuation of {@code cursor} and advances it furthest:
     * the entry with the largest {@code guess <=ᵤ cursor} whose last key is still {@code > cursor}.
     * Prunes fully-overlapped ({@code last <= cursor}) and out-of-bound ({@code guess >= hi}) buffered
     * pages as it goes. Returns the trimmed, adopted page or {@code null}.
     */
    private Adopted pollAdoptable(byte[] cursor) {
        for (Iterator<Map.Entry<byte[], ListPage>> it = buffer.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<byte[], ListPage> e = it.next();
            byte[] g = e.getKey();
            if (KeyBytes.compareUnsigned(g, hi) >= 0) {
                it.remove();
                record("cancelled_split");
                continue;
            }
            byte[] last = e.getValue().entries().getLast().key().rawUnsafe();
            if (KeyBytes.compareUnsigned(last, cursor) <= 0) {
                it.remove();
                record("discarded_overlap");
            }
        }
        byte[] g = buffer.floorKey(cursor);   // largest guess <= cursor (adoptable; last > cursor now)
        if (g == null) {
            return null;
        }
        ListPage page = buffer.remove(g);
        List<ListEntry> trimmed = trimAboveExclusive(page.entries(), cursor);
        // Defensive, not currently reachable: the prune pass above already
        // removed every buffered entry with `last <= cursor`, and `absorb` never buffers an
        // empty-entries page, so the survivor at `g` always has `last > cursor` — meaning
        // trimAboveExclusive always leaves at least `last` behind. Kept as a belt-and-suspenders
        // guard (cheap, and load-bearing only if that upstream invariant ever changes) rather than
        // removed, so a future refactor of the prune ordering fails safe instead of returning an
        // empty-but-"adopted" page.
        if (trimmed.isEmpty()) {
            record("discarded_overlap");
            return null;
        }
        record("adopted_page");
        return new Adopted(trimmed, page.truncated());
    }

    /** Remove and return the in-flight guess with the largest {@code guess <=ᵤ cursor}, or null. */
    private InFlight takeInFlightAtOrBelow(byte[] cursor) {
        InFlight best = null;
        for (InFlight f : inFlight) {
            if (KeyBytes.compareUnsigned(f.guess(), cursor) <= 0
                    && (best == null || KeyBytes.compareUnsigned(f.guess(), best.guess()) > 0)) {
                best = f;
            }
        }
        if (best != null) {
            inFlight.remove(best);
        }
        return best;
    }

    private static List<ListEntry> trimAboveExclusive(List<ListEntry> entries, byte[] cursor) {
        int i = 0;
        while (i < entries.size()
                && KeyBytes.compareUnsigned(entries.get(i).key().rawUnsafe(), cursor) <= 0) {
            i++;
        }
        return i == 0 ? entries : entries.subList(i, entries.size());
    }

    private ListPage await(Future<ListPage> future) throws ListingException, InterruptedException {
        try {
            ListPage page = future.get();
            recordPageShape(page);
            return page;
        } catch (ExecutionException e) {
            throw unwrapExecutionException(e);
        }
    }

    /**
     * The SAME wait as {@link #await}, but bounded to whatever remains of
     * {@code f}'s {@link #fetchBudgetNanos} clock (started at {@link #launch} time) — used ONLY by
     * {@link #tryNextPage}'s pipelined continuation, the one call site that would otherwise block the
     * caller (the owner's own {@code RangeScanner} loop) on an unbounded voting-throttle retry storm.
     * A {@link TimeoutException} is a checked {@code Exception} like {@link ListingException}/
     * {@link InterruptedException}, so the caller's single {@code catch (Exception e)} already
     * handles it via the identical fail-soft path — no separate catch needed there.
     */
    private ListPage awaitBounded(InFlight f)
            throws ListingException, InterruptedException, TimeoutException {
        long remaining = Math.max(0L, fetchBudgetNanos - (System.nanoTime() - f.launchedAtNanos()));
        try {
            ListPage page = f.future().get(remaining, TimeUnit.NANOSECONDS);
            recordPageShape(page);
            return page;
        } catch (ExecutionException e) {
            throw unwrapExecutionException(e);
        }
    }

    private void recordPageShape(ListPage page) {
        if (metrics != null) {
            metrics.recordListingPageShape(page.entries().size(), page.truncated(), maxKeys);
        }
    }

    private static RuntimeException unwrapExecutionException(ExecutionException e)
            throws ListingException, InterruptedException {
        switch (e.getCause()) {
            case ListingException le -> throw le;
            case InterruptedException ie -> {
                Thread.currentThread().interrupt();
                throw ie;
            }
            case RuntimeException re -> throw re;
            case Error err -> throw err;
            case null -> throw new ListingException("speculative fetch failed");
            default -> throw new ListingException("speculative fetch failed: " + e.getCause());
        }
    }

    /**
     * Throw away a guess the drain no longer wants — the budget expired, a split narrowed past it, its
     * pipelined await timed out, or the engagement is closing — surfacing a
     * {@link ProtocolViolationException} it has ALREADY completed with instead of dropping it.
     *
     * <p>{@link Future#cancel} returns {@code false} for a future that already finished, and its
     * result is then simply thrown away. So a guess that came back over-serving but lost the race to
     * the discard would vanish into teardown while the serial refetch of the same region answered
     * conformingly and the run finished — precisely the intermittently-over-serving endpoint the
     * fail-soft carve-out exists to catch (see the class javadoc). Every OTHER completed fault stays
     * fail-soft here: a throttle or a cancellation dropped on these paths is deliberate.
     *
     * <p>This surfaces a violation that has already landed; it never waits for one that has not. A
     * guess genuinely still in flight is cancelled as before, and a response it was on the brink of
     * raising dies with it — the alternative, joining every disposable guess before a range may
     * finish, is a cost the drain cannot carry.
     */
    private void discard(Future<ListPage> future) throws ProtocolViolationException {
        if (future.cancel(true) || future.isCancelled()) {
            return;   // still in flight (now cancelled), or already cancelled: no result to observe
        }
        try {
            future.get();   // no longer cancellable ⇒ finished or finishing: this does not await the fetch
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ProtocolViolationException violation) {
                throw violation;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void record(String reason) {
        if (metrics != null) {
            metrics.recordStealReason("READAHEAD", reason);
        }
    }

    /**
     * End the engagement: cancel every guess still in flight, drop the buffer, stop the executor.
     *
     * <p>Teardown always completes before anything is raised, so the engagement is fully released
     * either way. This is the LAST chance to see a violation a guess already came back with: a range
     * that ends normally closes without draining its futures again, so a violation that landed after
     * the final page's drain has no other way out. Because the raise happens from {@code close}, a
     * teardown that runs while another exception is unwinding replaces it — acceptable, since every
     * exception that can be unwinding here is itself fatal to the run, and the endpoint verdict is
     * the more consequential of the two.
     *
     * @throws ProtocolViolationException raised by a guess that had already come back over-serving
     *                                    (see {@link #discard})
     */
    @Override
    public void close() throws ProtocolViolationException {
        if (closed) {
            return;
        }
        closed = true;
        ProtocolViolationException violation = null;
        for (InFlight f : inFlight) {
            try {
                discard(f.future());
            } catch (ProtocolViolationException e) {
                violation = violation == null ? e : violation;
            }
        }
        inFlight.clear();
        buffer.clear();
        exec.shutdownNow();
        if (violation != null) {
            throw violation;
        }
    }

    // ---- test seams (package-private) ---------------------------------------------

    /** Speculative pages currently buffered (HOLD/awaiting adoption) — bounded by {@code K}. */
    int bufferedPageCount() {
        return buffer.size();
    }

    int inFlightCount() {
        return inFlight.size();
    }
}

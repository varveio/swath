/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import io.varve.swath.error.CancelledException;
import io.varve.swath.error.ListingException;
import io.varve.swath.error.SwathException;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.output.ControlCharEscaper;
import io.varve.swath.runtime.CancellationToken;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageFetcher;
import io.varve.swath.store.PageRequest;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Paginates a single range {@code (lo, hi]} purely by {@code start_after = last emitted key}
 * (algorithms.md §2), emitting one batch per page, with pagination defenses (I9). RangeScanner
 * owns no transaction; a checkpointed caller commits each page before its batch is emitted (I1
 * commit-before-emit) from inside its page-consumer callback. {@code hi} is re-read as a
 * volatile per key so a thief can narrow the bound mid-flight. The boundary semantics
 * ({@code (A, B]}: emit {@code k <= B}, boundary-belongs-left) are fixed here.
 *
 * <p>Do not add a naive one-page-ahead double-buffer prefetch: pre-fetching one page ahead does
 * not pay off when the commit itself is fast relative to network round-trip time, so the extra
 * buffer adds complexity for no latency win. This could differ on a very low-RTT store (e.g.
 * local MinIO-class); re-adding it would need re-measurement on such a store first.
 *
 * <p><b>Depth-K speculative readahead (opt-in).</b> Unlike a plain one-page-ahead double-buffer,
 * when a dense opaque range collapses to a serial owner drain, an opt-in {@link ReadaheadConfig}
 * lets {@link SpeculativeReadahead} fire {@code K} concurrent guessed {@code start-after} fetches
 * ahead of the cursor and adopt them back into this loop in contiguous cursor order — hiding
 * {@code K} RTTs of the serial page-to-page chain, not one commit. Off by default; when engaged it
 * changes only <i>how</i> the next contiguous page's entries are obtained, never what/when this loop
 * hands to {@code consumer}, so I1 commit-before-emit and byte-exact in-order emission are unchanged.
 */
public final class RangeScanner {

    private static final Logger log = LoggerFactory.getLogger(RangeScanner.class);

    /** Receives one page's in-range batch (already bound-checked). */
    @FunctionalInterface
    public interface BatchEmitter {
        void emit(List<ListEntry> batch) throws SwathException, InterruptedException;
    }

    /**
     * Receives one page at a time — the raw, in-range batch plus the pagination
     * facts the checkpoint needs: {@code lastEmittedKey} (the last key S3 returned
     * in-range = {@code start_after} on resume; {@code null} for an empty terminal
     * page) and whether this page <b>completed</b> the range (reached its bound or
     * the listing ended). Called once per page <i>including</i> an empty terminal
     * page, so a node always gets its commit-before-emit + COMPLETED commit (I1/§4.2).
     */
    @FunctionalInterface
    public interface PageConsumer {
        void accept(List<ListEntry> inRangeBatch, byte[] lastEmittedKey, boolean completed)
                throws SwathException, InterruptedException;
    }

    private final PageFetcher fetcher;
    private final int maxKeys;
    private final RunMetrics metrics;
    private final ReadaheadConfig readahead;
    private final PageFetcher speculativeFetcher;
    private final BooleanSupplier stealingAllowed;

    /**
     * The single construction path. {@code metrics} may be {@code null} (a metrics-less unit test
     * driving a {@link RangeScanner} directly). {@code readahead} enables intra-range speculative
     * readahead (see {@link ReadaheadConfig} / {@link SpeculativeReadahead}); {@code null} — like
     * {@link ReadaheadConfig#off()} — is exact serial behavior with zero {@code READAHEAD.*} counters.
     *
     * <p>{@code speculativeFetcher} is a DISTINCT fetcher used ONLY for {@link SpeculativeReadahead}'s
     * guess-ahead fetches — never {@code fetcher} itself (the worker's own slot-gated, cancel-capable
     * fetcher). See {@link SpeculativeReadahead}'s class javadoc for the full isolation rationale:
     * a disposable speculative guess must never consume a concurrency-gauge slot,
     * vote AIMD, or — under {@code RetryPolicy.BOUNDED} — exhaust a transient-retry cap and cancel the
     * WHOLE RUN as a side effect. {@code stealingAllowed} is the SAME cheap gate {@code
     * WorkStealingScan}'s idle-thief loop already reads ({@code ConcurrencyGauge#isStealingAllowed()});
     * while it reports {@code false} (an active throttle backoff), {@link SpeculativeReadahead} stops
     * LAUNCHING new guesses (already in-flight/buffered ones are still served) — a crude safety valve
     * against adding more off-gauge load exactly when the store asked to back off. A testkit
     * object-mother supplies the defaults the (now removed) shorter overloads used to — {@code
     * speculativeFetcher = null} falling back to {@code fetcher} and {@code stealingAllowed = null}
     * meaning always-allowed are both test convenience only; production ({@code WorkStealingScan})
     * always threads its own dedicated speculative fetcher and gauge gate.
     *
     * @param speculativeFetcher the fail-soft, off-gauge fetcher for guess-ahead fetches; {@code null}
     *                           falls back to {@code fetcher} (test convenience only)
     * @param stealingAllowed    consulted before launching new guesses; {@code null} means always
     *                           allowed (test convenience only)
     */
    public RangeScanner(PageFetcher fetcher, int maxKeys, RunMetrics metrics, ReadaheadConfig readahead,
                        PageFetcher speculativeFetcher, BooleanSupplier stealingAllowed) {
        this.fetcher = fetcher;
        this.maxKeys = maxKeys;
        this.metrics = metrics;
        this.readahead = readahead == null ? ReadaheadConfig.off() : readahead;
        this.speculativeFetcher = speculativeFetcher == null ? fetcher : speculativeFetcher;
        this.stealingAllowed = stealingAllowed == null ? () -> true : stealingAllowed;
    }

    /**
     * Scan {@code (lo, hi]} of {@code prefix}, emitting each in-range page batch.
     *
     * @param startAfter exclusive lower bound = the node cursor ({@code lo} on a fresh node); null = ⊥
     * @param hi         inclusive upper bound {@code B}; null = unbounded frontier
     * @return number of entries emitted
     */
    public long runRange(byte[] prefix, ListingMode mode, byte[] startAfter, byte[] hi,
                         CancellationToken token, BatchEmitter emitter)
            throws SwathException, InterruptedException {
        // Adapter over the page-level consumer: emit only non-empty batches (the
        // non-checkpointed sinks have no use for the completion/cursor facts).
        return runRange(prefix, mode, startAfter, hi, token, (batch, lastKey, completed) -> {
            if (!batch.isEmpty()) {
                emitter.emit(batch);
            }
        });
    }

    /**
     * Scan {@code (lo, hi]} of {@code prefix}, handing each page to {@code consumer}
     * with the cursor/completion facts the checkpoint needs (commit-before-emit).
     *
     * <p>Fixed-bound entry point: delegates to the volatile-bound overload with a
     * constant supplier, so the per-key bound never changes mid-page.
     */
    public long runRange(byte[] prefix, ListingMode mode, byte[] startAfter, byte[] hi,
                         CancellationToken token, PageConsumer consumer)
            throws SwathException, InterruptedException {
        return runRange(prefix, mode, startAfter, () -> hi, token, consumer);
    }

    /**
     * Scan {@code (lo, hi]} of {@code prefix} where {@code hi} is re-read <b>per key</b>
     * from {@code hiSupplier} (algorithms.md §2 — the volatile {@code node.hi}). This is
     * the defense for the in-flight-page-vs-steal race (edge-case 7): when a thief narrows
     * the bound to {@code m} mid-page, the victim stops at the first key {@code k > m} and
     * the crossing page completes the node WITHOUT double-emitting keys now owned by the
     * child. The supplier is a minimal seam — {@code WorkerState} plugs its volatile
     * {@code AtomicReference<KeyBytes>} bound in here as {@code () -> hi}; a {@code null}
     * return means the unbounded frontier.
     *
     * <p>Each page is handed to {@code consumer} with the cursor/completion facts the
     * checkpoint needs (commit-before-emit).
     */
    public long runRange(byte[] prefix, ListingMode mode, byte[] startAfter,
                         Supplier<byte[]> hiSupplier, CancellationToken token, PageConsumer consumer)
            throws SwathException, InterruptedException {

        long emitted = 0;

        // Intra-range speculative readahead state (all off / no-op unless the toggle engages).
        SpeculativeReadahead ra = null;
        int consecutiveFullSerialPages = 0;   // dense-drain signal: full, truncated, un-split pages
        // The CURRENT streak's own start key / emitted-key count, used ONLY to scope the
        // est-remaining engage gate below (StealMath#estRemaining wants a REAL, local reference
        // point — the whole range's true lo is frequently null for an un-split top-level range,
        // which would dilute the density estimate; see ReadaheadConfig#minEngageRemainingPages).
        // Tracked/reset in lockstep with consecutiveFullSerialPages.
        byte[] streakStartKey = null;
        long streakKeysEmitted = 0;
        // Disengage-on-low-adoption: the recent per-page adoption window for the CURRENT engagement
        // (null unless engaged), plus a flag so a later re-engagement is distinguishable in the metrics.
        AdoptionWindow adoptionWindow = null;
        boolean disengagedThisRange = false;
        byte[] prevHi = hiSupplier.get();

        try {
            while (true) {
                if (token != null && token.isCancelled()) {
                    throw new CancelledException("listing cancelled");
                }

                byte[] hiNow = hiSupplier.get();
                // A split (thief / owner-split) narrowed the bound: retarget/discard speculation
                // beyond it, and reset the dense-drain signal (splitting IS attacking this range).
                if (ra != null && narrowed(prevHi, hiNow)) {
                    ra.onHiNarrowed(hiNow);
                } else if (ra == null && narrowed(prevHi, hiNow)) {
                    consecutiveFullSerialPages = 0;
                    streakStartKey = null;
                    streakKeysEmitted = 0;
                }
                prevHi = hiNow;

                // Obtain this page's entries — adopted from speculation (zero extra RTT) or a serial
                // fetch. Adopted entries are already trimmed to keys > startAfter, contiguous with it.
                List<ListEntry> pageEntries;
                boolean pageTruncated;
                boolean adopted = false;
                SpeculativeReadahead.Adopted ap = (ra == null) ? null : ra.tryNextPage(startAfter);
                if (ap != null) {
                    pageEntries = ap.entries();
                    pageTruncated = ap.truncated();
                    adopted = true;
                } else {
                    if (ra != null && metrics != null) {
                        metrics.recordStealReason("READAHEAD", "guess_gap");   // engaged but no adoptable page
                    }
                    ListPage page = fetcher.fetchPage(PageRequest.objects(prefix, startAfter, maxKeys));
                    if (metrics != null) {
                        metrics.recordListingPageShape(page.entries().size(), page.truncated(), maxKeys);
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("range_page_fetched run_id={} worker_id={} node_id={} prefix={} start_after={} entries={} truncated={} status={} latency_ms={}",
                                RunContext.runIdOrNone(), RunContext.workerIdOrNone(), RunContext.nodeIdOrNone(),
                                describe(prefix), describe(startAfter), page.entries().size(), page.truncated(),
                                page.httpStatus(), page.latency().toMillis());
                    }
                    pageEntries = page.entries();
                    pageTruncated = page.truncated();
                }

                List<ListEntry> batch = new ArrayList<>(pageEntries.size());
                boolean reachedBound = false;
                byte[] lastKey = null;
                byte[] firstKey = null;
                for (ListEntry e : pageEntries) {
                    // Re-read hi PER KEY (volatile, not once per page): a thief may have lowered
                    // it to m since this page was fetched under the old, wider bound. STRICT k > hi
                    // so k == m stays on the victim (boundary-belongs-LEFT, I3); unsigned byte-exact
                    // compare via KeyBytes — never String.compareTo (I10). Adopted pages get the
                    // identical bound check, so a page fetched speculatively under a wider hi that a
                    // split has since narrowed completes the node exactly as a serial page would.
                    byte[] keyHiNow = hiSupplier.get();
                    byte[] k = e.key().rawUnsafe();
                    if (keyHiNow != null && KeyBytes.compareUnsigned(k, keyHiNow) > 0) {
                        reachedBound = true;   // reached our (possibly newly-narrowed) bound; do NOT emit k
                        break;
                    }
                    if (firstKey == null) {
                        firstKey = k;
                    }
                    batch.add(e);
                    lastKey = k;
                }

                // reachedBound is folded into `done` BEFORE the I9 empty-batch check below, so a
                // legitimately-empty post-narrow page (every key now > m) completes the node and is
                // NOT mistaken for a truncated-but-returned-no-keys break, and does NOT advance the
                // cursor (lastKey stays null) — §4.2.
                boolean done = reachedBound || !pageTruncated;
                if (log.isDebugEnabled() && reachedBound) {
                    log.debug("range_bound_reached run_id={} worker_id={} node_id={} prefix={} start_after={} batch_size={}",
                            RunContext.runIdOrNone(), RunContext.workerIdOrNone(), RunContext.nodeIdOrNone(),
                            describe(prefix), describe(startAfter), batch.size());
                }

                // I9 defenses run BEFORE the consumer, so a broken/stuck page is never
                // committed or emitted (preserving exactly-once even on the default sink). Adopted
                // pages carry keys already proven > startAfter and non-empty, so these never trip on
                // them — but running the identical guard keeps the property source-independent.
                if (!done) {
                    if (batch.isEmpty()) {
                        // Truncated, in-range, yet no keys ≤ hi and bound not reached: a broken page.
                        throw new ListingException("truncated page returned no keys <= hi (prefix="
                                + describe(prefix) + ")");
                    }
                    // start_after is exclusive, so a correct page advances: lastKey > startAfter.
                    // lastKey <= startAfter ⇒ the server did not move forward ⇒ stuck.
                    if (startAfter != null && KeyBytes.compareUnsigned(lastKey, startAfter) <= 0) {
                        throw new ListingException("no forward progress (stuck listing) at "
                                + describe(startAfter));
                    }
                }

                // One callback per page (incl. an empty terminal page), so the checkpoint
                // always gets the page's commit + COMPLETED flag (§4.2).  lastKey is null on
                // an empty page ⇒ cursor unchanged. Commit-before-emit (I1) and the page-ordered
                // durable cursor live entirely here — identically for serial and adopted pages, so
                // the committed cursor only ever advances through contiguous, fetched keys.
                consumer.accept(batch, lastKey, done);
                emitted += batch.size();

                if (done) {
                    return emitted;
                }
                startAfter = lastKey;   // OBJECTS: paginate purely by last emitted key

                // `consumer.accept` above is the caller's page-commit callback, and it can ITSELF
                // trigger an owner self-split that narrows hi (OwnerSelfSplit.maybeOwnerSelfSplit
                // runs from exactly that call site, under the SAME lock the commit holds). The
                // `hiNow` sampled at loop-top is therefore
                // potentially STALE by now — re-read before engage/replenish acts on it, so a new
                // guess is never placed at/above a bound that already narrowed during this very
                // commit (correctness was already protected either way by the per-key bound check
                // above and the adoption trim, but placing a guess past the bound wastes an off-gauge
                // request into the child's keyspace and would immediately self-report
                // `cancelled_split`). Keep `prevHi` in lockstep so the NEXT loop iteration's own
                // top-of-loop narrow-check does not redundantly re-detect the same narrow.
                byte[] hiAfterCommit = hiSupplier.get();
                if (ra != null && narrowed(hiNow, hiAfterCommit)) {
                    ra.onHiNarrowed(hiAfterCommit);
                }
                hiNow = hiAfterCommit;
                prevHi = hiAfterCommit;

                // ---- engage / replenish speculation (after the cursor advanced) -------------
                boolean fullSerialPage = !adopted && pageTruncated && batch.size() >= maxKeys;
                if (ra == null) {
                    if (fullSerialPage) {
                        if (consecutiveFullSerialPages == 0) {
                            streakStartKey = firstKey;   // the streak's own oldest observed key
                            streakKeysEmitted = 0;
                        }
                        consecutiveFullSerialPages++;
                        streakKeysEmitted += batch.size();
                    } else {
                        consecutiveFullSerialPages = 0;
                        streakStartKey = null;
                        streakKeysEmitted = 0;
                    }
                    // Engage once the range has drained enough consecutive full, un-split serial pages
                    // to be a bounded dense tail collapsed to a serial owner drain. The one observed
                    // page [firstKey, lastKey] seeds the reflected guess placement (needs firstKey <ᵤ
                    // lastKey — always true for a full page).
                    if (readahead.enabled() && hiNow != null && firstKey != null && lastKey != null
                            && consecutiveFullSerialPages >= readahead.engageAfterFullPages()
                            && KeyBytes.compareUnsigned(firstKey, lastKey) < 0) {
                        // The range must have enough ESTIMATED remaining
                        // runway to plausibly earn back the speculation overhead before it deferred/never
                        // engages otherwise (see ReadaheadConfig#minEngageRemainingPages for the full
                        // rationale, incl. why this is scoped to the STREAK's own start, not the range's
                        // true — often null — lower bound). Never resets the streak: a short remaining
                        // tail keeps deferring every subsequent full page until the range naturally ends.
                        //
                        // Precision guard: StealMath#estRemaining returns the RAW (unscaled, [0,1])
                        // remaining fraction — not a key-count estimate — when its own `consumed` (the
                        // streak's own span) rounds to <= 0.0 (its documented "no density signal yet"
                        // path, meant for a cursor genuinely still at lo). For a streak-scoped `lo` that
                        // is only `streakKeysEmitted` keys behind `cursor` inside a much larger overall
                        // keyspace, double-precision cancellation can drive that TRUE-BUT-TINY span to
                        // exactly 0.0 well before the streak count itself is small — silently returning a
                        // raw fraction that, misread as a key-count estimate, looks like "essentially
                        // zero" regardless of how much real data remains (a unit mismatch, not a
                        // meaningful signal). Recompute `consumed` independently and fail OPEN (treat as
                        // ample runway, never defer) whenever it triggers that path, rather than trust an
                        // estimate this gate cannot distinguish from a genuine short tail.
                        double consumedSpan = StealMath.spanIn(streakStartKey, startAfter, streakStartKey, hiNow);
                        double estRemainingPages = (readahead.minEngageRemainingPages() > 0 && consumedSpan > 0.0)
                                ? StealMath.estRemaining(startAfter, streakStartKey, hiNow, streakKeysEmitted)
                                        / Math.max(1, maxKeys)
                                : Double.POSITIVE_INFINITY;
                        if (estRemainingPages < readahead.minEngageRemainingPages()) {
                            if (metrics != null) {
                                metrics.recordStealReason("READAHEAD", "engage_deferred_est_remaining");
                            }
                        } else {
                            ra = new SpeculativeReadahead(speculativeFetcher, prefix, maxKeys,
                                    firstKey, lastKey, hiNow, readahead.k(),
                                    readahead.maxBufferedPages(), stealingAllowed,
                                    readahead.fetchBudgetMillis(), metrics);
                            // A fresh per-engagement adoption window; mark a re-engagement distinctly
                            // (the constructor already recorded `engaged`) so post-hoc analysis can tell a
                            // first engagement from a drain that recovered after a low-adoption disengage.
                            adoptionWindow = new AdoptionWindow(
                                    readahead.disengageWindow(), readahead.disengageMinAdoption());
                            if (disengagedThisRange && metrics != null) {
                                metrics.recordStealReason("READAHEAD", "re_engaged");
                            }
                            ra.replenish(startAfter);
                        }
                    }
                } else {
                    // Disengage-on-low-adoption: fold THIS page's outcome (adopted vs serial-fallback
                    // guess_gap) into the recent window; if the adopted fraction over the last full window
                    // dropped to the floor, the density-reflected guesses are not paying off (the encode
                    // transient-stretch signature) — stop guessing, close speculation and revert to plain
                    // serial. A fresh `engageAfterFullPages` streak can re-engage later (a sustained drain
                    // that resumes), counted `re_engaged` above.
                    adoptionWindow.record(adopted);
                    if (adoptionWindow.shouldDisengage()) {
                        if (metrics != null) {
                            metrics.recordStealReason("READAHEAD", "disengaged_low_adoption");
                        }
                        ra.close();
                        ra = null;
                        adoptionWindow = null;
                        consecutiveFullSerialPages = 0;
                        streakStartKey = null;
                        streakKeysEmitted = 0;
                        disengagedThisRange = true;
                    } else {
                        ra.replenish(startAfter);
                    }
                }
            }
        } catch (Throwable scanFailure) {
            // Teardown can itself raise (a protocol violation that landed on a discarded guess).
            // Whatever ended the scan is the primary story, so keep it and attach the other.
            if (ra != null) {
                try {
                    ra.close();
                } catch (Throwable teardownFailure) {
                    scanFailure.addSuppressed(teardownFailure);
                }
                ra = null;
            }
            throw scanFailure;
        } finally {
            if (ra != null) {
                ra.close();
            }
        }
    }

    /** {@code true} iff {@code now} is a strict narrowing of {@code prev} (a bounded lower {@code hi}). */
    private static boolean narrowed(byte[] prev, byte[] now) {
        return now != null && (prev == null || KeyBytes.compareUnsigned(now, prev) < 0);
    }

    private static String describe(byte[] b) {
        return b == null ? "<null>" : ControlCharEscaper.escape(
                new String(b, StandardCharsets.UTF_8));
    }
}

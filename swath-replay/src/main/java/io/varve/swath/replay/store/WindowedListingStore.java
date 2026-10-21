/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.store;

import io.varve.swath.replay.metrics.ReplayMetrics;
import io.varve.swath.replay.protocol.ByteKey;
import io.varve.swath.replay.protocol.ListedObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A sequential-window prefetch decorator over a {@link ListingStore}: one bounded {@code LIMIT ~50k}
 * delegate read costs ~the same on the engine as a single {@code LIMIT 1001} page — a ~54x
 * amortization — so a sequential walk can serve ~50 pages from one delegate touch.
 *
 * <p>On a range read this store looks up a cached <b>window</b> with compatible upper bound and
 * projection. A window
 * is the contiguous prefix of the sorted range {@code [windowLowerBound, toExclusive)} the delegate
 * returned for one {@code windowRows}-sized read; because the fixture is immutable there is no
 * staleness, so slicing {@code [from, from+limit)} out of it is <b>byte-identical</b> to a fresh
 * delegate call for the same bounds.
 *
 * <p><b>Hit condition.</b> The cached range contains the requested lower and upper bounds and
 * supplies {@code limit} qualifying rows, or proves a shorter answer by an observed key at/above
 * the requested upper bound or by a delegate-final fill. A bounded join of two overlapping or
 * touching contiguous windows can also satisfy one request. Any unproven suffix misses and reads
 * the delegate; cache state never decides listing correctness.
 *
 * <p><b>Concurrency.</b> A small shared LRU of windows (default {@code max-windows}) is guarded by one
 * plain lock that protects only the map — windows are immutable lists, so a hit reads them lock-free
 * once looked up, and the (slow) delegate fill on a miss runs <b>outside</b> the lock so N concurrent
 * walks (the work-stealing scan's client) never serialize on a fill.
 *
 * <p>The cache is keyed by {@code (toExclusive, projection, windowLowerBound)} and looked up by
 * <b>coverage</b>, not by equality: a lookup scans the (at most {@code max-windows}) entries with
 * compatible projection and upper bound, then tests the tightest lower bounds first. Position
 * <b>must</b> be part of the identity: a real S3 {@code ListObjectsV2} carries
 * no upper bound, so every {@code worker_page} and every single-row pivot probe a work-stealing scan
 * issues arrives here with {@code toExclusive == null} and the same projection. Keying on the bounds
 * alone therefore funnels an entire 64-worker fleet — walkers and probes alike — through <b>one</b>
 * cache slot at 64 different positions, where each request evicts the last one's window and hit rate
 * collapses to zero. The scan is over a knob-bounded, tiny map and costs microseconds against a
 * delegate read's milliseconds.
 *
 * <p><b>Eviction.</b> Shared windows remain until ordinary LRU eviction. Entry count and aggregate
 * cached rows are bounded by {@code max-windows} and {@code max-windows × window-rows}; a rounded
 * continuation fill can exceed one window's nominal row count but never increases the global cap.
 * Memory remains a function of config and never of
 * fixture size N (I11 spirit). The default permits 1.2M materialised rows and can therefore retain
 * hundreds of MiB, not merely a few tens: v1 holds full object graphs. A packed-window follow-up is a
 * deliberate, separately measured next rung, not built here.
 */
public final class WindowedListingStore implements ListingStore {

    /** System-property prefix, mirroring the replay server's {@code swath.replay.*} config idiom. */
    private static final String ENABLED_PROPERTY = "swath.replay.prefetch.enabled";
    private static final String WINDOW_ROWS_PROPERTY = "swath.replay.prefetch.window-rows";
    private static final String MAX_WINDOWS_PROPERTY = "swath.replay.prefetch.max-windows";

    static final boolean DEFAULT_ENABLED = true;
    static final int DEFAULT_WINDOW_ROWS = 12_500;
    static final int DEFAULT_MAX_WINDOWS = 96;

    /**
     * {@code swath.replay.prefetch.window.miss\{reason\}} tag values: a miss at a position registered
     * as the tail of an already-served page (a paginating client, whose fill ramps) versus any other
     * miss — a probe, or a seek into keyspace no window covers — whose fill stays at the caller's limit.
     */
    private static final String MISS_CONTINUATION = "continuation";
    private static final String MISS_COLD = "cold";

    /**
     * Growth factor applied to the previous fill when a miss lands on a registered continuation
     * anchor: a walk ramps {@code limit → 4×limit → …} until it reaches {@code windowRows}, so a
     * sequential client converges on full amortization within a few pages while a one-shot reader
     * never pays for rows it will not read.
     */
    private static final int FILL_RAMP_FACTOR = 4;

    /**
     * Continuation anchors retained per cached window. Anchors are pure hints — losing one costs a
     * un-ramped fill, never a wrong answer — so this only has to be large enough that a fleet of
     * {@code max-windows} walkers keeps its own anchors alive.
     */
    private static final int ANCHORS_PER_WINDOW = 4;

    private final ListingStore delegate;
    private final ReplayMetrics metrics;
    private final int windowRows;
    private final int maxWindows;
    private final long maxCachedRows;
    private final Object lock = new Object();
    private final LinkedHashMap<WindowKey, Window> windows;
    private final LinkedHashMap<ByteKey, Integer> continuationAnchors;
    private long cachedRows;

    public WindowedListingStore(ListingStore delegate, ReplayMetrics metrics, int windowRows, int maxWindows) {
        if (windowRows < 1) {
            throw new IllegalArgumentException("prefetch window-rows must be at least 1, got " + windowRows);
        }
        if (maxWindows < 1) {
            throw new IllegalArgumentException("prefetch max-windows must be at least 1, got " + maxWindows);
        }
        this.delegate = delegate;
        this.metrics = metrics;
        this.windowRows = windowRows;
        this.maxWindows = maxWindows;
        this.maxCachedRows = (long) windowRows * maxWindows;
        this.windows = new LinkedHashMap<>(16, 0.75f, true);
        int maxAnchors = (int) Math.min(Integer.MAX_VALUE, (long) maxWindows * ANCHORS_PER_WINDOW);
        this.continuationAnchors = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<ByteKey, Integer> eldest) {
                boolean evict = size() > maxAnchors;
                if (evict) {
                    metrics.recordPrefetchAnchor("evicted_before_claim");
                }
                return evict;
            }
        };
        metrics.registerPrefetchCacheGauges(this::liveWindows, this::liveAnchors);
        metrics.registerPrefetchCachedRowsGauge(this::liveCachedRows);
    }

    /** Prefetch configuration resolved from {@code swath.replay.prefetch.*} system properties. */
    public record Config(boolean enabled, int windowRows, int maxWindows) {

        public static Config fromSystemProperties() {
            return new Config(
                    boolProperty(ENABLED_PROPERTY, DEFAULT_ENABLED),
                    positiveIntProperty(WINDOW_ROWS_PROPERTY, DEFAULT_WINDOW_ROWS),
                    positiveIntProperty(MAX_WINDOWS_PROPERTY, DEFAULT_MAX_WINDOWS));
        }
    }

    @Override
    public List<DelimitedEntry> delimitedRollup(ByteKey from, boolean fromInclusive, ByteKey toExclusive,
                                                byte[] prefix, byte[] delimiter, int limit, Projection projection) {
        // A single call answered whole by the backing store (a skip-scan, when it overrides this
        // rather than declining) — not a sequence of paginated row reads, so there is no window to
        // serve it from or fill from. Delegate straight through to the backing store's fast path (or
        // its decline).
        return delegate.delimitedRollup(from, fromInclusive, toExclusive, prefix, delimiter, limit, projection);
    }

    @Override
    public List<DelimitedEntry> delimitedRollup(ByteKey from, boolean fromInclusive, ByteKey toExclusive,
                                                byte[] prefix, byte[] delimiter, int limit, Projection projection,
                                                boolean suppressPrefixAtOrBeforeFloor) {
        return delegate.delimitedRollup(from, fromInclusive, toExclusive, prefix, delimiter, limit, projection,
                suppressPrefixAtOrBeforeFloor);
    }

    @Override
    public List<ListedObject> rows(ByteKey from, boolean fromInclusive, ByteKey toExclusive, int limit,
                                   Projection projection) {
        return serve(from, fromInclusive, toExclusive, limit, projection);
    }

    private List<ListedObject> serve(ByteKey from, boolean fromInclusive, ByteKey toExclusive, int limit,
                                     Projection projection) {
        if (limit < 1) {
            return delegate.rows(from, fromInclusive, toExclusive, limit, projection);
        }
        if (limit > windowRows) {
            metrics.recordPrefetchUncached("request_exceeds_window");
            metrics.recordPrefetchMiss(MISS_COLD);
            metrics.recordPrefetchFillRows(limit);
            return fill(from, fromInclusive, toExclusive, projection, limit);
        }
        List<WindowCandidate> all = snapshotCandidates();
        List<WindowCandidate> candidates = new ArrayList<>();
        for (WindowCandidate candidate : all) {
            if (candidate.key().projection().equals(projection)
                    && upperContains(candidate.key().toExclusive(), toExclusive)) candidates.add(candidate);
        }
        candidates.sort(Comparator.comparing(WindowCandidate::key, (a, b) ->
                compareLower(b.lowerBound(), a.lowerBound())));
        boolean hadCover = false;
        List<WindowCandidate> partials = new ArrayList<>();
        for (WindowCandidate candidate : candidates) {
            if (!candidate.window().coversLowerBound(from, fromInclusive)) continue;
            WindowProbe probe = candidate.window().probe(from, fromInclusive, toExclusive);
            if (probe == null || probe.size() == 0 && !probe.complete()) continue;
            hadCover = true;
            if (probe.size() >= limit || probe.complete()) {
                List<ListedObject> page = candidate.window().slice(probe.start(), probe.end(), limit);
                metrics.recordPrefetchSelection(Objects.equals(candidate.key().toExclusive(), toExclusive)
                        ? "single_exact_bound" : "single_contained_bound");
                metrics.recordPrefetchHit();
                synchronized (lock) {
                    touch(candidate);
                    registerAnchors(page, ceilingRows(limit));
                }
                return page;
            }
            partials.add(candidate);
        }
        // Try joins only after every single candidate has had a chance to answer. A tighter short
        // window must not hide an older but sufficient one or force an unnecessary allocation.
        String joinDecline = null;
        for (WindowCandidate candidate : partials) {
            WindowProbe probe = candidate.window().probe(from, fromInclusive, toExclusive);
            JoinAttempt attempt = tryJoin(candidate, probe, all, projection, toExclusive, limit);
            Join joined = attempt.join();
            if (joined != null) {
                metrics.recordPrefetchJoin("hit");
                metrics.recordPrefetchSelection("two_window_join");
                metrics.recordPrefetchHit();
                synchronized (lock) {
                    touch(candidate);
                    touch(joined.second());
                    registerAnchors(joined.rows(), ceilingRows(limit));
                }
                return joined.rows();
            }
            if (joinDecline == null) joinDecline = attempt.declineReason();
        }
        if (joinDecline != null) metrics.recordPrefetchJoin(joinDecline);
        metrics.recordPrefetchSelection(hadCover ? "insufficient" : "no_cover");
        // A miss carries its own reason: `continuation` when this position was registered as the
        // tail of a page we previously served (a paginating client walking forward, which is worth
        // prefetching for) versus `cold` (a one-shot probe or a seek into unvisited keyspace, which
        // is not). See rampedFill.
        FillDecision decision = rampedFill(from, fromInclusive, limit);
        int requested = decision.requested();
        if (decision.continuation() && ceilingRows(limit) > maxCachedRows) {
            requested = limit;
            metrics.recordPrefetchUncached("rounded_exceeds_row_budget");
        }
        metrics.recordPrefetchMiss(decision.continuation() ? MISS_CONTINUATION : MISS_COLD);
        metrics.recordPrefetchFillRows(requested);
        List<ListedObject> filled = fill(from, fromInclusive, toExclusive, projection, requested);
        Window refreshed = new Window(filled, from, fromInclusive, filled.size() < requested);
        List<ListedObject> served = refreshed.slice(0, refreshed.rows.size(), limit);
        synchronized (lock) {
            // A final window remains useful to lagging walkers, even after this caller consumes it.
            // Ordinary cold reads with no surplus are one-shot probes and do not enter the LRU.
            if (!filled.isEmpty() && (filled.size() > served.size() || refreshed.delegateFinal)) {
                publish(new WindowKey(toExclusive, projection, from, fromInclusive), refreshed);
            }
            registerAnchors(served, ceilingRows(limit) > maxCachedRows ? limit : nextFill(requested, limit));
        }
        return served;
    }

    /** Snapshot immutable candidates under the lock; qualifying and sorting happen outside it. */
    private List<WindowCandidate> snapshotCandidates() {
        List<WindowCandidate> candidates = new ArrayList<>();
        synchronized (lock) {
            for (Map.Entry<WindowKey, Window> entry : windows.entrySet()) {
                candidates.add(new WindowCandidate(entry.getKey(), entry.getValue()));
            }
        }
        return candidates;
    }

    private static int compareLower(ByteKey a, ByteKey b) {
        return a == null ? (b == null ? 0 : -1) : (b == null ? 1 : a.compareTo(b));
    }

    private static boolean upperContains(ByteKey cached, ByteKey requested) {
        return cached == null || requested != null && cached.compareTo(requested) >= 0;
    }

    private JoinAttempt tryJoin(WindowCandidate first, WindowProbe firstProbe, List<WindowCandidate> candidates,
                                Projection projection, ByteKey toExclusive, int limit) {
        if (firstProbe.size() == 0) {
            throw new IllegalStateException("join needs a nonempty first window slice");
        }
        byte[] lastBytes = first.window().rows.get(firstProbe.end() - 1).key();
        ByteKey last = ByteKey.copyOf(lastBytes);
        int needed = limit - firstProbe.size();
        WindowCandidate best = null;
        WindowProbe bestProbe = null;
        boolean gap = false, projectionMismatch = false, upperMismatch = false, insufficient = false;
        for (WindowCandidate second : candidates) {
            if (second == first) continue;
            if (!second.window().coversLowerBound(last, false)) {
                if (second.key().projection().equals(projection)
                        && upperContains(second.key().toExclusive(), toExclusive)
                        && compareLower(second.key().lowerBound(), last) > 0) gap = true;
                continue;
            }
            if (!second.key().projection().equals(projection)) {
                projectionMismatch = true;
                continue;
            }
            if (!upperContains(second.key().toExclusive(), toExclusive)) {
                upperMismatch = true;
                continue;
            }
            WindowProbe probe = second.window().probe(last, false, toExclusive);
            if (probe == null || probe.size() < needed && !probe.complete()) {
                insufficient = true;
                continue;
            }
            if (bestProbe == null || probe.size() > bestProbe.size()) {
                best = second;
                bestProbe = probe;
            }
        }
        if (best == null) {
            String reason = insufficient ? "insufficient_second" : upperMismatch ? "upper_bound"
                    : projectionMismatch ? "projection" : gap ? "gap" : "no_second";
            return new JoinAttempt(null, reason);
        }
        List<ListedObject> page = new ArrayList<>(Math.min(limit, firstProbe.size() + bestProbe.size()));
        page.addAll(first.window().rows.subList(firstProbe.start(), firstProbe.end()));
        int take = Math.min(needed, bestProbe.size());
        page.addAll(best.window().rows.subList(bestProbe.start(), bestProbe.start() + take));
        return new JoinAttempt(new Join(page, best), null);
    }

    private void touch(WindowCandidate candidate) {
        // LinkedHashMap.get refreshes LRU. Immutable snapshots remain valid if concurrently evicted.
        windows.get(candidate.key());
    }

    private void publish(WindowKey key, Window window) {
        if (window.rows.size() > maxCachedRows) {
            metrics.recordPrefetchUncached("window_exceeds_row_budget");
            return;
        }
        Window replaced = windows.remove(key);
        if (replaced != null) cachedRows -= replaced.rows.size();
        while (!windows.isEmpty() && (windows.size() >= maxWindows
                || cachedRows + window.rows.size() > maxCachedRows)) {
            boolean rowBudgetExceeded = cachedRows + window.rows.size() > maxCachedRows;
            Map.Entry<WindowKey, Window> eldest = windows.entrySet().iterator().next();
            windows.remove(eldest.getKey());
            cachedRows -= eldest.getValue().rows.size();
            metrics.recordPrefetchWindowEviction();
            if (rowBudgetExceeded) metrics.recordPrefetchRowBudgetEviction();
        }
        windows.put(key, window);
        cachedRows += window.rows.size();
    }

    private long liveCachedRows() {
        synchronized (lock) {
            return cachedRows;
        }
    }

    /**
     * How many rows the delegate should be asked for on a miss at {@code from}.
     *
     * <p>Never below {@code limit}: {@code windowRows} is a perf-only prefetch size, never a ceiling
     * on what a single {@code rows()} call must return. A caller-supplied {@code limit > windowRows}
     * (e.g. an operator-configured small window-rows below a real page limit) would otherwise make
     * the window short-serve fewer rows than a bare delegate call — a silent {@link ListingStore}
     * contract violation, since {@link io.varve.swath.replay.protocol.ListObjectsV2Pager} serves a
     * page single-shot (no re-fetch loop) and would compute {@code truncated=false} and drop objects.
     *
     * <p>Above that floor, prefetch is spent only where it pays back. The protocol makes a
     * paginating client self-identifying: its next {@code from} is exactly a key at the tail of the
     * page it was just served (S3 {@code start-after} and this server's own continuation token are
     * both the last emitted key, and the pager over-fetches by one to detect truncation, so either
     * of the last two rows can become the next lower bound). Registering those keys as anchors, and
     * ramping only on an anchor hit, keeps the cost of the requests that <em>cannot</em> reuse a
     * window proportional to what they asked for: a single-row pivot probe reads ~1 row rather than
     * {@code windowRows}, and each {@code successor(P)} seek in a delimiter rollup reads one batch
     * rather than a full window per rolled-up prefix.
     *
     * <p>Returns whether an anchor actually drove the fill, not just its size: with {@code windowRows}
     * configured below the page {@code limit}, an anchor's ramped size saturates at {@code windowRows}
     * and {@code Math.max} clamps the request back up to {@code limit}, so {@code requested == limit}
     * even on a real continuation. The miss reason keys off the {@code continuation} flag rather than
     * {@code requested > limit}, so that config can't mislabel a continuation as {@code cold} — the
     * exact signal {@code MISS_CONTINUATION}/{@code MISS_COLD} exists to give.
     */
    private FillDecision rampedFill(ByteKey from, boolean fromInclusive, int limit) {
        Integer anchored;
        synchronized (lock) {
            anchored = from == null || fromInclusive ? null : continuationAnchors.remove(from);
        }
        if (anchored != null) {
            metrics.recordPrefetchAnchor("claimed");
            metrics.recordPrefetchRampCeiling(ceilingRows(limit));
        }
        return anchored == null
                ? new FillDecision(limit, false)
                : new FillDecision(Math.max(limit, Math.min(anchored, ceilingRows(limit))), true);
    }

    private int ceilingRows(int limit) {
        long rounded = ((long) windowRows + limit - 1) / limit * limit;
        return (int) Math.min(Integer.MAX_VALUE, rounded);
    }

    private int nextFill(int requested, int limit) {
        return (int) Math.min(ceilingRows(limit), Math.min(Integer.MAX_VALUE,
                (long) requested * FILL_RAMP_FACTOR));
    }

    /** A miss's fill size, plus whether a continuation anchor (not merely its size) drove it. */
    private record FillDecision(int requested, boolean continuation) { }

    /**
     * Registers the tail of a served page as a continuation anchor carrying the fill size the next
     * miss at that position should use. Called under {@link #lock}. Anchors are hints only: an
     * evicted or never-claimed anchor costs one un-ramped fill and nothing else.
     */
    private void registerAnchors(List<ListedObject> page, int nextFill) {
        for (int i = Math.max(0, page.size() - 2); i < page.size(); i++) {
            continuationAnchors.put(ByteKey.copyOf(page.get(i).key()), nextFill);
            metrics.recordPrefetchAnchor("registered");
        }
    }

    private int liveWindows() {
        synchronized (lock) {
            return windows.size();
        }
    }

    private int liveAnchors() {
        synchronized (lock) {
            return continuationAnchors.size();
        }
    }

    private List<ListedObject> fill(ByteKey from, boolean fromInclusive, ByteKey toExclusive, Projection projection,
                                    int requested) {
        var fillSample = metrics.startPrefetchFillTimer();
        try {
            return delegate.rows(from, fromInclusive, toExclusive, requested, projection);
        } finally {
            metrics.recordPrefetchFill(fillSample);
        }
    }

    @Override
    public void close() {
        delegate.close();
    }

    /** The buffered result of one delegate window read; immutable rows plus its coverage metadata. */
    private static final class Window {

        private final List<ListedObject> rows;
        private final ByteKey lowerBound;
        private final boolean lowerInclusive;
        private final boolean delegateFinal;

        Window(List<ListedObject> rows, ByteKey lowerBound, boolean lowerInclusive, boolean delegateFinal) {
            this.rows = List.copyOf(rows);
            this.lowerBound = lowerBound;
            this.lowerInclusive = lowerInclusive;
            this.delegateFinal = delegateFinal;
        }

        /** Identifies request-qualified rows and whether a short answer is proven final. */
        WindowProbe probe(ByteKey from, boolean fromInclusive, ByteKey toExclusive) {
            if (!coversLowerBound(from, fromInclusive)) {
                return null;
            }
            int start = startIndex(from, fromInclusive);
            int end = toExclusive == null ? rows.size() : startIndex(toExclusive, true);
            return new WindowProbe(start, Math.max(start, end), end < rows.size() || delegateFinal);
        }

        private List<ListedObject> slice(int start, int end, int limit) {
            int sliceEnd = Math.min(start + limit, end);
            // Copy the ≤limit-row slice so the returned page never retains the whole window's backing
            // array (a subList view would keep all ~windowRows rows alive for the page's lifetime).
            return new ArrayList<>(rows.subList(start, sliceEnd));
        }

        /** True iff every row this request would return is present in the window, i.e. the request's
         *  lower bound is at or above the window's own lower bound (no earlier row is missing). */
        boolean coversLowerBound(ByteKey from, boolean fromInclusive) {
            if (lowerBound == null) {
                return true;                 // window is open at the low end — covers any request start
            }
            if (from == null) {
                return false;                // request wants an open lower bound the window doesn't have
            }
            int cmp = from.compareTo(lowerBound);
            if (cmp > 0) {
                return true;
            }
            if (cmp < 0) {
                return false;
            }
            // from == windowLowerBound: covered unless the request includes the boundary key and the
            // window excluded it.
            return lowerInclusive || !fromInclusive;
        }

        private int startIndex(ByteKey from, boolean fromInclusive) {
            if (from == null) {
                return 0;
            }
            byte[] target = from.toByteArray();
            int lo = 0;
            int hi = rows.size();
            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                int cmp = Arrays.compareUnsigned(rows.get(mid).key(), target);
                boolean before = fromInclusive ? cmp < 0 : cmp <= 0;
                if (before) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            return lo;
        }
    }

    private record WindowCandidate(WindowKey key, Window window) { }
    private record WindowProbe(int start, int end, boolean complete) {
        int size() { return end - start; }
    }
    private record Join(List<ListedObject> rows, WindowCandidate second) { }
    private record JoinAttempt(Join join, String declineReason) { }

    /**
     * The window cache key: a sequential walk's constant bounds <b>plus its position</b>. The bounds
     * alone do not identify a walk — see the class-level note on why every work-stealing
     * {@code worker_page} and pivot probe shares {@code (null, <default projection>)}.
     */
    private record WindowKey(ByteKey toExclusive, Projection projection, ByteKey lowerBound, boolean lowerInclusive) {

    }

    private static boolean boolProperty(String name, boolean fallback) {
        String value = System.getProperty(name);
        return value == null ? fallback : Boolean.parseBoolean(value.trim());
    }

    private static int positiveIntProperty(String name, int fallback) {
        String value = System.getProperty(name);
        if (value == null) {
            return fallback;
        }
        int parsed = Integer.parseInt(value.trim());
        return parsed > 0 ? parsed : fallback;
    }
}

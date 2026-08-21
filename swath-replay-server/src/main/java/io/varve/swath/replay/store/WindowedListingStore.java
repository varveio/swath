/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.store;

import io.varve.swath.replay.protocol.ByteKey;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.server.ReplayMetrics;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A sequential-window prefetch decorator over a {@link ListingStore}: one bounded {@code LIMIT ~50k}
 * delegate read costs ~the same on the engine as a single {@code LIMIT 1001} page — a ~54x
 * amortization — so a sequential walk can serve ~50 pages from one delegate touch.
 *
 * <p>On a range read this store looks up a cached <b>window</b> keyed by {@code (toExclusive,
 * projection)} — the invariants a sequential walk holds constant while {@code from} advances. A window
 * is the contiguous prefix of the sorted range {@code [windowLowerBound, toExclusive)} the delegate
 * returned for one {@code windowRows}-sized read; because the fixture is immutable there is no
 * staleness, so slicing {@code [from, from+limit)} out of it is <b>byte-identical</b> to a fresh
 * delegate call for the same bounds.
 *
 * <p><b>Hit condition.</b> A lookup hits iff (1) the request's lower bound is covered by the window
 * (the window's own lower bound is at or below the request's, so no earlier row is missing) and (2)
 * the window holds at least {@code limit} rows at/after {@code from}, <b>or</b> the window is
 * delegate-final (the delegate returned fewer than {@code windowRows}, so the range is exhausted and a
 * short tail is the true, complete answer). Any other case is a miss: the window is too short and
 * non-final, or the position is uncovered — a miss re-fills and is only ever a perf event, never a
 * correctness one.
 *
 * <p><b>Concurrency.</b> A small shared LRU of windows (default {@code max-windows}) is guarded by one
 * plain lock that protects only the map — windows are immutable lists, so a hit reads them lock-free
 * once looked up, and the (slow) delegate fill on a miss runs <b>outside</b> the lock so N concurrent
 * walks (the work-stealing scan's client) never serialize on a fill.
 *
 * <p>The cache is keyed by {@code (toExclusive, projection, windowLowerBound)} and looked up by
 * <b>coverage</b>, not by equality: a lookup scans the (at most {@code max-windows}) entries sharing
 * the request's {@code (toExclusive, projection)} and picks the covering window with the greatest
 * lower bound. Position <b>must</b> be part of the identity: a real S3 {@code ListObjectsV2} carries
 * no upper bound, so every {@code worker_page} and every single-row pivot probe a work-stealing scan
 * issues arrives here with {@code toExclusive == null} and the same projection. Keying on the bounds
 * alone therefore funnels an entire 64-worker fleet — walkers and probes alike — through <b>one</b>
 * cache slot at 64 different positions, where each request evicts the last one's window and hit rate
 * collapses to zero. The scan is over a knob-bounded, tiny map and costs microseconds against a
 * delegate read's milliseconds.
 *
 * <p><b>Eviction.</b> Eager: when a serve consumes through a delegate-final window's last row the
 * window is dead and is dropped immediately. LRU (bounded {@code max-windows}) is the backstop. Memory
 * is bounded by {@code max-windows × window-rows × row footprint} — a few MB to tens of MB, a function
 * of config, never of the fixture size N (I11 spirit); v1 holds windows as materialised row lists (the
 * PageBlock-packed follow-up is a deliberate, separately-measured next rung, not built here).
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
    private final Object lock = new Object();
    private final LinkedHashMap<WindowKey, Window> windows;
    private final LinkedHashMap<ByteKey, Integer> continuationAnchors;

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
        this.windows = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<WindowKey, Window> eldest) {
                boolean evict = size() > maxWindows;
                if (evict) {
                    metrics.recordPrefetchWindowEviction();
                }
                return evict;
            }
        };
        int maxAnchors = maxWindows * ANCHORS_PER_WINDOW;
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
    public List<ListedObject> rows(ByteKey from, boolean fromInclusive, ByteKey toExclusive, int limit,
                                   Projection projection) {
        return serve(from, fromInclusive, toExclusive, limit, projection);
    }

    private List<ListedObject> serve(ByteKey from, boolean fromInclusive, ByteKey toExclusive, int limit,
                                     Projection projection) {
        WindowKey covering = null;
        Window window = null;
        synchronized (lock) {
            covering = findCovering(toExclusive, projection, from, fromInclusive);
            if (covering != null) {
                window = windows.get(covering);   // re-get so the LRU records the access
            }
        }
        if (window != null) {
            Served hit = window.tryServe(from, fromInclusive, limit);
            if (hit != null) {
                metrics.recordPrefetchHit();
                synchronized (lock) {
                    if (hit.exhausted()) {
                        windows.remove(covering, window);
                    }
                    registerAnchors(hit.rows(), windowRows);
                }
                return hit.rows();
            }
        }
        // A miss carries its own reason: `continuation` when this position was registered as the
        // tail of a page we previously served (a paginating client walking forward, which is worth
        // prefetching for) versus `cold` (a one-shot probe or a seek into unvisited keyspace, which
        // is not). See rampedFill.
        FillDecision decision = rampedFill(from, limit);
        int requested = decision.requested();
        metrics.recordPrefetchMiss(decision.continuation() ? MISS_CONTINUATION : MISS_COLD);
        metrics.recordPrefetchFillRows(requested);
        List<ListedObject> filled = fill(from, fromInclusive, toExclusive, projection, requested);
        Window refreshed = new Window(filled, from, fromInclusive, filled.size() < requested);
        Served served = refreshed.serveFromStart(limit);
        synchronized (lock) {
            // Cache only a window that has rows left to serve after this page. An exact-`limit` fill
            // (every probe, and the first page of any walk) is fully consumed here, so retaining it
            // would evict a live walker's window to hold a spent one — the pollution that makes a
            // coverage lookup degrade back toward the single-slot behaviour it replaces.
            if (!served.exhausted() && filled.size() > served.rows().size()) {
                windows.put(new WindowKey(toExclusive, projection, from, fromInclusive), refreshed);
            }
            registerAnchors(served.rows(), Math.min(requested * FILL_RAMP_FACTOR, windowRows));
        }
        return served.rows();
    }

    /**
     * The covering window for {@code from} under {@code (toExclusive, projection)}: of the windows
     * whose buffered range starts at or below the request, the one with the <b>greatest</b> lower
     * bound, i.e. the tightest fit and so the one most likely to still hold {@code limit} rows ahead
     * of {@code from}. Called under {@link #lock}; the scan is bounded by {@code max-windows}.
     */
    private WindowKey findCovering(ByteKey toExclusive, Projection projection, ByteKey from, boolean fromInclusive) {
        WindowKey best = null;
        for (Map.Entry<WindowKey, Window> entry : windows.entrySet()) {
            WindowKey candidate = entry.getKey();
            if (!candidate.sameBounds(toExclusive, projection)
                    || !entry.getValue().coversLowerBound(from, fromInclusive)) {
                continue;
            }
            if (best == null || best.precedes(candidate)) {
                best = candidate;
            }
        }
        return best;
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
    private FillDecision rampedFill(ByteKey from, int limit) {
        Integer anchored;
        synchronized (lock) {
            anchored = from == null ? null : continuationAnchors.remove(from);
        }
        if (anchored != null) {
            metrics.recordPrefetchAnchor("claimed");
        }
        return anchored == null
                ? new FillDecision(limit, false)
                : new FillDecision(Math.max(limit, anchored), true);
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

        /** A hit at the top of this window (the just-filled or continuation case where {@code from} is
         *  the window's own lower bound). */
        Served serveFromStart(int limit) {
            return slice(0, limit);
        }

        /** Serves {@code [from, from+limit)} from this window, or {@code null} on a miss. */
        Served tryServe(ByteKey from, boolean fromInclusive, int limit) {
            if (!coversLowerBound(from, fromInclusive)) {
                return null;
            }
            int start = startIndex(from, fromInclusive);
            int available = rows.size() - start;
            if (available >= limit || delegateFinal) {
                return slice(start, limit);
            }
            return null;
        }

        private Served slice(int start, int limit) {
            int end = Math.min(start + limit, rows.size());
            // Copy the ≤limit-row slice so the returned page never retains the whole window's backing
            // array (a subList view would keep all ~windowRows rows alive for the page's lifetime).
            List<ListedObject> page = new ArrayList<>(rows.subList(start, end));
            boolean exhausted = delegateFinal && end == rows.size();
            return new Served(page, exhausted);
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

    /** A served page plus whether serving it consumed a delegate-final window through its last row. */
    private record Served(List<ListedObject> rows, boolean exhausted) {
    }

    /**
     * The window cache key: a sequential walk's constant bounds <b>plus its position</b>. The bounds
     * alone do not identify a walk — see the class-level note on why every work-stealing
     * {@code worker_page} and pivot probe shares {@code (null, <default projection>)}.
     */
    private record WindowKey(ByteKey toExclusive, Projection projection, ByteKey lowerBound, boolean lowerInclusive) {

        boolean sameBounds(ByteKey otherToExclusive, Projection otherProjection) {
            return Objects.equals(toExclusive, otherToExclusive) && projection.equals(otherProjection);
        }

        /** Whether {@code other} starts at or after this key, i.e. is the tighter fit of the two. */
        boolean precedes(WindowKey other) {
            if (lowerBound == null) {
                return true;            // an open lower bound is the loosest fit there is
            }
            return other.lowerBound != null && lowerBound.compareTo(other.lowerBound) <= 0;
        }
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

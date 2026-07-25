/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import io.varve.swath.engine.RangeScanner;
import io.varve.swath.engine.ReadaheadConfig;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.store.PageFetcher;

/**
 * Terse {@link RangeScanner} factories for engine tests: the same effective defaults the (now
 * removed) short {@link RangeScanner} constructor overloads supplied — no metrics sink, readahead
 * off, and the scanner's own fetcher doubling as the speculative fetcher with stealing always
 * allowed. A test that needs a non-default seam passes it explicitly to the matching overload
 * here, or constructs {@link RangeScanner} directly.
 */
public final class RangeScanners {

    private RangeScanners() {
    }

    /**
     * A {@link RangeScanner} with {@code maxKeys} taken from the fetcher's own capabilities cap — the
     * effective construction the deleted 1-arg overload built.
     */
    public static RangeScanner of(PageFetcher fetcher) {
        return of(fetcher, Math.max(1, fetcher.capabilities().maxKeysCap()));
    }

    /** A metrics-less, readahead-off {@link RangeScanner} — the deleted 2-arg overload's construction. */
    public static RangeScanner of(PageFetcher fetcher, int maxKeys) {
        return new RangeScanner(fetcher, maxKeys, null, null, null, null);
    }

    /**
     * As {@link #of(PageFetcher, int)}, but with a caller-supplied metrics sink — the deleted 3-arg
     * overload's construction.
     */
    public static RangeScanner of(PageFetcher fetcher, int maxKeys, RunMetrics metrics) {
        return new RangeScanner(fetcher, maxKeys, metrics, null, null, null);
    }

    /**
     * As {@link #of(PageFetcher, int, RunMetrics)}, but with a caller-supplied {@link
     * ReadaheadConfig} — the deleted 4-arg overload's construction.
     */
    public static RangeScanner of(PageFetcher fetcher, int maxKeys, RunMetrics metrics, ReadaheadConfig readahead) {
        return new RangeScanner(fetcher, maxKeys, metrics, readahead, null, null);
    }
}

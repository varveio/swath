/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store;

import io.varve.swath.error.ListingException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logs {@code list_first_request_issued} / {@code list_first_page_returned} markers (each
 * with elapsed time since the run started) around the very FIRST {@link #fetchPage} call this run
 * makes — whichever fires first, the seed probe or the engine — since {@code ListCommand} shares
 * one fetcher instance between both (see {@code ListCommand#runWithCheckpoint}). This fires
 * before {@link io.varve.swath.observability.RunProgressReporter} starts — it only starts once
 * {@code ListRunner}'s engine dispatch is entered — so a hang on the very first
 * LIST/region-resolve is visible instead of silent.
 *
 * <p>Every call after the first passes straight through, at the cost of one {@code volatile} read
 * per call (a false {@link AtomicBoolean#compareAndSet} fast-exits) — no behavior change, pure
 * observability. Distinct from the liveness watchdogs: this never aborts or times anything out.
 */
public final class FirstRequestMarkerFetcher implements PageFetcher {

    private static final Logger log = LoggerFactory.getLogger(FirstRequestMarkerFetcher.class);

    private final PageFetcher delegate;
    private final long startedNs;
    private final AtomicBoolean firstRequestLogged = new AtomicBoolean(false);
    private final AtomicBoolean firstPageLogged = new AtomicBoolean(false);

    public FirstRequestMarkerFetcher(PageFetcher delegate, long startedNs) {
        this.delegate = delegate;
        this.startedNs = startedNs;
    }

    @Override
    public ListPage fetchPage(PageRequest req) throws ListingException, InterruptedException {
        if (firstRequestLogged.compareAndSet(false, true)) {
            log.info("list_first_request_issued elapsed_ms={}", elapsedMs());
        }
        ListPage page = delegate.fetchPage(req);
        if (firstPageLogged.compareAndSet(false, true)) {
            log.info("list_first_page_returned elapsed_ms={}", elapsedMs());
        }
        return page;
    }

    @Override
    public StoreCapabilities capabilities() {
        return delegate.capabilities();
    }

    private long elapsedMs() {
        return (System.nanoTime() - startedNs) / 1_000_000L;
    }
}

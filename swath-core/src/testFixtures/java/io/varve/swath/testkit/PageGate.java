/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import io.varve.swath.error.ListingException;
import io.varve.swath.store.PageRequest;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

/**
 * A deterministic stepping control for the <b>in-flight-page-vs-steal</b> interleaving
 * (algorithms.md §2/§3, edge-case 7). Plugged into {@link MockPageFetcher} as a
 * {@link MockPageFetcher.PageInterceptor}: when a fetched page matches {@link #target},
 * it signals {@link #awaitFetched()} (the page is now in-flight, fetched under the old
 * wider bound) and then <b>blocks</b> inside {@code fetchPage} until {@link #release()}.
 * The test uses that window to narrow the victim's {@code hi} to a pivot that falls inside
 * the in-flight page, then releases it — exercising the per-key volatile {@code hi} re-read
 * that stops the victim at {@code m} without double-emitting the keys now owned by the child.
 *
 * <p>Gates only the first matching page (subsequent matches pass straight through), so the
 * victim's completing page and the thief's probe are never blocked. Reusable by RES-3.
 */
public final class PageGate {

    private final Predicate<PageRequest> target;
    private final CountDownLatch fetched = new CountDownLatch(1);
    private final CountDownLatch released = new CountDownLatch(1);
    private final AtomicBoolean armed = new AtomicBoolean(true);

    public PageGate(Predicate<PageRequest> target) {
        this.target = target;
    }

    /** The interceptor to hand to {@link MockPageFetcher.Builder#interceptor}. */
    public MockPageFetcher.PageInterceptor interceptor() {
        return (req, idx, page) -> {
            if (target.test(req) && armed.compareAndSet(true, false)) {
                fetched.countDown();
                try {
                    if (!released.await(30, TimeUnit.SECONDS)) {
                        throw new ListingException("PageGate release timed out");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
            return page;
        };
    }

    /** Block until the gated page has been fetched and is parked in-flight. */
    public void awaitFetched() throws InterruptedException {
        if (!fetched.await(30, TimeUnit.SECONDS)) {
            throw new AssertionError("gated page was never fetched");
        }
    }

    /** Release the parked in-flight page so the worker resumes iterating its keys. */
    public void release() {
        released.countDown();
    }
}

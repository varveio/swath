/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Shared {@link PageFetcher} test scaffolding — the {@code REQ} request literal and
 * {@code stubFetcher} counting stub duplicated across {@link FirstRequestMarkerFetcherTest} and
 * {@link RateLimitedPageFetcherTest} — folded into a single package-private test-tree home in
 * the style of {@code ParquetPoolTestSupport} (OU5): both consumers are swath-core test classes,
 * so no testFixtures home is warranted.
 */
final class PageFetcherTestSupport {

    private PageFetcherTestSupport() {
    }

    static final PageRequest REQ = PageRequest.objects(new byte[0], null, 10);

    static PageFetcher stubFetcher(AtomicInteger calls) {
        return new PageFetcher() {
            @Override
            public ListPage fetchPage(PageRequest req) {
                calls.incrementAndGet();
                return new ListPage(List.of(), List.of(), false, null, null, null, 200, Duration.ZERO);
            }

            @Override
            public StoreCapabilities capabilities() {
                return StoreCapabilities.s3();
            }
        };
    }
}

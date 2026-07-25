/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store;

import io.varve.swath.error.ListingException;

/**
 * The store SPI: one page-fetch per call. <b>Not sealed</b> —
 * sealing would drag the test mock into the prod permits list. One impl per
 * object store; v1 ships {@code S3PageFetcher} only, plus the test
 * {@code MockPageFetcher}.
 */
public interface PageFetcher {

    ListPage fetchPage(PageRequest req) throws ListingException, InterruptedException;

    /** The router reads these to choose an engine — never assume {@code start_after}. */
    StoreCapabilities capabilities();
}

/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

/** A parsed request; paging runs with the shared read permit. */
public interface ListingOperation {
    /** Small output reservation required before paging starts. */
    int initialOutputBytes();

    default void beforePage() { }

    PreparedPage page();
}

/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

/**
 * Which {@link io.varve.swath.replay.store.ListingStore} backs a fixture (§2). The CLI accepts the
 * lowercase spellings ({@code sorted}, {@code duckdb}); {@code sorted} is the default.
 *
 * <p><b>There used to be an {@code auto} that chose between them</b>, serving sorted when the fixture
 * was eligible and silently falling back to DuckDB when it was not. That is a hazard for anything
 * measured: an unstamped or ineligible fixture would change the serving path — and with it the cost
 * of every request — without the run saying so anywhere a reader would look. A benchmark that
 * compares clients against "the server" cannot have the server quietly become a different one.
 * Choosing is now the operator's, stated up front, and an ineligible fixture under {@code sorted}
 * fails to start instead of degrading.
 */
public enum ServingMode {

    /** Require the sorted role-2 path; fail fast if the fixture is not sorted-eligible. The default. */
    SORTED,

    /** Force the DuckDB role-1 path (materialised) — the conformance oracle, works on any capture. */
    DUCKDB
}

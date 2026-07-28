/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.store;

import io.varve.swath.replay.fixture.SortedEligibility;
import java.nio.file.Path;
import java.util.List;

/**
 * A fixture a forced backend cannot serve because it is not sorted-eligible, raised where the
 * routing index is derived. Typed rather than a bare {@link IllegalArgumentException} for exactly
 * the reason {@link io.varve.swath.sort.RowGroupOrderException} is: the caller that hits this is a
 * sweep over a corpus of captures, and it must be able to tell <em>this capture is not
 * sorted-eligible</em> — corpus data, which the sweep records and carries on from — apart from any
 * other way the run can fail, from {@link #reason()} rather than by matching substrings of a
 * message. It stays an {@link IllegalArgumentException} because a forced backend's rejection has
 * always been one to its callers.
 *
 * <p>Unlike {@code RowGroupOrderException} this failure names no row group and no row, and cannot:
 * eligibility is a decision about a whole resolved file set — its stamps, its multi-file
 * completeness, and the ascent of its row-group first keys — taken before any run faults a row
 * group in. What it carries instead is the file set and the {@link SortedEligibility} reason.
 *
 * <p>{@link #redactedMessage()} is the same report with each file reduced to its name, for a record
 * that must not publish an operator's filesystem layout; the full paths stay in
 * {@link #getMessage()}, which is what a console log carries.
 */
public final class IneligibleFixtureException extends IllegalArgumentException {

    private static final long serialVersionUID = 1L;

    private final SimStoreBackend backend;
    private final String reason;
    private final List<Path> files;

    IneligibleFixtureException(SimStoreBackend backend, String reason, List<Path> files) {
        super(message(backend, reason, files.stream().map(Path::toString).toList()));
        this.backend = backend;
        this.reason = reason;
        this.files = List.copyOf(files);
    }

    /**
     * The machine-readable classification — one of the {@code serving.fallback} reasons
     * {@link io.varve.swath.replay.fixture.SortedFixtures} names.
     */
    public String reason() {
        return reason;
    }

    /** The backend that was asked for and declined. */
    public SimStoreBackend backend() {
        return backend;
    }

    /** The resolved file set the decision was taken over, in key order. */
    public List<Path> files() {
        return files;
    }

    /** {@link #getMessage()} with each file reduced to its file name — see the class javadoc. */
    public String redactedMessage() {
        return message(backend, reason, files.stream().map(file -> file.getFileName().toString()).toList());
    }

    private static String message(SimStoreBackend backend, String reason, List<String> files) {
        return "backend " + backend + " requires a sorted-eligible fixture (reason=" + reason + "), use "
                + SimStoreBackend.PARQUET + " instead: " + files;
    }
}

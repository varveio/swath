/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;
import java.nio.file.Path;

/**
 * A staged-segment corruption failure that carries a greppable {@code error_class} fingerprint into the
 * JSON run summary. Plain physical-integrity failures (bad magic/version, a CRC
 * mismatch, a truncated file) still throw an unclassified {@link IOException} — this subtype exists for
 * the corruption classes an operator must be able to triage post-hoc from {@code summary.json} alone,
 * without grepping stderr.
 *
 * <p><b>How the class reaches {@code summary.json}.</b> {@code SortTransform} wraps any merge-time
 * {@link IOException} into an {@code OutputException} and the run unwinds without {@code complete()}, so
 * the sidecar's terminal write is the one the {@code JsonRunSummaryWriter#close} path produces — a
 * {@code StopReason.CRASH} whose {@code error_class} is null unless the cause chain carries this type.
 * {@code ListRunner}'s merge catch walks the cause chain for this type and records
 * {@link #errorClass()} on {@code RunMetrics#recordFatalErrorClass}, which the CRASH terminal status
 * reads — the same typed-exception → terminal-status → {@code error_class} seam a classified seed
 * failure uses. (The engagement counter travels separately, in the serialized meter registry.)
 */
public final class SegmentCorruptionException extends IOException {

    private static final long serialVersionUID = 1L;

    /** A page-run segment's page {@code minKey}s went backwards within one file. */
    static final String PAGE_RUN_MIN_REGRESSION = "page_run_min_regression";

    private final String errorClass;

    SegmentCorruptionException(Path path, String errorClass, String message) {
        super("page-run segment " + path + ": error_class=" + errorClass + ": " + message);
        this.errorClass = errorClass;
    }

    /** The greppable fingerprint written to {@code summary.json}'s {@code error_class} field. */
    public String errorClass() {
        return errorClass;
    }
}

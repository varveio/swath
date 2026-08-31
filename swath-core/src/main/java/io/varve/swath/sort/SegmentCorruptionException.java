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
 * JSON run summary. This subtype exists for
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

    /** An admitted page's raw keys regressed, making its checkpoint durable cursor unsafe. */
    static final String PAGE_RUN_RAW_KEY_REGRESSION = "page_run_raw_key_regression";

    /** Adjacent pages in one segment violate the persisted disjointness contract. */
    static final String PAGE_RUN_PAGE_OVERLAP = "page_run_page_overlap";

    /** The CRC-protected page-run header envelope is malformed or corrupt. */
    static final String PAGE_RUN_HEADER_CORRUPTION = "page_run_header_corruption";

    /** The CRC-protected fixed trailer is malformed or corrupt. */
    static final String PAGE_RUN_TRAILER_CORRUPTION = "page_run_trailer_corruption";

    /** A CRC-valid page body failed structural or decoded-row validation. */
    static final String PAGE_RUN_BODY_CORRUPTION = "page_run_body_corruption";

    /** A page's decoded payload claim exceeds the residency admitted for its segment. */
    static final String PAGE_RUN_DECODED_PAGE_LIMIT = "page_run_decoded_page_limit";

    /** A page-bound key exceeds the CRC-protected maximum declared by its segment trailer. */
    static final String PAGE_RUN_KEY_LENGTH_LIMIT = "page_run_key_length_limit";

    /** A CRC-valid page-index claim disagreed with the physical page-run body. */
    static final String PAGE_RUN_INDEX_MISMATCH = "page_run_index_mismatch";

    /** Checkpoint-declared PageRun format metadata disagreed with the physical segment. */
    static final String PAGE_RUN_FORMAT_MISMATCH = "page_run_format_mismatch";

    private final String errorClass;

    SegmentCorruptionException(Path path, String errorClass, String message) {
        super("page-run segment " + path + ": error_class=" + errorClass + ": " + message);
        this.errorClass = errorClass;
    }

    SegmentCorruptionException(Path path, String errorClass, String message, Throwable cause) {
        super("page-run segment " + path + ": error_class=" + errorClass + ": " + message, cause);
        this.errorClass = errorClass;
    }

    /** The greppable fingerprint written to {@code summary.json}'s {@code error_class} field. */
    public String errorClass() {
        return errorClass;
    }
}

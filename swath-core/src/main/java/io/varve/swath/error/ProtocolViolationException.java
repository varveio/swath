/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.error;

/**
 * The store answered a well-formed request with a response no conforming implementation may
 * produce. Store-agnostic on purpose: any {@code PageFetcher} that validates a response shape
 * raises this rather than inventing its own type.
 *
 * <p><b>Fatal, never retried.</b> Like {@link AccessDeniedException} this is a plain {@link
 * ListingException} (exit 1) and not a {@link ThrottleException}, which is what the retry loops
 * ({@code TransientRetryFetcher}, {@code GaugedFetcher}) catch — so it propagates on the first
 * occurrence. It is also the one fetch fault the speculative readahead path refuses to absorb into
 * its otherwise fail-soft "drop the guess and refetch serially" handling, since an endpoint that
 * over-serves only intermittently could answer that serial refetch conformingly. That disposition is
 * deliberate: an endpoint that violates the protocol cannot be trusted for the rest of the listing,
 * retrying turns the defect into a spin, and repairing the response locally (dropping the excess
 * entries) would silently corrupt the listing. Refusing the endpoint is the only outcome that
 * neither loops nor lies.
 */
public final class ProtocolViolationException extends ListingException {

    private final String errorClass;

    private ProtocolViolationException(String errorClass, String message) {
        super("object store protocol violation: error_class=" + errorClass + ": " + message);
        this.errorClass = errorClass;
    }

    /**
     * A page carrying more entries than the request's max-keys bound — the response-side bound on
     * a hostile or broken endpoint that ignores the requested page size, since without it the
     * entry count swath materialises is whatever the wire says. The counts are named so a false
     * positive against a third-party S3-compatible implementation is diagnosable from this one
     * line rather than from a bug report.
     *
     * @param target the listed container, for the operator to identify (a bucket, for S3)
     */
    public static ProtocolViolationException oversizedPage(String target, int maxKeys,
                                                           int keys, int commonPrefixes) {
        return new ProtocolViolationException("oversized_page",
                "listing " + target + " returned " + (keys + commonPrefixes) + " entries ("
                        + keys + " keys + " + commonPrefixes
                        + (commonPrefixes == 1 ? " common prefix) for a request " : " common prefixes) for a request ")
                        + "bounded at max_keys=" + maxKeys + "; refusing the endpoint rather than "
                        + "truncating the listing");
    }

    @Override
    public String errorClass() {
        return errorClass;
    }
}

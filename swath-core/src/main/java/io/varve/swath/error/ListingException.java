/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.error;

/**
 * Unrecoverable listing error: stuck continuation token,
 * truncation-without-token, repeated permanent 5xx, access denied mid-run,
 * a response no conforming store may produce. Exit 1.
 */
public sealed class ListingException extends SwathException
        permits ThrottleException, RegionRedirectException,
                AccessDeniedException, UnauthorizedException, NoSuchBucketException,
                ProtocolViolationException {
    public ListingException(String message) {
        super(message);
    }

    public ListingException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public int exitCode() {
        return 1;
    }

    /**
     * The post-hoc, greppable {@code error_class} fingerprint for this listing failure — a stable
     * snake_case tag (e.g. {@code access_denied}, {@code no_such_bucket}, {@code region_redirect})
     * a fleet supervisor reads off the run summary to tell a well-known permanent failure apart
     * from an unclassified crash. {@code null} on the base type (a generic/unattributed
     * listing error carries no fingerprint); the typed permanent subtypes override it.
     */
    public String errorClass() {
        return null;
    }
}

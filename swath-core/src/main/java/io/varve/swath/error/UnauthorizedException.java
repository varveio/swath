/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.error;

/**
 * An S3 {@code 401 Unauthorized}: the request carried missing/invalid/expired credentials (e.g. an
 * {@code InvalidAccessKeyId} / {@code InvalidToken} / signature-mismatch class rejected at the
 * authentication layer).
 *
 * <p>Like {@link AccessDeniedException}, this is a <b>fatal, non-retryable</b> {@link
 * ListingException} (exit 1): the same credentials will be rejected forever, so it must never fall
 * through as the opaque {@code unexpected error} crash. It exists so a credential/auth failure is
 * <b>always</b> a typed, greppable {@code error_class=unauthorized} in the run summary rather than
 * an unclassified crash.
 */
public final class UnauthorizedException extends ListingException {

    public UnauthorizedException(String bucket, Throwable cause) {
        super("S3 listObjectsV2 unauthorized (401) for bucket=" + bucket
                + "; the request credentials were missing/invalid/expired", cause);
    }

    @Override
    public String errorClass() {
        return "unauthorized";
    }
}

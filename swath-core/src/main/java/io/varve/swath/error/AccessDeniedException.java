/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.error;

/**
 * An S3 {@code 403 AccessDenied}: the caller is not authorized to LIST the bucket (a
 * permissions/config problem — e.g. {@code --no-sign-request} against a bucket that denies
 * anonymous LIST, or credentials without {@code s3:ListBucket}).
 *
 * <p>Like {@link RegionRedirectException}, this is a <b>fatal, non-retryable</b> {@link
 * ListingException} (exit 1): the same client/credentials will get the same 403 forever, so
 * retrying is pointless and it must never fall through as the opaque {@code unexpected error}
 * crash. It exists so a permissions problem is <b>always</b> a typed, greppable
 * {@code error_class=access_denied} in the run summary instead of an unclassified crash, letting a
 * supervisor tell a config/permissions failure apart from an in-process bug.
 */
public final class AccessDeniedException extends ListingException {

    public AccessDeniedException(String bucket, Throwable cause) {
        super("S3 listObjectsV2 denied (403 AccessDenied) for bucket=" + bucket
                + "; the caller is not authorized to list this bucket (check credentials / "
                + "--no-sign-request / bucket policy)", cause);
    }

    @Override
    public String errorClass() {
        return "access_denied";
    }
}

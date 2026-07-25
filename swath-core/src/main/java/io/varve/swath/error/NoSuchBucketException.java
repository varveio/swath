/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.error;

/**
 * An S3 {@code 404 NoSuchBucket}: the named bucket does not exist (a typo'd/decommissioned bucket,
 * or the wrong endpoint).
 *
 * <p>Like {@link AccessDeniedException}, this is a <b>fatal, non-retryable</b> {@link
 * ListingException} (exit 1): the bucket will not materialize on retry, so it must never fall
 * through as the opaque {@code unexpected error} crash. It exists so a missing-bucket failure is
 * <b>always</b> a typed, greppable {@code error_class=no_such_bucket} in the run summary rather
 * than an unclassified crash.
 */
public final class NoSuchBucketException extends ListingException {

    public NoSuchBucketException(String bucket, Throwable cause) {
        super("S3 listObjectsV2 failed (404 NoSuchBucket) for bucket=" + bucket
                + "; the bucket does not exist (check the name and endpoint)", cause);
    }

    @Override
    public String errorClass() {
        return "no_such_bucket";
    }
}

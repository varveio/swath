/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.error;

/**
 * An S3 {@code 301 PermanentRedirect}: the bucket lives in a region other than the
 * one the {@code S3Client} was constructed for (the {@code --region}/registry value
 * passed to swath does not match the bucket's real region).
 *
 * <p>Unlike {@link ThrottleException}, this is <b>not</b> retried — the same client
 * will get the same redirect forever, so it is fatal (exit 1, {@code ListingException}
 * contract) rather than fed into AIMD. It exists so a 301 is <b>always</b> a typed,
 * clearly-attributed error instead of surfacing as an opaque/untyped crash: the
 * {@link #correctRegion()} (read off the S3-supplied {@code x-amz-bucket-region}
 * response header) tells the caller exactly which {@code --region} to retry with.
 *
 * <p>Rebuilding the {@code S3Client} and retrying automatically is deliberately out
 * of scope here: the fetcher's client is long-lived and shared across every worker
 * and thief thread in the engine for the life of the run (one {@code S3PageFetcher}
 * per run, wired in {@code ListCommand}), so swapping it out from inside a single
 * {@code fetchPage} call is a concurrency change to design deliberately, not one
 * to improvise here.
 */
public final class RegionRedirectException extends ListingException {

    private final String bucket;
    private final String correctRegion;

    public RegionRedirectException(String bucket, String correctRegion, Throwable cause) {
        super(message(bucket, correctRegion), cause);
        this.bucket = bucket;
        this.correctRegion = correctRegion;
    }

    private static String message(String bucket, String correctRegion) {
        return correctRegion != null && !correctRegion.isBlank()
                ? "S3 bucket '" + bucket + "' is in a different region than configured "
                        + "(301 PermanentRedirect); retry with --region " + correctRegion
                : "S3 bucket '" + bucket + "' is in a different region than configured "
                        + "(301 PermanentRedirect); the correct region could not be determined "
                        + "from the response (no x-amz-bucket-region header)";
    }

    @Override
    public String errorClass() {
        return "region_redirect";
    }

    public String bucket() {
        return bucket;
    }

    /** The bucket's real region, from the {@code x-amz-bucket-region} response header; nullable. */
    public String correctRegion() {
        return correctRegion;
    }
}

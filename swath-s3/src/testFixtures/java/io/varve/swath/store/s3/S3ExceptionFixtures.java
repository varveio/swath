/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Shared {@link S3Exception} builders for store/s3 fault-classification tests — replaces
 * the 3x hand-rolled {@code s3Exception()}/{@code redirectException()} "mirror" copies in {@code
 * S3PageFetcherUnitTest}, {@code S3PageFetcherFaultTaxonomyTest}, and {@code
 * S3PageFetcherErrorClassificationTest}.
 */
public final class S3ExceptionFixtures {

    private S3ExceptionFixtures() {
    }

    /** An {@link S3Exception} with the given HTTP status and AWS error code. */
    public static S3Exception s3Exception(int statusCode, String errorCode) {
        return (S3Exception) S3Exception.builder()
                .statusCode(statusCode)
                .awsErrorDetails(AwsErrorDetails.builder().errorCode(errorCode).build())
                .message(errorCode)
                .build();
    }

    /**
     * A {@code 301 PermanentRedirect} {@link S3Exception}, optionally carrying the
     * {@code x-amz-bucket-region} header ({@code bucketRegionHeader}, or none when {@code null}).
     */
    public static S3Exception redirectException(String bucketRegionHeader) {
        return redirectException(301, "PermanentRedirect", bucketRegionHeader);
    }

    /**
     * The fully general form: an arbitrary status/error code, optionally carrying the
     * {@code x-amz-bucket-region} redirect header.
     */
    public static S3Exception redirectException(int statusCode, String errorCode, String bucketRegionHeader) {
        SdkHttpResponse.Builder httpResponse = SdkHttpResponse.builder();
        if (bucketRegionHeader != null) {
            httpResponse.putHeader("x-amz-bucket-region", bucketRegionHeader);
        }
        return (S3Exception) S3Exception.builder()
                .statusCode(statusCode)
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode(errorCode)
                        .sdkHttpResponse(httpResponse.build())
                        .build())
                .message(errorCode)
                .build();
    }
}

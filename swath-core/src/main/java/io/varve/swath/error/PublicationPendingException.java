/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.error;

/**
 * Terminal dataset publication or its committed post-publication cleanup remains pending.
 *
 * <p>For managed Parquet the checkpoint remains the authoritative complete part set, so this
 * failure must leave the run resumable: a later invocation can retry publication without issuing
 * another object-store listing. For sorted output, the same type also carries a cleanup failure
 * after the authority listener returned; that dataset is already published and resume performs
 * PUBLISHED cleanup only. One-shot sinks still surface the same output error but have no resume
 * contract.
 */
public final class PublicationPendingException extends OutputException {

    public PublicationPendingException(String message, Throwable cause) {
        super(message, cause);
    }
}

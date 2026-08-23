/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.error;

/**
 * Terminal dataset publication failed after its parts were already made durable.
 *
 * <p>For managed Parquet the checkpoint remains the authoritative complete part set, so this
 * failure must leave the run resumable: a later invocation can retry publication without issuing
 * another object-store listing. One-shot sinks still surface the same output error but have no
 * resume contract.
 */
public final class PublicationPendingException extends OutputException {

    public PublicationPendingException(String message, Throwable cause) {
        super(message, cause);
    }
}

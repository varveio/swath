/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.error;

/**
 * Resumable sorted-output merge refusal raised before publication begins. Durable staging and the
 * MERGING checkpoint phase remain authoritative, so a later invocation can retry with zero LISTs.
 */
public final class MergePendingException extends OutputException {

    public MergePendingException(String message, Throwable cause) {
        super(message, cause);
    }
}

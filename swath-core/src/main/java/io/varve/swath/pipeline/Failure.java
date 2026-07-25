/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.pipeline;

/** Abnormal end-of-stream carrying the producer's failure cause. */
public record Failure<T>(Throwable cause) implements Msg<T> {
}

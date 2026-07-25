/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.pipeline;

/** Normal end-of-stream: exactly one is emitted after the producer quiesces. */
public record End<T>() implements Msg<T> {
}

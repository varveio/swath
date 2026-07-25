/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.pipeline;

/** A data element flowing downstream. */
public record Item<T>(T value) implements Msg<T> {
}

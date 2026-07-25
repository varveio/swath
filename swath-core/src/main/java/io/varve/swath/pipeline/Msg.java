/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.pipeline;

/**
 * Channel envelope: close/failure are carried by this sealed type, never by
 * polluting {@code ListEntry}. Exactly one producer per channel emits exactly
 * one {@link End} (or a {@link Failure}) after quiescence.
 */
public sealed interface Msg<T> permits Item, End, Failure {
}

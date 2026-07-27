/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

/**
 * Supplies a fresh OAuth-style bearer token on demand. Implementations own their own
 * caching/refresh policy; {@link #token()} may be called once per request and must be cheap in the
 * common case. Never log the returned value.
 */
@FunctionalInterface
public interface BearerTokenSupplier {

    String token();
}

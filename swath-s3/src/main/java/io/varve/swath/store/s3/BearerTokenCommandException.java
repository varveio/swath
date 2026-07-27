/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

/**
 * Unchecked: thrown from inside the AWS SDK's signing pipeline ({@link BearerTokenAuthScheme}),
 * whose {@code IdentityProvider}/{@code HttpSigner} callbacks declare no checked exception. Surfaces
 * to the caller as the cause of an SDK-level failure around the affected request, same as any other
 * signing-time fault.
 */
final class BearerTokenCommandException extends RuntimeException {

    BearerTokenCommandException(String message) {
        super(message);
    }

    BearerTokenCommandException(String message, Throwable cause) {
        super(message, cause);
    }
}

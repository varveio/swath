/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import io.varve.swath.observability.SafeInput;
import software.amazon.awssdk.identity.spi.Identity;

/**
 * The {@link Identity} carrying a resolved bearer token through the AWS SDK's auth-scheme SPI.
 *
 * <p>Deliberately a class with a redacting {@link #toString()} rather than a {@code record}: a
 * record would auto-generate a {@code toString()} printing the live token, and the SDK's own
 * diagnostics do stringify identities. Keep it this way.
 */
final class BearerTokenIdentity implements Identity {

    private final String token;

    BearerTokenIdentity(String token) {
        this.token = token;
    }

    String token() {
        return token;
    }

    @Override
    public String toString() {
        return "BearerTokenIdentity[token=" + SafeInput.REDACTED_SECRET + "]";
    }
}

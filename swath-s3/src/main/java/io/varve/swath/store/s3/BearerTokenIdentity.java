/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import software.amazon.awssdk.identity.spi.Identity;

/** The {@link Identity} carrying a resolved bearer token through the AWS SDK's auth-scheme SPI. */
final class BearerTokenIdentity implements Identity {

    private final String token;

    BearerTokenIdentity(String token) {
        this.token = token;
    }

    String token() {
        return token;
    }
}

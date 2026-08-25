/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.model;

/**
 * An object-store last-modified value could not be interpreted by a consumer that requires typed
 * timestamp semantics. Raw-text output never raises this exception.
 */
public final class LastModifiedParseException extends IllegalArgumentException {

    private final KeyBytes key;
    private final String lastModifiedText;

    LastModifiedParseException(KeyBytes key, String lastModifiedText, RuntimeException cause) {
        super("invalid last-modified timestamp in object-store response", cause);
        this.key = key;
        this.lastModifiedText = lastModifiedText;
    }

    /** The entry key whose timestamp was invalid. */
    public KeyBytes key() {
        return key;
    }

    /** The unmodified object-store timestamp text. */
    public String lastModifiedText() {
        return lastModifiedText;
    }
}

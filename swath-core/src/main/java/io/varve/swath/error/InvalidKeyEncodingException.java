/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.error;

import java.util.HexFormat;

/** A Parquet row carried key bytes that cannot be represented by its required STRING column. */
public final class InvalidKeyEncodingException extends OutputException {

    private static final int PREFIX_BYTES = 16;

    private InvalidKeyEncodingException(String message) {
        super(message);
    }

    /** Builds the typed output failure while exposing only a bounded, byte-exact key prefix. */
    public static InvalidKeyEncodingException forKey(byte[] key) {
        int prefixLength = Math.min(key.length, PREFIX_BYTES);
        String suffix = key.length > prefixLength ? "..." : "";
        return new InvalidKeyEncodingException(
                "Parquet key is not well-formed UTF-8: key_hex_prefix="
                        + HexFormat.of().formatHex(key, 0, prefixLength) + suffix);
    }
}

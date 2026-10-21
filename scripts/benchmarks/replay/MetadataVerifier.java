/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.bench;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** Warmup-only digest of emitted key, size, and provider-wire-precision last-modified time. */
final class MetadataVerifier {
    private final MessageDigest digest;
    private long count;
    private boolean finished;

    MetadataVerifier() {
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    void accept(byte[] key, long size, long wireEpochUnit) {
        if (finished || key == null) throw new IllegalStateException("metadata digest is closed or key is null");
        updateInt(digest, key.length);
        digest.update(key);
        updateLong(digest, size);
        updateLong(digest, wireEpochUnit);
        count++;
    }

    FixtureMetadataOracle.Digest finish() {
        if (finished) throw new IllegalStateException("metadata digest already finalized");
        finished = true;
        return new FixtureMetadataOracle.Digest(count, HexFormat.of().formatHex(digest.digest()));
    }

    private static void updateInt(MessageDigest digest, int value) {
        for (int shift = 24; shift >= 0; shift -= 8) digest.update((byte) (value >>> shift));
    }

    private static void updateLong(MessageDigest digest, long value) {
        for (int shift = 56; shift >= 0; shift -= 8) digest.update((byte) (value >>> shift));
    }
}

/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol.azure;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;

/** Bounded, stateless replay marker bound to one Azure list scope and fixture. */
final class AzureToken {
    private static final String PREFIX = "az1.";
    private static final int DIGEST_BYTES = 16;

    private AzureToken() {
    }

    static String encode(boolean inclusive, byte[] boundary, byte[] binding, int injectionProgress) {
        ByteBuffer data = ByteBuffer.allocate(2 + DIGEST_BYTES + boundary.length);
        data.put((byte) (inclusive ? 1 : 0)).put((byte) injectionProgress).put(binding).put(boundary);
        return PREFIX + Base64.getUrlEncoder().withoutPadding().encodeToString(data.array());
    }

    static Cursor decode(String marker, byte[] binding) {
        if (marker == null || marker.length() > 8192 || !marker.startsWith(PREFIX)) {
            throw new IllegalArgumentException("invalid Azure marker");
        }
        try {
            byte[] raw = Base64.getUrlDecoder().decode(marker.substring(PREFIX.length()));
            if (raw.length < 2 + DIGEST_BYTES || raw.length > 2 + DIGEST_BYTES + 4096
                    || raw[0] < 0 || raw[0] > 1 || raw[1] < 0 || raw[1] > 1
                    || !MessageDigest.isEqual(Arrays.copyOfRange(raw, 2, 2 + DIGEST_BYTES), binding)) {
                throw new IllegalArgumentException("invalid Azure marker");
            }
            return new Cursor(raw[0] == 1, Arrays.copyOfRange(raw, 2 + DIGEST_BYTES, raw.length), raw[1]);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid Azure marker", e);
        }
    }

    static byte[] binding(AzureListRequest request, String fixtureIdentity, String injectionProfile) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (String part : new String[] {"azure-flat-v1", "synthetic-v1", fixtureIdentity,
                    request.account(), request.container(), request.version(), request.prefix(),
                    request.delimiter(), request.startFrom(), injectionProfile}) {
                byte[] value = part == null ? new byte[0] : part.getBytes(StandardCharsets.UTF_8);
                digest.update(ByteBuffer.allocate(4).putInt(value.length).array());
                digest.update(value);
            }
            return Arrays.copyOf(digest.digest(), DIGEST_BYTES);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    record Cursor(boolean inclusive, byte[] boundary, int injectionProgress) {
    }
}

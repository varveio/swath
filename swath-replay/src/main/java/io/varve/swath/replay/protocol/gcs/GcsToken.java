/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol.gcs;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;

/** Stateless replay cursor bound to fixture and GCS request scope; never accepts provider tokens. */
final class GcsToken {
    private static final String PREFIX = "gcs1.";
    private static final int DIGEST_BYTES = 16;

    private GcsToken() {
    }

    static String encode(boolean inclusive, byte[] boundary, byte[] binding, int injectionProgress) {
        ByteBuffer data = ByteBuffer.allocate(2 + DIGEST_BYTES + boundary.length);
        data.put((byte) (inclusive ? 1 : 0)).put((byte) injectionProgress).put(binding).put(boundary);
        return PREFIX + Base64.getUrlEncoder().withoutPadding().encodeToString(data.array());
    }

    static Cursor decode(String token, byte[] binding) {
        if (token == null || token.length() > 2048 || !token.startsWith(PREFIX)) {
            throw new IllegalArgumentException("invalid GCS pageToken");
        }
        try {
            byte[] raw = Base64.getUrlDecoder().decode(token.substring(PREFIX.length()));
            if (raw.length < 2 + DIGEST_BYTES || raw.length > 2 + DIGEST_BYTES + 1024
                    || raw[0] < 0 || raw[0] > 1 || raw[1] < 0 || raw[1] > 1
                    || !MessageDigest.isEqual(Arrays.copyOfRange(raw, 2, 2 + DIGEST_BYTES), binding)) {
                throw new IllegalArgumentException("invalid GCS pageToken");
            }
            return new Cursor(raw[0] == 1, Arrays.copyOfRange(raw, 2 + DIGEST_BYTES, raw.length), raw[1]);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid GCS pageToken", e);
        }
    }

    static byte[] binding(GcsListRequest request, String fixtureIdentity, String injectionProfile) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (String part : new String[] {"gcs-json-v1", "synthetic-v1", fixtureIdentity,
                    request.bucket(), request.prefix(), request.delimiter(), request.startOffset(),
                    request.endOffset(), injectionProfile}) {
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

/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.testkit;

import io.varve.swath.replay.server.OwnedBody;
import java.nio.ByteBuffer;

/** Test-only copy of a chunked response body for exact byte assertions. */
public final class OwnedBodyBytes {
    private OwnedBodyBytes() { }

    public static byte[] copy(OwnedBody body) {
        byte[] bytes = new byte[body.length()];
        int offset = 0;
        for (ByteBuffer view : body.views()) {
            ByteBuffer source = view.asReadOnlyBuffer();
            int count = source.remaining();
            source.get(bytes, offset, count);
            offset += count;
        }
        if (offset != bytes.length) throw new AssertionError("response chunk lengths differ");
        return bytes;
    }
}

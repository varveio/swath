/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.bench;

import io.varve.swath.replay.server.BudgetedOutput;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;

/** Reads frozen contiguous and current chunked body views outside timed encoder sections. */
final class RenderBenchWire {
    private RenderBenchWire() { }

    static String sha256(BudgetedOutput output) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try {
            Object body = BudgetedOutput.class.getMethod("body").invoke(output);
            @SuppressWarnings("unchecked")
            List<ByteBuffer> views = (List<ByteBuffer>) body.getClass().getMethod("views").invoke(body);
            for (ByteBuffer view : views) digest.update(view.asReadOnlyBuffer());
        } catch (NoSuchMethodException oldDistribution) {
            ByteBuffer contiguous = (ByteBuffer) BudgetedOutput.class.getMethod("buffer").invoke(output);
            digest.update(contiguous.asReadOnlyBuffer());
        }
        return HexFormat.of().formatHex(digest.digest());
    }
}

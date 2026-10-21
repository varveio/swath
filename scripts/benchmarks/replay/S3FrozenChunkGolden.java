/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.bench;

import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.protocol.S3ListRequest;
import io.varve.swath.replay.protocol.S3ListResult;
import io.varve.swath.replay.protocol.S3ResultEntry;
import io.varve.swath.replay.protocol.S3Xml;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

/**
 * Reproduces ChunkedProviderDifferentialTest's frozen S3 byte hashes. Compile/run this against
 * the unmodified 69cb809 installDist, not the candidate classes:
 *
 * <pre>
 * javac -cp "$BASE/lib/*" -d /tmp/swath-frozen-golden scripts/benchmarks/replay/S3FrozenChunkGolden.java
 * java -cp "/tmp/swath-frozen-golden:$BASE/lib/*" io.varve.swath.replay.bench.S3FrozenChunkGolden
 * </pre>
 */
public final class S3FrozenChunkGolden {
    private S3FrozenChunkGolden() { }

    public static void main(String[] ignored) throws Exception {
        List<ListedObject> rows = new ArrayList<>(1000);
        String longPart = "x".repeat(650);
        for (int i = 0; i < 1000; i++) {
            String special = i == 499 ? "é&<>\r" : "";
            String key = "prefix/" + longPart + special + "%04d".formatted(i);
            rows.add(new ListedObject(key.getBytes(StandardCharsets.UTF_8), i,
                    1_767_225_600_000_000L + i * 1000L, "etag-" + i,
                    "STANDARD", null, null, null, null));
        }
        List<S3ResultEntry> entries = rows.stream().map(S3ResultEntry.ObjectResult::new)
                .map(entry -> (S3ResultEntry) entry).toList();
        for (boolean url : new boolean[] {true, false}) {
            S3ListResult result = new S3ListResult(new S3ListRequest("bench", null, null,
                    null, null, 1000, url, false), entries, false, null);
            ByteBuffer rendered = S3Xml.listBucketBuffer(result);
            byte[] body = new byte[rendered.remaining()];
            rendered.get(body);
            System.out.println((url ? "url" : "xml") + " " + body.length + " "
                    + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(body)));
        }
    }
}

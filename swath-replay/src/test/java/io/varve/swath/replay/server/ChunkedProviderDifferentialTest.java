/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.protocol.S3ListRequest;
import io.varve.swath.replay.protocol.S3ListResult;
import io.varve.swath.replay.protocol.S3ResultEntry;
import io.varve.swath.replay.protocol.S3Xml;
import io.varve.swath.replay.protocol.azure.AzureListRequest;
import io.varve.swath.replay.protocol.azure.AzureListResult;
import io.varve.swath.replay.protocol.azure.AzureXml;
import io.varve.swath.replay.protocol.gcs.GcsJson;
import io.varve.swath.replay.protocol.gcs.GcsListRequest;
import io.varve.swath.replay.protocol.gcs.GcsPage;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Large provider pages cross every chunk boundary; production views match one contiguous render. */
class ChunkedProviderDifferentialTest {
    private static final int CAP = 16 * 1024 * 1024;
    private static final int[] CHUNKS = {7, 4095, 4096, 4097, 64 * 1024, 128 * 1024, 256 * 1024};
    // Reproduce with scripts/benchmarks/replay/S3FrozenChunkGolden.java against the unmodified
    // 69cb809 installDist; these hashes are for that source's exact S3 corpus and request bytes.
    private static final String FROZEN_S3_URL_SHA =
            "257280d43823b1ccb025a1db442f1f57e5e0664e0b6eb6e7ac7abf5affb4f18f";
    private static final String FROZEN_S3_XML_SHA =
            "34e51a14791bbc3fd3349d840bbdd49ded0e9356a286155fd3a823564dfe591a";

    @Test
    void largeS3GcsAzureBodiesAreByteIdenticalAcrossEveryChunkSize() throws Exception {
        for (String protocol : new String[] {"s3-url", "s3-xml", "gcs", "azure"}) {
            List<ListedObject> rows = rows(protocol);
            byte[] reference = render(protocol, CAP, rows, true);
            assertThat(reference.length).as(protocol + " large page").isGreaterThan(600 * 1024);
            for (int chunk : CHUNKS) {
                assertThat(render(protocol, chunk, rows, false))
                        .as(protocol + " chunk=" + chunk).isEqualTo(reference);
            }
            if (protocol.equals("s3-url")) assertThat(sha256(reference)).isEqualTo(FROZEN_S3_URL_SHA);
            if (protocol.equals("s3-xml")) assertThat(sha256(reference)).isEqualTo(FROZEN_S3_XML_SHA);
        }
    }

    private static byte[] render(String protocol, int chunk, List<ListedObject> rows,
                                 boolean oneChunk) {
        ResponseByteBudget budget = new ResponseByteBudget(32L * 1024 * 1024);
        int initial = switch (protocol) {
            case "s3-url", "s3-xml" -> 512 + rows.size() * 320;
            case "gcs" -> 1024 + rows.size() * 384;
            case "azure" -> 2048 + rows.size() * 512;
            default -> throw new IllegalArgumentException(protocol);
        };
        try (BudgetedOutput output = new BudgetedOutput(budget, initial, CAP, chunk)) {
            if (oneChunk) output.setFirstChunkHint(CAP);
            OwnedBody body = switch (protocol) {
                case "s3-url", "s3-xml" -> {
                    List<S3ResultEntry> entries = rows.stream().map(S3ResultEntry.ObjectResult::new)
                            .map(entry -> (S3ResultEntry) entry).toList();
                    S3ListResult page = new S3ListResult(new S3ListRequest("bench", null, null, null,
                            null, 1000, protocol.equals("s3-url"), false), entries, false, null);
                    yield S3Xml.listBucketBody(page, output);
                }
                case "gcs" -> {
                    GcsListRequest request = new GcsListRequest("bench", null, null, null, null,
                            1000, null, false);
                    GcsJson.write(request, new GcsPage(rows, List.of(), null), output);
                    yield output.body();
                }
                case "azure" -> {
                    AzureListRequest request = new AzureListRequest("replay", "bench", "2026-06-06",
                            null, null, null, null, 1000, "1000", false, false, false, true, null);
                    List<AzureListResult.Entry> entries = new ArrayList<>(rows.size());
                    for (ListedObject row : rows) entries.add(new AzureListResult.Entry.Blob(row));
                    AzureXml.write(new AzureListResult(request, entries, null),
                            "http://127.0.0.1:19090/replay/", output);
                    yield output.body();
                }
                default -> throw new IllegalArgumentException(protocol);
            };
            if (oneChunk) assertThat(body.views()).hasSize(1);
            assertThat(output.capacity()).isLessThanOrEqualTo(body.length() + chunk);
            assertThat(budget.charged()).isEqualTo(output.capacity());
            return bytes(body);
        } finally {
            assertThat(budget.charged()).isZero();
        }
    }

    private static List<ListedObject> rows(String protocol) {
        List<ListedObject> rows = new ArrayList<>(1000);
        String longPart = "x".repeat(650);
        for (int i = 0; i < 1000; i++) {
            String special = i == 499 ? switch (protocol) {
                case "gcs" -> "é&<>";
                case "azure" -> "é&<>\ufffe";
                default -> "é&<>\r";
            } : "";
            if (protocol.equals("azure") && i == 498) special = "é&<>";
            if (protocol.equals("azure") && i == 500) special = "&<>'\"";
            String key = "prefix/" + longPart + special + "%04d".formatted(i);
            rows.add(new ListedObject(key.getBytes(StandardCharsets.UTF_8), i,
                    1_767_225_600_000_000L + i * 1000L, "etag-" + i,
                    "STANDARD", null, null, null, null));
        }
        return rows;
    }

    private static byte[] bytes(OwnedBody body) {
        byte[] result = new byte[body.length()];
        int offset = 0;
        for (ByteBuffer view : body.views()) {
            ByteBuffer copy = view.duplicate();
            int count = copy.remaining();
            copy.get(result, offset, count);
            offset += count;
        }
        return result;
    }

    private static String sha256(byte[] body) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(body));
    }
}

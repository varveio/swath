/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.replay.metrics.ReplayMetrics;
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
import io.varve.swath.replay.store.DuckDbListingStore;
import io.varve.swath.replay.store.Projection;
import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Every native route sends the exact large chunked body and Content-Length it rendered. */
class ReplayChunkedWireTest {
    @TempDir Path temp;

    @Test
    void actualHttpBodiesMatchIndependentRenderedViewsAboveSixHundredKiB() throws Exception {
        Path fixture = temp.resolve("large.parquet");
        try (var writer = ParquetFixtures.open(fixture)) {
            for (int i = 0; i < 1000; i++) {
                writer.write(ObjectEntries.bare("prefix/" + "x".repeat(650) + "%04d".formatted(i)));
            }
        }
        ReplayMetrics directMetrics = new ReplayMetrics();
        List<ListedObject> rows;
        try (DuckDbListingStore store = new DuckDbListingStore(fixture, directMetrics, 1)) {
            rows = store.rows(null, true, null, 1000, Projection.KEYS_ONLY);
        } finally {
            directMetrics.registry().close();
        }
        assertThat(rows).hasSize(1000);
        ServeConfig config = new ServeConfig(fixture, "127.0.0.1", 0, "bucket", ServingMode.DUCKDB,
                2, 16, Set.of(Protocol.S3, Protocol.GCS, Protocol.AZURE), "replay",
                16L * 1024 * 1024, 8 * 1024 * 1024,
                Duration.ofSeconds(10), Duration.ofSeconds(30), Duration.ofSeconds(30), null,
                (request, result) -> Duration.ZERO);
        try (ReplayServer server = ReplayServer.open(config);
             HttpClient client = HttpClient.newHttpClient()) {
            server.start();
            for (String protocol : new String[] {"s3", "gcs", "azure"}) {
                String path = switch (protocol) {
                    case "s3" -> "/bucket?list-type=2&max-keys=1000&encoding-type=url";
                    case "gcs" -> "/storage/v1/b/bucket/o?maxResults=1000";
                    case "azure" -> "/replay/bucket?restype=container&comp=list&maxresults=1000";
                    default -> throw new IllegalStateException(protocol);
                };
                HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(
                        "http://127.0.0.1:" + server.port() + path));
                if (protocol.equals("azure")) request.header("x-ms-version", "2026-06-06");
                HttpResponse<byte[]> response = client.send(request.build(), HttpResponse.BodyHandlers.ofByteArray());
                assertThat(response.statusCode()).as(protocol).isEqualTo(200);
                byte[] expected = expected(protocol, rows, server.port());
                assertThat(expected.length).as(protocol).isGreaterThan(600 * 1024);
                assertThat(response.headers().firstValue("content-length")).contains(Integer.toString(expected.length));
                assertThat(response.body()).as(protocol + " wire bytes").isEqualTo(expected);
            }
        }
    }

    private static byte[] expected(String protocol, List<ListedObject> rows, int port) {
        ResponseByteBudget budget = new ResponseByteBudget(16L * 1024 * 1024);
        try (BudgetedOutput output = new BudgetedOutput(budget, 512 * 1024, 8 * 1024 * 1024,
                128 * 1024)) {
            OwnedBody body = switch (protocol) {
                case "s3" -> {
                    List<S3ResultEntry> entries = rows.stream().map(S3ResultEntry.ObjectResult::new)
                            .map(entry -> (S3ResultEntry) entry).toList();
                    S3ListResult result = new S3ListResult(new S3ListRequest("bucket", null, null,
                            null, null, 1000, true, false), entries, false, null);
                    yield S3Xml.listBucketBody(result, output);
                }
                case "gcs" -> {
                    GcsListRequest request = new GcsListRequest("bucket", null, null,
                            null, null, 1000, null, false);
                    GcsJson.write(request, new GcsPage(rows, List.of(), null), output);
                    yield output.body();
                }
                case "azure" -> {
                    AzureListRequest request = new AzureListRequest("replay", "bucket", "2026-06-06",
                            null, null, null, null, 1000, "1000", false, false, false, true, null);
                    List<AzureListResult.Entry> entries = new ArrayList<>(rows.size());
                    for (ListedObject row : rows) entries.add(new AzureListResult.Entry.Blob(row));
                    AzureXml.write(new AzureListResult(request, entries, null),
                            "http://127.0.0.1:" + port + "/replay/", output);
                    yield output.body();
                }
                default -> throw new IllegalStateException(protocol);
            };
            byte[] bytes = new byte[body.length()];
            int offset = 0;
            for (ByteBuffer view : body.views()) {
                ByteBuffer copy = view.duplicate();
                int count = copy.remaining();
                copy.get(bytes, offset, count);
                offset += count;
            }
            return bytes;
        } finally {
            assertThat(budget.charged()).isZero();
        }
    }
}

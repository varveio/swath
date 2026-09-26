/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.conformance.provider;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ProviderExchangeComparatorTest {
    private static final String DATE = "Thu, 01 Oct 2026 00:00:00 GMT";

    @Test
    void gcsStatusHeadersAndErrorsUseNamedVolatilesOnly() throws Exception {
        byte[] nativeBody = bytes("""
                {"error":{"code":400,"message":"native text","errors":[{"reason":"invalid"}]}}
                """);
        byte[] replayBody = bytes("""
                {"error":{"code":400,"message":"replay text","errors":[{"reason":"invalid"}]}}
                """);
        var nativeExchange = exchange(400, Map.of("Content-Type", List.of("application/json"),
                "Date", List.of(DATE), "x-goog-request-id", List.of("native-id")), nativeBody);
        var replayExchange = exchange(400, Map.of("content-type", List.of("application/json; charset=utf-8"),
                "date", List.of(DATE), "x-swath-replay-error", List.of("invalid_query")), replayBody);
        assertThatCode(() -> ProviderExchangeComparator.assertGcs(nativeExchange, replayExchange,
                "live", "replay", false)).doesNotThrowAnyException();
        assertThatThrownBy(() -> ProviderExchangeComparator.assertGcs(nativeExchange,
                exchange(500, replayExchange.headers(), replayBody), "live", "replay", false))
                .hasMessageContaining("HTTP status");
        assertThatThrownBy(() -> ProviderExchangeComparator.assertGcs(nativeExchange,
                exchange(400, replayExchange.headers(), bytes("""
                        {"error":{"code":400,"message":"x","errors":[{"reason":"notFound"}]}}
                        """)), "live", "replay", false)).hasMessageContaining("error reason");
        assertThatThrownBy(() -> ProviderExchangeComparator.assertGcs(nativeExchange,
                exchange(400, Map.of("content-type", List.of("application/json"),
                        "x-new-header", List.of("unclassified")), replayBody), "live", "replay", false))
                .hasMessageContaining("unclassified HTTP header");
    }

    @Test
    void azureVersionClientRequestIdAndErrorCodeAreChecked() throws Exception {
        byte[] nativeBody = bytes("""
                <?xml version="1.0"?><Error><Code>InvalidHeaderValue</Code><Message>native</Message></Error>
                """);
        byte[] replayBody = bytes("""
                <?xml version="1.0"?><Error><Code>InvalidHeaderValue</Code><Message>replay</Message></Error>
                """);
        var nativeExchange = exchange(400, Map.of("content-type", List.of("application/xml"),
                "x-ms-version", List.of("2026-06-06"), "x-ms-error-code", List.of("InvalidHeaderValue"),
                "x-ms-client-request-id", List.of("probe-1"), "x-ms-request-id", List.of("native-id")), nativeBody);
        var replayExchange = exchange(400, Map.of("content-type", List.of("application/xml"),
                "x-ms-version", List.of("2026-06-06"), "x-ms-error-code", List.of("InvalidHeaderValue"),
                "x-ms-client-request-id", List.of("probe-1"), "x-ms-request-id", List.of("replay-id")), replayBody);
        assertThatCode(() -> ProviderExchangeComparator.assertAzure(nativeExchange, replayExchange,
                "https://live.blob.core.windows.net/", "http://127.0.0.1/replay/",
                "live", "bucket", "2026-06-06", "probe-1", null, null)).doesNotThrowAnyException();
        assertThatThrownBy(() -> ProviderExchangeComparator.assertAzure(nativeExchange,
                exchange(400, Map.of("content-type", List.of("application/xml"),
                        "x-ms-version", List.of("2026-10-06"),
                        "x-ms-error-code", List.of("InvalidHeaderValue"),
                        "x-ms-client-request-id", List.of("probe-1")), replayBody),
                "https://live.blob.core.windows.net/", "http://127.0.0.1/replay/",
                "live", "bucket", "2026-06-06", "probe-1", null, null))
                .hasMessageContaining("replay x-ms-version");
        assertThatThrownBy(() -> ProviderExchangeComparator.assertAzure(nativeExchange,
                exchange(400, Map.of("content-type", List.of("application/xml"),
                        "x-ms-version", List.of("2026-06-06"),
                        "x-ms-error-code", List.of("ContainerNotFound"),
                        "x-ms-client-request-id", List.of("probe-1")), replayBody),
                "https://live.blob.core.windows.net/", "http://127.0.0.1/replay/",
                "live", "bucket", "2026-06-06", "probe-1", null, null))
                .hasMessageContaining("replay x-ms-error-code");
    }

    @Test
    void successfulEmptyPagesStillCheckContentTypeAndBodyLength() throws Exception {
        byte[] body = bytes("{" + "\"kind\":\"storage#objects\"}");
        var nativeExchange = exchange(200, Map.of("content-type", List.of("application/json"),
                "content-length", List.of(Integer.toString(body.length))), body);
        var replayExchange = exchange(200, Map.of("content-type", List.of("application/json; charset=utf-8")), body);
        assertThatCode(() -> ProviderExchangeComparator.assertGcs(nativeExchange, replayExchange,
                "live", "replay", false)).doesNotThrowAnyException();
        assertThatThrownBy(() -> ProviderExchangeComparator.assertGcs(
                exchange(200, Map.of("content-type", List.of("application/json"),
                        "content-length", List.of("999")), body), replayExchange,
                "live", "replay", false)).hasMessageContaining("Content-Length");
    }

    @Test
    void walkHeadersAreCheckedWithoutMatchingProviderPageBoundaries() throws Exception {
        String nativeItemA = item("live", "a", false);
        String nativeItemB = item("live", "b", false);
        String replayItemA = item("replay", "a", true);
        String replayItemB = item("replay", "b", true);
        var nativePages = List.of(
                new ProviderExchangeComparator.CapturedExchangePage(null,
                        exchange(200, Map.of("content-type", List.of("application/json")),
                                bytes(page(nativeItemA, "native-1")))),
                new ProviderExchangeComparator.CapturedExchangePage("native-1",
                        exchange(200, Map.of("content-type", List.of("application/json")),
                                bytes(page(nativeItemB, null)))));
        var replayPages = List.of(new ProviderExchangeComparator.CapturedExchangePage(null,
                exchange(200, Map.of("content-type", List.of("application/json; charset=utf-8")),
                        bytes(page(replayItemA + "," + replayItemB, null)))));
        assertThatCode(() -> ProviderExchangeComparator.assertGcsWalk(nativePages, replayPages,
                "live", "replay", false, 3, 10)).doesNotThrowAnyException();
        assertThatThrownBy(() -> ProviderExchangeComparator.assertGcsWalk(nativePages,
                List.of(new ProviderExchangeComparator.CapturedExchangePage(null,
                        exchange(200, Map.of("content-type", List.of("text/plain")),
                                bytes(page(replayItemA + "," + replayItemB, null))))),
                "live", "replay", false, 3, 10)).hasMessageContaining("Content-Type");
    }

    @Test
    void azureWalkChecksVersionAndRawXmlPreamble() throws Exception {
        String nativeXml = "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
                + "<EnumerationResults ServiceEndpoint=\"https://live.blob.core.windows.net/\""
                + " ContainerName=\"live\"><Blobs></Blobs><NextMarker></NextMarker></EnumerationResults>";
        String replayXml = nativeXml.replace("https://live.blob.core.windows.net/", "http://127.0.0.1/replay/")
                .replace("ContainerName=\"live\"", "ContainerName=\"bucket\"");
        var nativePages = List.of(new ProviderExchangeComparator.CapturedExchangePage(null,
                exchange(200, Map.of("content-type", List.of("application/xml"),
                        "x-ms-version", List.of("2026-06-06")), bytes(nativeXml))));
        var replayPages = List.of(new ProviderExchangeComparator.CapturedExchangePage(null,
                exchange(200, Map.of("content-type", List.of("application/xml"),
                        "x-ms-version", List.of("2026-06-06")), bytes(replayXml))));
        assertThatCode(() -> ProviderExchangeComparator.assertAzureWalk(nativePages, replayPages,
                "https://live.blob.core.windows.net/", "http://127.0.0.1/replay/",
                "live", "bucket", "2026-06-06", 2, 10)).doesNotThrowAnyException();
        assertThatThrownBy(() -> ProviderExchangeComparator.assertAzureWalk(nativePages,
                List.of(new ProviderExchangeComparator.CapturedExchangePage(null,
                        exchange(200, Map.of("content-type", List.of("application/xml"),
                                "x-ms-version", List.of("2026-06-06")), bytes("\uFEFF" + replayXml)))),
                "https://live.blob.core.windows.net/", "http://127.0.0.1/replay/",
                "live", "bucket", "2026-06-06", 2, 10)).hasMessageContaining("BOM/prolog");
    }

    private static String page(String items, String token) {
        return "{\"kind\":\"storage#objects\",\"items\":[" + items + "]"
                + (token == null ? "}" : ",\"nextPageToken\":\"" + token + "\"}");
    }

    private static String item(String bucket, String name, boolean replay) {
        return "{\"kind\":\"storage#object\",\"bucket\":\"" + bucket + "\",\"name\":\"" + name
                + "\",\"size\":\"1\",\"updated\":\"2026-01-01T00:00:00.000000Z\","
                + "\"etag\":\"etag\",\"generation\":\"" + (replay ? "1" : "2")
                + "\",\"metageneration\":\"1\",\"storageClass\":\"STANDARD\","
                + "\"contentType\":\"application/octet-stream\"}";
    }

    private static ProviderExchangeComparator.Exchange exchange(int status, Map<String, List<String>> headers,
                                                                 byte[] body) {
        return new ProviderExchangeComparator.Exchange(status, headers, body);
    }

    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
}

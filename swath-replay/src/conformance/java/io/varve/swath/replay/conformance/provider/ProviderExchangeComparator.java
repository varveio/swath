/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.conformance.provider;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/** Strict status/header/error comparison layered over provider-specific successful body policies. */
public final class ProviderExchangeComparator {
    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private static final Set<String> GCS_HEADERS = Set.of("content-type", "date", "x-goog-request-id",
            "content-length", "server", "cache-control", "vary", "transfer-encoding", "x-swath-replay-error");
    private static final Set<String> AZURE_HEADERS = Set.of("content-type", "date", "x-ms-request-id",
            "x-ms-version", "x-ms-error-code", "x-ms-client-request-id", "content-length", "server",
            "cache-control", "vary", "transfer-encoding", "x-swath-replay-error");
    private static final Set<String> AZURE_NATIVE_ERROR_DETAILS = Set.of("QueryParameterName",
            "QueryParameterValue", "HeaderName", "HeaderValue", "Reason", "RequestId", "Time",
            "AuthenticationErrorDetail");

    private ProviderExchangeComparator() {
    }

    /** Compare successful GCS walks with independent endpoint tokens and page boundaries. */
    public static void assertGcsWalk(List<CapturedExchangePage> nativePages,
                                     List<CapturedExchangePage> replayPages,
                                     String nativeBucket, String replayBucket, boolean fullProjection,
                                     int maxPages, int maxEntries) throws IOException {
        List<GcsProfileComparator.CapturedPage> nativeBodies = new java.util.ArrayList<>();
        List<GcsProfileComparator.CapturedPage> replayBodies = new java.util.ArrayList<>();
        for (CapturedExchangePage page : nativePages) {
            requireSuccessHeaders(page.exchange(), GCS_HEADERS, "application/json");
            nativeBodies.add(new GcsProfileComparator.CapturedPage(page.requestToken(), page.exchange().body()));
        }
        for (CapturedExchangePage page : replayPages) {
            requireSuccessHeaders(page.exchange(), GCS_HEADERS, "application/json");
            replayBodies.add(new GcsProfileComparator.CapturedPage(page.requestToken(), page.exchange().body()));
        }
        GcsProfileComparator.assertWalk(nativeBodies, replayBodies, nativeBucket, replayBucket,
                fullProjection, maxPages, maxEntries);
    }

    /** Compare successful Azure walks, checking headers on every page before inventory comparison. */
    public static void assertAzureWalk(List<CapturedExchangePage> nativePages,
                                       List<CapturedExchangePage> replayPages,
                                       String nativeEndpoint, String replayEndpoint,
                                       String nativeContainer, String replayContainer,
                                       String requestedVersion, int maxPages, int maxEntries) throws IOException {
        List<AzureProfileComparator.CapturedPage> nativeBodies = new java.util.ArrayList<>();
        List<AzureProfileComparator.CapturedPage> replayBodies = new java.util.ArrayList<>();
        String nativePreamble = null;
        String replayPreamble = null;
        for (CapturedExchangePage page : nativePages) {
            requireSuccessHeaders(page.exchange(), AZURE_HEADERS, "application/xml");
            equal("native x-ms-version", requestedVersion, single(page.exchange(), "x-ms-version"));
            String preamble = xmlPreamble(page.exchange().body());
            if (nativePreamble == null) nativePreamble = preamble;
            else equal("native XML preamble across pages", nativePreamble, preamble);
            nativeBodies.add(new AzureProfileComparator.CapturedPage(page.requestToken(),
                    page.requestPrefix(), page.requestDelimiter(), page.requestMaxResults(),
                    page.exchange().body()));
        }
        for (CapturedExchangePage page : replayPages) {
            requireSuccessHeaders(page.exchange(), AZURE_HEADERS, "application/xml");
            equal("replay x-ms-version", requestedVersion, single(page.exchange(), "x-ms-version"));
            String preamble = xmlPreamble(page.exchange().body());
            if (replayPreamble == null) replayPreamble = preamble;
            else equal("replay XML preamble across pages", replayPreamble, preamble);
            replayBodies.add(new AzureProfileComparator.CapturedPage(page.requestToken(),
                    page.requestPrefix(), page.requestDelimiter(), page.requestMaxResults(),
                    page.exchange().body()));
        }
        equal("Azure BOM/prolog", nativePreamble, replayPreamble);
        AzureProfileComparator.assertWalk(nativeBodies, replayBodies, nativeEndpoint, replayEndpoint,
                nativeContainer, replayContainer, maxPages, maxEntries);
    }

    private static void requireSuccessHeaders(Exchange exchange, Set<String> allowed, String contentType) {
        if (exchange.status() != 200) fail("walk page has non-200 HTTP status");
        checkHeaders(exchange, allowed, contentType);
    }

    public static void assertGcs(Exchange nativeExchange, Exchange replayExchange,
                                 String nativeBucket, String replayBucket, boolean fullProjection)
            throws IOException {
        equal("GCS HTTP status", nativeExchange.status(), replayExchange.status());
        checkHeaders(nativeExchange, GCS_HEADERS, "application/json");
        checkHeaders(replayExchange, GCS_HEADERS, "application/json");
        if (nativeExchange.status() == 200) {
            GcsProfileComparator.assertPage(nativeExchange.body(), replayExchange.body(),
                    nativeBucket, replayBucket, fullProjection);
        } else {
            GcsError nativeError = gcsError(nativeExchange);
            GcsError replayError = gcsError(replayExchange);
            equal("GCS error reason", nativeError.reason(), replayError.reason());
        }
    }

    public static void assertAzure(Exchange nativeExchange, Exchange replayExchange,
                                   String nativeEndpoint, String replayEndpoint,
                                   String nativeContainer, String replayContainer,
                                   String requestedVersion, String clientRequestId,
                                   String nativeRequestMarker, String replayRequestMarker) throws IOException {
        assertAzure(nativeExchange, replayExchange, nativeEndpoint, replayEndpoint,
                nativeContainer, replayContainer, requestedVersion, clientRequestId,
                nativeRequestMarker, replayRequestMarker, "GET");
    }

    public static void assertAzure(Exchange nativeExchange, Exchange replayExchange,
                                   String nativeEndpoint, String replayEndpoint,
                                   String nativeContainer, String replayContainer,
                                   String requestedVersion, String clientRequestId,
                                   String nativeRequestMarker, String replayRequestMarker,
                                   String requestMethod) throws IOException {
        equal("Azure HTTP status", nativeExchange.status(), replayExchange.status());
        boolean head = "HEAD".equals(requestMethod);
        checkHeaders(nativeExchange, AZURE_HEADERS, "application/xml", head);
        checkHeaders(replayExchange, AZURE_HEADERS, "application/xml", head);
        equal("native x-ms-version", requestedVersion, single(nativeExchange, "x-ms-version"));
        equal("replay x-ms-version", requestedVersion, single(replayExchange, "x-ms-version"));
        if (clientRequestId != null) {
            equal("native x-ms-client-request-id", clientRequestId,
                    single(nativeExchange, "x-ms-client-request-id"));
            equal("replay x-ms-client-request-id", clientRequestId,
                    single(replayExchange, "x-ms-client-request-id"));
        }
        if (head) {
            if (nativeExchange.status() < 400 || nativeExchange.status() > 599
                    || nativeExchange.body().length != 0
                    || replayExchange.body().length != 0) {
                fail("Azure HEAD error profile requires empty non-success bodies");
            }
            String nativeCode = single(nativeExchange, "x-ms-error-code");
            String replayCode = single(replayExchange, "x-ms-error-code");
            if (nativeCode == null || nativeCode.isEmpty() || replayCode == null || replayCode.isEmpty()) {
                fail("Azure HEAD error code header is missing");
            }
            equal("Azure HEAD error code", nativeCode, replayCode);
            return;
        }
        equal("Azure BOM/prolog", xmlPreamble(nativeExchange.body()), xmlPreamble(replayExchange.body()));
        if (nativeExchange.status() == 200) {
            AzureProfileComparator.assertPage(nativeExchange.body(), replayExchange.body(),
                    nativeEndpoint, replayEndpoint, nativeContainer, replayContainer,
                    nativeRequestMarker, replayRequestMarker);
        } else {
            AzureError nativeError = azureError(nativeExchange, true);
            AzureError replayError = azureError(replayExchange, false);
            equal("Azure error code", nativeError.code(), replayError.code());
            equal("native x-ms-error-code", nativeError.code(), single(nativeExchange, "x-ms-error-code"));
            equal("replay x-ms-error-code", replayError.code(), single(replayExchange, "x-ms-error-code"));
        }
    }

    public static void assertAzureMapped(Exchange nativeExchange, Exchange replayExchange,
                                         AzureNamespaceMapping mapping, String nativePath, String replayPath,
                                         String nativeEndpoint, String replayEndpoint,
                                         String requestedVersion, String clientRequestId,
                                         String nativeRequestMarker, String replayRequestMarker) throws IOException {
        mapping.assertMapped(nativePath, replayPath, nativeEndpoint, replayEndpoint);
        assertAzure(nativeExchange, replayExchange, nativeEndpoint, replayEndpoint,
                mapping.nativeContainer(), mapping.replayContainer(), requestedVersion,
                clientRequestId, nativeRequestMarker, replayRequestMarker);
    }

    private static void checkHeaders(Exchange exchange, Set<String> allowed, String contentType) {
        checkHeaders(exchange, allowed, contentType, false);
    }

    private static void checkHeaders(Exchange exchange, Set<String> allowed, String contentType,
                                     boolean head) {
        for (String header : exchange.headers().keySet()) {
            if (!allowed.contains(header)) fail("unclassified HTTP header " + header);
        }
        String actualType = single(exchange, "content-type");
        if (actualType == null ? !head
                : !actualType.toLowerCase(Locale.ROOT).split(";", 2)[0].trim().equals(contentType)) {
            fail("HTTP Content-Type differs from " + contentType);
        }
        String length = single(exchange, "content-length");
        if (length != null) {
            try {
                long declared = Long.parseLong(length);
                if (declared < 0 || !head && declared != exchange.body().length) {
                    fail("HTTP Content-Length differs from body");
                }
            } catch (NumberFormatException e) {
                fail("HTTP Content-Length is malformed");
            }
        }
        String date = single(exchange, "date");
        if (date != null) {
            if (!date.matches("[A-Za-z]{3}, [0-9]{2} [A-Za-z]{3} [0-9]{4} "
                    + "[0-9]{2}:[0-9]{2}:[0-9]{2} GMT")) fail("HTTP Date spelling is not padded GMT");
            ZonedDateTime.parse(date, DateTimeFormatter.RFC_1123_DATE_TIME);
        }
        for (String id : List.of("x-goog-request-id", "x-ms-request-id")) {
            if (exchange.headers().containsKey(id) && single(exchange, id).isEmpty()) {
                fail(id + " must be nonempty");
            }
        }
        for (String classified : List.of("server", "cache-control", "vary", "transfer-encoding",
                "x-swath-replay-error")) {
            if (exchange.headers().containsKey(classified) && single(exchange, classified).isEmpty()) {
                fail(classified + " must be nonempty");
            }
        }
    }

    private static GcsError gcsError(Exchange exchange) throws IOException {
        JsonNode root = JSON.readTree(exchange.body());
        if (!root.isObject() || root.size() != 1 || !root.has("error")) {
            fail("unknown GCS error envelope field");
        }
        JsonNode error = root.path("error");
        error.fieldNames().forEachRemaining(field -> {
            if (!Set.of("code", "message", "errors").contains(field)) {
                fail("unknown GCS error field " + field);
            }
        });
        if (!error.isObject() || !error.path("code").isIntegralNumber()
                || error.path("code").intValue() != exchange.status()
                || !error.path("message").isTextual()) {
            fail("invalid GCS error envelope");
        }
        JsonNode errors = error.path("errors");
        if (!errors.isArray() || errors.isEmpty() || !errors.get(0).path("reason").isTextual()) {
            fail("GCS error reason missing");
        }
        for (JsonNode detail : errors) {
            if (!detail.isObject()) fail("GCS error detail must be an object");
            detail.fieldNames().forEachRemaining(field -> {
                if (!Set.of("domain", "reason", "message", "location", "locationType").contains(field)) {
                    fail("unknown GCS error detail field " + field);
                }
            });
        }
        return new GcsError(errors.get(0).path("reason").textValue());
    }

    private static AzureError azureError(Exchange exchange, boolean nativeResponse) throws IOException {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            factory.setXIncludeAware(false);
            factory.setExpandEntityReferences(false);
            Element root = factory.newDocumentBuilder()
                    .parse(new ByteArrayInputStream(exchange.body())).getDocumentElement();
            if (!"Error".equals(root.getTagName())) fail("invalid Azure error root");
            String code = null;
            String message = null;
            Set<String> seenDetails = new java.util.HashSet<>();
            NodeList children = root.getChildNodes();
            for (int i = 0; i < children.getLength(); i++) {
                if (!(children.item(i) instanceof Element element)) continue;
                if ("Code".equals(element.getTagName()) && code == null) code = element.getTextContent();
                else if ("Message".equals(element.getTagName()) && message == null) message = element.getTextContent();
                else if (nativeResponse && AZURE_NATIVE_ERROR_DETAILS.contains(element.getTagName())
                        && seenDetails.add(element.getTagName()) && !element.hasAttributes()
                        && element.getElementsByTagName("*").getLength() == 0) {
                    // Named native error details are retained for classification but are not part
                    // of the first replay error profile. Their presence and scalar type are checked.
                }
                else fail("unknown or duplicate Azure error element");
            }
            if (code == null || code.isEmpty() || message == null) fail("invalid Azure error envelope");
            return new AzureError(code);
        } catch (ParserConfigurationException | SAXException e) {
            throw new IOException("invalid Azure error XML", e);
        }
    }

    private static String xmlPreamble(byte[] body) {
        String text = new String(body, StandardCharsets.UTF_8);
        int start = text.startsWith("\uFEFF") ? 1 : 0;
        if (!text.startsWith("<?xml", start)) fail("Azure XML declaration is missing");
        int end = text.indexOf("?>", start);
        if (end < 0) fail("Azure XML declaration is incomplete");
        return text.substring(0, end + 2);
    }

    private static String single(Exchange exchange, String name) {
        List<String> values = exchange.headers().get(name);
        if (values == null) return null;
        if (values.size() != 1) fail("duplicate HTTP header " + name);
        return values.getFirst();
    }

    private static void equal(String label, Object expected, Object actual) {
        if (!java.util.Objects.equals(expected, actual)) {
            fail(label + " differs: expected=" + expected + " actual=" + actual);
        }
    }

    private static void fail(String message) { throw new IllegalArgumentException(message); }

    public record Exchange(int status, Map<String, List<String>> headers, byte[] body) {
        public Exchange {
            Map<String, List<String>> normalized = new HashMap<>();
            headers.forEach((name, values) -> {
                String lower = name.toLowerCase(Locale.ROOT);
                if (normalized.putIfAbsent(lower, List.copyOf(values)) != null) {
                    fail("duplicate HTTP header name " + name);
                }
            });
            headers = Map.copyOf(normalized);
            body = body.clone();
        }

        @Override
        public byte[] body() { return body.clone(); }
    }

    public record CapturedExchangePage(String requestToken, String requestPrefix,
                                       String requestDelimiter, String requestMaxResults,
                                       Exchange exchange) {
        public CapturedExchangePage(String requestToken, Exchange exchange) {
            this(requestToken, null, null, null, exchange);
        }
    }

    private record GcsError(String reason) { }
    private record AzureError(String code) { }
}

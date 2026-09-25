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
            "transfer-encoding", "x-swath-replay-error");

    private ProviderExchangeComparator() {
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
        equal("Azure HTTP status", nativeExchange.status(), replayExchange.status());
        checkHeaders(nativeExchange, AZURE_HEADERS, "application/xml");
        checkHeaders(replayExchange, AZURE_HEADERS, "application/xml");
        equal("native x-ms-version", requestedVersion, single(nativeExchange, "x-ms-version"));
        equal("replay x-ms-version", requestedVersion, single(replayExchange, "x-ms-version"));
        if (clientRequestId != null) {
            equal("native x-ms-client-request-id", clientRequestId,
                    single(nativeExchange, "x-ms-client-request-id"));
            equal("replay x-ms-client-request-id", clientRequestId,
                    single(replayExchange, "x-ms-client-request-id"));
        }
        if (nativeExchange.status() == 200) {
            AzureProfileComparator.assertPage(nativeExchange.body(), replayExchange.body(),
                    nativeEndpoint, replayEndpoint, nativeContainer, replayContainer,
                    nativeRequestMarker, replayRequestMarker);
        } else {
            AzureError nativeError = azureError(nativeExchange);
            AzureError replayError = azureError(replayExchange);
            equal("Azure error code", nativeError.code(), replayError.code());
            equal("native x-ms-error-code", nativeError.code(), single(nativeExchange, "x-ms-error-code"));
            equal("replay x-ms-error-code", replayError.code(), single(replayExchange, "x-ms-error-code"));
        }
    }

    private static void checkHeaders(Exchange exchange, Set<String> allowed, String contentType) {
        for (String header : exchange.headers().keySet()) {
            if (!allowed.contains(header)) fail("unclassified HTTP header " + header);
        }
        String actualType = single(exchange, "content-type");
        if (actualType == null || !actualType.toLowerCase(Locale.ROOT).split(";", 2)[0].trim().equals(contentType)) {
            fail("HTTP Content-Type differs from " + contentType);
        }
        String length = single(exchange, "content-length");
        if (length != null) {
            try {
                if (Long.parseLong(length) != exchange.body().length) fail("HTTP Content-Length differs from body");
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
    }

    private static GcsError gcsError(Exchange exchange) throws IOException {
        JsonNode root = JSON.readTree(exchange.body());
        JsonNode error = root.path("error");
        if (!error.isObject() || !error.path("code").isIntegralNumber()
                || error.path("code").intValue() != exchange.status()
                || !error.path("message").isTextual()) {
            fail("invalid GCS error envelope");
        }
        JsonNode errors = error.path("errors");
        if (!errors.isArray() || errors.isEmpty() || !errors.get(0).path("reason").isTextual()) {
            fail("GCS error reason missing");
        }
        return new GcsError(errors.get(0).path("reason").textValue());
    }

    private static AzureError azureError(Exchange exchange) throws IOException {
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
            NodeList children = root.getChildNodes();
            for (int i = 0; i < children.getLength(); i++) {
                if (!(children.item(i) instanceof Element element)) continue;
                if ("Code".equals(element.getTagName()) && code == null) code = element.getTextContent();
                else if ("Message".equals(element.getTagName()) && message == null) message = element.getTextContent();
                else fail("unknown or duplicate Azure error element");
            }
            if (code == null || code.isEmpty() || message == null) fail("invalid Azure error envelope");
            return new AzureError(code);
        } catch (ParserConfigurationException | SAXException e) {
            throw new IOException("invalid Azure error XML", e);
        }
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

    private record GcsError(String reason) { }
    private record AzureError(String code) { }
}

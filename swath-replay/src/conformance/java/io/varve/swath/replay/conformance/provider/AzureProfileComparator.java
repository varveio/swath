/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.conformance.provider;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/** Field-policy comparator for deterministic Azure flat XML pages, independent of replay code. */
public final class AzureProfileComparator {
    private static final List<String> ECHO_ORDER = List.of("Prefix", "Marker", "MaxResults", "Delimiter");
    private static final List<String> REPLAY_PROPERTIES = List.of("Last-Modified", "Etag", "Content-Length",
            "Content-Type", "BlobType", "AccessTier", "AccessTierInferred", "LeaseStatus", "LeaseState");
    private static final Set<String> NATIVE_ONLY_PROPERTIES = Set.of("Creation-Time", "Content-Encoding",
            "Content-Language", "Content-MD5", "Cache-Control", "ServerEncrypted", "Content-Disposition",
            "Content-CRC64", "AccessTierChangeTime", "EncryptionScope", "EncryptionContext", "TagCount");

    private AzureProfileComparator() {
    }

    static AzureEvidence.Page project(byte[] body) throws IOException {
        Page parsed = parse(body, false);
        List<AzureEvidence.Entry> entries = new ArrayList<>();
        for (Entry entry : parsed.entries()) {
            entries.add(new AzureEvidence.Entry(entry.kind(), entry.name(), entry.encoded()));
        }
        return new AzureEvidence.Page(List.copyOf(entries), parsed.nextMarker(),
                parsed.endpoint(), parsed.container());
    }

    public static void assertPage(byte[] nativeBody, byte[] replayBody, String nativeEndpoint,
                                  String replayEndpoint, String nativeContainer, String replayContainer,
                                  String nativeRequestMarker, String replayRequestMarker) throws IOException {
        Page nativePage = parse(nativeBody, false);
        Page replayPage = parse(replayBody, true);
        compareParsed(nativePage, replayPage, nativeEndpoint, replayEndpoint, nativeContainer,
                replayContainer, nativeRequestMarker, replayRequestMarker, true);
    }

    /** Compare completed marker walks without requiring identical page boundaries. */
    public static void assertWalk(List<CapturedPage> nativePages, List<CapturedPage> replayPages,
                                  String nativeEndpoint, String replayEndpoint,
                                  String nativeContainer, String replayContainer,
                                  int maxPages, int maxEntries) throws IOException {
        Page nativeWalk = aggregate(nativePages, false, maxPages, maxEntries);
        Page replayWalk = aggregate(replayPages, true, maxPages, maxEntries);
        compareParsed(nativeWalk, replayWalk, nativeEndpoint, replayEndpoint, nativeContainer,
                replayContainer, null, null, false);
    }

    private static Page aggregate(List<CapturedPage> pages, boolean replay,
                                  int maxPages, int maxEntries) throws IOException {
        if (pages.isEmpty() || pages.size() > maxPages || maxEntries < 0) {
            fail("Azure walk page bound violated");
        }
        List<Entry> entries = new ArrayList<>();
        String expectedMarker = null;
        Set<String> seen = new HashSet<>();
        Page first = null;
        for (int i = 0; i < pages.size(); i++) {
            CapturedPage capture = pages.get(i);
            equal("Azure request marker at page " + i, expectedMarker, capture.requestMarker());
            Page page = parse(capture.body(), replay);
            if (first == null) first = page;
            else {
                equal("Azure endpoint across pages", first.endpoint(), page.endpoint());
                equal("Azure container across pages", first.container(), page.container());
            }
            entries.addAll(page.entries());
            if (entries.size() > maxEntries) fail("Azure walk entry bound violated");
            expectedMarker = page.nextMarker().isEmpty() ? null : page.nextMarker();
            if (i + 1 < pages.size() && (expectedMarker == null || !seen.add(expectedMarker))) {
                fail("Azure walk stopped early or repeated marker");
            }
        }
        if (expectedMarker != null) fail("Azure walk ended with nonterminal marker");
        return new Page(first.endpoint(), first.container(), Map.of(), List.copyOf(entries), "");
    }

    public record CapturedPage(String requestMarker, byte[] body) {
        public CapturedPage { body = body.clone(); }
        @Override
        public byte[] body() { return body.clone(); }
    }

    private static void compareParsed(Page nativePage, Page replayPage, String nativeEndpoint,
                                      String replayEndpoint, String nativeContainer, String replayContainer,
                                      String nativeRequestMarker, String replayRequestMarker,
                                      boolean checkEcho) {
        equal("native endpoint", nativeEndpoint, nativePage.endpoint());
        equal("replay endpoint", replayEndpoint, replayPage.endpoint());
        equal("native container", nativeContainer, nativePage.container());
        equal("replay container", replayContainer, replayPage.container());
        if (checkEcho) {
            equal("native marker echo", nativeRequestMarker, nativePage.echoes().get("Marker"));
            equal("replay marker echo", replayRequestMarker, replayPage.echoes().get("Marker"));
            for (String field : List.of("Prefix", "MaxResults", "Delimiter")) {
                equal(field + " echo", nativePage.echoes().get(field), replayPage.echoes().get(field));
            }
        }
        equal("entry count", nativePage.entries().size(), replayPage.entries().size());
        for (int i = 0; i < nativePage.entries().size(); i++) {
            Entry expected = nativePage.entries().get(i);
            Entry actual = replayPage.entries().get(i);
            equal("entry kind " + i, expected.kind(), actual.kind());
            equal("entry Name text " + i, expected.name(), actual.name());
            equal("entry Name Encoded attribute " + i, expected.encoded(), actual.encoded());
            if ("Blob".equals(actual.kind())) {
                equal("Content-Length " + i, decimal(expected.properties(), "Content-Length"),
                        decimal(actual.properties(), "Content-Length"));
                equal("Last-Modified " + i, time(expected.properties()), time(actual.properties()));
                replayValue(actual.properties(), "Etag", "0x000000000000001");
                replayValue(actual.properties(), "Content-Type", "application/octet-stream");
                replayValue(actual.properties(), "BlobType", "BlockBlob");
                replayValue(actual.properties(), "AccessTier", "Hot");
                replayValue(actual.properties(), "AccessTierInferred", "true");
                replayValue(actual.properties(), "LeaseStatus", "unlocked");
                replayValue(actual.properties(), "LeaseState", "available");
                if (!Set.of("BlockBlob", "PageBlob", "AppendBlob")
                        .contains(expected.properties().get("BlobType"))) {
                    fail("native BlobType has invalid value");
                }
                if (!Set.of("locked", "unlocked").contains(expected.properties().get("LeaseStatus"))) {
                    fail("native LeaseStatus has invalid value");
                }
                if (expected.properties().containsKey("AccessTierInferred")
                        && !Set.of("true", "false").contains(expected.properties().get("AccessTierInferred"))) {
                    fail("native AccessTierInferred has invalid value");
                }
                for (String synthetic : REPLAY_PROPERTIES) {
                    if (expected.properties().containsKey(synthetic)
                            && expected.properties().get(synthetic).isEmpty()) {
                        fail("native " + synthetic + " has wrong empty value");
                    }
                }
            }
        }
        equal("terminal marker", nativePage.nextMarker().isEmpty(), replayPage.nextMarker().isEmpty());
    }

    private static Page parse(byte[] xml, boolean replay) throws IOException {
        Element root;
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            factory.setXIncludeAware(false);
            factory.setExpandEntityReferences(false);
            root = factory.newDocumentBuilder().parse(new ByteArrayInputStream(xml)).getDocumentElement();
        } catch (ParserConfigurationException | SAXException e) {
            throw new IOException("invalid Azure XML", e);
        }
        if (!"EnumerationResults".equals(root.getTagName())) fail("expected EnumerationResults");
        attributes(root, Set.of("ServiceEndpoint", "ContainerName"));
        String endpoint = attribute(root, "ServiceEndpoint");
        if (!endpoint.endsWith("/")) fail("ServiceEndpoint must have trailing slash");
        String container = attribute(root, "ContainerName");
        List<Element> children = elements(root);
        Map<String, String> echoes = new HashMap<>();
        int cursor = 0;
        for (String name : ECHO_ORDER) {
            if (cursor < children.size() && name.equals(children.get(cursor).getTagName())) {
                echoes.put(name, scalar(children.get(cursor++), name));
            }
        }
        if (cursor >= children.size() || !"Blobs".equals(children.get(cursor).getTagName())) {
            fail("Azure Blobs missing or element order changed");
        }
        Element blobs = children.get(cursor++);
        if (cursor >= children.size() || !"NextMarker".equals(children.get(cursor).getTagName())
                || cursor + 1 != children.size()) {
            fail("Azure NextMarker missing or element order changed");
        }
        String nextMarker = scalar(children.get(cursor), "NextMarker");
        List<Entry> entries = new ArrayList<>();
        for (Element child : elements(blobs)) {
            String kind = child.getTagName();
            if (!"Blob".equals(kind) && !"BlobPrefix".equals(kind)) fail("unknown Azure entry element");
            List<Element> fields = elements(child);
            if (fields.isEmpty() || !"Name".equals(fields.getFirst().getTagName())) {
                fail("Azure Name missing or out of order");
            }
            Element name = fields.getFirst();
            attributes(name, Set.of("Encoded"));
            String encoded = name.hasAttribute("Encoded") ? name.getAttribute("Encoded") : null;
            if (encoded != null && !"true".equals(encoded)) fail("invalid Encoded attribute");
            String exactName = scalar(name, "Name");
            Map<String, String> properties = Map.of();
            if ("BlobPrefix".equals(kind)) {
                if (fields.size() != 1) fail("flat BlobPrefix has unexpected field");
            } else {
                if (fields.size() != 2 || !"Properties".equals(fields.get(1).getTagName())) {
                    fail("Blob Properties missing or out of order");
                }
                properties = properties(fields.get(1), replay);
            }
            entries.add(new Entry(kind, exactName, encoded, properties));
        }
        return new Page(endpoint, container, Map.copyOf(echoes), List.copyOf(entries), nextMarker);
    }

    private static Map<String, String> properties(Element parent, boolean replay) {
        List<Element> children = elements(parent);
        Map<String, String> fields = new HashMap<>();
        int previous = -1;
        for (Element child : children) {
            String name = child.getTagName();
            if (!REPLAY_PROPERTIES.contains(name) && (replay || !NATIVE_ONLY_PROPERTIES.contains(name))) {
                fail("unknown Azure Properties field " + name);
            }
            if (fields.putIfAbsent(name, scalar(child, name)) != null) fail("duplicate Azure Properties field");
            int index = REPLAY_PROPERTIES.indexOf(name);
            if (index >= 0) {
                if (index <= previous) fail("Azure Properties element order changed");
                previous = index;
            }
        }
        if (replay && !fields.keySet().equals(Set.copyOf(REPLAY_PROPERTIES))) {
            fail("Azure replay Properties field set differs");
        }
        return Map.copyOf(fields);
    }

    private static List<Element> elements(Element parent) {
        List<Element> out = new ArrayList<>();
        NodeList nodes = parent.getChildNodes();
        for (int i = 0; i < nodes.getLength(); i++) {
            Node node = nodes.item(i);
            if (node instanceof Element element) out.add(element);
            else if (node.getNodeType() == Node.TEXT_NODE && !node.getTextContent().isBlank()) {
                fail("unexpected Azure XML text");
            }
        }
        return out;
    }

    private static String scalar(Element element, String label) {
        if (element.hasAttributes() && !"Name".equals(label)) fail(label + " has unexpected attribute");
        NodeList nodes = element.getChildNodes();
        StringBuilder value = new StringBuilder();
        for (int i = 0; i < nodes.getLength(); i++) {
            Node node = nodes.item(i);
            if (node.getNodeType() != Node.TEXT_NODE && node.getNodeType() != Node.CDATA_SECTION_NODE) {
                fail(label + " has unexpected child");
            }
            value.append(node.getNodeValue());
        }
        return value.toString();
    }

    private static void attributes(Element element, Set<String> allowed) {
        var attrs = element.getAttributes();
        for (int i = 0; i < attrs.getLength(); i++) {
            if (!allowed.contains(attrs.item(i).getNodeName())) fail("unknown Azure XML attribute");
        }
    }

    private static String attribute(Element element, String name) {
        if (!element.hasAttribute(name)) fail("missing Azure " + name + " attribute");
        return element.getAttribute(name);
    }

    private static String decimal(Map<String, String> fields, String name) {
        String value = fields.get(name);
        if (value == null || !value.matches("0|[1-9][0-9]*")) fail(name + " must be decimal");
        return value;
    }

    private static java.time.Instant time(Map<String, String> fields) {
        String value = fields.get("Last-Modified");
        if (value == null) fail("Last-Modified missing");
        if (!value.matches("[A-Za-z]{3}, [0-9]{2} [A-Za-z]{3} [0-9]{4} "
                + "[0-9]{2}:[0-9]{2}:[0-9]{2} GMT")) {
            fail("Last-Modified must use padded GMT wire format");
        }
        return ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();
    }

    private static void replayValue(Map<String, String> properties, String field, String value) {
        equal("synthetic " + field, value, properties.get(field));
    }

    private static void equal(String label, Object expected, Object actual) {
        if (!java.util.Objects.equals(expected, actual)) {
            fail(label + " differs: expected=" + expected + " actual=" + actual);
        }
    }

    private static void fail(String reason) { throw new IllegalArgumentException(reason); }

    private record Entry(String kind, String name, String encoded, Map<String, String> properties) { }
    private record Page(String endpoint, String container, Map<String, String> echoes,
                        List<Entry> entries, String nextMarker) { }
}

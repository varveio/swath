/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.conformance.provider;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Field-policy comparator for S3 ListObjectsV2 pages, independent of replay code.
 *
 * <p>S3 returns keys that XML 1.0 cannot carry (for example U+0001) when {@code encoding-type}
 * is absent, so bodies are read with a small lenient tokenizer rather than an XML parser: tag
 * names, the root's namespace declaration, the five XML entities and numeric character
 * references. Key text is compared exactly; nothing is trimmed.
 */
public final class S3ProfileComparator {
    private static final String NAMESPACE = "http://s3.amazonaws.com/doc/2006-03-01/";
    private static final Set<String> TOP_LEVEL = Set.of("Name", "Prefix", "Delimiter", "StartAfter",
            "ContinuationToken", "NextContinuationToken", "KeyCount", "MaxKeys", "EncodingType", "IsTruncated",
            "Marker", "NextMarker", "Contents", "CommonPrefixes");
    private static final List<String> EXACT_SCALARS = List.of("Prefix", "Delimiter", "StartAfter", "EncodingType",
            "KeyCount", "MaxKeys", "IsTruncated", "Marker");
    // StartAfter is not a scope field: native S3 echoes it only on the first page of a walk,
    // so each side's per-page StartAfter sequence is compared instead.
    private static final List<String> SCOPE_SCALARS = List.of("Prefix", "Delimiter", "EncodingType", "MaxKeys");
    private static final Set<String> CONTENTS_FIELDS = Set.of("Key", "LastModified", "ETag", "ChecksumAlgorithm",
            "ChecksumType", "Size", "StorageClass", "Owner", "RestoreStatus");
    private static final Set<String> OWNER_FIELDS = Set.of("ID", "DisplayName");
    // Owner values may be synthetic on replay, so only the child sequence is compared. It is
    // compared as a list so duplicated or reordered children do not pass.
    private static final List<String> OWNER_SEQUENCE = List.of("ID", "DisplayName");
    private static final String PROFILE_OMITTED = "RestoreStatus";

    private S3ProfileComparator() {
    }

    /** Compare one deterministic page; continuation tokens are compared per endpoint. */
    public static void assertPage(byte[] nativeBody, byte[] replayBody, String nativeBucket, String replayBucket,
                                  String nativeRequestToken, String replayRequestToken) {
        Page nativePage = page(nativeBody);
        Page replayPage = page(replayBody);
        checkPage(nativePage, "native", nativeBucket, decodedToken(nativeRequestToken));
        checkPage(replayPage, "replay", replayBucket, decodedToken(replayRequestToken));
        equal("top-level element sequence", nativePage.sequence(), replayPage.sequence());
        for (String scalar : EXACT_SCALARS) {
            equal(scalar, nativePage.scalar(scalar), replayPage.scalar(scalar));
        }
        equal("NextContinuationToken presence", nativePage.scalar("NextContinuationToken") != null,
                replayPage.scalar("NextContinuationToken") != null);
        compareContents(nativePage.contents(), replayPage.contents());
        equal("CommonPrefixes", nativePage.prefixes(), replayPage.prefixes());
    }

    /** Compare complete walks; each endpoint follows its own tokens and page boundaries may differ. */
    public static void assertWalk(List<CapturedPage> nativePages, List<CapturedPage> replayPages,
                                  String nativeBucket, String replayBucket, int maxPages, int maxEntries) {
        Walk nativeWalk = aggregate(nativePages, "native", nativeBucket, maxPages, maxEntries);
        Walk replayWalk = aggregate(replayPages, "replay", replayBucket, maxPages, maxEntries);
        for (String scalar : SCOPE_SCALARS) {
            equal("walk " + scalar, nativeWalk.first().scalar(scalar), replayWalk.first().scalar(scalar));
        }
        // Page boundaries may differ, so compare the first page's echo and, when both walks
        // continue, whether continuation pages echo StartAfter at all.
        equal("walk first-page StartAfter", nativeWalk.startAfterByPage().getFirst(),
                replayWalk.startAfterByPage().getFirst());
        if (nativeWalk.startAfterByPage().size() > 1 && replayWalk.startAfterByPage().size() > 1) {
            equal("walk continuation pages echo StartAfter", continuationEchoes(nativeWalk),
                    continuationEchoes(replayWalk));
        }
        compareContents(nativeWalk.contents(), replayWalk.contents());
        equal("walk CommonPrefixes", nativeWalk.prefixes(), replayWalk.prefixes());
    }

    /** The decoded NextContinuationToken of a page, or {@code null} at the end of a walk. */
    public static String nextContinuationToken(byte[] body) {
        return page(body).scalar("NextContinuationToken");
    }

    /** The service error code of an S3 {@code Error} body. */
    public static String errorCode(byte[] body) {
        Node root = parse(body);
        if (!root.name().equals("Error")) fail("S3 error body has root " + root.name());
        String code = null;
        for (Node child : root.children()) {
            if (child.name().equals("Code")) {
                if (code != null) fail("S3 error has duplicate Code");
                code = child.text();
            }
        }
        if (code == null || code.isEmpty()) fail("S3 error lacks Code");
        return code;
    }

    public record CapturedPage(String requestToken, byte[] body) {
        public CapturedPage {
            body = body.clone();
        }

        @Override
        public byte[] body() { return body.clone(); }
    }

    private static Walk aggregate(List<CapturedPage> pages, String side, String bucket, int maxPages,
                                  int maxEntries) {
        if (pages.isEmpty() || pages.size() > maxPages || maxEntries < 0) fail("S3 walk page bound violated");
        List<List<Field>> contents = new ArrayList<>();
        List<String> prefixes = new ArrayList<>();
        Set<String> emitted = new HashSet<>();
        Set<String> seenTokens = new HashSet<>();
        String expectedToken = null;
        Page first = null;
        List<String> startAfterByPage = new ArrayList<>();
        for (int i = 0; i < pages.size(); i++) {
            CapturedPage capture = pages.get(i);
            String requestToken = decodedToken(capture.requestToken());
            equal(side + " request token at page " + i, expectedToken, requestToken);
            Page page = page(capture.body());
            checkPage(page, side, bucket, requestToken);
            if (first == null) first = page;
            startAfterByPage.add(page.scalar("StartAfter"));
            for (String scalar : SCOPE_SCALARS) {
                equal(side + " " + scalar + " across pages", first.scalar(scalar), page.scalar(scalar));
            }
            contents.addAll(page.contents());
            for (String prefix : page.prefixes()) {
                if (!emitted.add(prefix)) fail("cross-page repeated S3 CommonPrefix is unsupported by replay profile");
                prefixes.add(prefix);
            }
            if (contents.size() + prefixes.size() > maxEntries) fail("S3 walk entry bound violated");
            expectedToken = page.scalar("NextContinuationToken");
            if (i + 1 < pages.size() && (expectedToken == null || !seenTokens.add(expectedToken))) {
                fail(side + " S3 walk stopped early or repeated token");
            }
        }
        if (expectedToken != null) fail(side + " S3 walk ended with a nonterminal page");
        return new Walk(first, contents, prefixes, startAfterByPage);
    }

    private static void checkPage(Page page, String side, String bucket, String requestToken) {
        equal(side + " Name", bucket, page.scalar("Name"));
        String truncated = page.scalar("IsTruncated");
        if (!"true".equals(truncated) && !"false".equals(truncated)) fail(side + " IsTruncated is not a boolean");
        boolean next = page.scalar("NextContinuationToken") != null;
        if (next != "true".equals(truncated)) fail(side + " IsTruncated and NextContinuationToken disagree");
        equal(side + " ContinuationToken echo", requestToken == null || requestToken.isEmpty() ? null
                : requestToken, page.scalar("ContinuationToken"));
    }

    private static void compareContents(List<List<Field>> nativeContents, List<List<Field>> replayContents) {
        equal("Contents count", nativeContents.size(), replayContents.size());
        for (int i = 0; i < nativeContents.size(); i++) {
            List<Field> nativeFields = new ArrayList<>(nativeContents.get(i));
            List<Field> replayFields = replayContents.get(i);
            nativeFields.removeIf(field -> field.name().equals(PROFILE_OMITTED));
            for (Field field : replayFields) {
                if (field.name().equals(PROFILE_OMITTED)) fail("replay contains profile-omitted " + PROFILE_OMITTED);
            }
            equal("Contents " + i + " field sequence", names(nativeFields), names(replayFields));
            for (int j = 0; j < nativeFields.size(); j++) {
                Field expected = nativeFields.get(j);
                Field actual = replayFields.get(j);
                String label = "Contents " + i + " " + expected.name();
                switch (expected.name()) {
                    case "LastModified" -> equal(label, instant(expected.text()), instant(actual.text()));
                    case "Owner" -> {
                        equal(label + " children", expected.children(), OWNER_SEQUENCE);
                        equal(label + " replay children", actual.children(), OWNER_SEQUENCE);
                    }
                    default -> equal(label, expected.text(), actual.text());
                }
            }
        }
    }

    private static List<String> names(List<Field> fields) {
        return fields.stream().map(Field::name).toList();
    }

    private static Instant instant(String value) {
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("LastModified must be an RFC 3339 instant", e);
        }
    }

    private static Page page(byte[] body) {
        Node root = parse(body);
        if (!root.name().equals("ListBucketResult")) fail("S3 listing root is " + root.name());
        if (!NAMESPACE.equals(root.namespace())) fail("S3 listing namespace differs");
        List<String> sequence = new ArrayList<>();
        java.util.LinkedHashMap<String, String> scalars = new java.util.LinkedHashMap<>();
        List<List<Field>> contents = new ArrayList<>();
        List<String> prefixes = new ArrayList<>();
        for (Node child : root.children()) {
            if (!TOP_LEVEL.contains(child.name())) fail("unknown S3 listing element " + child.name());
            sequence.add(child.name());
            switch (child.name()) {
                case "Contents" -> contents.add(contentsFields(child));
                case "CommonPrefixes" -> {
                    if (child.children().size() != 1 || !child.children().getFirst().name().equals("Prefix")) {
                        fail("CommonPrefixes must hold exactly one Prefix");
                    }
                    prefixes.add(child.children().getFirst().text());
                }
                default -> {
                    if (!child.children().isEmpty()) fail(child.name() + " must be a text element");
                    if (scalars.putIfAbsent(child.name(), child.text()) != null) {
                        fail("duplicate S3 listing element " + child.name());
                    }
                }
            }
        }
        return new Page(List.copyOf(sequence), Map.copyOf(scalars), contents, prefixes);
    }

    private static List<Field> contentsFields(Node contents) {
        List<Field> fields = new ArrayList<>();
        for (Node field : contents.children()) {
            if (!CONTENTS_FIELDS.contains(field.name())) fail("unknown S3 Contents element " + field.name());
            List<String> children = new ArrayList<>();
            for (Node child : field.children()) {
                if (!field.name().equals("Owner") && !field.name().equals(PROFILE_OMITTED)) {
                    fail(field.name() + " must be a text element");
                }
                if (field.name().equals("Owner") && !OWNER_FIELDS.contains(child.name())) {
                    fail("unknown S3 Owner element " + child.name());
                }
                children.add(child.name());
            }
            fields.add(new Field(field.name(), field.text(), List.copyOf(children)));
        }
        return fields;
    }

    // Lenient tokenizer --------------------------------------------------------------

    private static Node parse(byte[] body) {
        String text;
        try {
            text = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(body)).toString();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("S3 body is not valid UTF-8", e);
        }
        Cursor cursor = new Cursor(text);
        if (cursor.text.startsWith("﻿")) cursor.position++;
        cursor.skipWhitespace();
        if (cursor.text.startsWith("<?xml", cursor.position)) {
            int end = cursor.text.indexOf("?>", cursor.position);
            if (end < 0) fail("S3 XML declaration is incomplete");
            cursor.position = end + 2;
        }
        cursor.skipWhitespace();
        Node root = element(cursor, true);
        cursor.skipWhitespace();
        if (cursor.position != cursor.text.length()) fail("trailing content after the S3 root element");
        return root;
    }

    private static Node element(Cursor cursor, boolean root) {
        if (!cursor.text.startsWith("<", cursor.position) || cursor.text.startsWith("</", cursor.position)) {
            fail("expected an S3 element start tag");
        }
        int close = cursor.text.indexOf('>', cursor.position);
        if (close < 0) fail("unterminated S3 start tag");
        String tag = cursor.text.substring(cursor.position + 1, close);
        cursor.position = close + 1;
        boolean empty = tag.endsWith("/");
        if (empty) tag = tag.substring(0, tag.length() - 1);
        String[] parts = tag.trim().split("\\s+", 2);
        String name = parts[0];
        if (!name.matches("[A-Za-z][A-Za-z0-9]*")) fail("unsupported S3 element name " + name);
        String namespace = null;
        if (parts.length == 2) {
            var match = java.util.regex.Pattern.compile("xmlns=\"([^\"]*)\"").matcher(parts[1].trim());
            if (!root || !match.matches()) fail("unexpected S3 attribute on " + name);
            namespace = match.group(1);
        }
        if (empty) return new Node(name, namespace, "", List.of());
        List<Node> children = new ArrayList<>();
        StringBuilder raw = new StringBuilder();
        while (true) {
            int next = cursor.text.indexOf('<', cursor.position);
            if (next < 0) fail("unterminated S3 element " + name);
            raw.append(cursor.text, cursor.position, next);
            cursor.position = next;
            if (cursor.text.startsWith("</", next)) {
                int end = cursor.text.indexOf('>', next);
                if (end < 0 || !cursor.text.substring(next + 2, end).trim().equals(name)) {
                    fail("mismatched S3 end tag for " + name);
                }
                cursor.position = end + 1;
                break;
            }
            children.add(element(cursor, false));
        }
        String text = decodeEntities(raw.toString());
        if (!children.isEmpty() && !text.isBlank()) fail("mixed text in S3 element " + name);
        return new Node(name, namespace, children.isEmpty() ? text : "", List.copyOf(children));
    }

    private static String decodeEntities(String raw) {
        StringBuilder out = new StringBuilder(raw.length());
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (c != '&') {
                out.append(c);
                continue;
            }
            int end = raw.indexOf(';', i);
            if (end < 0) fail("unterminated S3 entity");
            String entity = raw.substring(i + 1, end);
            switch (entity) {
                case "lt" -> out.append('<');
                case "gt" -> out.append('>');
                case "amp" -> out.append('&');
                case "quot" -> out.append('"');
                case "apos" -> out.append('\'');
                default -> {
                    int codePoint;
                    try {
                        codePoint = entity.startsWith("#x") ? Integer.parseInt(entity.substring(2), 16)
                                : entity.startsWith("#") ? Integer.parseInt(entity.substring(1)) : -1;
                    } catch (NumberFormatException e) {
                        codePoint = -1;
                    }
                    if (codePoint < 0 || !Character.isValidCodePoint(codePoint)) fail("unknown S3 entity " + entity);
                    out.appendCodePoint(codePoint);
                }
            }
            i = end;
        }
        return out.toString();
    }

    private static String decodedToken(String value) {
        return value == null ? null : percentDecode(value);
    }

    private static String percentDecode(String value) {
        if (value.indexOf('%') < 0) return value;
        byte[] out = new byte[value.length()];
        int length = 0;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '%') {
                if (i + 2 >= value.length()) fail("malformed percent escape in continuation token");
                try {
                    out[length++] = (byte) Integer.parseInt(value.substring(i + 1, i + 3), 16);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("malformed percent escape in continuation token", e);
                }
                i += 2;
            } else {
                byte[] bytes = String.valueOf(c).getBytes(StandardCharsets.UTF_8);
                System.arraycopy(bytes, 0, out, length, bytes.length);
                length += bytes.length;
            }
        }
        return new String(out, 0, length, StandardCharsets.UTF_8);
    }

    private static void equal(String label, Object expected, Object actual) {
        if (!java.util.Objects.equals(expected, actual)) {
            fail(label + " differs: expected=" + expected + " actual=" + actual);
        }
    }

    private static void fail(String message) { throw new IllegalArgumentException(message); }

    private static final class Cursor {
        private final String text;
        private int position;

        Cursor(String text) { this.text = text; }

        void skipWhitespace() {
            while (position < text.length() && Character.isWhitespace(text.charAt(position))) position++;
        }
    }

    private record Node(String name, String namespace, String text, List<Node> children) { }

    private record Field(String name, String text, List<String> children) { }

    private record Page(List<String> sequence, Map<String, String> scalars, List<List<Field>> contents,
                        List<String> prefixes) {
        String scalar(String name) { return scalars.get(name); }
    }

    private static java.util.Set<String> continuationEchoes(Walk walk) {
        return new java.util.HashSet<>(walk.startAfterByPage().subList(1, walk.startAfterByPage().size()));
    }

    private record Walk(Page first, List<List<Field>> contents, List<String> prefixes,
                        List<String> startAfterByPage) { }
}

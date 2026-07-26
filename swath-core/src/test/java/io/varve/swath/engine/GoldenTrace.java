/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageRequest;
import io.varve.swath.testkit.MockPageFetcher;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * The B1 decision-trace golden recorder's shared plumbing: canonical JSON event building
 * (byte-valued fields hex-encoded, fixed field order), a {@link MockPageFetcher.PageInterceptor}
 * that logs every probe request/response a decision-site call issues (in call order — this is
 * what makes the pivot cascade's probe verdicts part of the golden), a {@link RunMetrics}
 * before/after diff (the already-instrumented {@code recordStealReason} category/reason
 * engagement counters — §5 — read back as the cascade-branch signal), and the
 * update-or-verify golden-file ergonomics ({@code -Dswath.goldens.update=true}, mirroring
 * {@code HelpUsageGoldenTest}).
 *
 * <p>Zero production behavior change: every seam used here already exists for tests
 * (MockPageFetcher's interceptor, {@link RunMetrics#diagnostics}/{@link RunMetrics#summary}, and
 * the production {@link io.varve.swath.observability.TraceSink} interface implemented by {@link
 * RecordingTraceSink}). See {@code docs/ops/dev/decision-trace-goldens.md}.
 */
final class GoldenTrace {

    static final boolean UPDATE = Boolean.getBoolean("swath.goldens.update");

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HexFormat HEX = HexFormat.of();
    private static final Path GOLDEN_DIR = Path.of("src/test/resources/goldens/decision-trace");
    private static final String RESOURCE_ROOT = "/goldens/decision-trace/";

    private GoldenTrace() {
    }

    // ---- JSON node building (fixed field order, hex-encoded byte fields) ---------------------

    static ObjectNode newNode() {
        return MAPPER.createObjectNode();
    }

    static ArrayNode newArray() {
        return MAPPER.createArrayNode();
    }

    /** {@code site, fixture, seq} — the envelope every decision event shares, in that field order. */
    static ObjectNode newEvent(String site, String fixture, int seq) {
        ObjectNode e = newNode();
        e.put("site", site);
        e.put("fixture", fixture);
        e.put("seq", seq);
        return e;
    }

    /** Hex-encodes a key-valued byte field; {@code null} (⊥/open-frontier) renders as a JSON null. */
    static void putHex(ObjectNode node, String field, byte[] raw) {
        if (raw == null) {
            node.putNull(field);
        } else {
            node.put(field, HEX.formatHex(raw));
        }
    }

    // ---- probe log: every MockPageFetcher call issued during one decision-site invocation -----

    /** Ordered log of {@code (request, response)} pairs a single decision-site call issued. */
    static final class ProbeLog {
        private final List<ObjectNode> calls = new ArrayList<>();

        List<ObjectNode> calls() {
            return calls;
        }

        void clear() {
            calls.clear();
        }

        /** A {@link MockPageFetcher.PageInterceptor} that logs, unchanged, every call it sees. */
        MockPageFetcher.PageInterceptor interceptor() {
            return (PageRequest req, int callIndex, ListPage computed) -> {
                log(req, computed);
                return computed;
            };
        }

        /** Logs one call directly — for a fixture that composes its own interceptor (e.g. to also
         *  inject a mid-call race) alongside this log. */
        void log(PageRequest req, ListPage page) {
            calls.add(callEvent(req, page));
        }

        private static ObjectNode callEvent(PageRequest req, ListPage page) {
            ObjectNode e = newNode();
            ObjectNode request = newNode();
            request.put("max_keys", req.maxKeys());
            putHex(request, "prefix", req.prefix());
            putHex(request, "delimiter", req.delimiter());
            putHex(request, "start_after", req.startAfter());
            e.set("request", request);
            ObjectNode response = newNode();
            ArrayNode entries = response.putArray("entries");
            for (ListEntry entry : page.entries()) {
                entries.add(HEX.formatHex(entry.key().rawUnsafe()));
            }
            ArrayNode commonPrefixes = response.putArray("common_prefixes");
            for (KeyBytes cp : page.commonPrefixes()) {
                commonPrefixes.add(HEX.formatHex(cp.rawUnsafe()));
            }
            response.put("truncated", page.truncated());
            e.set("response", response);
            return e;
        }
    }

    // ---- reason-deltas: the RunMetrics engagement-counter diff over one invocation -----------

    /** A {@code Map<"outcome.reason", count>} snapshot of {@link RunMetrics#diagnostics}. */
    static Map<String, Long> snapshotReasons(RunMetrics metrics) {
        return metrics.diagnostics(Duration.ZERO).stealReasons();
    }

    /** The counters that changed between two snapshots, key-sorted for determinism. */
    static SortedMap<String, Long> reasonDeltas(Map<String, Long> before, Map<String, Long> after) {
        SortedMap<String, Long> deltas = new TreeMap<>();
        for (Map.Entry<String, Long> e : after.entrySet()) {
            long delta = e.getValue() - before.getOrDefault(e.getKey(), 0L);
            if (delta != 0L) {
                deltas.put(e.getKey(), delta);
            }
        }
        return deltas;
    }

    static ObjectNode reasonDeltasNode(Map<String, Long> before, Map<String, Long> after) {
        ObjectNode node = newNode();
        for (Map.Entry<String, Long> e : reasonDeltas(before, after).entrySet()) {
            node.put(e.getKey(), e.getValue());
        }
        return node;
    }

    // ---- fixture file collection: one JSONL golden per fixture --------------------------------

    /** Accumulates one fixture's ordered events and writes/verifies them as one golden JSONL file. */
    static final class Fixture {
        private final String name;
        private final List<String> lines = new ArrayList<>();

        Fixture(String name) {
            this.name = name;
        }

        String name() {
            return name;
        }

        int nextSeq() {
            return lines.size();
        }

        void record(ObjectNode event) {
            try {
                lines.add(MAPPER.writeValueAsString(event));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        List<String> lines() {
            return lines;
        }
    }

    /** Regenerates ({@code -Dswath.goldens.update=true}) or asserts {@code fixture} against its committed golden. */
    static void writeOrVerify(Fixture fixture) {
        if (UPDATE) {
            writeGolden(fixture);
            return;
        }
        List<String> golden = readGolden(fixture.name());
        assertThat(fixture.lines())
                .as("decision-trace golden drift for fixture '%s' (regenerate intentionally with "
                        + "-Dswath.goldens.update=true, then review the diff)", fixture.name())
                .containsExactlyElementsOf(golden);
    }

    private static void writeGolden(Fixture fixture) {
        try {
            Files.createDirectories(GOLDEN_DIR);
            Path path = GOLDEN_DIR.resolve(fixture.name() + ".jsonl");
            String content = String.join("\n", fixture.lines());
            Files.writeString(path, fixture.lines().isEmpty() ? "" : content + "\n", StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static List<String> readGolden(String fixtureName) {
        String resource = RESOURCE_ROOT + fixtureName + ".jsonl";
        try (InputStream in = GoldenTrace.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("missing golden " + resource
                        + " (generate with -Dswath.goldens.update=true)");
            }
            String content = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            if (content.isEmpty()) {
                return List.of();
            }
            return Arrays.stream(content.split("\n", -1))
                    .filter(l -> !l.isEmpty())
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

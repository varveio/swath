/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Guards the canonical public run records and the prose that quotes them.
 *
 * <p>Two different captures of {@code s3://noaa-gestofs-pds/} are published: the README terminal
 * recording and the interactive trace. They were once presented as one run, with figures from each
 * attributed to the other. These checks keep every capture identified by its own run id, keep
 * unknown provenance an explicit {@code null} rather than a missing or guessed field, keep the two
 * records one interchangeable shape, and keep every headline number in the prose equal to the
 * record it came from.
 */
final class PublicRunFactsTest {

    private static final Path ROOT = Path.of("..").toAbsolutePath().normalize();
    private static final Path RUNS = ROOT.resolve("site/data/runs");

    private static final String RECORDING = "noaa-gestofs-pds-2026-08-03-505ae26";
    private static final String TRACE = "noaa-gestofs-pds-field-guide-trace";

    /** Objects whose key sets must be identical across every record, so one reader fits all. */
    private static final List<String> SHARED_SHAPES =
            List.of("", "/swath", "/captured_at_bounds", "/target", "/client", "/command",
                    "/output", "/config", "/clocks", "/clocks/listing_wall",
                    "/clocks/invocation_wall", "/clocks/trace_event_span",
                    "/clocks/video_playback", "/result", "/artifacts");

    /** Provenance this recording does not establish. Absent is not good enough; it must be null. */
    private static final List<String> RECORDING_UNKNOWNS = List.of(
            "/target/region",
            "/client/provider", "/client/region", "/client/machine_type", "/client/cpu",
            "/client/memory_bytes", "/client/published_description",
            "/result/api_calls", "/result/api_calls_per_1k_objects", "/result/committed_pages",
            "/result/pages_per_1k_objects", "/result/initial_ranges",
            "/result/empty_initial_ranges", "/result/splits", "/result/owner_splits",
            "/result/uniform_pivot_splits", "/result/claimed_ranges", "/result/completed_ranges",
            "/result/failed_ranges", "/result/listing_workers_observed",
            "/result/peak_live_ranges", "/result/heaviest_initial_range",
            "/result/median_initial_range_objects",
            "/result/parquet_parts", "/result/parquet_bytes", "/result/peak_rss_bytes",
            "/analysis", "/trace_model",
            "/artifacts/summary", "/artifacts/raw_trace", "/artifacts/interactive_trace",
            "/clocks/listing_wall/ms", "/clocks/listing_wall/display",
            "/clocks/invocation_wall/ms", "/clocks/invocation_wall/display",
            "/clocks/trace_event_span/ms", "/clocks/trace_event_span/display",
            "/clocks/video_playback/ms", "/clocks/video_playback/display");

    /** Provenance the trace does not establish. */
    private static final List<String> TRACE_UNKNOWNS = List.of(
            "/swath/version", "/swath/commit", "/swath/runtime",
            "/captured_at", "/captured_at_precision", "/captured_at_bounds/earliest",
            "/target/region",
            "/client/provider", "/client/region", "/client/machine_type", "/client/cpu",
            "/client/memory_bytes",
            "/command/list", "/command/resume",
            "/output/format", "/output/destination_kind", "/output/destination_kind_source",
            "/output/sorted", "/output/sorted_source",
            "/config/region",
            "/result/api_calls", "/result/api_calls_per_1k_objects", "/result/parquet_parts",
            "/result/parquet_bytes", "/result/peak_rss_bytes",
            "/result/listed_object_bytes_display", "/result/earliest_object_last_modified",
            "/result/latest_object_last_modified",
            "/invocations",
            "/artifacts/summary", "/artifacts/raw_trace",
            "/clocks/listing_wall/ms", "/clocks/listing_wall/display",
            "/clocks/invocation_wall/ms", "/clocks/invocation_wall/display",
            "/clocks/video_playback/ms");

    /** Fields a consumer will do arithmetic on: never a string that merely looks numeric. */
    private static final List<String> NUMERIC = List.of(
            "/result/objects", "/result/api_calls", "/result/committed_pages",
            "/result/initial_ranges", "/result/splits", "/result/claimed_ranges",
            "/result/completed_ranges", "/result/failed_ranges",
            "/result/listing_workers_observed", "/result/heaviest_initial_range/objects",
            "/result/heaviest_initial_range/share_percent",
            "/config/concurrency_ceiling",
            "/clocks/listing_wall/ms", "/clocks/invocation_wall/ms",
            "/clocks/trace_event_span/ms", "/clocks/video_playback/ms");

    /** The image link that must never point at the other capture's trace page again. */
    private static final Pattern RECORDING_IMAGE_LINK = Pattern.compile(
            "\\[!\\[[^\\]]*]\\(docs/assets/swath-demo-v0\\.2\\.1\\.gif\\)]\\(([^)\\s]+)\\)");

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void everyRecordIsIdentifiedByItsOwnRunId() throws Exception {
        List<Path> records = records();
        List<String> ids = new ArrayList<>();
        for (Path record : records) {
            JsonNode facts = MAPPER.readTree(record.toFile());
            String stem = stem(record);
            assertThat(facts.path("schema_version").asText())
                    .as(stem + " schema version")
                    .isEqualTo("swath-public-run-v1");
            assertThat(facts.path("run_id").asText())
                    .as(stem + " run id matches its file name")
                    .isEqualTo(stem);
            assertThat(facts.path("notes").isArray()).as(stem + " notes").isTrue();
            assertThat(facts.path("evidence").isArray()).as(stem + " evidence").isTrue();
            facts.path("clocks").properties().forEach(clock ->
                    assertThat(clock.getValue().path("measures").asText(""))
                            .as(stem + " clock " + clock.getKey() + " says what it measures")
                            .isNotBlank());
            ids.add(stem);
        }
        assertThat(ids).contains(RECORDING, TRACE);
    }

    @Test
    void everyRecordHasTheSameShape() throws Exception {
        JsonNode reference = read(RECORDING);
        for (Path record : records()) {
            JsonNode facts = MAPPER.readTree(record.toFile());
            for (String pointer : SHARED_SHAPES) {
                assertThat(keys(facts.at(pointer)))
                        .as(stem(record) + " keys at '" + pointer + "'")
                        .isEqualTo(keys(reference.at(pointer)));
            }
        }
    }

    @Test
    void unknownProvenanceStaysExplicitlyNull() throws Exception {
        assertExplicitNulls(read(TRACE), TRACE, TRACE_UNKNOWNS);
        assertExplicitNulls(read(RECORDING), RECORDING, RECORDING_UNKNOWNS);

        JsonNode recording = read(RECORDING);
        assertThat(recording.at("/swath/version").asText()).isEqualTo("0.2.1");
        assertThat(recording.at("/swath/commit").asText()).isEqualTo("505ae26e6019");
        assertThat(recording.at("/command/list").asText()).contains("s3://noaa-gestofs-pds/");

        for (Path record : records()) {
            JsonNode facts = MAPPER.readTree(record.toFile());
            for (String pointer : NUMERIC) {
                JsonNode value = facts.at(pointer);
                String parent = pointer.substring(0, pointer.lastIndexOf('/'));
                if (value.isMissingNode() && facts.at(parent).isNull()) {
                    continue; // the whole block is one explicit null
                }
                assertThat(value.isNull() || value.isNumber())
                        .as(stem(record) + " " + pointer + " is a number or an explicit null")
                        .isTrue();
            }
        }
    }

    @Test
    void everyReferencedArtifactCarriesADigest() throws Exception {
        for (Path record : records()) {
            JsonNode artifacts = read(stem(record)).at("/artifacts");
            List<JsonNode> referenced = new ArrayList<>();
            artifacts.path("video").forEach(referenced::add);
            if (artifacts.path("interactive_trace").isObject()) {
                referenced.add(artifacts.path("interactive_trace"));
            }
            for (JsonNode artifact : referenced) {
                String where = stem(record) + " artifact " + artifact.path("source").asText("?");
                assertThat(artifact.path("sha256").asText(""))
                        .as(where + " digest")
                        .matches("[0-9a-f]{64}");
                assertThat(artifact.path("source").asText("")).as(where + " origin").isNotBlank();
                if (artifact.path("path").isNull() || artifact.path("path").isMissingNode()) {
                    continue;
                }
                Path asset = ROOT.resolve(artifact.path("path").asText());
                assertThat(asset).as(where + " file").isRegularFile();
                assertThat(sha256(asset)).as(where + " still hashes to its recorded digest")
                        .isEqualTo(artifact.path("sha256").asText());
            }
        }
    }

    @Test
    void eachCaptureQuotesOnlyItsOwnFactsWhereItIsDescribed() throws Exception {
        JsonNode recording = read(RECORDING);
        JsonNode trace = read(TRACE);
        JsonNode resume = recording.at("/invocations/1");
        JsonNode initial = recording.at("/invocations/0");

        List<String> recordingFacts = List.of(
                RECORDING,
                recording.at("/swath/version").asText(),
                recording.at("/swath/commit").asText(),
                recording.at("/captured_at").asText(),
                grouped(recording.at("/result/objects").asLong()),
                String.valueOf(recording.at("/config/concurrency_ceiling").asInt()),
                grouped(resume.path("api_calls").asLong()),
                String.valueOf(resume.path("parquet_parts").asInt()),
                resume.path("parquet_bytes_display").asText(),
                resume.path("peak_rss_display").asText(),
                resume.path("elapsed_display").asText());

        // Figures that belong to the trace alone. Attributing any of them to the recording is the
        // exact defect this record set exists to prevent, so they must not appear where the
        // recording is described.
        List<String> traceOnlyFacts = List.of(
                grouped(trace.at("/result/objects").asLong()),
                grouped(trace.at("/result/committed_pages").asLong()),
                grouped(trace.at("/result/initial_ranges").asLong()),
                grouped(trace.at("/result/splits").asLong()),
                grouped(trace.at("/result/completed_ranges").asLong()),
                decimal(trace.at("/result/heaviest_initial_range/share_percent").asDouble()) + "%",
                grouped(trace.at("/clocks/trace_event_span/ms").asDouble()));

        String readme = text("README.md");
        String demo = text("docs/full-scale-demo.md");
        String readmeSection = between(readme, "## See it at full scale", "\n## ");
        String readmeRecording = between(readmeSection, "*Run `", "The [interactive run trace]");
        String readmeTrace = from(readmeSection, "The [interactive run trace]");
        String demoRecording = between(demo, "## What the recording shows",
                "## The interactive trace is a separate capture");
        String demoTrace = between(demo, "## The interactive trace is a separate capture",
                "## Before you run it");

        quotes(readmeRecording, "README recording caption", recordingFacts);
        quotes(demoRecording, "full-scale-demo recording section", recordingFacts);
        quotes(demoRecording, "full-scale-demo recording section", List.of(
                grouped(initial.path("objects").asLong()),
                grouped(initial.path("api_calls").asLong())));
        absent(readmeRecording, "README recording caption", traceOnlyFacts);
        absent(demoRecording, "full-scale-demo recording section", traceOnlyFacts);

        quotes(readmeTrace, "README trace paragraph", List.of(TRACE, "separate capture",
                grouped(trace.at("/result/objects").asLong())));
        quotes(demoTrace, "full-scale-demo trace section", traceOnlyFacts);
        quotes(demoTrace, "full-scale-demo trace section", List.of(TRACE));
        quotes(demo, "docs/full-scale-demo.md", List.of("separate capture"));

        // The difference between the two captures is the whole point; keep it stated.
        long gap = trace.at("/result/objects").asLong() - recording.at("/result/objects").asLong();
        assertThat(gap).isNotZero();
        quotes(readmeTrace, "README trace paragraph", List.of(grouped(gap)));
        quotes(demoTrace, "full-scale-demo trace section", List.of(grouped(gap)));
    }

    @Test
    void noRecordingImageLinksToTheOtherCapture() throws Exception {
        String readme = text("README.md");
        Matcher link = RECORDING_IMAGE_LINK.matcher(readme);
        int found = 0;
        while (link.find()) {
            found++;
            assertThat(link.group(1))
                    .as("README image link " + found + " destination")
                    .isEqualTo("docs/full-scale-demo.md");
        }
        assertThat(found).as("README embeds the recording as an inline linked image").isOne();
        assertThat(readme.split("swath-demo-v0\\.2\\.1\\.gif", -1))
                .as("the recording image appears exactly once")
                .hasSize(2);
    }

    private static String between(String page, String start, String end) {
        int from = page.indexOf(start);
        assertThat(from).as("section start '" + start + "'").isNotNegative();
        int to = page.indexOf(end, from + start.length());
        return to < 0 ? page.substring(from) : page.substring(from, to);
    }

    private static String from(String page, String start) {
        int at = page.indexOf(start);
        assertThat(at).as("section start '" + start + "'").isNotNegative();
        return page.substring(at);
    }

    private static void quotes(String page, String name, List<String> facts) {
        for (String fact : facts) {
            assertThat(page).as(name + " must quote '" + fact + "' from the run records")
                    .contains(fact);
        }
    }

    private static void absent(String page, String name, List<String> facts) {
        for (String fact : facts) {
            assertThat(page).as(name + " must not carry '" + fact + "', which belongs to the "
                    + "other capture").doesNotContain(fact);
        }
    }

    private static void assertExplicitNulls(JsonNode facts, String runId, List<String> pointers) {
        for (String pointer : pointers) {
            assertThat(facts.at(pointer).isNull())
                    .as(runId + " must record " + pointer + " as an explicit null")
                    .isTrue();
        }
    }

    private static Set<String> keys(JsonNode node) {
        Set<String> names = new LinkedHashSet<>();
        for (Map.Entry<String, JsonNode> field : node.properties()) {
            names.add(field.getKey());
        }
        return names;
    }

    private static List<Path> records() throws IOException {
        try (Stream<Path> files = Files.list(RUNS)) {
            return files.filter(p -> p.toString().endsWith(".json")).sorted().toList();
        }
    }

    private static String stem(Path record) {
        return record.getFileName().toString().replace(".json", "");
    }

    private static String grouped(long value) {
        return String.format(Locale.ROOT, "%,d", value);
    }

    private static String grouped(double value) {
        return String.format(Locale.ROOT, "%,.1f", value);
    }

    private static String decimal(double value) {
        return String.format(Locale.ROOT, "%.1f", value);
    }

    private static String sha256(Path file) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        return HexFormat.of().formatHex(digest.digest(Files.readAllBytes(file)));
    }

    private static JsonNode read(String runId) throws IOException {
        return MAPPER.readTree(RUNS.resolve(runId + ".json").toFile());
    }

    private static String text(String relative) throws IOException {
        return Files.readString(ROOT.resolve(relative));
    }
}

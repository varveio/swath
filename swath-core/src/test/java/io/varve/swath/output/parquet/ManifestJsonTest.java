/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.assertj.core.api.AbstractStringAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The JSON <b>shape</b> contract of the two artifacts {@link Manifest} writes — the consumer
 * {@code manifest.json} and the internal {@code .swath-state.json}. These are public output-surface
 * files (contracts.md §4.1): the field set, the field ORDER, the null vs. absent distinction, and
 * the value types are the contract, so they are asserted on the parsed tree rather than on bytes.
 */
class ManifestJsonTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String SCHEMA = "message swath { required binary key (STRING); }";

    @Test
    void sortedManifestCarriesEveryFieldInOrder(@TempDir Path dir) throws IOException {
        List<PartInfo> parts = List.of(
                new PartInfo("data/part-00000.parquet", 0, 1234L, 98765L,
                        "d41d8cd98f00b204e9800998ecf8427e", "a/first", "m/last"),
                new PartInfo("data/part-00001.parquet", 1, 7L, 42L,
                        "0f1e2d3c4b5a69788796a5b4c3d2e1f0", "n/first", "z/last"));

        Manifest.write(dir, "my-bucket", SCHEMA, parts, true, "key ASC");

        JsonNode root = read(dir, Manifest.FILE_NAME);
        assertThat(fieldNames(root)).containsExactly("sourceBucket", "version", "creationTimestamp",
                "fileFormat", "fileSchema", "sorted", "sortKey", "files");
        // Types are asserted with isTextual()/isIntegralNumber(), never bare asText()/asLong():
        // asText() happily renders a number and asLong() happily parses a numeric string, so the
        // lenient accessors alone would not catch a field silently changing JSON type.
        assertThatString(root, "sourceBucket").isEqualTo("my-bucket");
        assertThatString(root, "version")
                .as("version is the STRING \"1\" (S3-Inventory style), not the number 1").isEqualTo("1");
        assertThat(root.get("creationTimestamp").isIntegralNumber())
                .as("creationTimestamp is epoch millis as a JSON integer, never a string").isTrue();
        assertThat(root.get("creationTimestamp").asLong()).isPositive();
        assertThatString(root, "fileFormat").isEqualTo("Parquet");
        assertThatString(root, "fileSchema").isEqualTo(SCHEMA);
        assertThat(root.get("sorted").isBoolean()).isTrue();
        assertThat(root.get("sorted").asBoolean()).isTrue();
        assertThatString(root, "sortKey").isEqualTo("key ASC");

        JsonNode files = root.get("files");
        assertThat(files.isArray()).isTrue();
        assertThat(files).hasSize(2);
        JsonNode first = files.get(0);
        assertThat(fieldNames(first))
                .containsExactly("key", "size", "MD5checksum", "rowCount", "minKey", "maxKey");
        assertThatString(first, "key").isEqualTo("data/part-00000.parquet");
        assertThat(first.get("size").isIntegralNumber())
                .as("size is a JSON number, never a stringified long").isTrue();
        assertThat(first.get("size").asLong()).isEqualTo(98765L);
        assertThatString(first, "MD5checksum").isEqualTo("d41d8cd98f00b204e9800998ecf8427e");
        assertThat(first.get("rowCount").isIntegralNumber())
                .as("rowCount is a JSON number, never a stringified long").isTrue();
        assertThat(first.get("rowCount").asLong()).isEqualTo(1234L);
        assertThatString(first, "minKey").isEqualTo("a/first");
        assertThatString(first, "maxKey").isEqualTo("m/last");
        assertThatString(files.get(1), "key")
                .as("files[] preserves the caller's part order").isEqualTo("data/part-00001.parquet");
    }

    /**
     * {@code minKey} and {@code maxKey} are emitted by two INDEPENDENT null checks, so a part
     * carrying only one of them must emit only that one. Fixtures that always set both or neither
     * would still pass if the two conditions were accidentally coupled into a single branch.
     */
    @Test
    void keyRangeFieldsArePresentIndependently(@TempDir Path dir) throws IOException {
        Manifest.write(dir, "b", SCHEMA, List.of(
                new PartInfo("data/only-min.parquet", 0, 1L, 2L, "", "a/first", null),
                new PartInfo("data/only-max.parquet", 1, 1L, 2L, "", null, "z/last")), true, "key ASC");

        JsonNode files = read(dir, Manifest.FILE_NAME).get("files");
        assertThat(fieldNames(files.get(0)))
                .as("a part with only minKey emits minKey and NOT maxKey")
                .containsExactly("key", "size", "MD5checksum", "rowCount", "minKey");
        assertThat(fieldNames(files.get(1)))
                .as("a part with only maxKey emits maxKey and NOT minKey")
                .containsExactly("key", "size", "MD5checksum", "rowCount", "maxKey");
    }

    @Test
    void unsortedManifestHasNullSortKeyAndNoKeyRange(@TempDir Path dir) throws IOException {
        Manifest.write(dir, "b", SCHEMA, List.of(new PartInfo("data/part-w0-00000.parquet", 0, 3L, 11L, null)),
                false, null);

        JsonNode root = read(dir, Manifest.FILE_NAME);
        assertThat(root.has("sortKey"))
                .as("sortKey is an explicit JSON null for an unsorted publish, never a missing key").isTrue();
        assertThat(root.get("sortKey").isNull()).isTrue();
        assertThat(root.get("sorted").asBoolean()).isFalse();

        JsonNode file = root.get("files").get(0);
        assertThat(fieldNames(file))
                .as("minKey/maxKey are omitted entirely for an unsorted part")
                .containsExactly("key", "size", "MD5checksum", "rowCount");
        assertThat(file.get("MD5checksum").asText())
                .as("an absent checksum is the empty string, not null").isEmpty();
    }

    @Test
    void emptyPartListStillWritesAFilesArray(@TempDir Path dir) throws IOException {
        Manifest.write(dir, "b", SCHEMA, List.of(), false, null);

        JsonNode files = read(dir, Manifest.FILE_NAME).get("files");
        assertThat(files.isArray()).isTrue();
        assertThat(files).isEmpty();
    }

    @Test
    void keysWithJsonMetacharactersRoundTrip(@TempDir Path dir) throws IOException {
        String nasty = "a/\"quoted\"\\back\nnewline\ttab\u0001ctl/\u00e9/\ud83d\udce6";
        Manifest.write(dir, nasty, SCHEMA, List.of(
                new PartInfo("data/part-00000.parquet", 0, 1L, 2L, "", nasty, nasty)), true, nasty);

        JsonNode root = read(dir, Manifest.FILE_NAME);
        assertThat(root.get("sourceBucket").asText()).isEqualTo(nasty);
        assertThat(root.get("sortKey").asText()).isEqualTo(nasty);
        assertThat(root.get("files").get(0).get("minKey").asText()).isEqualTo(nasty);
        assertThat(root.get("files").get(0).get("maxKey").asText()).isEqualTo(nasty);
    }

    @Test
    void stateFileCarriesIdentityAndRoundTripsThroughReadIdentity(@TempDir Path dir) throws IOException {
        Manifest.writeState(dir, "argshash123", 42L);

        JsonNode root = read(dir, Manifest.STATE_FILE_NAME);
        assertThat(fieldNames(root)).containsExactly("args_hash", "run_id");
        assertThatString(root, "args_hash").isEqualTo("argshash123");
        assertThat(root.get("run_id").isIntegralNumber()).isTrue();
        assertThat(root.get("run_id").asLong()).isEqualTo(42L);
        assertThat(Manifest.readIdentity(dir)).contains(new Manifest.Identity("argshash123", 42L));
    }

    /**
     * {@code .swath-state.json} is byte-identical to what the pre-Jackson hand-rolled writer
     * produced, and that is deliberate: only {@code manifest.json}'s layout was allowed to change.
     * Pinned as exact bytes (not a parsed tree) because the whole point is that the FORMATTING —
     * two-space indent, {@code ": "} separator, LF, trailing newline — did not move. This is the
     * test that fails if {@code ArtifactPrettyPrinter} is ever retuned.
     */
    @Test
    void stateFileIsByteIdenticalToTheLegacyLayout(@TempDir Path dir) throws IOException {
        Manifest.writeState(dir, "argshash123", 42L);
        assertThat(Files.readString(dir.resolve(Manifest.STATE_FILE_NAME), StandardCharsets.UTF_8))
                .isEqualTo("{\n  \"args_hash\": \"argshash123\",\n  \"run_id\": 42\n}\n");

        Manifest.writeState(dir, "h", null);
        assertThat(Files.readString(dir.resolve(Manifest.STATE_FILE_NAME), StandardCharsets.UTF_8))
                .isEqualTo("{\n  \"args_hash\": \"h\",\n  \"run_id\": null\n}\n");
    }

    /**
     * The manifest's own formatting, pinned where it is load-bearing: two-space indent and the
     * {@code ": "} separator (Jackson's default is {@code " : "}), which is what keeps the existing
     * substring assertions across swath-core and swath-cli satisfied.
     */
    @Test
    void manifestKeepsTwoSpaceIndentAndCompactSeparator(@TempDir Path dir) throws IOException {
        Manifest.write(dir, "my-bucket", SCHEMA,
                List.of(new PartInfo("data/part-00000.parquet", 0, 1L, 2L, "", "a", "z")), true, "key ASC");

        String text = Files.readString(dir.resolve(Manifest.FILE_NAME), StandardCharsets.UTF_8);
        assertThat(text).startsWith("{\n  \"sourceBucket\": \"my-bucket\",\n");
        assertThat(text).as("no \" : \" — Jackson's default separator would break substring consumers")
                .doesNotContain("\" : ");
        assertThat(text).as("files[] entries are indented under a 4-space member level")
                .contains("\n    {\n      \"key\": \"data/part-00000.parquet\",\n");
    }

    @Test
    void stateFileWritesAnExplicitNullRunId(@TempDir Path dir) throws IOException {
        Manifest.writeState(dir, "argshash123", null);

        JsonNode root = read(dir, Manifest.STATE_FILE_NAME);
        assertThat(root.has("run_id"))
                .as("run_id is an explicit JSON null when absent, never a missing key").isTrue();
        assertThat(root.get("run_id").isNull()).isTrue();
        assertThat(Manifest.readIdentity(dir)).contains(new Manifest.Identity("argshash123", null));
    }

    @Test
    void readIdentityRejectsIncompleteOrWronglyTypedForeignJson(@TempDir Path dir) throws IOException {
        Path state = dir.resolve(Manifest.STATE_FILE_NAME);
        for (String json : List.of(
                "{}", "null", "[]", "{\"args_hash\":null,\"run_id\":1}",
                "{\"args_hash\":\"h\",\"run_id\":\"1\"}",
                "{\"args_hash\":\"h\"}",
                "{\"args_hash\":\"h\",\"run_id\":1,\"foreign\":true}")) {
            Files.writeString(state, json, StandardCharsets.UTF_8);
            assertThat(Manifest.readIdentity(dir)).as(json).isEmpty();
        }
    }

    @Test
    void probeRejectsGenericOrIncompleteJsonButAcceptsCurrentManifest(@TempDir Path dir)
            throws IOException {
        Path manifest = dir.resolve(Manifest.FILE_NAME);
        for (String json : List.of(
                "{}", "null", "[]", "{\"files\":[]}",
                "{\"sourceBucket\":\"b\",\"version\":\"1\","
                        + "\"creationTimestamp\":1,\"fileFormat\":\"JSONL\","
                        + "\"fileSchema\":\"schema\",\"sorted\":false,"
                        + "\"sortKey\":null,\"files\":{}}")) {
            Files.writeString(manifest, json, StandardCharsets.UTF_8);
            assertThat(Manifest.probe(dir)).as(json).isEqualTo(Manifest.ManifestState.DAMAGED);
        }

        Manifest.write(dir, "b", "JSONL", "schema", List.of(), false, null);
        assertThat(Manifest.probe(dir)).isEqualTo(Manifest.ManifestState.VALID);
    }

    @Test
    void artifactsAreLfTerminatedUtf8(@TempDir Path dir) throws IOException {
        Manifest.write(dir, "b", SCHEMA, List.of(), false, null);
        Manifest.writeState(dir, "h", 1L);

        for (String name : List.of(Manifest.FILE_NAME, Manifest.STATE_FILE_NAME)) {
            String text = Files.readString(dir.resolve(name), StandardCharsets.UTF_8);
            assertThat(text).as("%s ends with a single trailing newline", name).endsWith("}\n");
            assertThat(text).as("%s uses LF, never CRLF", name).doesNotContain("\r");
        }
    }

    private static JsonNode read(Path dir, String name) throws IOException {
        return MAPPER.readTree(Files.readString(dir.resolve(name), StandardCharsets.UTF_8));
    }

    /**
     * Asserts {@code field} is genuinely a JSON string before comparing it — {@code asText()} on its
     * own would silently accept a number or boolean that had been given the right rendering.
     */
    private static AbstractStringAssert<?> assertThatString(JsonNode node, String field) {
        assertThat(node.get(field).isTextual()).as("%s is a JSON string", field).isTrue();
        return assertThat(node.get(field).asText()).as("%s", field);
    }

    private static List<String> fieldNames(JsonNode node) {
        List<String> names = new ArrayList<>();
        node.fieldNames().forEachRemaining(names::add);
        return names;
    }
}

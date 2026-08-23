/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.varve.swath.output.dataset.DurableFiles;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The format-neutral on-disk output layout for swath dataset sinks. The dataset root (the
 * {@code -o} directory) holds:
 *
 * <ul>
 *   <li>{@code data/} — pure data-part files for the manifest's declared {@code fileFormat}
 *       (no markers and no manifest metadata);</li>
 *   <li>{@code manifest.json} — the <b>consumer</b> manifest (S3-Inventory schema plus sortedness
 *       fields: {@code sourceBucket}, {@code version},
 *       {@code creationTimestamp}, {@code fileFormat}, {@code fileSchema}, top-level {@code sorted}
 *       (bool) + {@code sortKey} (non-null iff {@code sorted}), {@code files[{key, size,
 *       MD5checksum, rowCount, minKey?, maxKey?}]} — {@code minKey}/{@code maxKey} (plain UTF-8 key
 *       text, not base64/hex) present only when {@code sorted}), written atomically ({@code .tmp},
 *       fsync, rename) exactly once at successful consumer publication;</li>
 *   <li>{@code .swath-state.json} — the INTERNAL resume identity ({@code args_hash}, {@code run_id}),
 *       NOT consumer-facing;</li>
 *   <li>{@code _SUCCESS} — an empty whole-snapshot completion marker written LAST, only on success;</li>
 *   <li>{@code symlink.txt} — newline-delimited {@code data/<part>} paths.</li>
 * </ul>
 *
 * <p>The consumer manifest never carries {@code args_hash}/{@code run_id} — those live in
 * {@code .swath-state.json}, which {@link #readIdentity} reads to answer "published by this run".
 */
public final class Manifest {

    public static final String FILE_NAME = "manifest.json";
    public static final String STATE_FILE_NAME = ".swath-state.json";
    public static final String SUCCESS_FILE_NAME = "_SUCCESS";
    public static final String SYMLINK_FILE_NAME = "symlink.txt";

    /** The pure-data subdirectory of the dataset root; the canonical {@code data/<part>} prefix. */
    public static final String DATA_DIR = "data";

    /** Consumer-manifest schema version (S3-Inventory-style; a string, not the old integer). */
    private static final String MANIFEST_VERSION = "1";

    private static final Logger log = LoggerFactory.getLogger(Manifest.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Writes both JSON artifacts in the fixed layout below — never Jackson's platform defaults. */
    private static final ObjectWriter WRITER = MAPPER.writer(new ArtifactPrettyPrinter());

    private Manifest() {
    }

    /**
     * Ternary state of a dataset's {@code manifest.json}, for the §4.3 fresh-run directory guard:
     * {@link #ABSENT} (no manifest — an empty or never-published dir), {@link #VALID} (a parseable
     * swath consumer manifest — a JSON object carrying a {@code files} array), or {@link #DAMAGED}
     * (a manifest that exists but does not parse as one — foreign or corrupt). A fresh run refuses a
     * {@code DAMAGED} manifest with a diagnostic rather than overwriting it blindly.
     */
    public enum ManifestState { ABSENT, VALID, DAMAGED }

    /**
     * Classify the {@code manifest.json} in {@code dir} (the dataset root) without mutating anything —
     * see {@link ManifestState}. A parse failure or a JSON shape that is not a swath consumer manifest
     * (no {@code files} array) reads as {@link ManifestState#DAMAGED}, never silently as absent.
     */
    public static ManifestState probe(Path dir) {
        Path file = dir.resolve(FILE_NAME);
        if (!Files.isRegularFile(file)) {
            return ManifestState.ABSENT;
        }
        try {
            JsonNode root = MAPPER.readTree(file.toFile());
            if (isCompleteManifest(root)) {
                return ManifestState.VALID;
            }
            return ManifestState.DAMAGED;
        } catch (IOException | RuntimeException e) {
            log.debug("failed to parse manifest at {}; treating as damaged/foreign", file, e);
            return ManifestState.DAMAGED;
        }
    }

    /** Require the complete typed shape emitted by {@link #write}; generic JSON is never ownership. */
    private static boolean isCompleteManifest(JsonNode root) {
        if (root == null || !root.isObject()
                || !root.path("sourceBucket").isTextual()
                || !root.path("version").isTextual()
                || !MANIFEST_VERSION.equals(root.path("version").textValue())
                || !root.path("creationTimestamp").isIntegralNumber()
                || !root.path("fileFormat").isTextual()
                || !root.path("fileSchema").isTextual()
                || !root.path("sorted").isBoolean()
                || !root.has("sortKey")
                || !root.path("files").isArray()) {
            return false;
        }
        boolean sorted = root.path("sorted").booleanValue();
        JsonNode sortKey = root.get("sortKey");
        if (sorted ? !sortKey.isTextual() : !sortKey.isNull()) {
            return false;
        }
        for (JsonNode file : root.path("files")) {
            if (!file.isObject()
                    || !file.path("key").isTextual()
                    || !file.path("size").isIntegralNumber()
                    || !file.path("MD5checksum").isTextual()
                    || !file.path("rowCount").isIntegralNumber()
                    || (file.has("minKey") && !file.path("minKey").isTextual())
                    || (file.has("maxKey") && !file.path("maxKey").isTextual())) {
                return false;
            }
        }
        return true;
    }

    /**
     * The internal resume identity ({@code args_hash}, {@code run_id}) recorded in
     * {@code .swath-state.json}. The sorted publish path checks BOTH before trusting an
     * existing dataset as "this run's published output" — a stale state file left by a DIFFERENT prior
     * run sharing the same output directory (a fresh / {@code --restart} run only discards checkpoint
     * rows, never output files) must never be mistaken for THIS run's publish. {@code runId} is
     * {@code null} for a state file written by a path that doesn't record a checkpoint run id (the
     * unsorted pool close).
     */
    public record Identity(String argsHash, Long runId) {
    }

    /**
     * Atomically write the <b>consumer</b> {@code manifest.json} into {@code dir} (the dataset root).
     * {@code parts} carry their {@code data/}-prefixed keys, byte sizes, row counts, and
     * lowercase-hex MD5s.
     *
     * @param sourceBucket the source bucket the listing came from ({@code sourceBucket}); the empty
     *                     string only for a defensively-constructed path with no bucket
     * @param schema       the Parquet {@code MessageType} string ({@code fileSchema})
     * @param sorted       whether this publish is a {@code --sort} run;
     *                     drives the top-level {@code sorted} boolean, the crash-state legibility
     *                     signal a consumer uses to tell a sorted-complete dataset from an
     *                     unsorted-complete one
     * @param sortKey      the total order the {@code files[]} are sorted by (non-null iff
     *                     {@code sorted}); {@code null} for an unsorted publish
     */
    public static void write(Path dir, String sourceBucket, String schema, List<PartInfo> parts,
                             boolean sorted, String sortKey) throws IOException {
        write(dir, sourceBucket, "Parquet", schema, parts, sorted, sortKey);
    }

    /** Common manifest writer for every partitioned dataset format. */
    public static void write(Path dir, String sourceBucket, String fileFormat, String schema,
                             List<PartInfo> parts, boolean sorted, String sortKey) throws IOException {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("sourceBucket", sourceBucket);
        root.put("version", MANIFEST_VERSION);
        root.put("creationTimestamp", System.currentTimeMillis());
        root.put("fileFormat", fileFormat);
        root.put("fileSchema", schema);
        root.put("sorted", sorted);
        root.put("sortKey", sortKey);   // an explicit JSON null for an unsorted publish, never absent
        ArrayNode files = root.putArray("files");
        for (PartInfo p : parts) {
            ObjectNode file = files.addObject();
            file.put("key", p.path());
            file.put("size", p.bytes());
            file.put("MD5checksum", p.md5() == null ? "" : p.md5());
            file.put("rowCount", p.rows());
            if (p.minKey() != null) {   // key-range fields are present only for a sorted part
                file.put("minKey", p.minKey());
            }
            if (p.maxKey() != null) {
                file.put("maxKey", p.maxKey());
            }
        }
        atomicWriteJson(dir, FILE_NAME, root);
    }

    /**
     * Atomically write the INTERNAL {@code .swath-state.json} resume identity into {@code dir}.
     * {@code runId} is written as {@code null} when absent (the unsorted pool close).
     */
    public static void writeState(Path dir, String argsHash, Long runId) throws IOException {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("args_hash", argsHash == null ? "" : argsHash);
        root.put("run_id", runId);   // an explicit JSON null when absent, never a missing key
        atomicWriteJson(dir, STATE_FILE_NAME, root);
    }

    /**
     * Write the empty {@code _SUCCESS} whole-snapshot completion marker LAST (after {@code
     * manifest.json} and {@code .swath-state.json}), fsynced, only on successful completion.
     * Its presence is the consumer's "this dataset is complete" signal.
     */
    public static void writeSuccess(Path dir) throws IOException {
        atomicWrite(dir, SUCCESS_FILE_NAME, new byte[0]);
    }

    /**
     * Write {@code symlink.txt}: a newline-delimited list of the {@code data/<part>} relative paths,
     * at the same final commit point as {@code _SUCCESS}.
     */
    public static void writeSymlink(Path dir, List<PartInfo> parts) throws IOException {
        StringBuilder sb = new StringBuilder(parts.size() * 48);
        for (PartInfo p : parts) {
            sb.append(p.path()).append('\n');
        }
        atomicWrite(dir, SYMLINK_FILE_NAME, sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    /** Serialize {@code root} (LF-terminated, UTF-8) and {@link #atomicWrite} it as {@code fileName}. */
    private static void atomicWriteJson(Path dir, String fileName, ObjectNode root) throws IOException {
        byte[] bytes = (WRITER.writeValueAsString(root) + "\n").getBytes(StandardCharsets.UTF_8);
        atomicWrite(dir, fileName, bytes);
    }

    /** Atomic {@code .tmp} → fsync → {@code ATOMIC_MOVE} → directory-fsync write of {@code fileName}. */
    private static void atomicWrite(Path dir, String fileName, byte[] bytes) throws IOException {
        Path tmp = dir.resolve(fileName + ".tmp");
        Files.write(tmp, bytes,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
            ch.force(true);
        }
        Files.move(tmp, dir.resolve(fileName), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        DurableFiles.directory(dir);   // the atomic rename's directory entry must be durable too (I6)
    }

    /**
     * Read just the identity fields ({@code args_hash}, {@code run_id}) from an existing {@code
     * .swath-state.json} in {@code dir} — {@link Optional#empty()} when the file is
     * absent, unreadable, or unparseable. A corrupt/foreign state file is deliberately never trusted:
     * the caller treats "no identity" exactly like "no dataset" (never a silent PUBLISHED match).
     */
    public static Optional<Identity> readIdentity(Path dir) {
        Path file = dir.resolve(STATE_FILE_NAME);
        if (!Files.isRegularFile(file)) {
            return Optional.empty();
        }
        try {
            JsonNode root = MAPPER.readTree(file.toFile());
            if (root == null || !root.isObject() || root.size() != 2
                    || !root.path("args_hash").isTextual() || !root.has("run_id")) {
                return Optional.empty();
            }
            JsonNode runIdNode = root.get("run_id");
            if (!runIdNode.isNull() && !runIdNode.isIntegralNumber()) {
                return Optional.empty();
            }
            String argsHash = root.path("args_hash").textValue();
            Long runId = runIdNode.isNull() ? null : runIdNode.longValue();
            return Optional.of(new Identity(argsHash, runId));
        } catch (IOException | RuntimeException e) {
            // Empty-identity fallback here is intentional (see javadoc above): a corrupt/foreign
            // state file must never be trusted. Logged at debug so a genuine parse bug is
            // distinguishable from an expected corrupt/foreign file, without changing the
            // empty-Optional contract.
            log.debug("failed to read resume identity from {}; treating as no identity", file, e);
            return Optional.empty();
        }
    }

    /**
     * Deterministic layout for these downstream-consumed artifacts: LF newlines (never the platform
     * separator) and a two-space indent for objects <i>and</i> arrays, with {@code "field": value}
     * rather than Jackson's default {@code "field" : value}.
     */
    private static final class ArtifactPrettyPrinter extends DefaultPrettyPrinter {

        private static final DefaultIndenter INDENTER = new DefaultIndenter("  ", "\n");

        ArtifactPrettyPrinter() {
            indentObjectsWith(INDENTER);
            indentArraysWith(INDENTER);
        }

        private ArtifactPrettyPrinter(ArtifactPrettyPrinter base) {
            super(base);
        }

        @Override
        public DefaultPrettyPrinter createInstance() {
            return new ArtifactPrettyPrinter(this);
        }

        @Override
        public void writeObjectFieldValueSeparator(JsonGenerator g) throws IOException {
            g.writeRaw(": ");
        }
    }
}

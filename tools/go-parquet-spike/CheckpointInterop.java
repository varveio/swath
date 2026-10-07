/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.PageCommit;
import io.varve.swath.checkpoint.PartFinalize;
import io.varve.swath.checkpoint.PartRef;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.SoftRestoreContext;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ListingMode;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.output.parquet.Manifest;
import io.varve.swath.output.parquet.ParquetResume;
import io.varve.swath.output.parquet.ParquetSchema;
import io.varve.swath.output.parquet.PartInfo;
import io.varve.swath.output.parquet.PartWriter;
import io.varve.swath.output.parquet.fixture.ParquetEntryReader;
import io.varve.swath.runtime.ArgsHashFields;
import io.varve.swath.runtime.CancellationToken;
import io.varve.swath.runtime.ListRunner;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.store.ListPage;
import io.varve.swath.testkit.MockPageFetcher;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.schema.MessageType;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/** Cross-language direct-output recovery oracle using swath's actual store and listing runtime. */
public final class CheckpointInterop {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String BUCKET = "checkpoint-probe";
    private static final int PAGE_ROWS = 64;
    private static final int PART_ROWS = 128;
    private static final int WRITERS = 2;
    private static final List<String> FIELDS =
            ParquetSchema.canonical().getFields().stream().map(type -> type.getName()).toList();

    private CheckpointInterop() {}

    public static void main(String[] args) {
        ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger("ROOT").setLevel(Level.OFF);
        try {
            require(
                    args.length > 0,
                    "create OUTPUT_DIR INPUT_JSONL | recover OUTPUT_DIR | inspect OUTPUT_DIR");
            switch (args[0]) {
                case "create" -> {
                    require(args.length == 3, "create requires output directory and input JSONL");
                    create(
                            Path.of(args[1]).toAbsolutePath().normalize(),
                            Path.of(args[2]).toAbsolutePath().normalize());
                }
                case "recover" -> {
                    require(args.length == 2, "recover requires output directory");
                    recover(Path.of(args[1]).toAbsolutePath().normalize());
                }
                case "inspect" -> {
                    require(args.length == 2, "inspect requires output directory");
                    emit(snapshot(Path.of(args[1]).toAbsolutePath().normalize()));
                }
                default -> throw new IllegalArgumentException("unknown mode " + args[0]);
            }
        } catch (Exception | AssertionError failure) {
            try {
                emit(
                        Map.of(
                                "status",
                                "error",
                                "error_class",
                                failure.getClass().getName(),
                                "message",
                                String.valueOf(failure.getMessage())));
            } catch (Exception outputFailure) {
                failure.addSuppressed(outputFailure);
            }
            failure.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static RunKey runKey(Path root) {
        String hash = ArgsHashFields.forListing("s3", null, BUCKET, "").hash();
        // This is the supported core/test construction path: no CLI ResumeRegistry identity is
        // invented.
        var context =
                new SoftRestoreContext(
                        true, null, null, false, false, root.toString(), false, "DIRECTORY", "dir");
        return new RunKey(
                "s3",
                null,
                BUCKET,
                new byte[0],
                hash,
                "auto",
                ListingMode.OBJECTS,
                "",
                "PARQUET",
                context,
                false);
    }

    private static Path database(Path root) {
        return root.resolve(".swath").resolve("checkpoint.sqlite");
    }

    private static void create(Path root, Path input) throws Exception {
        Fixture fixture = load(input);
        require(
                fixture.entries().size() >= PART_ROWS + PAGE_ROWS + 1,
                "fixture needs at least 193 OBJECT rows");
        if (Files.exists(root)) {
            try (var entries = Files.list(root)) {
                require(entries.findAny().isEmpty(), "create refuses a nonempty output directory");
            }
        }
        Files.createDirectories(database(root).getParent());
        DatasetLayout layout = DatasetLayout.of(root);
        Files.createDirectories(layout.dataDir());
        Map<String, Object> receipt = new LinkedHashMap<>();
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(database(root))) {
            var run = store.openRun(runKey(root), false, false);
            long node = store.insertNodes(List.of(NodeSpec.rootRange(run.id()))).getFirst();
            int writerId = (int) (node % WRITERS);
            String baselineName =
                    String.format(java.util.Locale.ROOT, "part-w%d-%05d.parquet", writerId, 0);
            Path baseline = layout.dataFile(baselineName);
            PartWriter writer = new PartWriter(baseline, ParquetSchema.canonical());
            try {
                for (int first = 0; first < PART_ROWS; first += PAGE_ROWS) {
                    int last = first + PAGE_ROWS;
                    store.commitPage(
                            new PageCommit(
                                    node,
                                    fixture.entries().get(last - 1).key().rawUnsafe(),
                                    false));
                    for (ObjectEntry entry : fixture.entries().subList(first, last)) {
                        writer.write(entry);
                    }
                }
                writer.close();
            } catch (Exception | Error failure) {
                writer.discard();
                throw failure;
            }
            String baselineKey = DatasetLayout.key(baselineName);
            store.partFinalized(
                    new PartFinalize(
                            run.id(),
                            writerId,
                            baselineKey,
                            "parquet",
                            PART_ROWS,
                            Files.size(baseline),
                            List.of(
                                    new PartFinalize.DurableAdvance(
                                            node,
                                            fixture.entries()
                                                    .get(PART_ROWS - 1)
                                                    .key()
                                                    .rawUnsafe()))));
            // Leading scan progress is deliberately farther than durable output. The Go producer
            // must reset to the durable cursor before emitting, even before its first injected
            // crash.
            store.commitPage(
                    new PageCommit(
                            node,
                            fixture.entries().get(PART_ROWS + PAGE_ROWS - 1).key().rawUnsafe(),
                            false));
            receipt.put("status", "ok");
            receipt.put("operation", "create");
            receipt.put("scope", "single-root OBJECTS direct Parquet; core runtime, not CLI");
            receipt.put("run_id", run.id());
            receipt.put("node_id", node);
            receipt.put("writer_id", writerId);
            receipt.put("writers", WRITERS);
            receipt.put("args_hash", run.argsHash());
            receipt.put("identity_spec", run.identitySpec());
            receipt.put("checkpoint", database(root).toString());
            receipt.put("input_jsonl", input.toString());
            receipt.put("input_sha256", digest(input, "SHA-256"));
            receipt.put("total_rows", fixture.entries().size());
            receipt.put("page_rows", PAGE_ROWS);
            receipt.put("part_rows", PART_ROWS);
            receipt.put("baseline_part", baselineKey);
            receipt.put("baseline_rows", PART_ROWS);
            receipt.put("baseline_md5", writer.md5());
            receipt.put(
                    "baseline_cursor_hex",
                    HexFormat.of()
                            .formatHex(fixture.entries().get(PART_ROWS - 1).key().rawUnsafe()));
            receipt.put(
                    "committed_cursor_hex",
                    HexFormat.of()
                            .formatHex(
                                    fixture.entries()
                                            .get(PART_ROWS + PAGE_ROWS - 1)
                                            .key()
                                            .rawUnsafe()));
            require(
                    writer.md5().equals(digest(baseline, "MD5")),
                    "production writer digest disagrees with physical file");
        }
        receipt.put("schema_version", snapshot(root).get("schema_version"));
        Files.writeString(
                root.resolve("fixture.json"),
                JSON.writeValueAsString(receipt) + "\n",
                StandardCharsets.UTF_8);
        emit(receipt);
    }

    private static void recover(Path root) throws Exception {
        JsonNode receipt = JSON.readTree(root.resolve("fixture.json").toFile());
        Path input = Path.of(receipt.path("input_jsonl").textValue());
        require(
                digest(input, "SHA-256").equals(receipt.path("input_sha256").textValue()),
                "input fixture changed");
        Fixture fixture = load(input);
        DatasetLayout layout = DatasetLayout.of(root);
        String baselineKey = receipt.path("baseline_part").textValue();
        String baselineMd5 = receipt.path("baseline_md5").textValue();
        require(
                digest(layout.resolveKey(baselineKey), "MD5").equals(baselineMd5),
                "baseline part changed before recovery");
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "ok");
        result.put("operation", "recover");
        result.put(
                "scope",
                "actual CLI work-stealing lifecycle with MockPageFetcher; core"
                        + " invocation, not CLI command");
        result.put("checkpoint_before", snapshot(root));
        Map<String, String> filesBefore = physicalDigests(layout);
        List<String> removed;
        Map<String, String> retained = new LinkedHashMap<>();
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(database(root))) {
            var run = store.openRun(runKey(root), true, false);
            require(run.id() == receipt.path("run_id").longValue(), "wrong checkpoint run");
            require(
                    run.argsHash().equals(receipt.path("args_hash").textValue()),
                    "checkpoint args_hash changed");
            require(run.identitySpec() == null, "probe did not use the core identity path");
            require(
                    !run.sortEnabled() && run.mode() == ListingMode.OBJECTS && !run.fatalError(),
                    "unexpected run contract");
            List<PartRef> finalized = store.finalizedParts(run.id());
            Set<String> names = finalized.stream().map(PartRef::path).collect(Collectors.toSet());
            require(names.contains(baselineKey), "baseline part lost from checkpoint");
            for (PartRef part : finalized) {
                require(
                        part.format().equals("parquet")
                                && part.formatVersion() == null
                                && part.extensionType() == null,
                        "unexpected direct output format metadata");
                retained.put(part.path(), digest(layout.resolveKey(part.path()), "MD5"));
            }
            removed =
                    filesBefore.keySet().stream()
                            .filter(name -> !names.contains(name))
                            .sorted()
                            .toList();
            ParquetResume.discardNonFinalized(root, names);
            for (String orphan : removed) {
                require(
                        !Files.exists(layout.resolveKey(orphan)),
                        "actual resume cleanup retained orphan " + orphan);
            }
            List<Node> nodes = store.loadResumable(run.id(), true);
            result.put("checkpoint_after_reset", snapshot(root));
            require(nodes.size() <= 1, "probe expected one root range");
            List<PartInfo> existing =
                    finalized.stream()
                            .map(
                                    part ->
                                            new PartInfo(
                                                    part.path(),
                                                    part.writerId(),
                                                    part.rows(),
                                                    part.bytes(),
                                                    retained.get(part.path())))
                            .toList();
            if (nodes.isEmpty()) {
                require(
                        finalized.stream().mapToLong(PartRef::rows).sum() == fixture.rows().size(),
                        "output-complete worklist disagrees with complete fixture");
                result.put("recovery_start_index", fixture.rows().size());
                result.put(
                        "runtime_completion",
                        "empty-worklist ListRunner lifecycle closes the production"
                                + " ParquetWriterPool");
            } else {
                Node node = nodes.getFirst();
                require(
                        node.id() == receipt.path("node_id").longValue()
                                && node.parentId() == null
                                && node.rangeStart() == null
                                && node.rangeEnd() == null,
                        "unexpected root range");
                require(
                        Arrays.equals(node.cursor(), node.durableCursor()),
                        "actual recovery did not reset cursor to durability");
                result.put("recovery_start_index", firstAfter(fixture.entries(), node.cursor()));
                result.put("runtime_completion", "actual ListRunner.runToParquetWorkStealing");
            }
            var spec =
                    new ListRunner.ParquetSpec(
                            new byte[0],
                            PAGE_ROWS * 4,
                            PAGE_ROWS,
                            FilterChain.EMPTY,
                            WRITERS,
                            Long.MAX_VALUE,
                            8,
                            run.argsHash(),
                            null,
                            null,
                            0L,
                            PART_ROWS,
                            BUCKET);
            SimpleMeterRegistry registry = new SimpleMeterRegistry();
            try {
                var context = new RunContext(new CancellationToken(), registry);
                context.metrics()
                        .recordRecoveredObjects(finalized.stream().mapToLong(PartRef::rows).sum());
                if (nodes.isEmpty()) {
                    context.metrics().recordStealReason("RESUME", "publication_only");
                }
                MockPageFetcher fetcher = fetcher(fixture);
                // ListCommand's direct publication-only branch invokes this same lifecycle with
                // empty seeds. WorkStealingScan returns before worker forks; pool.close publishes.
                var statistics =
                        new ListRunner()
                                .runToParquetWorkStealing(
                                        context, fetcher, root, spec, store, run.id(), 1, nodes,
                                        existing);
                result.put("source_pages", fetcher.apiCalls());
                result.put("relisted_rows", statistics.totalRows());
                result.put("runtime_entrypoint", "runToParquetWorkStealing");
                result.put(
                        "runtime_method",
                        nodes.isEmpty() ? "ParquetWriterPool.close" : "runToParquetWorkStealing");
                result.put("requested_runtime_workers", 1);
                var workers =
                        registry.find("swath.steal_reason")
                                .tags("outcome", "SEED_SCHED", "reason", "distinct_seed_worker")
                                .counter();
                long observedWorkers = workers == null ? 0 : Math.round(workers.count());
                require(
                        observedWorkers == (nodes.isEmpty() ? 0 : 1),
                        "unexpected observed seed workers");
                result.put("runtime_workers", observedWorkers);
                result.put("publication_only", nodes.isEmpty());
                if (nodes.isEmpty()) {
                    require(
                            fetcher.apiCalls() == 0 && statistics.totalRows() == 0,
                            "publication-only recovery relisted rows");
                }
            } finally {
                registry.close();
            }
            require(
                    store.loadResumable(run.id(), true).isEmpty(),
                    "completed output has resumable nodes");
            require(
                    store.finalizedParts(run.id()).stream().mapToLong(PartRef::rows).sum()
                            == fixture.rows().size(),
                    "checkpoint finalized row total mismatch");
        }
        Map<String, String> filesAfter = physicalDigests(layout);
        for (var part : retained.entrySet()) {
            require(
                    part.getValue().equals(filesAfter.get(part.getKey())),
                    "checkpoint-finalized part was rewritten " + part.getKey());
        }
        verifyDataset(layout, fixture);
        verifyManifest(
                root, receipt.path("args_hash").textValue(), fixture.rows().size(), filesAfter);
        result.put("recovered", true);
        result.put("total_rows", fixture.rows().size());
        result.put(
                "unique_keys", fixture.entries().stream().map(ObjectEntry::key).distinct().count());
        result.put("baseline_md5_unchanged", baselineMd5.equals(filesAfter.get(baselineKey)));
        result.put("retained_part_md5_unchanged", true);
        result.put("retained_md5", retained);
        result.put("orphans_removed", removed);
        result.put("files_before_md5", filesBefore);
        result.put("files_after_md5", filesAfter);
        result.put("final_part_names", filesAfter.keySet());
        result.put("all_13_fields_and_nulls_exact", true);
        result.put("manifest_and_success_verified", true);
        result.put("checkpoint_after", snapshot(root));
        emit(result);
    }

    private static MockPageFetcher fetcher(Fixture fixture) {
        Map<KeyBytes, ObjectEntry> original = new HashMap<>();
        fixture.entries().forEach(entry -> original.put(entry.key(), entry));
        return MockPageFetcher.builder()
                .keys(fixture.entries().stream().map(entry -> entry.key().rawUnsafe()).toList())
                .maxKeysCap(PAGE_ROWS)
                .interceptor(
                        (request, call, computed) -> {
                            List<ListEntry> entries =
                                    computed.entries().stream()
                                            .map(entry -> (ListEntry) original.get(entry.key()))
                                            .toList();
                            require(
                                    entries.stream().allMatch(entry -> entry != null),
                                    "mock page contains unknown source key");
                            return new ListPage(
                                    entries,
                                    computed.commonPrefixes(),
                                    computed.truncated(),
                                    computed.nextContinuationToken(),
                                    computed.nextKeyMarker(),
                                    computed.nextVersionIdMarker(),
                                    computed.httpStatus(),
                                    Duration.ZERO);
                        })
                .build();
    }

    private static int firstAfter(List<ObjectEntry> entries, byte[] cursor) {
        int first = 0;
        while (cursor != null
                && first < entries.size()
                && KeyBytes.compareUnsigned(entries.get(first).key().rawUnsafe(), cursor) <= 0) {
            first++;
        }
        return first;
    }

    private static Map<String, String> physicalDigests(DatasetLayout layout) throws Exception {
        Map<String, String> result = new LinkedHashMap<>();
        for (Path part : layout.dataParts()) {
            result.put(DatasetLayout.key(part.getFileName().toString()), digest(part, "MD5"));
        }
        return result;
    }

    private static void verifyDataset(DatasetLayout layout, Fixture fixture) throws Exception {
        int offset = 0;
        Set<KeyBytes> keys = new HashSet<>();
        for (Path part : layout.dataParts()) {
            try (var reader = ParquetFileReader.open(new LocalInputFile(part))) {
                MessageType schema = reader.getFooter().getFileMetaData().getSchema();
                requireCanonicalMeaning(schema);
                var columnIo = new ColumnIOFactory().getColumnIO(schema);
                org.apache.parquet.column.page.PageReadStore pages;
                while ((pages = reader.readNextRowGroup()) != null) {
                    try (var ownedPages = pages) {
                        var records =
                                columnIo.getRecordReader(
                                        ownedPages, new GroupRecordConverter(schema));
                        for (long row = 0; row < ownedPages.getRowCount(); row++) {
                            require(
                                    offset < fixture.rows().size(),
                                    "published dataset contains extra rows");
                            ObjectNode actual = row(records.read(), schema);
                            require(
                                    actual.equals(fixture.rows().get(offset)),
                                    "dataset row mismatch at " + offset);
                            require(
                                    keys.add(KeyBytes.ofUtf8(actual.path("key").textValue())),
                                    "duplicate published key");
                            offset++;
                        }
                    }
                }
            }
            // The current production whole-row reader must also accept every retained Go part.
            try (var reader = new ParquetEntryReader(part)) {
                while (reader.hasNext()) {
                    require(reader.next() instanceof ObjectEntry, "non-OBJECT production readback");
                }
            }
        }
        require(
                offset == fixture.rows().size() && keys.size() == fixture.rows().size(),
                "published dataset is incomplete");
    }

    private static void verifyManifest(
            Path root, String hash, int expectedRows, Map<String, String> files) throws Exception {
        DatasetLayout layout = DatasetLayout.of(root);
        require(
                Manifest.probe(root) == Manifest.ManifestState.VALID,
                "production manifest is not valid");
        require(
                Files.isRegularFile(layout.success()) && Files.size(layout.success()) == 0,
                "missing success marker");
        var identity = Manifest.readIdentity(root).orElseThrow();
        require(identity.argsHash().equals(hash), "published args_hash mismatch");
        JsonNode manifest = JSON.readTree(layout.manifest().toFile());
        require(
                !manifest.path("sorted").booleanValue()
                        && manifest.path("fileFormat").textValue().equals("Parquet"),
                "unexpected consumer output format");
        Set<String> published = new HashSet<>();
        long rows = 0;
        for (JsonNode part : manifest.path("files")) {
            String key = part.path("key").textValue();
            require(published.add(key), "duplicate manifest part");
            require(
                    files.containsKey(key)
                            && files.get(key).equals(part.path("MD5checksum").textValue()),
                    "manifest digest mismatch");
            require(
                    Files.size(layout.resolveKey(key)) == part.path("size").longValue(),
                    "manifest byte-size mismatch");
            rows += part.path("rowCount").longValue();
        }
        require(
                rows == expectedRows && published.equals(files.keySet()),
                "manifest does not cover the exact dataset");
        List<String> symlinkLines = Files.readAllLines(layout.symlink(), StandardCharsets.UTF_8);
        Set<String> symlink = new HashSet<>(symlinkLines);
        require(
                symlink.equals(published) && symlinkLines.size() == published.size(),
                "symlink does not match manifest");
    }

    private static void requireCanonicalMeaning(MessageType actual) {
        MessageType expected = ParquetSchema.canonical();
        require(
                actual.getName().equals(expected.getName())
                        && actual.getFieldCount() == expected.getFieldCount(),
                "canonical schema root mismatch");
        for (int i = 0; i < expected.getFieldCount(); i++) {
            var observed = actual.getType(i).asPrimitiveType();
            var canonical = expected.getType(i).asPrimitiveType();
            require(
                    observed.getName().equals(canonical.getName())
                            && observed.getRepetition() == canonical.getRepetition()
                            && observed.getPrimitiveTypeName() == canonical.getPrimitiveTypeName()
                            && java.util.Objects.equals(
                                    observed.getLogicalTypeAnnotation(),
                                    canonical.getLogicalTypeAnnotation())
                            && observed.getOriginalType() == canonical.getOriginalType(),
                    "canonical schema field mismatch at " + i);
        }
    }

    private static ObjectNode row(Group group, MessageType schema) {
        ObjectNode row = JSON.createObjectNode();
        for (String name : FIELDS) {
            int count = group.getFieldRepetitionCount(name);
            require(count <= 1, "repeated canonical field");
            if (count == 0) {
                row.putNull(name);
                continue;
            }
            switch (schema.getType(name).asPrimitiveType().getPrimitiveTypeName()) {
                case BINARY -> {
                    byte[] bytes = group.getBinary(name, 0).getBytes();
                    require(KeyBytes.isValidUtf8(bytes), "non-UTF8 canonical field");
                    row.put(name, new String(bytes, StandardCharsets.UTF_8));
                }
                case INT64 -> row.put(name, group.getLong(name, 0));
                case BOOLEAN -> row.put(name, group.getBoolean(name, 0));
                default -> throw new IllegalArgumentException("unexpected canonical type");
            }
        }
        return row;
    }

    private static Fixture load(Path input) throws Exception {
        List<ObjectNode> rows = new ArrayList<>();
        List<ObjectEntry> entries = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(input, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                JsonNode parsed = JSON.readTree(line);
                require(parsed instanceof ObjectNode, "fixture row is not an object");
                ObjectNode row = (ObjectNode) parsed;
                Set<String> names = new TreeSet<>();
                row.fieldNames().forEachRemaining(names::add);
                require(names.equals(new TreeSet<>(FIELDS)), "fixture field set mismatch");
                require(
                        row.path("row_type").asText().equals("OBJECT")
                                && row.path("is_delete_marker").isBoolean()
                                && !row.path("is_delete_marker").asBoolean()
                                && row.path("version_id").isNull()
                                && row.path("is_latest").isNull(),
                        "fixture is not current OBJECTS output");
                for (String name : List.of("size", "last_modified")) {
                    JsonNode value = row.get(name);
                    require(
                            value.isNull() || value.isIntegralNumber() && value.canConvertToLong(),
                            "invalid integer fixture field");
                    if (!value.isNull()) {
                        row.put(name, value.longValue());
                    }
                }
                require(
                        !row.path("size").isNull() && row.path("size").longValue() >= 0,
                        "missing or invalid OBJECT size");
                require(
                        row.path("last_modified").isNull()
                                || row.path("last_modified").longValue() != 0,
                        "the current writer represents the zero timestamp sentinel as JSON null");
                String keyText = row.path("key").textValue();
                require(keyText != null, "missing fixture key");
                KeyBytes key = KeyBytes.ofUtf8(keyText);
                require(
                        KeyBytes.isValidUtf8(key.rawUnsafe())
                                && key.length() <= 1024
                                && key.asString().equals(keyText),
                        "invalid fixture key");
                require(
                        entries.isEmpty() || entries.getLast().key().compareTo(key) < 0,
                        "fixture is not in strict byte order");
                ObjectEntry entry =
                        new ObjectEntry(
                                key,
                                row.path("size").longValue(),
                                row.path("last_modified").isNull()
                                        ? 0
                                        : row.path("last_modified").longValue(),
                                nullable(row, "etag"),
                                nullable(row, "storage_class"),
                                null,
                                false,
                                nullable(row, "owner_id"),
                                nullable(row, "owner_display_name"),
                                nullable(row, "checksum_algorithm"),
                                nullable(row, "checksum_type"));
                rows.add(row);
                entries.add(entry);
            }
        }
        return new Fixture(List.copyOf(rows), List.copyOf(entries));
    }

    private static String nullable(ObjectNode row, String field) {
        JsonNode value = row.get(field);
        require(value.isNull() || value.isTextual(), "invalid optional string " + field);
        return value.isNull() ? null : value.textValue();
    }

    private static Map<String, Object> snapshot(Path root) throws Exception {
        Map<String, Object> result = new LinkedHashMap<>();
        try (Connection connection =
                        DriverManager.getConnection(
                                "jdbc:sqlite:" + database(root).toUri() + "?mode=ro");
                var statement = connection.createStatement()) {
            try (var rs = statement.executeQuery("PRAGMA user_version")) {
                require(rs.next(), "missing checkpoint version");
                result.put("schema_version", rs.getInt(1));
            }
            List<Map<String, Object>> nodes = new ArrayList<>();
            try (var rs =
                    statement.executeQuery(
                            "SELECT"
                                + " id,run_id,status,cursor,durable_cursor,generation,pages_emitted,api_calls"
                                + " FROM listing_node ORDER BY id")) {
                while (rs.next()) {
                    Map<String, Object> node = new LinkedHashMap<>();
                    node.put("id", rs.getLong(1));
                    node.put("run_id", rs.getLong(2));
                    node.put("status", rs.getString(3));
                    node.put("cursor_hex", hex(rs.getBytes(4)));
                    node.put("durable_cursor_hex", hex(rs.getBytes(5)));
                    node.put("generation", rs.getLong(6));
                    node.put("pages_emitted", rs.getLong(7));
                    node.put("api_calls", rs.getLong(8));
                    nodes.add(node);
                }
            }
            result.put("nodes", nodes);
            List<Map<String, Object>> parts = new ArrayList<>();
            try (var rs =
                    statement.executeQuery(
                            "SELECT"
                                + " run_id,writer_id,path,format,format_version,extension_type,finalized,rows,bytes"
                                + " FROM part_file ORDER BY id")) {
                while (rs.next()) {
                    Map<String, Object> part = new LinkedHashMap<>();
                    part.put("run_id", rs.getLong(1));
                    part.put("writer_id", rs.getInt(2));
                    part.put("path", rs.getString(3));
                    part.put("format", rs.getString(4));
                    part.put("format_version", rs.getObject(5));
                    part.put("extension_type", rs.getObject(6));
                    part.put("finalized", rs.getInt(7));
                    part.put("rows", rs.getLong(8));
                    part.put("bytes", rs.getLong(9));
                    parts.add(part);
                }
            }
            result.put("parts", parts);
            List<Map<String, Object>> runs = new ArrayList<>();
            try (var rs =
                    statement.executeQuery(
                            "SELECT id,args_hash,identity_spec,status,sort_enabled FROM run_meta"
                                    + " ORDER BY id")) {
                while (rs.next()) {
                    Map<String, Object> run = new LinkedHashMap<>();
                    run.put("id", rs.getLong(1));
                    run.put("args_hash", rs.getString(2));
                    run.put("identity_spec", rs.getString(3));
                    run.put("status", rs.getString(4));
                    run.put("sort_enabled", rs.getInt(5));
                    runs.add(run);
                }
            }
            result.put("runs", runs);
        }
        return result;
    }

    private static String hex(byte[] bytes) {
        return bytes == null ? null : HexFormat.of().formatHex(bytes);
    }

    private static String digest(Path path, String algorithm) throws Exception {
        MessageDigest digest = MessageDigest.getInstance(algorithm);
        try (InputStream input = Files.newInputStream(path)) {
            byte[] bytes = new byte[64 * 1024];
            int length;
            while ((length = input.read(bytes)) != -1) {
                digest.update(bytes, 0, length);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static void emit(Object value) throws Exception {
        System.out.println(JSON.writeValueAsString(value));
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    private record Fixture(List<ObjectNode> rows, List<ObjectEntry> entries) {}
}

/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.DeleteMarkerEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.output.parquet.ParquetSchema;
import io.varve.swath.output.parquet.fixture.ParquetEntryReader;
import io.varve.swath.output.parquet.sorted.RowGroupOrderException;
import io.varve.swath.output.parquet.sorted.SortedParquetIndex;
import io.varve.swath.output.parquet.sorted.SortedParquetRangeReader;
import io.varve.swath.output.parquet.sorted.SortedParquetRowGroupReader;
import io.varve.swath.output.parquet.sorted.SortedParquetRowGroupReader.ObjectRow;
import io.varve.swath.output.parquet.sorted.SortedParquetStamp;
import io.varve.swath.output.parquet.sorted.SortedParquetWriter;
import io.varve.swath.sort.ListEntryComparator;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortMode;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** Standalone, production-code interoperability oracle for the bounded Go writer spike. */
public final class SwathParity {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final List<String> FIELDS =
            List.of(
                    "key",
                    "size",
                    "last_modified",
                    "etag",
                    "storage_class",
                    "version_id",
                    "is_latest",
                    "is_delete_marker",
                    "owner_id",
                    "owner_display_name",
                    "checksum_algorithm",
                    "checksum_type",
                    "row_type");
    private static final List<String> EDGE_KEYS =
            List.of(
                    "a",
                    "a\u0000",
                    "a\n",
                    "a%",
                    "a+",
                    "a/",
                    "café/🚀",
                    "中文/",
                    "\uE000",
                    "\uD800\uDC00",
                    "x".repeat(1023) + "a",
                    "x".repeat(1023) + "b");

    private SwathParity() {}

    public static void main(String[] args) {
        try {
            if (args.length == 0) {
                throw new IllegalArgumentException(
                        "generate OUTPUT_JSONL COUNT PROFILE | write INPUT_JSONL OUTPUT_PARQUET"
                                + " PAGE_ROWS GROUP_BYTES ITERATIONS | verify INPUT_JSONL FILE");
            }
            switch (args[0]) {
                case "generate" -> {
                    require(args.length == 4, "generate requires output, count, profile");
                    generate(Path.of(args[1]), Integer.parseInt(args[2]), args[3]);
                }
                case "write" -> {
                    require(
                            args.length == 6,
                            "write requires input, output, page rows, group bytes, iterations");
                    write(
                            Path.of(args[1]),
                            Path.of(args[2]),
                            Integer.parseInt(args[3]),
                            Long.parseLong(args[4]),
                            Integer.parseInt(args[5]));
                }
                case "verify" -> {
                    require(args.length == 3, "verify requires input JSONL and Parquet file");
                    verify(Path.of(args[1]), Path.of(args[2]));
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
            } catch (IOException outputFailure) {
                failure.addSuppressed(outputFailure);
            }
            failure.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static void generate(Path output, int count, String profile) throws IOException {
        require(count >= 0, "count must be nonnegative");
        require(
                Set.of(
                                "objects",
                                "wide",
                                "edges",
                                "versions",
                                "mixed",
                                "versions/mixed",
                                "empty",
                                "onepage",
                                "tinytail",
                                "equal-page-minima",
                                "long-prefix",
                                "unicode-truncation")
                        .contains(profile),
                "unknown profile " + profile);
        if (profile.equals("empty")) {
            count = 0;
        }
        List<ListEntry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String key = fixtureKey(i, profile);
            long micros = i % 7 == 0 ? 0 : 1_700_000_000_000_000L + i * 1001L;
            String version = profile.equals("versions") ? "v" + i % 3 : null;
            if ((profile.equals("mixed") || profile.equals("versions/mixed")) && i % 3 == 1) {
                entries.add(new CommonPrefixEntry(KeyBytes.ofUtf8(key)));
            } else if ((profile.equals("mixed") || profile.equals("versions/mixed"))
                    && i % 3 == 2) {
                entries.add(
                        new DeleteMarkerEntry(
                                KeyBytes.ofUtf8(key),
                                "v" + i,
                                i % 2 == 0,
                                micros,
                                optional(i, "owner")));
            } else {
                entries.add(
                        new ObjectEntry(
                                KeyBytes.ofUtf8(key),
                                i % 11 == 0 ? 0 : (1L << 33) + i,
                                micros,
                                profile.equals("wide") ? fixtureEtag(i) : optional(i, "etag-7"),
                                optional(i + 1, "STANDARD"),
                                version,
                                i % 2 == 0,
                                optional(i + 2, "owner"),
                                optional(i + 3, "display-中文"),
                                optional(i + 4, "SHA256"),
                                optional(i + 5, "FULL_OBJECT")));
            }
        }
        entries.sort(new ListEntryComparator());
        try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            for (ListEntry entry : entries) {
                writer.write(JSON.writeValueAsString(row(entry)));
                writer.newLine();
            }
        }
        emit(
                Map.of(
                        "status",
                        "ok",
                        "mode",
                        "generate",
                        "profile",
                        profile,
                        "rows",
                        entries.size(),
                        "path",
                        output.toString(),
                        "logical_schema",
                        FIELDS));
    }

    private static String fixtureKey(int i, String profile) {
        String suffix =
                String.format(
                        java.util.Locale.ROOT, "%012d", profile.equals("versions") ? i / 3 : i);
        return switch (profile) {
            case "edges" -> i < EDGE_KEYS.size() ? EDGE_KEYS.get(i) : "edge/" + suffix;
            case "equal-page-minima", "long-prefix" -> "x".repeat(1012) + suffix;
            case "wide" -> "inventory/" + "x".repeat(490 + (i % 2) * 512) + suffix;
            case "unicode-truncation" -> "界".repeat(21) + "é" + "界".repeat(315) + "x" + suffix;
            default -> suffix;
        };
    }

    private static String optional(int i, String present) {
        return i % 3 == 0 ? null : i % 3 == 1 ? "" : present;
    }

    private static String fixtureEtag(int index) {
        try {
            byte[] value = ByteBuffer.allocate(Long.BYTES).putLong(index).array();
            return java.util.HexFormat.of()
                    .formatHex(java.security.MessageDigest.getInstance("MD5").digest(value));
        } catch (java.security.NoSuchAlgorithmException unavailable) {
            throw new AssertionError(
                    "JDK does not supply MD5 for deterministic fixture metadata", unavailable);
        }
    }

    private static void write(
            Path input, Path output, int pageRows, long groupBytes, int iterations)
            throws IOException {
        require(
                pageRows > 0 && groupBytes > 0 && iterations > 0,
                "writer geometry and iterations must be positive");
        // Fixture parsing, object construction and ordering checks are deliberately outside encode
        // clocks.
        Fixture fixture = load(input, false);
        SortMode mode =
                fixture.entries().stream()
                                .anyMatch(
                                        entry ->
                                                entry instanceof DeleteMarkerEntry
                                                        || entry instanceof ObjectEntry object
                                                                && object.versionId() != null)
                        ? SortMode.VERSIONS
                        : SortMode.OBJECTS;
        SortConfig config =
                SortConfig.DEFAULT.withFinalPageRows(pageRows).withFinalRowGroupBytes(groupBytes);
        for (int iteration = 0; iteration < iterations; iteration++) {
            Path path = iterations == 1 ? output : Path.of(output + "." + iteration + ".parquet");
            long openStarted = System.nanoTime();
            SortedParquetWriter writer = new SortedParquetWriter(path, config, mode, 1);
            long encodeStarted = System.nanoTime();
            long encoded;
            long closed;
            try {
                for (ListEntry entry : fixture.entries()) {
                    writer.write(entry);
                }
                encoded = System.nanoTime();
                writer.markFinal();
                writer.close();
                closed = System.nanoTime();
            } catch (IOException | RuntimeException | Error failure) {
                writer.discard();
                throw failure;
            }
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("status", "ok");
            result.put("mode", "write");
            result.put("writer", "swath-java-production");
            result.put("iteration", iteration);
            result.put("warmup", iterations > 1 && iteration == 0);
            result.put("rows", fixture.entries().size());
            result.put("page_rows", pageRows);
            result.put("row_group_bytes", groupBytes);
            result.put("open_nanos", encodeStarted - openStarted);
            result.put("encode_nanos", encoded - encodeStarted);
            result.put("footer_close_nanos", closed - encoded);
            result.put("writer_total_nanos", closed - openStarted);
            result.put("close_includes_file_and_parent_fsync", true);
            result.put("bytes", Files.size(path));
            result.put("path", path.toString());
            emit(result);
        }
    }

    private static void verify(Path input, Path file) throws IOException {
        Fixture fixture = load(input);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "ok");
        result.put("mode", "verify");
        result.put("path", file.toString());
        result.put("rows", fixture.rows().size());
        result.put("schema_exact", true);
        result.put("rows_exact_including_nulls", true);
        result.put("bytes", Files.size(file));
        List<Map<String, Object>> groups = new ArrayList<>();
        Map<String, ColumnTotals> totals = new LinkedHashMap<>();
        int offset = 0;
        long keyPages = 0;
        long maxKeyPageRows = 0;
        int missingIndexes = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            result.put("java_schema_object_equal", schema.equals(ParquetSchema.canonical()));
            result.put("schema_type_length_differences", validateCanonicalSchema(schema));
            result.put(
                    "schema_comparison",
                    "canonical field semantics; absent or natural-width type_length allowed");
            result.put("created_by", reader.getFooter().getFileMetaData().getCreatedBy());
            result.put(
                    "footer_metadata", reader.getFooter().getFileMetaData().getKeyValueMetaData());
            require(reader.getRecordCount() == fixture.rows().size(), "footer row count mismatch");
            var columnIo = new ColumnIOFactory().getColumnIO(schema);
            for (int block = 0; block < reader.getRowGroups().size(); block++) {
                BlockMetaData metadata = reader.getRowGroups().get(block);
                require(
                        metadata.getRowCount() <= Integer.MAX_VALUE,
                        "spike row group too large to materialize");
                List<ObjectNode> actual = new ArrayList<>((int) metadata.getRowCount());
                try (var pages = reader.readRowGroup(block)) {
                    var records = columnIo.getRecordReader(pages, new GroupRecordConverter(schema));
                    for (long row = 0; row < pages.getRowCount(); row++) {
                        ObjectNode observed = row(records.read(), schema);
                        require(offset < fixture.rows().size(), "unexpected extra row");
                        require(
                                observed.equals(fixture.rows().get(offset)),
                                "row mismatch at "
                                        + offset
                                        + ": expected="
                                        + fixture.rows().get(offset)
                                        + " actual="
                                        + observed);
                        actual.add(observed);
                        offset++;
                    }
                }
                Map<String, Object> group = new LinkedHashMap<>();
                group.put("row_group", block);
                group.put("rows", metadata.getRowCount());
                List<Map<String, Object>> columns = new ArrayList<>();
                for (ColumnChunkMetaData column : metadata.getColumns()) {
                    ColumnIndex ci = reader.readColumnIndex(column);
                    OffsetIndex oi = reader.readOffsetIndex(column);
                    Map<String, Object> fact =
                            inspectColumn(column, ci, oi, actual, Files.size(file));
                    columns.add(fact);
                    totals.computeIfAbsent(
                                    column.getPath().toDotString(), ignored -> new ColumnTotals())
                            .add(column, ci, oi);
                    if (ci == null || oi == null) {
                        missingIndexes++;
                    }
                    if (column.getPath().toDotString().equals("key") && oi != null) {
                        keyPages += oi.getPageCount();
                        for (int page = 0; page < oi.getPageCount(); page++) {
                            maxKeyPageRows =
                                    Math.max(
                                            maxKeyPageRows,
                                            oi.getLastRowIndex(page, actual.size())
                                                    - oi.getFirstRowIndex(page)
                                                    + 1);
                        }
                    }
                }
                group.put("columns", columns);
                groups.add(group);
            }
        }
        require(offset == fixture.rows().size(), "decoded row count mismatch");
        verifyEntryReader(fixture, file);
        require(
                SortedParquetIndex.rowCount(file) == fixture.rows().size(),
                "production index count mismatch");
        var bounds = SortedParquetIndex.bounds(file, () -> {});
        require(bounds.rowCount() == fixture.rows().size(), "production bounds count mismatch");
        if (!fixture.rows().isEmpty()) {
            require(
                    Arrays.equals(bounds.firstKey(), key(fixture.rows().getFirst())),
                    "production first key mismatch");
            require(
                    Arrays.equals(bounds.lastKey(), key(fixture.rows().getLast())),
                    "production last key mismatch");
        }
        result.put("row_groups", groups);
        result.put("key_pages", keyPages);
        result.put("max_key_page_rows", maxKeyPageRows);
        result.put("missing_index_pairs", missingIndexes);
        result.put("physical_indexes_validated", missingIndexes == 0);
        Map<String, Object> columnFacts = new LinkedHashMap<>();
        totals.forEach((name, total) -> columnFacts.put(name, total.report()));
        result.put("column_totals", columnFacts);
        var stamp = SortedParquetStamp.read(file);
        Map<String, Object> stampFact = new LinkedHashMap<>();
        stampFact.put("recognized", stamp.isPresent());
        stamp.ifPresent(
                value -> {
                    stampFact.put("order", value.order());
                    stampFact.put("mode", value.mode().value());
                    stampFact.put("format_version", value.formatVersion());
                    stampFact.put("supported", value.isKnownFormatVersion());
                    stampFact.put("file_index", value.fileIndex());
                    stampFact.put("file_final", value.fileFinal());
                });
        result.put("sorted_stamp", stampFact);
        result.put("serving", verifyServing(fixture, file, missingIndexes));
        emit(result);
    }

    private static List<Map<String, Object>> validateCanonicalSchema(MessageType actual) {
        MessageType expected = ParquetSchema.canonical();
        require(
                actual.getName().equals(expected.getName())
                        && actual.getRepetition() == expected.getRepetition()
                        && java.util.Objects.equals(actual.getId(), expected.getId())
                        && java.util.Objects.equals(
                                actual.getLogicalTypeAnnotation(),
                                expected.getLogicalTypeAnnotation())
                        && actual.getOriginalType() == expected.getOriginalType()
                        && actual.getFieldCount() == expected.getFieldCount(),
                "canonical root schema mismatch: " + actual);
        List<Map<String, Object>> lengthDifferences = new ArrayList<>();
        for (int field = 0; field < expected.getFieldCount(); field++) {
            var canonical = expected.getType(field).asPrimitiveType();
            var observedType = actual.getType(field);
            require(
                    observedType.isPrimitive(),
                    "canonical field is not primitive: " + observedType.getName());
            var observed = observedType.asPrimitiveType();
            require(
                    observed.getName().equals(canonical.getName())
                            && observed.getRepetition() == canonical.getRepetition()
                            && observed.getPrimitiveTypeName() == canonical.getPrimitiveTypeName()
                            && java.util.Objects.equals(
                                    observed.getLogicalTypeAnnotation(),
                                    canonical.getLogicalTypeAnnotation())
                            && observed.getOriginalType() == canonical.getOriginalType()
                            && java.util.Objects.equals(observed.getId(), canonical.getId())
                            && observed.columnOrder().equals(canonical.columnOrder()),
                    "canonical schema field mismatch at "
                            + field
                            + ": expected="
                            + canonical
                            + " actual="
                            + observed);
            int length = observed.getTypeLength();
            // parquet-format SchemaElement field 2 permits an optional maximum bit length for
            // non-FIXED_LEN_BYTE_ARRAY values. Natural full widths do not narrow the value domain.
            // parquet-java includes this advisory field in Type.equals even when Java omits it.
            int fullWidth =
                    switch (canonical.getPrimitiveTypeName()) {
                        case INT64 -> Long.SIZE;
                        case BOOLEAN -> 1;
                        default -> 0;
                    };
            require(
                    length == canonical.getTypeLength() || fullWidth != 0 && length == fullWidth,
                    "canonical type_length mismatch for " + canonical.getName() + ": " + length);
            if (length != canonical.getTypeLength()) {
                lengthDifferences.add(
                        Map.of(
                                "field",
                                canonical.getName(),
                                "java",
                                canonical.getTypeLength(),
                                "observed",
                                length,
                                "meaning",
                                "natural full bit width"));
            }
        }
        return List.copyOf(lengthDifferences);
    }

    private static Map<String, Object> inspectColumn(
            ColumnChunkMetaData column,
            ColumnIndex ci,
            OffsetIndex oi,
            List<ObjectNode> rows,
            long fileBytes) {
        String name = column.getPath().toDotString();
        Map<String, Object> fact = new LinkedHashMap<>();
        fact.put("name", name);
        fact.put("codec", column.getCodec().name());
        fact.put("compressed_bytes", column.getTotalSize());
        fact.put("uncompressed_bytes", column.getTotalUncompressedSize());
        fact.put("encodings", column.getEncodings().stream().map(Enum::name).sorted().toList());
        fact.put("column_index", ci != null);
        fact.put("offset_index", oi != null);
        fact.put("boundary_order", ci == null ? "ABSENT" : ci.getBoundaryOrder().name());
        fact.put("dictionary", column.hasDictionaryPage());
        if (column.hasDictionaryPage()) {
            long dictionaryOffset = column.getDictionaryPageOffset();
            long extent = column.getFirstDataPageOffset() - dictionaryOffset;
            require(
                    dictionaryOffset >= 4 && extent > 0 && dictionaryOffset + extent <= fileBytes,
                    "invalid dictionary extent for " + name);
            fact.put("dictionary_offset", dictionaryOffset);
            fact.put("dictionary_bytes", extent);
        }
        if (oi == null) {
            return fact;
        }
        int pages = oi.getPageCount();
        require(rows.isEmpty() || pages > 0, "missing data page for " + name);
        if (ci != null) {
            require(
                    ci.getMinValues().size() == pages
                            && ci.getMaxValues().size() == pages
                            && ci.getNullPages().size() == pages,
                    "column index page count mismatch for " + name);
            require(
                    ci.getNullCounts() == null || ci.getNullCounts().size() == pages,
                    "null count page count mismatch for " + name);
        }
        long maxRows = 0;
        long previousEnd = -1;
        long previousByteEnd = -1;
        for (int page = 0; page < pages; page++) {
            long first = oi.getFirstRowIndex(page);
            long last = oi.getLastRowIndex(page, rows.size());
            long physical = oi.getOffset(page);
            int bytes = oi.getCompressedPageSize(page);
            require(
                    first == previousEnd + 1 && first >= 0 && last >= first && last < rows.size(),
                    "invalid page row extent for " + name + " page " + page);
            long chunkEnd = column.getStartingPos() + column.getTotalSize();
            require(
                    physical >= column.getFirstDataPageOffset()
                            && bytes > 0
                            && physical <= Math.min(fileBytes, chunkEnd) - bytes
                            && (page == 0
                                    ? physical == column.getFirstDataPageOffset()
                                    : physical == previousByteEnd),
                    "invalid page byte extent for " + name + " page " + page);
            previousEnd = last;
            previousByteEnd = physical + bytes;
            maxRows = Math.max(maxRows, last - first + 1);
            if (ci != null) {
                validatePageBounds(name, column.getType(), ci, page, rows, (int) first, (int) last);
            }
        }
        require(previousEnd == rows.size() - 1L, "page rows do not cover column " + name);
        fact.put("pages", pages);
        fact.put("max_page_rows", maxRows);
        if (ci != null && column.getType() == PrimitiveTypeName.BINARY) {
            int equalMinima = 0;
            int shortenedMinima = 0;
            byte[] previousMinimum = null;
            for (int page = 0; page < pages; page++) {
                if (ci.getNullPages().get(page)) {
                    continue;
                }
                byte[] minimum =
                        (byte[]) decodeIndex(ci.getMinValues().get(page), PrimitiveTypeName.BINARY);
                if (previousMinimum != null && Arrays.equals(previousMinimum, minimum)) {
                    equalMinima++;
                }
                if (name.equals("key")
                        && minimum.length < key(rows.get((int) oi.getFirstRowIndex(page))).length) {
                    shortenedMinima++;
                }
                previousMinimum = minimum;
            }
            fact.put("equal_adjacent_min_bounds", equalMinima);
            fact.put("shortened_min_bounds", shortenedMinima);
        }
        return fact;
    }

    private static void validatePageBounds(
            String name,
            PrimitiveTypeName type,
            ColumnIndex ci,
            int page,
            List<ObjectNode> rows,
            int first,
            int last) {
        int nulls = 0;
        Object min = null;
        Object max = null;
        for (int row = first; row <= last; row++) {
            JsonNode value = rows.get(row).get(name);
            if (value.isNull()) {
                nulls++;
                continue;
            }
            Object typed =
                    switch (type) {
                        case BINARY -> value.textValue().getBytes(StandardCharsets.UTF_8);
                        case INT64 -> value.longValue();
                        case BOOLEAN -> value.booleanValue();
                        default -> throw new AssertionError("unsupported canonical type " + type);
                    };
            if (min == null || compare(typed, min) < 0) {
                min = typed;
            }
            if (max == null || compare(typed, max) > 0) {
                max = typed;
            }
        }
        require(
                ci.getNullPages().get(page) == (min == null),
                "null page flag mismatch for " + name);
        if (ci.getNullCounts() != null) {
            require(ci.getNullCounts().get(page) == nulls, "null count mismatch for " + name);
        }
        if (min != null) {
            Object indexedMin = decodeIndex(ci.getMinValues().get(page), type);
            Object indexedMax = decodeIndex(ci.getMaxValues().get(page), type);
            require(
                    compare(indexedMin, min) <= 0 && compare(indexedMax, max) >= 0,
                    "nonconservative page bounds for " + name + " page " + page);
        }
    }

    private static Object decodeIndex(ByteBuffer source, PrimitiveTypeName type) {
        ByteBuffer buffer = source.duplicate().order(java.nio.ByteOrder.LITTLE_ENDIAN);
        return switch (type) {
            case BINARY -> {
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                yield bytes;
            }
            case INT64 -> {
                require(buffer.remaining() == Long.BYTES, "invalid INT64 page bound");
                yield buffer.getLong();
            }
            case BOOLEAN -> {
                require(buffer.remaining() == 1, "invalid boolean page bound");
                yield buffer.get() != 0;
            }
            default -> throw new AssertionError("unsupported index type " + type);
        };
    }

    private static int compare(Object left, Object right) {
        if (left instanceof byte[] bytes) {
            return Arrays.compareUnsigned(bytes, (byte[]) right);
        }
        if (left instanceof Long number) {
            return Long.compare(number, (Long) right);
        }
        return Boolean.compare((Boolean) left, (Boolean) right);
    }

    private static void verifyEntryReader(Fixture fixture, Path file) throws IOException {
        int offset = 0;
        try (ParquetEntryReader reader = new ParquetEntryReader(file)) {
            while (reader.hasNext()) {
                require(offset < fixture.rows().size(), "production entry reader extra row");
                require(
                        row(reader.next()).equals(fixture.rows().get(offset)),
                        "production entry reader mismatch at " + offset);
                offset++;
            }
        }
        require(offset == fixture.rows().size(), "production entry reader missing rows");
    }

    private static Map<String, Object> verifyServing(Fixture fixture, Path file, int missingIndexes)
            throws IOException {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("compatible", false);
        boolean objects =
                fixture.entries().stream().allMatch(entry -> entry instanceof ObjectEntry);
        boolean strict = true;
        for (int i = 1; i < fixture.rows().size(); i++) {
            strict &=
                    Arrays.compareUnsigned(
                                    key(fixture.rows().get(i - 1)), key(fixture.rows().get(i)))
                            < 0;
        }
        if (!objects || !strict) {
            result.put("tested", false);
            result.put("reason", "serving probes require unique OBJECT-only keys");
            return result;
        }
        result.put("tested", true);
        if (missingIndexes != 0) {
            result.put("reason", "missing physical column or offset indexes");
            return result;
        }
        int probes = 0;
        List<SortedParquetIndex.RowGroupSpan> spans = SortedParquetIndex.rowGroupSpans(file);
        long indexedRows = 0;
        int offset = 0;
        try (SortedParquetRowGroupReader reader = new SortedParquetRowGroupReader(file)) {
            for (var span : spans) {
                require(
                        Arrays.equals(span.firstKey(), key(fixture.rows().get(offset))),
                        "index first key mismatch");
                try (var cursor = reader.openKeyCursor(span.blockIndex())) {
                    int seen = 0;
                    while (cursor.hasCurrent()) {
                        require(seen < span.rowCount(), "key cursor extra row");
                        require(
                                Arrays.equals(
                                        cursor.currentKey(),
                                        key(fixture.rows().get(offset + seen))),
                                "production key cursor mismatch at " + (offset + seen));
                        require(
                                cursor.position() == seen,
                                "production key cursor position mismatch");
                        seen++;
                        cursor.advanceTo(cursor.currentKey(), false);
                    }
                    require(seen == span.rowCount(), "production key cursor count mismatch");
                }
                compareObjects(
                        reader.rows(span.blockIndex(), true),
                        fixture.rows(),
                        offset,
                        (int) span.rowCount(),
                        true);
                compareObjects(
                        reader.rows(span.blockIndex(), false),
                        fixture.rows(),
                        offset,
                        (int) span.rowCount(),
                        false);
                int mid = (int) span.rowCount() / 2;
                byte[] from = key(fixture.rows().get(offset + mid));
                int wanted = (int) Math.min(7, span.rowCount() - mid);
                compareObjects(
                        reader.objectRange(span.blockIndex(), from, true, null, wanted, true),
                        fixture.rows(),
                        offset + mid,
                        wanted,
                        true);
                probes++;
                try (var cursor = reader.openKeyCursor(span.blockIndex(), from, true, null)) {
                    // The index prunes pages; the cursor must still advance within a retained page.
                    cursor.advanceTo(from, true);
                    require(
                            cursor.hasCurrent() && Arrays.equals(cursor.currentKey(), from),
                            "bounded key cursor did not land at inclusive lower bound");
                    require(cursor.position() == mid, "bounded key cursor position mismatch");
                    cursor.advanceTo(from, false);
                    if (mid + 1 < span.rowCount()) {
                        require(
                                cursor.hasCurrent()
                                        && Arrays.equals(
                                                cursor.currentKey(),
                                                key(fixture.rows().get(offset + mid + 1))),
                                "exclusive cursor bound mismatch");
                    } else {
                        require(!cursor.hasCurrent(), "exclusive cursor should exhaust group");
                    }
                }
                indexedRows += span.rowCount();
                offset += (int) span.rowCount();
            }
        } catch (RowGroupOrderException incompatible) {
            if (incompatible.row() != -1 || incompatible.getSuppressed().length != 0) {
                throw incompatible;
            }
            // Record the unchanged production guard, including single-page UNORDERED results.
            result.put("reason", incompatible.reason());
            result.put("error_class", incompatible.getClass().getName());
            result.put("message", incompatible.getMessage());
            result.put("row_group", incompatible.rowGroup());
            result.put("row", incompatible.row());
            result.put("completed_range_probes", probes);
            try (ParquetFileReader metadata = ParquetFileReader.open(new LocalInputFile(file))) {
                for (ColumnChunkMetaData column :
                        metadata.getRowGroups().get(incompatible.rowGroup()).getColumns()) {
                    if (column.getPath().toDotString().equals("key")) {
                        ColumnIndex ci = metadata.readColumnIndex(column);
                        OffsetIndex oi = metadata.readOffsetIndex(column);
                        result.put("failed_key_page_count", oi == null ? null : oi.getPageCount());
                        result.put(
                                "failed_key_boundary_order",
                                ci == null ? "ABSENT" : ci.getBoundaryOrder().name());
                    }
                }
            }
            return result;
        }
        require(
                indexedRows == fixture.rows().size(),
                "production row-group index cardinality mismatch");
        try (SortedParquetRangeReader reader = new SortedParquetRangeReader(file, 1)) {
            require(reader.rowGroupCount() == spans.size(), "physical row-group count mismatch");
            if (!fixture.rows().isEmpty()) {
                int[] starts = {0, fixture.rows().size() / 2, fixture.rows().size() - 1};
                for (int start : starts) {
                    int group = groupFor(spans, start);
                    int want = Math.min(9, fixture.rows().size() - start);
                    byte[] from = key(fixture.rows().get(start));
                    byte[] upper =
                            start + want < fixture.rows().size()
                                    ? key(fixture.rows().get(start + want))
                                    : null;
                    // Toggle owner projections on the SAME pooled reader to exercise index cache
                    // priming.
                    compareObjects(
                            reader.range(group, from, true, upper, want + 1, false),
                            fixture.rows(),
                            start,
                            want,
                            false);
                    compareObjects(
                            reader.range(group, from, true, upper, want + 1, true),
                            fixture.rows(),
                            start,
                            want,
                            true);
                    int exclusiveWant = Math.max(0, want - 1);
                    compareObjects(
                            reader.range(group, from, false, upper, want + 1, true),
                            fixture.rows(),
                            start + 1,
                            exclusiveWant,
                            true);
                    // A byte prefix extended with NUL exercises a lower bound between source keys.
                    // Bounds are reader inputs, so this remains valid even for a 1,024-byte source
                    // key.
                    byte[] absent = Arrays.copyOf(from, from.length + 1);
                    int insertion = lowerBound(fixture.rows(), absent);
                    int absentWant = Math.min(9, fixture.rows().size() - insertion);
                    compareObjects(
                            reader.range(group, absent, true, null, 9, true),
                            fixture.rows(),
                            insertion,
                            absentWant,
                            true);
                    probes += 4;
                }
            }
        }
        result.put("compatible", true);
        result.put("completed_range_probes", probes);
        result.put("key_cursor_rows", indexedRows);
        return result;
    }

    private static int groupFor(List<SortedParquetIndex.RowGroupSpan> spans, int row) {
        long start = 0;
        for (var span : spans) {
            if (row < start + span.rowCount()) {
                return span.blockIndex();
            }
            start += span.rowCount();
        }
        throw new AssertionError("no row group for row " + row);
    }

    private static int lowerBound(List<ObjectNode> rows, byte[] target) {
        int low = 0;
        int high = rows.size();
        while (low < high) {
            int middle = (low + high) >>> 1;
            if (Arrays.compareUnsigned(key(rows.get(middle)), target) < 0) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        return low;
    }

    private static void compareObjects(
            List<ObjectRow> actual,
            List<ObjectNode> expected,
            int start,
            int count,
            boolean owner) {
        require(
                actual.size() == count,
                "production object range count mismatch: " + actual.size() + " != " + count);
        for (int i = 0; i < count; i++) {
            ObjectRow observed = actual.get(i);
            ObjectNode row = expected.get(start + i);
            require(Arrays.equals(observed.key(), key(row)), "production object key mismatch");
            require(
                    observed.size() == row.get("size").longValue(),
                    "production object size mismatch");
            require(
                    observed.lastModifiedEpochMicros() == longOrZero(row, "last_modified"),
                    "production object timestamp mismatch");
            require(
                    java.util.Objects.equals(observed.etag(), string(row, "etag")),
                    "production etag mismatch");
            require(
                    java.util.Objects.equals(observed.storageClass(), string(row, "storage_class")),
                    "production storage class mismatch");
            require(
                    java.util.Objects.equals(
                            observed.ownerId(), owner ? string(row, "owner_id") : null),
                    "production owner mismatch");
            require(
                    java.util.Objects.equals(
                            observed.ownerDisplayName(),
                            owner ? string(row, "owner_display_name") : null),
                    "production owner display mismatch");
            require(
                    java.util.Objects.equals(
                            observed.checksumAlgorithm(), string(row, "checksum_algorithm")),
                    "production checksum algorithm mismatch");
            require(
                    java.util.Objects.equals(observed.checksumType(), string(row, "checksum_type")),
                    "production checksum type mismatch");
        }
    }

    private static Fixture load(Path input) throws IOException {
        return load(input, true);
    }

    private static Fixture load(Path input, boolean retainRows) throws IOException {
        List<ObjectNode> rows = new ArrayList<>();
        List<ListEntry> entries = new ArrayList<>();
        ListEntryComparator comparator = new ListEntryComparator();
        try (BufferedReader reader = Files.newBufferedReader(input, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                require(!line.isBlank(), "blank fixture line at " + entries.size());
                JsonNode parsed = JSON.readTree(line);
                require(parsed instanceof ObjectNode, "fixture row must be an object");
                ObjectNode node = (ObjectNode) parsed;
                Set<String> names = new TreeSet<>();
                node.fieldNames().forEachRemaining(names::add);
                require(
                        names.equals(new TreeSet<>(FIELDS)),
                        "fixture field set mismatch at " + entries.size());
                require(
                        node.get("key").isTextual()
                                && node.get("row_type").isTextual()
                                && node.get("is_delete_marker").isBoolean(),
                        "required fixture value type mismatch");
                for (String numeric : List.of("size", "last_modified")) {
                    JsonNode value = node.get(numeric);
                    require(
                            value.isNull() || value.isIntegralNumber() && value.canConvertToLong(),
                            "fixture integer type mismatch for " + numeric);
                    if (!value.isNull()) {
                        node.put(numeric, value.longValue());
                    }
                }
                require(
                        KeyBytes.isValidUtf8(key(node)) && key(node).length <= 1024,
                        "invalid fixture key");
                ListEntry entry = entry(node);
                if (!row(entry).equals(node)) {
                    throw new IllegalArgumentException(
                            "fixture is not representable by current canonical writer: " + node);
                }
                require(
                        entries.isEmpty() || comparator.compare(entries.getLast(), entry) <= 0,
                        "fixture rows are not in canonical byte order");
                if (retainRows) {
                    rows.add(node);
                }
                entries.add(entry);
            }
        }
        return new Fixture(List.copyOf(rows), List.copyOf(entries));
    }

    private static ListEntry entry(ObjectNode row) {
        KeyBytes key = KeyBytes.of(key(row));
        return switch (row.get("row_type").textValue()) {
            case "OBJECT" ->
                    new ObjectEntry(
                            key,
                            row.get("size").longValue(),
                            longOrZero(row, "last_modified"),
                            string(row, "etag"),
                            string(row, "storage_class"),
                            string(row, "version_id"),
                            row.get("is_latest").asBoolean(false),
                            string(row, "owner_id"),
                            string(row, "owner_display_name"),
                            string(row, "checksum_algorithm"),
                            string(row, "checksum_type"));
            case "COMMON_PREFIX" -> new CommonPrefixEntry(key);
            case "DELETE_MARKER" ->
                    new DeleteMarkerEntry(
                            key,
                            string(row, "version_id"),
                            row.get("is_latest").asBoolean(false),
                            longOrZero(row, "last_modified"),
                            string(row, "owner_id"));
            default ->
                    throw new IllegalArgumentException("unknown row_type " + row.get("row_type"));
        };
    }

    private static ObjectNode row(ListEntry entry) {
        ObjectNode row = JSON.createObjectNode();
        FIELDS.forEach(row::putNull);
        row.put("key", entry.key().asString());
        switch (entry) {
            case ObjectEntry object -> {
                row.put("size", object.size());
                if (object.lastModifiedEpochMicros() != 0) {
                    row.put("last_modified", object.lastModifiedEpochMicros());
                }
                row.put("etag", object.etag());
                row.put("storage_class", object.storageClass());
                row.put("version_id", object.versionId());
                if (object.versionId() != null) {
                    row.put("is_latest", object.isLatest());
                }
                row.put("is_delete_marker", false);
                row.put("owner_id", object.ownerId());
                row.put("owner_display_name", object.ownerDisplayName());
                row.put("checksum_algorithm", object.checksumAlgorithm());
                row.put("checksum_type", object.checksumType());
                row.put("row_type", "OBJECT");
            }
            case CommonPrefixEntry ignored -> {
                row.put("is_delete_marker", false);
                row.put("row_type", "COMMON_PREFIX");
            }
            case DeleteMarkerEntry marker -> {
                if (marker.lastModifiedEpochMicros() != 0) {
                    row.put("last_modified", marker.lastModifiedEpochMicros());
                }
                row.put("version_id", marker.versionId());
                row.put("is_latest", marker.isLatest());
                row.put("is_delete_marker", true);
                row.put("owner_id", marker.ownerId());
                row.put("row_type", "DELETE_MARKER");
            }
        }
        return row;
    }

    private static ObjectNode row(Group group, MessageType schema) {
        ObjectNode row = JSON.createObjectNode();
        for (String name : FIELDS) {
            int present = group.getFieldRepetitionCount(name);
            require(present <= 1, "repeated value in canonical field " + name);
            if (present == 0) {
                row.putNull(name);
                continue;
            }
            switch (schema.getType(name).asPrimitiveType().getPrimitiveTypeName()) {
                case BINARY -> {
                    byte[] bytes = group.getBinary(name, 0).getBytes();
                    require(KeyBytes.isValidUtf8(bytes), "invalid UTF8 field " + name);
                    row.put(name, new String(bytes, StandardCharsets.UTF_8));
                }
                case INT64 -> row.put(name, group.getLong(name, 0));
                case BOOLEAN -> row.put(name, group.getBoolean(name, 0));
                default -> throw new AssertionError("unexpected canonical field type");
            }
        }
        return row;
    }

    private static byte[] key(ObjectNode row) {
        return row.get("key").textValue().getBytes(StandardCharsets.UTF_8);
    }

    private static long longOrZero(ObjectNode row, String field) {
        return row.get(field).isNull() ? 0 : row.get(field).longValue();
    }

    private static String string(ObjectNode row, String field) {
        return row.get(field).isNull() ? null : row.get(field).textValue();
    }

    private static void emit(Object value) throws IOException {
        System.out.println(JSON.writeValueAsString(value));
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    private record Fixture(List<ObjectNode> rows, List<ListEntry> entries) {}

    private static final class ColumnTotals {
        private long compressed;
        private long uncompressed;
        private long pages;
        private long dictionaryBytes;
        private final Set<String> encodings = new TreeSet<>();
        private final Set<String> boundaryOrders = new TreeSet<>();

        void add(ColumnChunkMetaData column, ColumnIndex ci, OffsetIndex oi) {
            compressed += column.getTotalSize();
            uncompressed += column.getTotalUncompressedSize();
            pages += oi == null ? 0 : oi.getPageCount();
            if (column.hasDictionaryPage()) {
                dictionaryBytes +=
                        column.getFirstDataPageOffset() - column.getDictionaryPageOffset();
            }
            column.getEncodings().forEach(encoding -> encodings.add(encoding.name()));
            boundaryOrders.add(ci == null ? "ABSENT" : ci.getBoundaryOrder().name());
        }

        Map<String, Object> report() {
            return Map.of(
                    "compressed_bytes",
                    compressed,
                    "uncompressed_bytes",
                    uncompressed,
                    "pages",
                    pages,
                    "dictionary_bytes",
                    dictionaryBytes,
                    "encodings",
                    encodings,
                    "boundary_orders",
                    boundaryOrders);
        }
    }
}

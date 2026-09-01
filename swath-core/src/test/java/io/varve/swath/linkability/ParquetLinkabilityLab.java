/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.linkability;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.output.parquet.ParquetSchema;
import io.varve.swath.output.parquet.PartWriter;
import io.varve.swath.output.parquet.fixture.ParquetEntryReader;
import io.varve.swath.output.parquet.sorted.SortedParquetIndex;
import io.varve.swath.output.parquet.sorted.SortedParquetRangeReader;
import io.varve.swath.output.parquet.sorted.SortedParquetRowGroupReader;
import io.varve.swath.output.parquet.sorted.SortedParquetStamp;
import io.varve.swath.output.parquet.sorted.SortedParquetWriter;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortMode;
import io.varve.swath.sort.SortedFileWriter;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.JarFile;
import java.util.stream.Stream;

/**
 * Standalone PR 7 linkage probe. The Gradle task supplies a runtime with no
 * {@code org.apache.hadoop*} coordinate and forks every operation into a fresh JVM so one failed
 * class initialization cannot contaminate a later edge.
 */
public final class ParquetLinkabilityLab {

    private static final String FORMAT = "swath-parquet-linkability-v1";
    private static final List<String> PARQUET_USING_CLASSES = List.of(
            "io.varve.swath.output.parquet.DigestingOutputFile",
            "io.varve.swath.output.parquet.ListEntryParquetWriters",
            "io.varve.swath.output.parquet.ListEntryWriteSupport",
            "io.varve.swath.output.parquet.ParquetCodecs",
            "io.varve.swath.output.parquet.ParquetDatasetFormat",
            "io.varve.swath.output.parquet.ParquetFiles",
            "io.varve.swath.output.parquet.ParquetSchema",
            "io.varve.swath.output.parquet.ParquetWriterPool",
            "io.varve.swath.output.parquet.PartWriter",
            "io.varve.swath.output.parquet.SyncableLocalOutputFile",
            "io.varve.swath.output.parquet.fixture.ParquetEntryReader",
            "io.varve.swath.output.parquet.sorted.SortedParquetIndex",
            "io.varve.swath.output.parquet.sorted.SortedParquetRangeReader",
            "io.varve.swath.output.parquet.sorted.SortedParquetRowGroupReader",
            "io.varve.swath.output.parquet.sorted.SortedParquetStamp",
            "io.varve.swath.output.parquet.sorted.SortedParquetWriter",
            "org.apache.parquet.SwathReadOptions");
    private static final List<String> PROBES = List.of(
            "classload", "direct_writer", "sorted_writer", "codec", "footer", "index", "segment",
            "range", "row_group", "reader_bridge", "reader_bridge_duckdb");

    private ParquetLinkabilityLab() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 2 && "prepare".equals(args[0])) {
            prepare(Path.of(args[1]));
            return;
        }
        if (args.length == 2 && "baseline".equals(args[0])) {
            baseline(Path.of(args[1]));
            return;
        }
        if (args.length == 5 && "probe".equals(args[0])) {
            probe(args[1], Path.of(args[2]), Path.of(args[3]), Path.of(args[4]));
            return;
        }
        if (args.length == 4 && "child".equals(args[0])) {
            child(args[1], args[2], Path.of(args[3]));
            return;
        }
        throw new IllegalArgumentException(
                "usage: prepare FIXTURE | baseline OUTPUT | probe VERSION FIXTURE OUTPUT SOURCE_ROOT"
                        + " | child VERSION PROBE FIXTURE");
    }

    private static void prepare(Path fixture) throws IOException {
        prepareSwath(fixture);
        prepareDuckdb(fixture, duckdbFixture(fixture));
    }

    private static void prepareSwath(Path fixture) throws IOException {
        Files.createDirectories(fixture.getParent());
        Files.deleteIfExists(fixture);
        SortConfig config = SortConfig.DEFAULT
                .withFinalRowGroupBytes(64L * 1024)
                .withFinalPageRows(128);
        try (SortedFileWriter writer = new SortedParquetWriter(fixture, config, SortMode.OBJECTS, 1)) {
            for (int i = 0; i < 2_048; i++) {
                writer.write(entry(i));
            }
        }
        if (SortedParquetIndex.rowCount(fixture) != 2_048) {
            throw new IllegalStateException("prepared fixture row count changed");
        }
    }

    private static void prepareDuckdb(Path swathFixture, Path duckdbFixture) throws IOException {
        Files.deleteIfExists(duckdbFixture);
        String output = duckdbFixture.toAbsolutePath().toString().replace("'", "''");
        String swath = swathFixture.toAbsolutePath().toString().replace("'", "''");
        try (var connection = DriverManager.getConnection("jdbc:duckdb:");
             var statement = connection.createStatement()) {
            statement.execute("COPY (SELECT encode(printf('key-%08d', range)) AS key"
                    + " FROM range(16)) TO '" + output + "' (FORMAT parquet, COMPRESSION zstd)");
            try (var rows = statement.executeQuery("SELECT count(*) FROM read_parquet('" + swath + "')")) {
                if (!rows.next() || rows.getLong(1) != 2_048) {
                    throw new IllegalStateException("DuckDB could not read the swath fixture");
                }
            }
        } catch (SQLException e) {
            throw new IOException("DuckDB fixture preparation failed", e);
        }
    }

    private static void baseline(Path output) throws Exception {
        Path work = output.getParent().resolve("operation-baseline-work");
        Files.createDirectories(work);
        List<String> lines = new ArrayList<>();
        lines.add(json(
                "probe", "operation_baseline",
                "status", "complete",
                "parquet_version", implementationVersion("org.apache.parquet.hadoop.ParquetFileReader"),
                "java_version", System.getProperty("java.version"),
                "os", System.getProperty("os.name") + " " + System.getProperty("os.arch"),
                "recorded_at", Instant.now().toString()));
        for (int iteration = 0; iteration < 5; iteration++) {
            Path file = work.resolve("iteration-" + iteration + ".parquet");
            Files.deleteIfExists(file);
            long writerStart = System.nanoTime();
            prepareSwath(file);
            long writerNanos = System.nanoTime() - writerStart;

            long footerStart = System.nanoTime();
            SortedParquetStamp stamp = SortedParquetStamp.read(file).orElseThrow();
            long footerNanos = System.nanoTime() - footerStart;

            long indexStart = System.nanoTime();
            List<SortedParquetIndex.RowGroupSpan> spans = SortedParquetIndex.rowGroupSpans(file);
            long indexNanos = System.nanoTime() - indexStart;

            long rangeStart = System.nanoTime();
            int rangeRows;
            try (SortedParquetRangeReader reader = new SortedParquetRangeReader(file, 1)) {
                rangeRows = reader.range(
                        0, KeyBytes.ofUtf8("key-00001000").raw(), true, null, 128, true).size();
            }
            long rangeNanos = System.nanoTime() - rangeStart;

            long rowGroupStart = System.nanoTime();
            int rowGroupRows;
            try (SortedParquetRowGroupReader reader = new SortedParquetRowGroupReader(file)) {
                rowGroupRows = reader.rows(spans.getFirst().blockIndex(), true).size();
            }
            long rowGroupNanos = System.nanoTime() - rowGroupStart;

            long segmentStart = System.nanoTime();
            int segmentRows = segmentRows(file);
            long segmentNanos = System.nanoTime() - segmentStart;

            if (rangeRows != 128 || rowGroupRows == 0 || segmentRows != 2_048
                    || stamp.mode() != SortMode.OBJECTS) {
                throw new IllegalStateException("operation baseline parity failed");
            }
            lines.add(json(
                    "probe", "operation_sample",
                    "status", "ok",
                    "iteration", Integer.toString(iteration),
                    "rows", "2048",
                    "bytes", Long.toString(Files.size(file)),
                    "row_groups", Integer.toString(spans.size()),
                    "range_rows", Integer.toString(rangeRows),
                    "first_row_group_rows", Integer.toString(rowGroupRows),
                    "segment_rows", Integer.toString(segmentRows),
                    "writer_ns", Long.toString(writerNanos),
                    "footer_ns", Long.toString(footerNanos),
                    "index_ns", Long.toString(indexNanos),
                    "range_ns", Long.toString(rangeNanos),
                    "row_group_ns", Long.toString(rowGroupNanos),
                    "segment_ns", Long.toString(segmentNanos),
                    "sha256", sha256(file)));
        }
        Files.createDirectories(output.getParent());
        Files.write(output, lines, StandardCharsets.UTF_8);
        System.out.printf("PARQUET_BASELINE report=%s samples=5%n", output);
    }

    private static void probe(String version, Path fixture, Path output, Path sourceRoot) throws Exception {
        verifyInventory(sourceRoot);
        ClasspathSummary classpath = inspectClasspath();
        Files.createDirectories(output.getParent());
        List<String> results = new ArrayList<>();
        results.add(json(
                "probe", "classpath",
                "status", "ok",
                "parquet_version", version,
                "classpath_entries", Integer.toString(classpath.entries()),
                "classpath_bytes", Long.toString(classpath.bytes()),
                "hadoop_artifacts", "0",
                "hadoop_class_entries", "0"));
        for (String name : PROBES) {
            results.addAll(runChild(version, name, fixture));
        }
        List<String> blocked = results.stream()
                .filter(line -> line.contains("\"status\":\"blocked\""))
                .toList();
        if (blocked.size() != 1 || !blocked.getFirst().contains("\"probe\":\"codec\"")) {
            throw new IllegalStateException(
                    "candidate must block only the deliberately unused Hadoop codec: " + blocked);
        }
        String header = json(
                "probe", "run",
                "status", "complete",
                "parquet_version", version,
                "java_version", System.getProperty("java.version"),
                "os", System.getProperty("os.name") + " " + System.getProperty("os.arch"),
                "fixture_bytes", Long.toString(Files.size(fixture)),
                "fixture_sha256", sha256(fixture),
                "recorded_at", Instant.now().toString());
        List<String> report = new ArrayList<>(results.size() + 1);
        report.add(header);
        report.addAll(results);
        Files.write(output, report, StandardCharsets.UTF_8);
        System.out.printf("PARQUET_LINKABILITY version=%s report=%s probes=%d%n",
                version, output, results.size() - 1);
    }

    private static List<String> runChild(String version, String name, Path fixture) throws Exception {
        Path stdout = Files.createTempFile("swath-parquet-linkability-", ".out");
        Path stderr = Files.createTempFile("swath-parquet-linkability-", ".err");
        try {
            String javaBinary = Path.of(System.getProperty("java.home"), "bin", "java").toString();
            Process process = new ProcessBuilder(
                    javaBinary,
                    "-cp", System.getProperty("java.class.path"),
                    ParquetLinkabilityLab.class.getName(),
                    "child", version, name, fixture.toString())
                    .redirectOutput(stdout.toFile())
                    .redirectError(stderr.toFile())
                    .start();
            if (!process.waitFor(60, java.util.concurrent.TimeUnit.SECONDS)) {
                process.destroyForcibly();
                throw new IllegalStateException("probe timed out: " + name);
            }
            List<String> lines = Files.readAllLines(stdout, StandardCharsets.UTF_8).stream()
                    .filter(line -> line.startsWith("{"))
                    .toList();
            if (process.exitValue() != 0 || lines.isEmpty()) {
                throw new IllegalStateException("probe " + name + " failed to report; exit="
                        + process.exitValue() + " stderr=" + Files.readString(stderr));
            }
            return lines;
        } finally {
            Files.deleteIfExists(stdout);
            Files.deleteIfExists(stderr);
        }
    }

    private static void child(String version, String probe, Path fixture) {
        if ("classload".equals(probe)) {
            for (String className : PARQUET_USING_CLASSES) {
                runOne(version, "classload:" + className, () -> Class.forName(className, true,
                        ParquetLinkabilityLab.class.getClassLoader()));
            }
            return;
        }
        runOne(version, probe, () -> {
            switch (probe) {
                case "direct_writer" -> directWriter(fixture.resolveSibling("direct-" + version + ".parquet"));
                case "sorted_writer" -> sortedWriter(fixture.resolveSibling("sorted-" + version + ".parquet"));
                case "codec" -> codec();
                case "footer" -> footer(fixture);
                case "index" -> index(fixture);
                case "segment" -> segment(fixture);
                case "range" -> range(fixture);
                case "row_group" -> rowGroup(fixture);
                case "reader_bridge" -> readerBridge(fixture);
                case "reader_bridge_duckdb" -> readerBridgeDuckdb(duckdbFixture(fixture));
                default -> throw new IllegalArgumentException("unknown probe " + probe);
            }
        });
    }

    private static void runOne(String version, String probe, CheckedRunnable runnable) {
        long start = System.nanoTime();
        try {
            runnable.run();
            System.out.println(json(
                    "probe", probe,
                    "status", "ok",
                    "parquet_version", version,
                    "elapsed_ns", Long.toString(System.nanoTime() - start)));
        } catch (Throwable failure) {
            Throwable root = deepest(failure);
            System.out.println(json(
                    "probe", probe,
                    "status", "blocked",
                    "parquet_version", version,
                    "elapsed_ns", Long.toString(System.nanoTime() - start),
                    "failure_class", failure.getClass().getName(),
                    "root_class", root.getClass().getName(),
                    "missing_class", missingClass(failure),
                    "failure_at", firstRelevantFrame(failure)));
        }
    }

    private static void directWriter(Path path) throws IOException {
        Files.deleteIfExists(path);
        try (PartWriter writer = new PartWriter(path, ParquetSchema.canonical())) {
            writer.write(entry(1));
        } finally {
            Files.deleteIfExists(path);
        }
    }

    private static void sortedWriter(Path path) throws IOException {
        Files.deleteIfExists(path);
        try (SortedFileWriter writer = new SortedParquetWriter(
                path, SortConfig.DEFAULT, SortMode.OBJECTS, 1)) {
            writer.write(entry(1));
        } finally {
            Files.deleteIfExists(path);
        }
    }

    private static void codec() throws Exception {
        Class<?> type = Class.forName("org.apache.parquet.hadoop.codec.ZstandardCodec", true,
                ParquetLinkabilityLab.class.getClassLoader());
        type.getDeclaredConstructor().newInstance();
    }

    private static void footer(Path fixture) throws IOException {
        if (SortedParquetStamp.read(fixture).isEmpty()) {
            throw new IllegalStateException("fixture lost its sorted stamp");
        }
    }

    private static void index(Path fixture) throws IOException {
        if (SortedParquetIndex.rowGroupSpans(fixture).isEmpty()) {
            throw new IllegalStateException("fixture must have an indexed row group");
        }
    }

    private static void segment(Path fixture) throws Exception {
        if (segmentRows(fixture) != 2_048) {
            throw new IllegalStateException("merge segment reader returned the wrong row count");
        }
    }

    private static int segmentRows(Path fixture) throws Exception {
        try (ParquetEntryReader reader = new ParquetEntryReader(fixture)) {
            int rows = 0;
            while (reader.hasNext()) {
                reader.next();
                rows++;
            }
            return rows;
        }
    }

    private static void range(Path fixture) throws IOException {
        try (SortedParquetRangeReader reader = new SortedParquetRangeReader(fixture, 1)) {
            byte[] from = KeyBytes.ofUtf8("key-00001000").raw();
            if (reader.range(0, from, true, null, 4, true).size() != 4) {
                throw new IllegalStateException("bounded range returned the wrong row count");
            }
        }
    }

    private static void rowGroup(Path fixture) throws IOException {
        int block = SortedParquetIndex.rowGroupSpans(fixture).getFirst().blockIndex();
        try (SortedParquetRowGroupReader reader = new SortedParquetRowGroupReader(fixture);
             SortedParquetRowGroupReader.KeyCursor cursor = reader.openKeyCursor(block)) {
            if (!cursor.hasCurrent() || cursor.currentKey().length == 0 || reader.rows(block, true).isEmpty()) {
                throw new IllegalStateException("row-group reader did not return the fixture rows");
            }
        }
    }

    private static void readerBridge(Path fixture) throws Exception {
        Class<?> bridge = Class.forName("org.apache.parquet.HadoopFreeReadOptionsBridge", true,
                ParquetLinkabilityLab.class.getClassLoader());
        Object result = bridge.getMethod("read", Path.class).invoke(null, fixture);
        if (!"rows=2048,row_groups=1,indexes=true,first_key=key-00000000".equals(result.toString())) {
            throw new IllegalStateException("unexpected bridge result: " + result);
        }
    }

    private static void readerBridgeDuckdb(Path fixture) throws Exception {
        Class<?> bridge = Class.forName("org.apache.parquet.HadoopFreeReadOptionsBridge", true,
                ParquetLinkabilityLab.class.getClassLoader());
        Object result = bridge.getMethod("read", Path.class).invoke(null, fixture);
        String value = result.toString();
        if (!value.startsWith("rows=16,row_groups=1,indexes=")
                || !value.endsWith(",first_key=key-00000000")) {
            throw new IllegalStateException("unexpected DuckDB bridge result: " + result);
        }
    }

    private static Path duckdbFixture(Path fixture) {
        return fixture.resolveSibling("duckdb-fixture.parquet");
    }

    private static ObjectEntry entry(int value) {
        String key = String.format(Locale.ROOT, "key-%08d", value);
        return new ObjectEntry(
                KeyBytes.ofUtf8(key), value, 1_777_777_000_000_000L + value,
                "etag-" + value, "STANDARD", null, true,
                "owner-id", "owner-display", "CRC32", "FULL_OBJECT");
    }

    private static void verifyInventory(Path sourceRoot) throws IOException {
        Set<String> actual = new TreeSet<>();
        try (Stream<Path> paths = Files.walk(sourceRoot)) {
            paths.filter(path -> path.toString().endsWith(".java"))
                    .filter(ParquetLinkabilityLab::importsParquet)
                    .map(path -> className(sourceRoot, path))
                    .forEach(actual::add);
        }
        Set<String> expected = new TreeSet<>(PARQUET_USING_CLASSES);
        if (!actual.equals(expected)) {
            throw new IllegalStateException("Parquet class inventory drifted; expected=" + expected
                    + " actual=" + actual);
        }
    }

    private static boolean importsParquet(Path source) {
        try (BufferedReader lines = Files.newBufferedReader(source, StandardCharsets.UTF_8)) {
            return lines.lines().anyMatch(line -> line.startsWith("import org.apache.parquet."));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String className(Path root, Path source) {
        String relative = root.relativize(source).toString();
        return relative.substring(0, relative.length() - ".java".length())
                .replace(source.getFileSystem().getSeparator(), ".");
    }

    private static ClasspathSummary inspectClasspath() throws IOException {
        int entries = 0;
        long bytes = 0L;
        for (String element : System.getProperty("java.class.path").split(java.io.File.pathSeparator)) {
            Path path = Path.of(element);
            entries++;
            if (Files.isRegularFile(path)) {
                bytes += Files.size(path);
                if (path.toString().endsWith(".jar")) {
                    try (JarFile jar = new JarFile(path.toFile())) {
                        boolean containsHadoop = jar.stream()
                                .anyMatch(entry -> entry.getName().startsWith("org/apache/hadoop/"));
                        if (containsHadoop) {
                            throw new IllegalStateException("Hadoop class entries found in " + path);
                        }
                    }
                }
            } else if (Files.isDirectory(path)) {
                try (Stream<Path> children = Files.walk(path)) {
                    List<Path> files = children.filter(Files::isRegularFile).toList();
                    bytes += files.stream().mapToLong(ParquetLinkabilityLab::size).sum();
                    boolean containsHadoop = files.stream()
                            .map(path::relativize)
                            .map(Path::toString)
                            .map(name -> name.replace(path.getFileSystem().getSeparator(), "/"))
                            .anyMatch(name -> name.startsWith("org/apache/hadoop/"));
                    if (containsHadoop) {
                        throw new IllegalStateException("Hadoop class entries found in " + path);
                    }
                }
            }
        }
        return new ClasspathSummary(entries, bytes);
    }

    private static long size(Path path) {
        try {
            return Files.size(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Throwable deepest(Throwable failure) {
        Throwable current = failure;
        Set<Throwable> seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        while (current.getCause() != null && seen.add(current)) {
            current = current.getCause();
        }
        return current;
    }

    private static String missingClass(Throwable failure) {
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (current instanceof NoClassDefFoundError || current instanceof ClassNotFoundException) {
                String message = current.getMessage();
                return message == null ? "unknown" : message.replace('/', '.');
            }
        }
        return "none";
    }

    private static String firstRelevantFrame(Throwable failure) {
        List<Throwable> causes = new ArrayList<>();
        for (Throwable current = failure; current != null; current = current.getCause()) {
            causes.add(current);
        }
        for (int i = causes.size() - 1; i >= 0; i--) {
            Throwable current = causes.get(i);
            for (StackTraceElement frame : current.getStackTrace()) {
                if (frame.getClassName().startsWith("io.varve.swath.")
                        || frame.getClassName().startsWith("org.apache.parquet.")) {
                    return frame.getClassName() + "." + frame.getMethodName() + ":" + frame.getLineNumber();
                }
            }
        }
        return "unknown";
    }

    private static String sha256(Path path) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (var input = Files.newInputStream(path)) {
                byte[] buffer = new byte[64 * 1024];
                for (int read; (read = input.read(buffer)) >= 0; ) {
                    digest.update(buffer, 0, read);
                }
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("JDK has no SHA-256", impossible);
        }
    }

    private static String implementationVersion(String className) throws ClassNotFoundException {
        Package owner = Class.forName(className).getPackage();
        return owner.getImplementationVersion() == null ? "unknown" : owner.getImplementationVersion();
    }

    private static String json(String... fields) {
        StringBuilder out = new StringBuilder("{\"format\":\"").append(FORMAT).append('"');
        for (int i = 0; i < fields.length; i += 2) {
            out.append(",\"").append(escape(fields[i])).append("\":\"")
                    .append(escape(fields[i + 1])).append('"');
        }
        return out.append('}').toString();
    }

    private static String escape(String value) {
        StringBuilder escaped = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> escaped.append("\\\"");
                case '\\' -> escaped.append("\\\\");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> escaped.append(c);
            }
        }
        return escaped.toString();
    }

    private record ClasspathSummary(int entries, long bytes) {
    }

    @FunctionalInterface
    private interface CheckedRunnable {
        void run() throws Exception;
    }
}

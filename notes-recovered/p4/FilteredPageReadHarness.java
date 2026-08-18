import dev.swath.output.parquet.ParquetSchema;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * P4 experiment harness -- the cold-path successor to the P1 kill (notes/2026-07-04-parquet-expert-
 * perf-ideas.md idea #2's custom-reader tier). Measurement-only, scratch code, no tracked file touched.
 *
 * P1 proved DuckDB's read_parquet ignores the Parquet ColumnIndex/OffsetIndex for a bounded
 * WHERE+ORDER BY+LIMIT scan (still decodes full spanned row groups). This experiment checks whether
 * parquet-java's OWN low-level ColumnIndex-filtered reading path (ParquetFileReader.
 * readNextFilteredRowGroup() with a FilterCompat row-range predicate, available since parquet-mr
 * 1.11) can decode just the matching pages of a ~1000-row window, at a cost far below the P1 triage's
 * earlier "raw single-threaded parquet-java full-row-group decode" measurement (342-842ms) -- the
 * hypothesis being that FULL-row-group decode was the wrong comparison; a PAGE-FILTERED decode of a
 * 1-2 page window should cost single-digit ms.
 */
public final class FilteredPageReadHarness {

    private static final int ROWS = 2_000_000;
    private static final int WINDOW = 1001;
    private static final int TARGETS = 50;
    private static final long SEED = 42L;
    private static final int WARMUPS = 3;
    private static final int REPS = 20;

    public static void main(String[] args) throws Exception {
        Path v1 = Path.of(args.length > 0 ? args[0]
                : "/home/sagi/.claude/jobs/58e2b269/tmp/p1-page-index-gate/data/v1-1mb-page.parquet");
        Path v2 = Path.of(args.length > 1 ? args[1]
                : "/home/sagi/.claude/jobs/58e2b269/tmp/p1-page-index-gate/data/v2-64kb-page.parquet");
        Path scratch = Path.of(args.length > 2 ? args[2] : "/tmp/p4-scratch");
        Files.createDirectories(scratch);

        System.out.println("=== §0.3/I10 hazard check: is parquet-java's BINARY filter comparison signed or unsigned? ===");
        boolean unsignedSafe = checkUnsignedFilterSemantics(scratch);
        if (!unsignedSafe) {
            System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            System.out.println("!! FLAG: parquet-java's BINARY row-group/page filtering is SIGNED, not unsigned. !!");
            System.out.println("!! A production reader on this version would SILENTLY DROP rows for any key    !!");
            System.out.println("!! byte >= 0x80 (contract 01 section 0.3 / I10 violation). Proceeding anyway   !!");
            System.out.println("!! per instructions, but this is a genuine correctness blocker for idea #2's   !!");
            System.out.println("!! custom-reader tier and must not be silently absorbed.                       !!");
            System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        } else {
            System.out.println("PASS: parquet-java " + Binary.class.getPackage().getImplementationVersion()
                    + " BINARY filtering is unsigned-lexicographic-safe (no workaround needed).");
        }

        System.out.println();
        System.out.println("Regenerating ground-truth sorted key list (same recipe as P1/P2) ...");
        List<String> sortedKeys = generateSortedKeys(ROWS);

        Random rnd = new Random(SEED);
        int[] starts = new int[TARGETS];
        for (int i = 0; i < TARGETS; i++) {
            starts[i] = rnd.nextInt(ROWS - WINDOW);
        }

        MessageType full = ParquetSchema.canonical();
        MessageType keyOnly = Types.buildMessage()
                .required(PrimitiveTypeName.BINARY).named("key")
                .named(ParquetSchema.NAME);

        System.out.println();
        System.out.println("=== Correctness: filtered read vs ground truth (all " + TARGETS + " targets) "
                + "+ vs plain full scan (spot-check 3) ===");
        for (Path file : List.of(v1, v2)) {
            for (MessageType requested : List.of(keyOnly, full)) {
                verifyAllTargets(file, requested, sortedKeys, starts);
            }
        }
        verifyAgainstPlainFullScan(v1, full, sortedKeys, starts[0]);
        verifyAgainstPlainFullScan(v1, full, sortedKeys, starts[1]);
        verifyAgainstPlainFullScan(v2, full, sortedKeys, starts[2]);
        System.out.println("All correctness checks passed.");

        System.out.println();
        System.out.println("=== Timing: COLD (fresh reader incl. footer parse) vs WARM (footer pre-parsed/reused) ===");
        for (Path file : List.of(v1, v2)) {
            for (var proj : List.of(new Object[] {"key-only", keyOnly}, new Object[] {"full", full})) {
                String projLabel = (String) proj[0];
                MessageType requested = (MessageType) proj[1];
                measure(file, requested, sortedKeys, starts, projLabel);
            }
        }
    }

    // ---- key generation (identical recipe to P1/P2) ----

    private static List<String> generateSortedKeys(int rows) {
        List<String> keys = new ArrayList<>(rows);
        for (int i = 0; i < rows; i++) {
            int month = (i % 12) + 1;
            int day = ((i / 12) % 28) + 1;
            long tail = splitmix64(i);
            keys.add(String.format("bucket-data/year=2024/month=%02d/day=%02d/%016x.dat", month, day, tail));
        }
        Collections.sort(keys);
        return keys;
    }

    private static long splitmix64(long index) {
        long z = index * 0x9E3779B97F4A7C15L;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    // ---- §0.3/I10 hazard check: does parquet-java's row-group/page filtering use unsigned byte order? ----

    /**
     * Writes a tiny 3-row file with single-byte BINARY keys 0x01, 0x7F, 0xFF (0xFF flips sign under a
     * SIGNED byte comparator: (byte)0xFF == -1, which is NOT &gt; 0x01 under signed comparison but IS
     * &gt; 0x01 (255 &gt; 1) under unsigned comparison -- the exact contract 01 §0.3/I10 hazard). Runs
     * the filter {@code key > 0x01} through the SAME end-to-end path (FilterApi + ParquetFileReader +
     * readNextFilteredRowGroup) the real experiment uses, and checks whether the 0xFF row survives.
     * A signed bug would SILENTLY DROP that row at the row-group/page filter level, before our own
     * client-side re-check ever sees it -- this is why the check must be end-to-end, not a unit test of
     * {@link Binary#compareTo}.
     */
    private static boolean checkUnsignedFilterSemantics(Path scratch) throws IOException {
        Path file = scratch.resolve("hazard-check.parquet");
        MessageType schema = Types.buildMessage()
                .required(PrimitiveTypeName.BINARY).named("k")
                .named("hazard_check");
        GroupWriteSupport.setSchema(schema, new Configuration(false));
        try (ParquetWriter<org.apache.parquet.example.data.Group> writer =
                     ExampleParquetWriter.builder(new LocalOutputFile(file))
                             .withType(schema)
                             .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                             .withDictionaryEncoding(false)
                             .build()) {
            var factory = new org.apache.parquet.example.data.simple.SimpleGroupFactory(schema);
            writer.write(factory.newGroup().append("k", Binary.fromConstantByteArray(new byte[] {0x01})));
            writer.write(factory.newGroup().append("k", Binary.fromConstantByteArray(new byte[] {0x7F})));
            writer.write(factory.newGroup().append("k", Binary.fromConstantByteArray(new byte[] {(byte) 0xFF})));
        }

        Operators.BinaryColumn col = FilterApi.binaryColumn("k");
        FilterPredicate predicate = FilterApi.gt(col, Binary.fromConstantByteArray(new byte[] {0x01}));
        ParquetReadOptions options = ParquetReadOptions.builder()
                .useColumnIndexFilter(true)
                .useStatsFilter(true)
                .useDictionaryFilter(true)
                .useRecordFilter(true)
                .withRecordFilter(FilterCompat.get(predicate))
                .build();

        List<byte[]> survivors = new ArrayList<>();
        try (ParquetFileReader reader = new ParquetFileReader(new LocalInputFile(file), options)) {
            reader.setRequestedSchema(schema);
            MessageColumnIO columnIo = new ColumnIOFactory().getColumnIO(schema, schema);
            PageReadStore pages;
            while ((pages = reader.readNextFilteredRowGroup()) != null) {
                RecordReader<Group> rr = columnIo.getRecordReader(pages, new GroupRecordConverter(schema));
                long n = pages.getRowCount();
                for (long i = 0; i < n; i++) {
                    Group g = rr.read();
                    byte[] k = g.getBinary("k", 0).getBytes();
                    if (compareUnsigned(k, new byte[] {0x01}) > 0) { // our own ground-truth unsigned check
                        survivors.add(k);
                    }
                }
            }
        }
        boolean found0x7f = survivors.stream().anyMatch(b -> b.length == 1 && b[0] == 0x7F);
        boolean found0xff = survivors.stream().anyMatch(b -> b.length == 1 && (b[0] & 0xFF) == 0xFF);
        System.out.println("  hazard-check filtered survivors (via readNextFilteredRowGroup, our own unsigned re-check): "
                + survivors.size() + " (expect 2: 0x7F and 0xFF)");
        System.out.println("  0x7F present=" + found0x7f + "  0xFF present=" + found0xff
                + "  (0xFF missing would mean parquet's own row-group/page filter dropped it BEFORE we ever saw it -- signed bug)");
        return found0x7f && found0xff;
    }

    private static int compareUnsigned(byte[] a, byte[] b) {
        return java.util.Arrays.compareUnsigned(a, b);
    }

    // ---- correctness ----

    private static void verifyAllTargets(Path file, MessageType requested, List<String> sortedKeys, int[] starts)
            throws IOException {
        for (int start : starts) {
            List<String> expected = sortedKeys.subList(start, start + WINDOW);
            List<String> actual = readWindow(file, requested, sortedKeys, start);
            if (!actual.equals(expected)) {
                throw new IllegalStateException("MISMATCH at start=" + start + " file=" + file
                        + " expected.size=" + expected.size() + " actual.size=" + actual.size()
                        + " first-expected=" + expected.get(0) + " first-actual="
                        + (actual.isEmpty() ? "<empty>" : actual.get(0)));
            }
        }
        System.out.println("  OK: " + file.getFileName() + " projection=" + requested.getName()
                + " -- all " + starts.length + " targets matched ground truth exactly.");
    }

    /** Independent cross-check: decode the SAME window via a plain, unfiltered sequential row-group scan
     *  (mirrors SegmentReader's pattern with no filter at all), slice to [start, start+WINDOW), and
     *  compare against the filtered read -- anchors correctness against parquet-java's own unfiltered
     *  path, not just our in-memory ground truth. */
    private static void verifyAgainstPlainFullScan(Path file, MessageType full, List<String> sortedKeys, int start)
            throws IOException {
        List<String> filtered = readWindow(file, full, sortedKeys, start);
        List<String> plain = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            MessageType fileSchema = reader.getFooter().getFileMetaData().getSchema();
            MessageColumnIO columnIo = new ColumnIOFactory().getColumnIO(fileSchema);
            long consumed = 0;
            PageReadStore pages;
            while ((pages = reader.readNextRowGroup()) != null) {
                long n = pages.getRowCount();
                RecordReader<Group> rr = columnIo.getRecordReader(pages, new GroupRecordConverter(fileSchema));
                for (long i = 0; i < n; i++) {
                    Group g = rr.read();
                    if (consumed >= start && consumed < start + WINDOW) {
                        plain.add(new String(g.getBinary("key", 0).getBytes(), StandardCharsets.UTF_8));
                    }
                    consumed++;
                    if (consumed >= start + WINDOW) {
                        break;
                    }
                }
                if (consumed >= start + WINDOW) {
                    break;
                }
            }
        }
        if (!filtered.equals(plain)) {
            throw new IllegalStateException("plain-full-scan cross-check MISMATCH at start=" + start
                    + " file=" + file);
        }
        System.out.println("  OK: " + file.getFileName() + " start=" + start
                + " -- filtered read matches an independent plain full scan (" + plain.size() + " rows).");
    }

    /** Decodes a window via the ColumnIndex-filtered path, keeping only rows whose key falls inside
     *  [K, K') (page-level filtering only guarantees pages MIGHT overlap, not that every decoded row
     *  is in range -- boundary pages can carry a few extra rows we must discard ourselves), stopping
     *  once WINDOW matches are collected. */
    private static List<String> readWindow(Path file, MessageType requested, List<String> sortedKeys, int start)
            throws IOException {
        String lower = sortedKeys.get(start);
        String upper = sortedKeys.get(start + WINDOW); // exclusive bound, one past the window
        FilterPredicate predicate = buildRangePredicate(lower, upper);
        ParquetReadOptions options = ParquetReadOptions.builder()
                .useColumnIndexFilter(true)
                .useStatsFilter(true)
                .useDictionaryFilter(true)
                .useRecordFilter(true)
                .withRecordFilter(FilterCompat.get(predicate))
                .build();
        try (ParquetFileReader reader = new ParquetFileReader(new LocalInputFile(file), options)) {
            return decode(reader, requested, lower, upper);
        }
    }

    private static FilterPredicate buildRangePredicate(String lower, String upper) {
        Operators.BinaryColumn col = FilterApi.binaryColumn("key");
        FilterPredicate ge = FilterApi.gtEq(col, Binary.fromConstantByteArray(lower.getBytes(StandardCharsets.UTF_8)));
        FilterPredicate lt = FilterApi.lt(col, Binary.fromConstantByteArray(upper.getBytes(StandardCharsets.UTF_8)));
        return FilterApi.and(ge, lt);
    }

    private static List<String> decode(ParquetFileReader reader, MessageType requested, String lower, String upper)
            throws IOException {
        reader.setRequestedSchema(requested);
        MessageType fileSchema = reader.getFooter().getFileMetaData().getSchema();
        MessageColumnIO columnIo = new ColumnIOFactory().getColumnIO(requested, fileSchema);
        return decode(reader, columnIo, requested, lower, upper);
    }

    /** As {@link #decode(ParquetFileReader, MessageType, String, String)}, but with a PRE-BUILT
     *  {@code columnIo} (schema materialization + column-IO construction are pure functions of
     *  (file, requestedSchema) -- a real server builds these ONCE per open file, never per request;
     *  reusing them here avoids double-counting that one-time cost into every timed lookup). */
    private static List<String> decode(ParquetFileReader reader, MessageColumnIO columnIo, MessageType requested,
                                       String lower, String upper) throws IOException {
        reader.setRequestedSchema(requested);
        List<String> out = new ArrayList<>(WINDOW);
        PageReadStore pages;
        while (out.size() < WINDOW && (pages = reader.readNextFilteredRowGroup()) != null) {
            long n = pages.getRowCount();
            RecordReader<Group> rr = columnIo.getRecordReader(pages, new GroupRecordConverter(requested));
            for (long i = 0; i < n && out.size() < WINDOW; i++) {
                Group g = rr.read();
                String key = new String(g.getBinary("key", 0).getBytes(), StandardCharsets.UTF_8);
                if (key.compareTo(lower) >= 0 && key.compareTo(upper) < 0) { // ASCII-safe: see P2/P1 notes
                    out.add(key);
                }
            }
        }
        return out;
    }

    /** {@code {filteredRowGroupMs, materializeMs}}: the 3rd split, breaking "decode" itself into
     *  (a) {@code readNextFilteredRowGroup()} -- column-index read/parse + RowRanges computation +
     *  PageReadStore construction -- vs (b) the actual per-row {@code RecordReader.read()} materialize
     *  loop. Answers "is the ColumnIndex machinery itself, not the page decode, the cost at fine page
     *  granularity?". */
    private static double[] decodeTimedSplit(ParquetFileReader reader, MessageColumnIO columnIo,
                                             MessageType requested, String lower, String upper,
                                             int expectedRows) throws IOException {
        reader.setRequestedSchema(requested);
        long filteredNanos = 0;
        long materializeNanos = 0;
        int collected = 0;
        while (collected < expectedRows) {
            long a0 = System.nanoTime();
            PageReadStore pages = reader.readNextFilteredRowGroup();
            long a1 = System.nanoTime();
            filteredNanos += (a1 - a0);
            if (pages == null) {
                break;
            }
            long n = pages.getRowCount();
            RecordReader<Group> rr = columnIo.getRecordReader(pages, new GroupRecordConverter(requested));
            long b0 = System.nanoTime();
            for (long i = 0; i < n && collected < expectedRows; i++) {
                Group g = rr.read();
                String key = new String(g.getBinary("key", 0).getBytes(), StandardCharsets.UTF_8);
                if (key.compareTo(lower) >= 0 && key.compareTo(upper) < 0) {
                    collected++;
                }
            }
            long b1 = System.nanoTime();
            materializeNanos += (b1 - b0);
        }
        if (collected != expectedRows) {
            throw new IllegalStateException("split-timing lookup returned " + collected + " rows, expected "
                    + expectedRows);
        }
        return new double[] {filteredNanos / 1_000_000.0, materializeNanos / 1_000_000.0};
    }

    // ---- timing ----

    private static void measure(Path file, MessageType requested, List<String> sortedKeys, int[] starts,
                                String projLabel) throws IOException {
        // fileSchema + columnIo are pure functions of (file, requestedSchema) -- a real server builds
        // these ONCE per open file, never per request. Building them once here (outside the timed
        // loop) avoids double-counting that one-time cost into every timed lookup (a bug in the first
        // pass of this harness: schema materialization alone turned out to cost ~4ms, which was being
        // silently re-paid on every single rep inside "decode").
        MessageType fileSchema;
        try (ParquetFileReader r = ParquetFileReader.open(new LocalInputFile(file))) {
            fileSchema = r.getFooter().getFileMetaData().getSchema();
        }
        MessageColumnIO columnIo = new ColumnIOFactory().getColumnIO(requested, fileSchema);

        // COLD: fresh reader per lookup, timed end-to-end (footer parse + row-group filter), THEN a
        // 3-way split of the read itself: readNextFilteredRowGroup() (column-index read/parse +
        // RowRanges computation + PageReadStore construction) vs the actual per-row materialize loop.
        for (int i = 0; i < WARMUPS; i++) {
            timeOnce(file, columnIo, requested, sortedKeys, starts[i % starts.length]);
        }
        List<double[]> coldSamples = new ArrayList<>(REPS); // [openMs, filteredRowGroupMs, materializeMs]
        for (int i = 0; i < REPS; i++) {
            coldSamples.add(timeOnce(file, columnIo, requested, sortedKeys, starts[i % starts.length]));
        }
        report(file.getFileName() + " [" + projLabel + "] COLD (reopen+filter; columnIo/schema cached once)",
                coldSamples);

        // "Footer parse only" isolation: a server-realistic footer cache would amortize exactly this
        // part to ~0 after the first request. (The (Configuration, Path, ParquetMetadata,
        // ParquetReadOptions) constructor that would let us literally REUSE a pre-parsed footer across
        // lookups requires org.apache.hadoop.fs.Path -> HadoopInputFile -> UserGroupInformation, which
        // throws on this JDK -- see the FLAG note below. Isolating the footer-parse cost by itself,
        // hadoop-free, gives the same "what would a footer cache save you" answer without that landmine.)
        for (int i = 0; i < WARMUPS; i++) {
            footerOnly(file);
        }
        List<Double> footerMs = new ArrayList<>(REPS);
        for (int i = 0; i < REPS; i++) {
            footerMs.add(footerOnly(file));
        }
        double avgOpen = avg(toList(coldSamples, 0));
        double avgFooter = avg(footerMs);
        System.out.printf("%-70s footer-parse-only avg=%6.3fms  (of COLD's open avg=%6.3fms; "
                        + "row-group-filter-only ~= %6.3fms)%n",
                file.getFileName() + " [" + projLabel + "]", avgFooter, avgOpen, Math.max(0, avgOpen - avgFooter));
        System.out.println("  NOTE: a true 'reuse a pre-parsed footer across many lookups' measurement "
                + "was NOT possible -- parquet-java's ParquetMetadata-reuse constructor requires "
                + "org.apache.hadoop.fs.Path (-> HadoopInputFile -> UserGroupInformation.getCurrentUser()), "
                + "which throws java.lang.UnsupportedOperationException: getSubject is not supported on "
                + "this JDK 25 (Subject.getSubject was hard-disabled by the Security Manager removal, "
                + "JEP 486) -- an environment/library-version issue, not a research-question issue. "
                + "footer-parse-only (above) is the closest measurable proxy for what a footer cache saves.");
    }

    private static List<Double> toList(List<double[]> samples, int idx) {
        List<Double> out = new ArrayList<>(samples.size());
        for (double[] s : samples) {
            out.add(s[idx]);
        }
        return out;
    }

    /** Pure footer read+parse cost (no filter, no requestedSchema) -- isolates what a footer cache
     *  would amortize to ~0, hadoop-free. */
    private static double footerOnly(Path file) throws IOException {
        long t0 = System.nanoTime();
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            reader.getFooter().getFileMetaData().getSchema(); // force full materialization, not just the object ref
        }
        return (System.nanoTime() - t0) / 1_000_000.0;
    }

    /** Returns {openMs, filteredRowGroupMs, materializeMs} for one lookup: fresh ParquetFileReader
     *  (footer parse + row-group filter via the constructor), then the 3-way-split filtered read
     *  (columnIo/fileSchema reused, built once by the caller). */
    private static double[] timeOnce(Path file, MessageColumnIO columnIo, MessageType requested,
                                     List<String> sortedKeys, int start) throws IOException {
        String lower = sortedKeys.get(start);
        String upper = sortedKeys.get(start + WINDOW);
        FilterPredicate predicate = buildRangePredicate(lower, upper);
        ParquetReadOptions options = ParquetReadOptions.builder()
                .useColumnIndexFilter(true)
                .useStatsFilter(true)
                .useDictionaryFilter(true)
                .useRecordFilter(true)
                .withRecordFilter(FilterCompat.get(predicate))
                .build();

        long t0 = System.nanoTime();
        ParquetFileReader reader = new ParquetFileReader(new LocalInputFile(file), options);
        long t1 = System.nanoTime();
        double[] split;
        try {
            split = decodeTimedSplit(reader, columnIo, requested, lower, upper, WINDOW);
        } finally {
            reader.close();
        }
        return new double[] {(t1 - t0) / 1_000_000.0, split[0], split[1]};
    }

    private static void report(String label, List<double[]> samples) {
        List<Double> openMs = new ArrayList<>();
        List<Double> filteredMs = new ArrayList<>();
        List<Double> materializeMs = new ArrayList<>();
        List<Double> totalMs = new ArrayList<>();
        for (double[] s : samples) {
            openMs.add(s[0]);
            filteredMs.add(s[1]);
            materializeMs.add(s[2]);
            totalMs.add(s[0] + s[1] + s[2]);
        }
        System.out.printf(
                "%-70s total avg=%7.3fms p50=%7.3fms p99=%7.3fms  |  open avg=%6.3fms  "
                        + "readNextFilteredRowGroup avg=%6.3fms  materialize avg=%6.3fms%n",
                label, avg(totalMs), pct(totalMs, 0.50), pct(totalMs, 0.99), avg(openMs), avg(filteredMs),
                avg(materializeMs));
    }

    private static double avg(List<Double> xs) {
        return xs.stream().mapToDouble(Double::doubleValue).average().orElseThrow();
    }

    private static double pct(List<Double> xs, double p) {
        List<Double> sorted = new ArrayList<>(xs);
        Collections.sort(sorted);
        int idx = (int) Math.max(0, Math.min(sorted.size() - 1, Math.ceil(p * sorted.size()) - 1));
        return sorted.get(idx);
    }
}

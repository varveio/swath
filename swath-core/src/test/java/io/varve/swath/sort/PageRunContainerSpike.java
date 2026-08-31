/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import com.sun.management.ThreadMXBean;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/** Runnable measurement harness for the PR 6b container decision spike. */
public final class PageRunContainerSpike {

    private static final int SEGMENTS = 8;
    private static final int PAGE_ROWS = 1000;
    private static final int WRITER_PAGES = 4;
    private static final ListEntryComparator CMP = new ListEntryComparator();
    private static final ThreadMXBean THREADS =
            (ThreadMXBean) ManagementFactory.getThreadMXBean();

    private PageRunContainerSpike() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("usage: measure <dir> [rows] | merge <arm> <dir> <rows> | inspect <file> | deep <dir> [rows] | writerheap <dir>");
        }
        switch (args[0]) {
            case "measure" -> measure(Path.of(args[1]),
                    args.length > 2 ? Integer.parseInt(args[2]) : 1_000_000);
            case "merge" -> mergeChild(Arm.of(args[1]), Path.of(args[2]),
                    Integer.parseInt(args[3]),
                    args.length > 4 ? Integer.parseInt(args[4]) : 1);
            case "inspect" -> System.out.print(AvroPageRunContainer.inspect(Path.of(args[1])));
            case "deep" -> deep(Path.of(args[1]),
                    args.length > 2 ? Integer.parseInt(args[2]) : 1_000_000);
            case "writerheap" -> writerHeap(Path.of(args[1]));
            default -> throw new IllegalArgumentException("unknown mode " + args[0]);
        }
    }

    private static void measure(Path directory, int rows) throws Exception {
        if (rows <= 0 || rows % (SEGMENTS * PAGE_ROWS) != 0) {
            throw new IllegalArgumentException("rows must be positive and divisible by "
                    + (SEGMENTS * PAGE_ROWS));
        }
        Files.createDirectories(directory);
        warmContainerClasses(directory);
        System.out.printf(Locale.ROOT,
                "PROTOCOL rows=%d segments=%d rowsPerPage=%d pageCodec=LZ4 avroCodec=null avgKeyBytes=%.1f%n",
                rows, SEGMENTS, PAGE_ROWS, averageKeyBytes(rows));

        List<WriterMeasurement> writers = new ArrayList<>();
        for (int concurrency : List.of(8, 64)) {
            writers.add(measureWriters(Arm.CUSTOM, concurrency, directory));
            writers.add(measureWriters(Arm.AVRO, concurrency, directory));
        }
        System.out.println("WRITERS arm concurrency retainedHeapBytesPerOpen cpuMsPerFlush allocatedBytesPerFlush payloadBytesPerFlush containerAllocationBytesPerFlush");
        for (WriterMeasurement writer : writers) {
            System.out.printf(Locale.ROOT, "WRITERS %s %d %d %.3f %d %d %d%n",
                    writer.arm().label, writer.concurrency(), writer.retainedHeapBytesPerOpen(),
                    writer.cpuNanosPerFlush() / 1_000_000.0, writer.allocatedBytesPerFlush(),
                    writer.payloadBytesPerFlush(), writer.containerAllocationBytesPerFlush());
        }

        prepareCorpus(directory, rows);
        long customBytes = corpusBytes(directory, Arm.CUSTOM);
        long avroBytes = corpusBytes(directory, Arm.AVRO);
        long customExtensionBytes = customExtensionBytes(directory);
        System.out.printf(Locale.ROOT, "DISK customBytes=%d customTrailerExtensionBytes=%d avroBytes=%d deltaBytes=%d deltaPercent=%.4f%n",
                customBytes, customExtensionBytes, avroBytes, avroBytes - customBytes,
                100.0 * (avroBytes - customBytes) / customBytes);

        System.out.println("MERGES sample arm wallMs rssPeakKiB rows digest");
        Map<Arm, Integer> samples = new HashMap<>();
        for (Arm arm : List.of(Arm.CUSTOM, Arm.AVRO, Arm.AVRO, Arm.CUSTOM, Arm.CUSTOM, Arm.AVRO)) {
            int sample = samples.merge(arm, 1, Integer::sum);
            MergeMeasurement result = runMergeChild(arm, directory, rows);
            System.out.printf(Locale.ROOT, "MERGES %d %s %.3f %d %d %d%n", sample,
                    arm.label, result.wallNanos() / 1_000_000.0, result.rssPeakKiB(),
                    result.rows(), result.digest());
        }
        Path inspect = segmentPath(directory, Arm.AVRO, 0);
        System.out.println("INSPECT_COMMAND ./gradlew :swath-core:pr6bSpike -PspikeArgs='inspect "
                + inspect + "'");
        System.out.print(AvroPageRunContainer.inspect(inspect));
    }

    // ------------------------------------------------------------ deep dive

    private static final List<Arm> DEEP_ARMS = List.of(
            Arm.CUSTOM, Arm.AVRO, Arm.AVRO_REUSE, Arm.AVRO_RAW, Arm.AVRO_RAW2);

    private static void deep(Path directory, int rows) throws Exception {
        if (rows <= 0 || rows % (SEGMENTS * PAGE_ROWS) != 0) {
            throw new IllegalArgumentException("rows must be divisible by "
                    + (SEGMENTS * PAGE_ROWS));
        }
        Files.createDirectories(directory);
        warmContainerClasses(directory);
        prepareCorpus(directory, rows);
        prepareReorderedCorpus(directory, rows);
        System.out.printf(Locale.ROOT, "DEEP_PROTOCOL rows=%d segments=%d rowsPerPage=%d%n",
                rows, SEGMENTS, PAGE_ROWS);
        System.out.println("DEEP_DISK arm bytes");
        for (Arm arm : DEEP_ARMS) {
            System.out.printf(Locale.ROOT, "DEEP_DISK %s %d%n", arm.label,
                    corpusBytes(directory, arm));
        }

        System.out.println("DEEP_MERGE order arm repeat wallMs headerMs bodyMs rssPeakKiB rows digest");
        // Round-robin so no arm systematically owns the warm page cache; then a reversed pass.
        for (int round = 1; round <= 3; round++) {
            List<Arm> order = new ArrayList<>(DEEP_ARMS);
            if (round % 2 == 0) {
                java.util.Collections.reverse(order);
            }
            for (Arm arm : order) {
                for (MergeMeasurement result : runMergeChild(arm, directory, rows, 3)) {
                    System.out.printf(Locale.ROOT, "DEEP_MERGE %d %s %d %.3f %.3f %.3f %d %d %d%n",
                            round, arm.label, result.repeat(), result.wallNanos() / 1e6,
                            result.headerNanos() / 1e6, result.bodyNanos() / 1e6,
                            result.rssPeakKiB(), result.rows(), result.digest());
                }
            }
        }
    }

    private static void prepareReorderedCorpus(Path directory, int rows) throws IOException {
        int perSegment = rows / SEGMENTS;
        for (int segment = 0; segment < SEGMENTS; segment++) {
            List<PageBlock> pages = new ArrayList<>(perSegment / PAGE_ROWS);
            for (int offset = 0; offset < perSegment; offset += PAGE_ROWS) {
                List<ListEntry> page = new ArrayList<>(PAGE_ROWS);
                for (int row = 0; row < PAGE_ROWS; row++) {
                    page.add(object(segment + (long) (offset + row) * SEGMENTS));
                }
                pages.add(PageBlock.pack(page, CMP, PageCodec.LZ4));
            }
            try (AvroPageRunVariants.Writer writer = new AvroPageRunVariants.Writer(
                    segmentPath(directory, Arm.AVRO_RAW2, segment), PageCodec.LZ4, true, 64)) {
                for (PageBlock page : pages) {
                    writer.append(page);
                }
                writer.seal();
            }
        }
    }

    // -------------------------------------------------- writer heap deep dive

    private static void writerHeap(Path directory) throws Exception {
        Files.createDirectories(directory);
        warmContainerClasses(directory);
        List<PageBlock> pages = writerPages();
        System.out.println("HEAP shape concurrency state heapBytesPerOpen");
        for (int concurrency : List.of(8, 64)) {
            for (boolean afterAppend : List.of(false, true)) {
                System.out.printf(Locale.ROOT, "HEAP %s %d %s %d%n", "custom", concurrency,
                        afterAppend ? "after-append" : "just-opened",
                        openHeap(directory, concurrency, afterAppend, pages,
                                (path) -> PageRunSegmentEncoder.open(
                                        path, SortMetrics.NO_OP, null, SortMode.OBJECTS)));
                System.out.printf(Locale.ROOT, "HEAP %s %d %s %d%n", "avro-default-sync",
                        concurrency, afterAppend ? "after-append" : "just-opened",
                        openHeap(directory, concurrency, afterAppend, pages,
                                (path) -> AvroPageRunContainer.openWriter(path, PageCodec.LZ4)));
                System.out.printf(Locale.ROOT, "HEAP %s %d %s %d%n", "avro-sync64", concurrency,
                        afterAppend ? "after-append" : "just-opened",
                        openHeap(directory, concurrency, afterAppend, pages,
                                (path) -> new AvroPageRunVariants.Writer(
                                        path, PageCodec.LZ4, false, 64)));
            }
        }
    }

    private interface WriterFactory {
        AutoCloseable open(Path path) throws IOException;
    }

    private static long openHeap(Path directory, int count, boolean afterAppend,
            List<PageBlock> pages, WriterFactory factory) throws Exception {
        forceGc();
        long baseline = usedHeap();
        List<AutoCloseable> open = new ArrayList<>();
        try {
            for (int index = 0; index < count; index++) {
                Path path = directory.resolve("heap-" + count + "-" + index + "-"
                        + System.nanoTime());
                AutoCloseable writer = factory.open(path);
                open.add(writer);
                if (afterAppend) {
                    appendOne(writer, pages.getFirst());
                }
            }
            forceGc();
            return Math.max(0, usedHeap() - baseline) / count;
        } finally {
            for (AutoCloseable writer : open) {
                try {
                    writer.close();
                } catch (Exception ignored) {
                    // measurement teardown
                }
            }
        }
    }

    private static void appendOne(AutoCloseable writer, PageBlock page) throws IOException {
        switch (writer) {
            case PageRunSegmentEncoder encoder -> encoder.append(page);
            case AvroPageRunContainer.Writer avro -> avro.append(page);
            case AvroPageRunVariants.Writer avro -> avro.append(page);
            default -> throw new IllegalStateException("unknown writer " + writer);
        }
    }

    private static void warmContainerClasses(Path directory) throws IOException {
        List<PageBlock> page = List.of(PageBlock.pack(
                List.of(object(0)), CMP, PageCodec.LZ4));
        Path custom = directory.resolve("warm.pageseg");
        try (PageRunSegmentEncoder writer = PageRunSegmentEncoder.open(
                custom, SortMetrics.NO_OP, null, SortMode.OBJECTS)) {
            writer.append(page.getFirst());
            writer.finish(SegmentKind.CASCADE_INTERMEDIATE);
        }
        Path avro = directory.resolve("warm.avro");
        try (AvroPageRunContainer.Writer writer =
                AvroPageRunContainer.openWriter(avro, PageCodec.LZ4)) {
            writer.append(page.getFirst());
            writer.seal();
        }
        try (PageRunSegmentIo ignored = PageRunSegmentIo.open(custom, SortMetrics.NO_OP);
                AvroPageRunContainer.Reader ignoredAvro =
                        AvroPageRunContainer.openReader(avro)) {
            // class initialization only
        }
        List<PageBlock> writerPages = writerPages();
        flush(Arm.CUSTOM, directory.resolve("warm-flush.pageseg"), writerPages);
        flush(Arm.AVRO, directory.resolve("warm-flush.avro"), writerPages);
    }

    private static WriterMeasurement measureWriters(Arm arm, int concurrency, Path directory)
            throws Exception {
        long retained = retainedOpenWriterHeap(arm, concurrency, directory);
        List<PageBlock> pages = writerPages();
        long payloadBytes = pages.stream().mapToLong(page -> page.serialize().length).sum();
        CountDownLatch ready = new CountDownLatch(concurrency);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        List<Future<ThreadCost>> futures = new ArrayList<>();
        try {
            for (int index = 0; index < concurrency; index++) {
                int writerIndex = index;
                futures.add(executor.submit(() -> {
                    ready.countDown();
                    start.await();
                    long thread = Thread.currentThread().threadId();
                    long cpuBefore = THREADS.getThreadCpuTime(thread);
                    long allocationBefore = THREADS.getThreadAllocatedBytes(thread);
                    flush(arm, directory.resolve("writers-" + arm.label + "-" + concurrency
                            + "-" + writerIndex), pages);
                    return new ThreadCost(THREADS.getThreadCpuTime(thread) - cpuBefore,
                            THREADS.getThreadAllocatedBytes(thread) - allocationBefore);
                }));
            }
            ready.await();
            start.countDown();
            long cpu = 0;
            long allocated = 0;
            for (Future<ThreadCost> future : futures) {
                ThreadCost cost = future.get();
                cpu += cost.cpuNanos();
                allocated += cost.allocatedBytes();
            }
            long allocationPerFlush = allocated / concurrency;
            return new WriterMeasurement(arm, concurrency, retained, cpu / concurrency,
                    allocationPerFlush, payloadBytes, allocationPerFlush - payloadBytes);
        } finally {
            executor.shutdownNow();
        }
    }

    private static long retainedOpenWriterHeap(Arm arm, int count, Path directory)
            throws IOException, InterruptedException {
        forceGc();
        long baseline = usedHeap();
        List<AutoCloseable> open = new ArrayList<>();
        try {
            for (int index = 0; index < count; index++) {
                Path path = directory.resolve("open-" + arm.label + "-" + count + "-" + index);
                open.add(switch (arm) {
                    case CUSTOM -> PageRunSegmentEncoder.open(
                            path, SortMetrics.NO_OP, null, SortMode.OBJECTS);
                    default -> AvroPageRunContainer.openWriter(path, PageCodec.LZ4);
                });
            }
            forceGc();
            return Math.max(0, usedHeap() - baseline) / count;
        } finally {
            IOException failure = null;
            for (AutoCloseable writer : open) {
                try {
                    writer.close();
                } catch (Exception closeFailure) {
                    if (failure == null) {
                        failure = new IOException("closing retained-heap writer", closeFailure);
                    } else {
                        failure.addSuppressed(closeFailure);
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    private static void flush(Arm arm, Path path, List<PageBlock> source) throws IOException {
        List<PageBlock> pages = new ArrayList<>(source);
        switch (arm) {
            default -> {
                long entries = pages.stream().mapToLong(PageBlock::count).sum();
                long estimated = pages.stream().mapToLong(PageBlock::stagingEstimatedBytes).sum();
                SealedBuffer sealed = new SealedBuffer(pages, 1,
                        Map.of(1L, pages.getLast().lastKey()), entries,
                        SealTrigger.DRAIN, estimated);
                new PageRunSegmentWriter(CMP, DuplicateHook.NO_OP, SortMetrics.NO_OP,
                        PageCodec.LZ4).flush(sealed, path);
            }
            case AVRO, AVRO_REUSE, AVRO_RAW, AVRO_RAW2 -> {
                try (AvroPageRunContainer.Writer writer =
                        AvroPageRunContainer.openWriter(path, PageCodec.LZ4)) {
                    for (PageBlock page : pages) {
                        writer.append(page);
                    }
                    writer.seal();
                }
            }
        }
    }

    private static List<PageBlock> writerPages() {
        List<PageBlock> pages = new ArrayList<>();
        for (int page = 0; page < WRITER_PAGES; page++) {
            List<ListEntry> entries = new ArrayList<>(PAGE_ROWS);
            for (int row = 0; row < PAGE_ROWS; row++) {
                entries.add(object((long) page * PAGE_ROWS + row));
            }
            pages.add(PageBlock.pack(entries, CMP, PageCodec.LZ4));
        }
        return pages;
    }

    private static void prepareCorpus(Path directory, int rows) throws IOException {
        int perSegment = rows / SEGMENTS;
        for (int segment = 0; segment < SEGMENTS; segment++) {
            List<PageBlock> pages = new ArrayList<>(perSegment / PAGE_ROWS);
            for (int offset = 0; offset < perSegment; offset += PAGE_ROWS) {
                List<ListEntry> page = new ArrayList<>(PAGE_ROWS);
                for (int row = 0; row < PAGE_ROWS; row++) {
                    page.add(object(segment + (long) (offset + row) * SEGMENTS));
                }
                pages.add(PageBlock.pack(page, CMP, PageCodec.LZ4));
            }

            Path custom = segmentPath(directory, Arm.CUSTOM, segment);
            long estimated = pages.stream().mapToLong(PageBlock::stagingEstimatedBytes).sum();
            SealedBuffer sealed = new SealedBuffer(pages, 1,
                    Map.of((long) segment, pages.getLast().lastKey()), perSegment,
                    SealTrigger.DRAIN, estimated);
            new PageRunSegmentWriter(CMP, DuplicateHook.NO_OP, SortMetrics.NO_OP,
                    PageCodec.LZ4).flush(sealed, custom);

            Path avro = segmentPath(directory, Arm.AVRO, segment);
            try (AvroPageRunContainer.Writer writer =
                    AvroPageRunContainer.openWriter(avro, PageCodec.LZ4)) {
                for (PageBlock page : pages) {
                    writer.append(page);
                }
                writer.seal();
            }
        }
    }

    private static void mergeChild(Arm arm, Path directory, int expectedRows, int repeats)
            throws IOException {
        for (int repeat = 1; repeat <= repeats; repeat++) {
            mergeOnce(arm, directory, expectedRows, repeat);
        }
    }

    private static void mergeOnce(Arm arm, Path directory, int expectedRows, int repeat)
            throws IOException {
        forceGcUnchecked();
        long start = System.nanoTime();
        for (int segment = 0; segment < SEGMENTS; segment++) {
            Path path = segmentPath(directory, arm, segment);
            switch (arm) {
                case CUSTOM -> scanCustomHeaders(path);
                case AVRO -> AvroPageRunContainer.scanHeaders(path);
                case AVRO_REUSE -> scanReuseHeaders(path);
                case AVRO_RAW -> scanRawHeaders(path, false);
                case AVRO_RAW2 -> AvroPageRunVariants.scanHeadersSeeking(path);
            }
        }
        long afterHeaders = System.nanoTime();

        List<EntryStream> streams = new ArrayList<>();
        try {
            for (int segment = 0; segment < SEGMENTS; segment++) {
                Path path = segmentPath(directory, arm, segment);
                streams.add(switch (arm) {
                    case CUSTOM -> new CustomStream(path);
                    case AVRO -> new AvroStream(path);
                    case AVRO_REUSE -> new ReuseStream(path);
                    case AVRO_RAW, AVRO_RAW2 -> new RawStream(path, arm.reordered());
                });
            }
            long afterOpen = System.nanoTime();
            long rows = 0;
            long digest = 0xcbf29ce484222325L;
            try (StreamingMerger merge = new StreamingMerger(streams, CMP, ignored -> { })) {
                while (merge.hasNext()) {
                    ListEntry entry = merge.next();
                    digest ^= Arrays.hashCode(entry.key().rawUnsafe());
                    digest *= 0x100000001b3L;
                    rows++;
                }
            }
            if (rows != expectedRows) {
                throw new IOException("merged " + rows + " rows, expected " + expectedRows);
            }
            long wall = System.nanoTime() - start;
            System.out.printf(Locale.ROOT, "CHILD_RESULT %s %d %d %d %d %d %d %d%n",
                    arm.label, wall, readVmHwmKiB(), rows, digest,
                    afterHeaders - start, System.nanoTime() - afterOpen, repeat);
        } catch (IOException | RuntimeException failure) {
            for (EntryStream stream : streams) {
                try {
                    stream.close();
                } catch (IOException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            throw failure;
        }
    }

    private static void scanReuseHeaders(Path path) throws IOException {
        AvroPageRunVariants.Frame frame = new AvroPageRunVariants.Frame();
        try (AvroPageRunVariants.ReuseReader reader =
                new AvroPageRunVariants.ReuseReader(path, true)) {
            while (reader.next(frame, false)) {
                // projected header pass
            }
        }
    }

    private static void scanRawHeaders(Path path, boolean reordered) throws IOException {
        AvroPageRunVariants.Frame frame = new AvroPageRunVariants.Frame();
        try (AvroPageRunVariants.RawReader reader =
                new AvroPageRunVariants.RawReader(path, reordered)) {
            while (reader.next(frame, false)) {
                // raw-block header pass
            }
        }
    }

    private static void scanCustomHeaders(Path path) throws IOException {
        try (PageRunSegmentIo io = PageRunSegmentIo.open(path, SortMetrics.NO_OP)) {
            while (io.nextRoutingPage() != null) {
                // projected routing-header pass
            }
            io.checkRoutingComplete();
        }
    }

    private static MergeMeasurement runMergeChild(Arm arm, Path directory, int rows)
            throws IOException, InterruptedException {
        return runMergeChild(arm, directory, rows, 1).getFirst();
    }

    private static List<MergeMeasurement> runMergeChild(Arm arm, Path directory, int rows,
            int repeats) throws IOException, InterruptedException {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        Process process = new ProcessBuilder(java, "-Xms128m", "-Xmx2g", "-cp",
                System.getProperty("java.class.path"), PageRunContainerSpike.class.getName(),
                "merge", arm.label, directory.toString(), Integer.toString(rows),
                Integer.toString(repeats))
                .redirectErrorStream(true)
                .start();
        List<MergeMeasurement> results = new ArrayList<>();
        try (BufferedReader output = new BufferedReader(
                new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = output.readLine()) != null) {
                if (line.startsWith("CHILD_RESULT ")) {
                    String[] fields = line.split(" ");
                    results.add(new MergeMeasurement(arm, Long.parseLong(fields[2]),
                            Long.parseLong(fields[3]), Long.parseLong(fields[4]),
                            Long.parseLong(fields[5]), Long.parseLong(fields[6]),
                            Long.parseLong(fields[7]), Integer.parseInt(fields[8])));
                } else {
                    System.out.println("CHILD " + line);
                }
            }
        }
        int exit = process.waitFor();
        if (exit != 0 || results.size() != repeats) {
            throw new IOException("merge child failed for " + arm.label + " (exit " + exit + ")");
        }
        return results;
    }

    private static long corpusBytes(Path directory, Arm arm) throws IOException {
        long bytes = 0;
        for (int segment = 0; segment < SEGMENTS; segment++) {
            bytes += Files.size(segmentPath(directory, arm, segment));
        }
        return bytes;
    }

    private static long customExtensionBytes(Path directory) throws IOException {
        long bytes = 0;
        for (int segment = 0; segment < SEGMENTS; segment++) {
            try (PageRunSegmentIo io = PageRunSegmentIo.open(
                    segmentPath(directory, Arm.CUSTOM, segment), SortMetrics.NO_OP)) {
                PageRunTrailer.Trailer trailer = PageRunTrailer.read(io);
                bytes += io.fileSize - PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES
                        - trailer.extensionStart();
            }
        }
        return bytes;
    }

    private static Path segmentPath(Path directory, Arm arm, int segment) {
        String stem = switch (arm) {
            case CUSTOM -> "custom";
            case AVRO_RAW2 -> "avro2";
            default -> "avro";
        };
        return directory.resolve(String.format(Locale.ROOT, "%s-%02d.%s", stem, segment,
                arm == Arm.CUSTOM ? "pageseg" : "avro"));
    }

    private static double averageKeyBytes(int rows) {
        long samples = Math.min(rows, 10_000);
        long bytes = 0;
        for (int index = 0; index < samples; index++) {
            bytes += key(index).length();
        }
        return (double) bytes / samples;
    }

    private static ObjectEntry object(long ordinal) {
        long mixed = ordinal * 0x9e3779b97f4a7c15L;
        long size = 512L + Long.remainderUnsigned(mixed, 64L * 1024 * 1024);
        return new ObjectEntry(KeyBytes.ofUtf8(key(ordinal)), size,
                1_788_091_200_000_000L + ordinal * 1_000_000L,
                String.format(Locale.ROOT, "%032x", mixed),
                ordinal % 20 == 0 ? "STANDARD_IA" : "STANDARD", null, false,
                "79a59df900b949e55d96a1e698f0ee8f6c6e0f0d1f88d8791807b9c3d38b5d6d",
                null, ordinal % 4 == 0 ? "SHA256" : null,
                ordinal % 4 == 0 ? "FULL_OBJECT" : null);
    }

    private static String key(long ordinal) {
        return String.format(Locale.ROOT,
                "tenant=0042/root=v1/region=us-east-1/year=2026/month=08/day=%02d/hour=%02d/"
                        + "partition=%04d/object-%012d.dat",
                1 + (ordinal / 86_400) % 31, (ordinal / 3_600) % 24,
                (ordinal / 1_000) % 10_000, ordinal);
    }

    private static long usedHeap() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private static void forceGc() throws InterruptedException {
        for (int attempt = 0; attempt < 3; attempt++) {
            System.gc();
            Thread.sleep(50);
        }
    }

    private static void forceGcUnchecked() {
        try {
            forceGc();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while settling heap", interrupted);
        }
    }

    private static long readVmHwmKiB() throws IOException {
        for (String line : Files.readAllLines(Path.of("/proc/self/status"))) {
            if (line.startsWith("VmHWM:")) {
                return Long.parseLong(line.replaceAll("[^0-9]", ""));
            }
        }
        throw new IOException("/proc/self/status has no VmHWM");
    }

    private enum Arm {
        CUSTOM("custom"),
        AVRO("avro"),
        /** Spike shape, datum + ByteBuffer reuse, no defensive routing copies. */
        AVRO_REUSE("avro-reuse"),
        /** hasNext()/nextBlock() raw block iteration, one reused BinaryDecoder. */
        AVRO_RAW("avro-raw"),
        /** Raw blocks plus routing-first schema and a seek-over-payload header pass. */
        AVRO_RAW2("avro-raw2");

        private final String label;

        Arm(String label) {
            this.label = label;
        }

        static Arm of(String label) {
            for (Arm arm : values()) {
                if (arm.label.equals(label)) {
                    return arm;
                }
            }
            throw new IllegalArgumentException("unknown arm " + label);
        }

        boolean reordered() {
            return this == AVRO_RAW2;
        }
    }

    private record ThreadCost(long cpuNanos, long allocatedBytes) {
    }

    private record WriterMeasurement(Arm arm, int concurrency, long retainedHeapBytesPerOpen,
                                     long cpuNanosPerFlush, long allocatedBytesPerFlush,
                                     long payloadBytesPerFlush,
                                     long containerAllocationBytesPerFlush) {
    }

    private record MergeMeasurement(Arm arm, long wallNanos, long rssPeakKiB, long rows,
                                    long digest, long headerNanos, long bodyNanos, int repeat) {
    }

    private abstract static class BlockStream implements EntryStream {

        private PageBlockCursor cursor;
        private ListEntry head;

        final void initialize() throws IOException {
            advance();
        }

        abstract PageBlock nextBlock() throws IOException;

        private void advance() throws IOException {
            while (cursor == null || !cursor.hasNext()) {
                PageBlock block = nextBlock();
                if (block == null) {
                    head = null;
                    return;
                }
                cursor = block.cursor();
            }
            head = cursor.next();
        }

        @Override
        public boolean hasNext() {
            return head != null;
        }

        @Override
        public ListEntry peek() {
            return head;
        }

        @Override
        public ListEntry next() throws IOException {
            ListEntry result = head;
            advance();
            return result;
        }
    }

    private static final class CustomStream extends BlockStream {

        private final PageRunSegmentIo io;
        private long page;
        private long entries;

        CustomStream(Path path) throws IOException {
            this(PageRunSegmentIo.open(path, SortMetrics.NO_OP));
        }

        private CustomStream(PageRunSegmentIo io) throws IOException {
            this.io = io;
            initialize();
        }

        @Override
        PageBlock nextBlock() throws IOException {
            if (page == io.totalRecords) {
                io.checkComplete(entries);
                return null;
            }
            PageRunSegmentIo.Page next = io.nextPage();
            page++;
            entries += next.header().count();
            return next.decode(io.path());
        }

        @Override
        public void close() throws IOException {
            io.close();
        }
    }

    private static final class ReuseStream extends BlockStream {

        private final AvroPageRunVariants.ReuseReader reader;
        private final AvroPageRunVariants.Frame frame = new AvroPageRunVariants.Frame();
        private final Path path;

        ReuseStream(Path path) throws IOException {
            this.path = path;
            this.reader = new AvroPageRunVariants.ReuseReader(path, false);
            initialize();
        }

        @Override
        PageBlock nextBlock() throws IOException {
            if (!reader.next(frame, true)) {
                return null;
            }
            return decodePage(frame, path);
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    private static final class RawStream extends BlockStream {

        private final AvroPageRunVariants.RawReader reader;
        private final AvroPageRunVariants.Frame frame = new AvroPageRunVariants.Frame();
        private final Path path;

        RawStream(Path path, boolean reordered) throws IOException {
            this.path = path;
            this.reader = new AvroPageRunVariants.RawReader(path, reordered);
            initialize();
        }

        @Override
        PageBlock nextBlock() throws IOException {
            if (!reader.next(frame, true)) {
                return null;
            }
            return decodePage(frame, path);
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    /**
     * Shared body-pass work for the Avro variants: parse the PageBlock header, cross-check it
     * against the duplicated OCF routing metadata, and decode. This is exactly the work the spike's
     * {@code AvroPageRunContainer.Reader.nextPage} does, minus its per-field defensive copies.
     */
    private static PageBlock decodePage(AvroPageRunVariants.Frame frame, Path path)
            throws IOException {
        byte[] body = frame.page;
        PageBlockCodec.Header header;
        try {
            header = PageRunSegmentIo.parsePageHeader(body);
        } catch (IllegalArgumentException failure) {
            throw new IOException("Avro page-run segment " + path + ": malformed PageBlock",
                    failure);
        }
        if (!Arrays.equals(header.minKey(), frame.minKey)
                || !Arrays.equals(header.maxKey(), frame.maxKey)
                || header.count() != frame.count
                || header.rawPayloadLength() != frame.rawPayloadLength) {
            throw new IOException("Avro page-run segment " + path
                    + ": PageBlock metadata disagrees with OCF record metadata");
        }
        return PageBlockCodec.deserialize(body, header, path);
    }

    private static final class AvroStream extends BlockStream {

        private final AvroPageRunContainer.Reader reader;

        AvroStream(Path path) throws IOException {
            this(AvroPageRunContainer.openReader(path));
        }

        private AvroStream(AvroPageRunContainer.Reader reader) throws IOException {
            this.reader = reader;
            initialize();
        }

        @Override
        PageBlock nextBlock() throws IOException {
            return reader.nextPage();
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }
}

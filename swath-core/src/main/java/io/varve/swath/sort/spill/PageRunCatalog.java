/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.spill;

import io.varve.swath.output.sorted.StagingNames;
import io.varve.swath.sort.SortMetrics;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** One immutable kickoff catalog of validated page-run inputs and primitive metadata. */
public final class PageRunCatalog {

    private final List<PageRunSegmentDescriptor> descriptors;
    private final List<Path> paths;
    private final Map<Path, PageRunSegmentDescriptor> byPath;
    private final long maxRecordLen;
    private final int maxKeyLength;
    private final long totalRecords;
    private final long totalEntries;

    private PageRunCatalog(List<PageRunSegmentDescriptor> descriptors) {
        this.descriptors = List.copyOf(descriptors);
        List<Path> orderedPaths = new ArrayList<>(descriptors.size());
        Map<Path, PageRunSegmentDescriptor> indexed = new LinkedHashMap<>();
        Set<Path> identities = new LinkedHashSet<>();
        long maximum = -1;
        int maximumKey = 0;
        long records = 0;
        long entries = 0;
        for (PageRunSegmentDescriptor descriptor : descriptors) {
            Path identity = normalizedIdentity(descriptor.path());
            if (!identities.add(identity)) {
                throw new IllegalArgumentException("duplicate page-run catalog path: " + identity);
            }
            orderedPaths.add(descriptor.path());
            indexed.put(descriptor.path(), descriptor);
            maximum = Math.max(maximum, descriptor.trailer().maxRecordLen());
            maximumKey = Math.max(maximumKey, descriptor.maxKeyLength());
            records = Math.addExact(records, descriptor.trailer().totalRecords());
            entries = Math.addExact(entries, descriptor.trailer().totalEntries());
        }
        paths = List.copyOf(orderedPaths);
        byPath = Map.copyOf(indexed);
        maxRecordLen = maximum;
        maxKeyLength = maximumKey;
        totalRecords = records;
        totalEntries = entries;
    }

    public static void requirePageRunNames(List<Path> paths) {
        for (Path path : paths) {
            if (!hasPageRunName(path)) {
                throw new IllegalArgumentException(
                        "unsupported sort staging segment (expected "
                                + StagingNames.PAGE_RUN_SUFFIX + "): " + path);
            }
        }
    }

    static boolean hasPageRunName(Path path) {
        return path.getFileName().toString().endsWith(StagingNames.PAGE_RUN_SUFFIX);
    }

    public static PageRunCatalog preflight(List<Path> paths, Opener opener) throws IOException {
        return preflight(paths, opener, Map.of(), SortMetrics.NO_OP);
    }

    public static PageRunCatalog preflight(List<Path> paths, Opener opener,
            Map<Path, PageRunFormat> expectedFormats, SortMetrics metrics) throws IOException {
        List<Path> normalizedPaths = requireUniqueNormalizedPaths(paths);
        Map<Path, PageRunFormat> normalizedExpected = normalizeExpectedFormats(expectedFormats);
        List<PageRunSegmentDescriptor> descriptors = new ArrayList<>(normalizedPaths.size());
        for (Path path : normalizedPaths) {
            PageRunFormat expected = normalizedExpected.get(path);
            try (PageRunSegmentIo io = opener.open(path)) {
                PageRunTrailer.Trailer trailer = PageRunTrailer.read(io);
                PageRunFormat physical = new PageRunFormat(
                        Short.toUnsignedInt(io.formatVersion()), PageRunFormat.ABSENT_EXTENSION);
                if (expected != null && !expected.equals(physical)) {
                    metrics.recordStealReason("SORT", "page_run_format_mismatch");
                    throw new SegmentCorruptionException(path,
                            SegmentCorruptionException.PAGE_RUN_FORMAT_MISMATCH,
                            "checkpoint format metadata disagrees with physical segment: recorded="
                                    + expected + ", physical=" + physical);
                }
                descriptors.add(new PageRunSegmentDescriptor(path, io.fileSize, io.trailerStart,
                        trailer, trailer.maxRawPayloadLength(), trailer.maxKeyLength(),
                        physical, io.headerBytes, io.orderingMode()));
            }
        }
        return new PageRunCatalog(descriptors);
    }

    private static Map<Path, PageRunFormat> normalizeExpectedFormats(
            Map<Path, PageRunFormat> expectedFormats) throws IOException {
        Map<Path, PageRunFormat> normalized = new LinkedHashMap<>();
        for (Map.Entry<Path, PageRunFormat> entry : expectedFormats.entrySet()) {
            Path path = normalizedIdentity(entry.getKey());
            if (normalized.putIfAbsent(path, entry.getValue()) != null) {
                throw new IOException("duplicate expected page-run format path: " + path);
            }
        }
        return Map.copyOf(normalized);
    }

    private static List<Path> requireUniqueNormalizedPaths(List<Path> paths) throws IOException {
        List<Path> normalizedPaths = new ArrayList<>(paths.size());
        Set<Path> identities = new LinkedHashSet<>();
        for (Path path : paths) {
            Path normalized = normalizedIdentity(path);
            if (!identities.add(normalized)) {
                throw new IOException("duplicate page-run catalog path: " + normalized);
            }
            normalizedPaths.add(normalized);
        }
        return List.copyOf(normalizedPaths);
    }

    private static Path normalizedIdentity(Path path) {
        return path.toAbsolutePath().normalize();
    }

    public static PageRunCatalog fromDescriptors(List<PageRunSegmentDescriptor> descriptors) {
        return new PageRunCatalog(descriptors);
    }

    public List<PageRunSegmentDescriptor> descriptors() {
        return descriptors;
    }

    public List<Path> paths() {
        return paths;
    }

    Map<Path, PageRunSegmentDescriptor> byPath() {
        return byPath;
    }

    public long maxRecordLen() {
        return maxRecordLen;
    }

    public int maxKeyLength() {
        return maxKeyLength;
    }

    public long totalRecords() {
        return totalRecords;
    }

    public long totalEntries() {
        return totalEntries;
    }

    @FunctionalInterface
    public interface Opener {
        PageRunSegmentIo open(Path path) throws IOException;
    }
}

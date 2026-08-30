/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

/**
 * One immutable kickoff catalog of page-run inputs and their retained primitive metadata.
 *
 * <p>Name validation is deliberately separate from descriptor preflight: {@link SortTransform}
 * validates every suffix, then validates retained-staging ownership, and only then opens inputs.
 * That preserves the rule that an unsafe external retained path is rejected before it is read.
 * Descriptor kickoff streams optional sample keys into the caller's bounded sink; neither this
 * catalog nor its descriptors retain a sample-key collection.
 */
final class PageRunCatalog {

    private final List<PageRunSegmentDescriptor> descriptors;
    private final List<Path> paths;
    private final Map<Path, PageRunSegmentDescriptor> byPath;
    private final long maxRecordLen;
    private final int maxRawPayloadLength;
    private final int maxKeyLength;
    private final long totalRecords;
    private final long totalEntries;

    private PageRunCatalog(List<PageRunSegmentDescriptor> descriptors) {
        this.descriptors = List.copyOf(descriptors);
        List<Path> orderedPaths = new ArrayList<>(descriptors.size());
        Map<Path, PageRunSegmentDescriptor> indexed = new LinkedHashMap<>();
        Set<Path> normalizedIdentities = new LinkedHashSet<>();
        long maximum = -1;
        int maximumRaw = 0;
        int maximumKey = 0;
        long records = 0;
        long entries = 0;
        for (PageRunSegmentDescriptor descriptor : descriptors) {
            Path identity = normalizedIdentity(descriptor.path());
            if (!normalizedIdentities.add(identity)) {
                throw new IllegalArgumentException("duplicate page-run catalog path: " + identity);
            }
            orderedPaths.add(descriptor.path());
            indexed.put(descriptor.path(), descriptor);
            maximum = Math.max(maximum, descriptor.trailer().maxRecordLen());
            if (descriptor.hasDecodedPageMaximum()) {
                maximumRaw = Math.max(maximumRaw, descriptor.maxRawPayloadLength());
            }
            maximumKey = Math.max(maximumKey, Math.max(
                    descriptor.trailer().segMinKey().length,
                    descriptor.trailer().segMaxKey().length));
            records = Math.addExact(records, descriptor.trailer().totalRecords());
            entries = Math.addExact(entries, descriptor.trailer().totalEntries());
        }
        this.paths = List.copyOf(orderedPaths);
        this.byPath = Map.copyOf(indexed);
        this.maxRecordLen = maximum;
        this.maxRawPayloadLength = maximumRaw;
        this.maxKeyLength = maximumKey;
        this.totalRecords = records;
        this.totalEntries = entries;
    }

    /** Validate the complete input list before any path is opened or working file is swept. */
    static void requirePageRunNames(List<Path> paths) {
        for (Path path : paths) {
            if (!hasPageRunName(path)) {
                throw new IllegalArgumentException(
                        "unsupported sort staging segment (expected " + StagingNames.PAGE_RUN_SUFFIX
                                + "): " + path);
            }
        }
    }

    static boolean hasPageRunName(Path path) {
        return path.getFileName().toString().endsWith(StagingNames.PAGE_RUN_SUFFIX);
    }

    /**
     * Open every already-name-validated input once for its trailer and optional-extension header.
     * A present boundary sink requests full sparse-index validation; serial/arbitrary callers retain
     * only the O(1) physical extension identity and rely on runtime decoded-page admission.
     */
    static PageRunCatalog preflight(List<Path> paths, Opener opener,
            Optional<Consumer<byte[]>> boundaryKeySink) throws IOException {
        return preflight(paths, opener, boundaryKeySink, Map.of());
    }

    /**
     * Preflight with checkpoint-declared formats keyed by input path. Missing entries are legacy
     * unrecorded inputs; present entries must match the physical header and extension exactly.
     */
    static PageRunCatalog preflight(List<Path> paths, Opener opener,
            Optional<Consumer<byte[]>> boundaryKeySink,
            Map<Path, PageRunFormat> expectedFormats) throws IOException {
        return preflight(paths, opener, boundaryKeySink, expectedFormats, SortMetrics.NO_OP);
    }

    /** Preflight with the run's format-rejection telemetry sink. */
    static PageRunCatalog preflight(List<Path> paths, Opener opener,
            Optional<Consumer<byte[]>> boundaryKeySink,
            Map<Path, PageRunFormat> expectedFormats, SortMetrics metrics) throws IOException {
        List<Path> normalizedPaths = requireUniqueNormalizedPaths(paths);
        Map<Path, PageRunFormat> normalizedExpected = normalizeExpectedFormats(expectedFormats);
        List<PageRunSegmentDescriptor> descriptors = new ArrayList<>(normalizedPaths.size());
        for (Path path : normalizedPaths) {
            PageRunFormat expected = normalizedExpected.get(path);
            try (PageRunSegmentIo io = opener.open(path)) {
                PageRunTrailer.Trailer trailer = PageRunTrailer.read(io);
                PageRunPageIndex.Probe probe = PageRunPageIndex.probe(io, trailer);
                PageRunFormat physicalFormat = new PageRunFormat(
                        Short.toUnsignedInt(io.formatVersion()),
                        Short.toUnsignedInt(probe.extensionType()));
                requireExpectedFormat(path, expected, physicalFormat, probe, metrics);
                PageRunPageIndex.ReadResult extension = boundaryKeySink.isPresent()
                        ? PageRunPageIndex.read(io, trailer, boundaryKeySink.orElseThrow(), probe)
                        : PageRunPageIndex.skipped(trailer.totalRecords(), probe);
                int maxRawPayloadLength = extension.hasDecodedPageMaximum()
                        ? extension.maxRawPayloadLength()
                        : -1;
                descriptors.add(new PageRunSegmentDescriptor(path, io.fileSize, io.trailerStart,
                        trailer, extension, maxRawPayloadLength, physicalFormat,
                        io.headerBytes, io.orderingMode()));
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

    private static void requireExpectedFormat(Path path, PageRunFormat expected,
            PageRunFormat physical, PageRunPageIndex.Probe probe, SortMetrics metrics)
            throws IOException {
        if (expected == null) {
            return;
        }
        if (!probe.supportedPhysicalType() || !expected.equals(physical)) {
            metrics.recordStealReason("SORT", "page_run_format_mismatch");
            throw new SegmentCorruptionException(path,
                    SegmentCorruptionException.PAGE_RUN_FORMAT_MISMATCH,
                    "checkpoint format metadata disagrees with physical segment: recorded="
                            + expected + ", physical=" + physical
                            + ", extension_status="
                            + (probe.status() == null ? "HEADER" : probe.status()));
        }
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

    /** Assemble a catalog from descriptors already opened by a focused planner/worker test. */
    static PageRunCatalog fromDescriptors(List<PageRunSegmentDescriptor> descriptors) {
        return new PageRunCatalog(descriptors);
    }

    List<PageRunSegmentDescriptor> descriptors() {
        return descriptors;
    }

    List<Path> paths() {
        return paths;
    }

    Map<Path, PageRunSegmentDescriptor> byPath() {
        return byPath;
    }

    long maxRecordLen() {
        return maxRecordLen;
    }

    /** Exact maximum decoded payload bytes for one original input page. */
    int maxRawPayloadLength() {
        return maxRawPayloadLength;
    }

    /** Largest actual segment-bound key retained by the validated post-cascade catalog. */
    int maxKeyLength() {
        return maxKeyLength;
    }

    long totalRecords() {
        return totalRecords;
    }

    /** Exact source-row authority summed from every independently validated original trailer. */
    long totalEntries() {
        return totalEntries;
    }

    @FunctionalInterface
    interface Opener {
        PageRunSegmentIo open(Path path) throws IOException;
    }
}

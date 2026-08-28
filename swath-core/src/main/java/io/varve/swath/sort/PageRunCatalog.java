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
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

    private PageRunCatalog(List<PageRunSegmentDescriptor> descriptors) {
        this.descriptors = List.copyOf(descriptors);
        List<Path> orderedPaths = new ArrayList<>(descriptors.size());
        Map<Path, PageRunSegmentDescriptor> indexed = new LinkedHashMap<>();
        long maximum = -1;
        for (PageRunSegmentDescriptor descriptor : descriptors) {
            orderedPaths.add(descriptor.path());
            indexed.putIfAbsent(descriptor.path(), descriptor);
            maximum = Math.max(maximum, descriptor.trailer().maxRecordLen());
        }
        this.paths = List.copyOf(orderedPaths);
        this.byPath = Map.copyOf(indexed);
        this.maxRecordLen = maximum;
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
     * Open every already-name-validated input once for its trailer and optional boundary extension.
     * An empty sink is the serial/arbitrary-run policy: the extension is not read at all.
     */
    static PageRunCatalog preflight(List<Path> paths, Opener opener,
            Optional<Consumer<byte[]>> boundaryKeySink) throws IOException {
        List<PageRunSegmentDescriptor> descriptors = new ArrayList<>(paths.size());
        for (Path path : paths) {
            try (PageRunSegmentIo io = opener.open(path)) {
                PageRunTrailer.Trailer trailer = PageRunTrailer.read(io);
                PageRunPageIndex.ReadResult extension = boundaryKeySink.isPresent()
                        ? PageRunPageIndex.read(io, trailer, boundaryKeySink.orElseThrow())
                        : PageRunPageIndex.skipped(trailer.totalRecords());
                descriptors.add(new PageRunSegmentDescriptor(path, io.fileSize, io.trailerStart,
                        trailer, extension));
            }
        }
        return new PageRunCatalog(descriptors);
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

    @FunctionalInterface
    interface Opener {
        PageRunSegmentIo open(Path path) throws IOException;
    }
}

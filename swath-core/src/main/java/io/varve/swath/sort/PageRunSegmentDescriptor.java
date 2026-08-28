/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * One kickoff-opened page-run segment and its retained file, trailer, and extension metadata.
 * Reading all descriptors before cleanup makes an unreadable internal input a preflight failure
 * rather than an optional fan-in refinement that silently falls back and fails later after working
 * files were removed. Embedded sample keys flow into the merge-wide bounded candidate set while the
 * segment is open; a descriptor never retains those keys.
 */
record PageRunSegmentDescriptor(Path path, long fileSize, long trailerStart,
                                PageRunTrailer.Trailer trailer,
                                PageRunPageIndex.ReadResult extension) {

    /** Legacy-compatible boundary-sample view used by the existing range-boundary planner. */
    PageRunBoundarySample.ReadResult sample() {
        return extension.boundarySample();
    }

    /**
     * Open every segment once for its trailer and, when {@code boundaryKeySink} is present, its
     * optional boundary extension. An empty sink is the serial/arbitrary-run policy: the extension
     * is not read at all.
     */
    static List<PageRunSegmentDescriptor> readAll(List<Path> paths, Opener opener,
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
        return List.copyOf(descriptors);
    }

    static List<Path> paths(List<PageRunSegmentDescriptor> descriptors) {
        return descriptors.stream().map(PageRunSegmentDescriptor::path).toList();
    }

    static Map<Path, PageRunSegmentDescriptor> byPath(
            List<PageRunSegmentDescriptor> descriptors) {
        return descriptors.stream().collect(Collectors.toUnmodifiableMap(
                PageRunSegmentDescriptor::path, descriptor -> descriptor,
                (first, duplicate) -> first));
    }

    static long maxRecordLen(List<PageRunSegmentDescriptor> descriptors) {
        long max = -1;
        for (PageRunSegmentDescriptor descriptor : descriptors) {
            max = Math.max(max, descriptor.trailer().maxRecordLen());
        }
        return max;
    }

    @FunctionalInterface
    interface Opener {
        PageRunSegmentIo open(Path path) throws IOException;
    }
}

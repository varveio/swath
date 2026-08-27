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

/**
 * One kickoff-opened page-run segment and its retained file, trailer, and boundary-sample metadata.
 * Reading all descriptors before cleanup makes an unreadable internal input a preflight failure
 * rather than an optional fan-in refinement that silently falls back and fails later after working
 * files were removed.
 */
record PageRunSegmentDescriptor(Path path, long fileSize, long trailerStart,
                                PageRunSegmentReader.Trailer trailer,
                                PageRunBoundarySample.ReadResult sample) {

    static List<PageRunSegmentDescriptor> readAll(List<Path> paths) throws IOException {
        List<PageRunSegmentDescriptor> descriptors = new ArrayList<>(paths.size());
        for (Path path : paths) {
            try (PageRunSegmentIo io = PageRunSegmentIo.open(path)) {
                descriptors.add(new PageRunSegmentDescriptor(path, io.fileSize, io.trailerStart,
                        PageRunSegmentReader.readTrailer(io), PageRunBoundarySample.read(io)));
            }
        }
        return List.copyOf(descriptors);
    }

    static List<Path> paths(List<PageRunSegmentDescriptor> descriptors) {
        return descriptors.stream().map(PageRunSegmentDescriptor::path).toList();
    }

    static long maxRecordLen(List<PageRunSegmentDescriptor> descriptors) {
        long max = -1;
        for (PageRunSegmentDescriptor descriptor : descriptors) {
            max = Math.max(max, descriptor.trailer().maxRecordLen());
        }
        return max;
    }
}

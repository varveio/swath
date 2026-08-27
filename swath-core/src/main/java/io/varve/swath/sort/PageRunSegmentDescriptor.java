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
 * One kickoff-validated page-run segment and its immutable trailer metadata. Reading all descriptors
 * before cleanup makes an unreadable internal input a preflight failure rather than an optional
 * fan-in refinement that silently falls back and fails later after working files were removed.
 */
record PageRunSegmentDescriptor(Path path, PageRunSegmentReader.Trailer trailer) {

    static List<PageRunSegmentDescriptor> readAll(List<Path> paths) throws IOException {
        List<PageRunSegmentDescriptor> descriptors = new ArrayList<>(paths.size());
        for (Path path : paths) {
            descriptors.add(new PageRunSegmentDescriptor(path,
                    PageRunSegmentReader.readTrailer(path)));
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

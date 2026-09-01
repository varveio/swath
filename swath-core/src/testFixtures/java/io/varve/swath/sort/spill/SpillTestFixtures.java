/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.spill;

import io.varve.swath.sort.SortMetrics;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;

/** Test-fixture access to package-private page-run format details for non-spill tests. */
public final class SpillTestFixtures {

    private SpillTestFixtures() {
    }

    public static byte[] serialize(PageBlock block) {
        return block.serialize();
    }

    public static PageBlock deserialize(byte[] record) {
        return PageBlock.deserialize(record);
    }

    public static int pageRunHeaderBytes() {
        return PageRunWriter.HEADER_BYTES;
    }

    public static ByteBuffer readFrame(Path path, long offset, int length) throws IOException {
        try (PageRunReader reader = PageRunReader.open(path, SortMetrics.NO_OP)) {
            return reader.readAt(offset, length);
        }
    }

    public static PageRunCatalog catalog(List<PageRunDescriptor> descriptors) {
        return PageRunCatalog.fromDescriptors(descriptors);
    }

    public static PageRunTrailer.Trailer trailer(Path path) throws IOException {
        return PageRunTrailer.read(path);
    }
}

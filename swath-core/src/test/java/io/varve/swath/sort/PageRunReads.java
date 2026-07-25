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
 * Test-only reader for page-run staging segments, in-package so it can reach the package-private
 * {@link PageRunSegmentReader}. Exposed publicly so tests in other packages (e.g. the runtime
 * SORT-RESUME adversarial tests) can read staging keys back without a Parquet reader — the staging format is
 * page-run, not columnar Parquet.
 */
public final class PageRunReads {

    private PageRunReads() {
    }

    /** The keys of a page-run staging/intermediate segment, in stored (sorted) order. */
    public static List<String> keys(Path segment) throws IOException {
        List<String> out = new ArrayList<>();
        try (PageRunSegmentReader r = new PageRunSegmentReader(segment)) {
            while (r.hasNext()) {
                out.add(r.next().key().asString());
            }
        }
        return out;
    }
}

/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ParallelMergeBenchmarkTest {

    @Test
    void benchmarkArmIsExplicitlyMergeOnly() {
        assertThat(ParallelMergeBenchmark.ARM).isEqualTo("MERGE_ONLY_PAGE_RUN");
    }

    @Test
    void externalStagingMustExistAndBeADirectory(@TempDir Path temp) {
        Path missing = temp.resolve("missing");

        assertThatIllegalArgumentException().isThrownBy(() ->
                ParallelMergeBenchmark.requirePageRunInputs(missing))
                .withMessageContaining("must name a directory");
    }

    @Test
    void externalStagingMustContainPageRunSegments(@TempDir Path staging) {
        assertThatIllegalArgumentException().isThrownBy(() ->
                ParallelMergeBenchmark.requirePageRunInputs(staging))
                .withMessageContaining("contains no *.pageseg inputs");
    }

    @Test
    void externalStagingAcceptsAndOrdersPageRunSegments(@TempDir Path staging) throws Exception {
        ListEntryComparator comparator = new ListEntryComparator();
        Path later = SortTestSupport.writePageRun(staging.resolve("seg-2.pageseg"),
                List.of(SortTestSupport.object("b")), comparator);
        Path earlier = SortTestSupport.writePageRun(staging.resolve("seg-1.pageseg"),
                List.of(SortTestSupport.object("a")), comparator);

        assertThat(ParallelMergeBenchmark.requirePageRunInputs(staging))
                .containsExactly(earlier, later);
    }
}

/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.checkpoint.PartRef;
import io.varve.swath.error.OutputException;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class ListRunnerCheckpointPathTest {

    @Test
    void resolvesOnlyBareCheckpointPageRunNames(@TempDir Path stagingDir) throws Exception {
        PartRef segment = segment("run-00001.pageseg");

        assertThat(ListRunner.resolveCheckpointStagingPaths(stagingDir, List.of(segment)))
                .containsExactly(stagingDir.resolve("run-00001.pageseg"));
    }

    @Test
    void refusesCheckpointPathTraversalBeforeResolution(@TempDir Path stagingDir) {
        PartRef segment = segment("../outside.pageseg");

        assertThatThrownBy(() -> ListRunner.resolveCheckpointStagingPaths(
                stagingDir, List.of(segment)))
                .isInstanceOf(OutputException.class)
                .hasMessageContaining("unsafe sort staging segment path")
                .hasCauseInstanceOf(java.io.IOException.class);
    }

    private static PartRef segment(String path) {
        return new PartRef(1L, 0, path, ListRunner.SORT_SEGMENT_FORMAT,
                null, null, true, 1L, 1L);
    }
}

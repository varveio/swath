/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.error.OutputException;
import io.varve.swath.output.parquet.DatasetLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class ParquetResumeValidationTest {

    @Test
    void missingCheckpointFinalizedPartIsATypedOutputFailure(@TempDir Path outputDir)
            throws Exception {
        Files.createDirectories(DatasetLayout.of(outputDir).dataDir());

        assertThatThrownBy(() -> ListCommand.reconcileResumedParquet(
                outputDir, Set.of("data/part-w0-00000.parquet")))
                .isInstanceOf(OutputException.class)
                .hasMessageContaining("checkpoint-finalized Parquet part is missing")
                .hasCauseInstanceOf(java.io.IOException.class);
    }
}

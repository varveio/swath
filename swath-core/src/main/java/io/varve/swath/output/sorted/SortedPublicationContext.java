/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.sorted;

import java.nio.file.Path;
import java.util.Objects;

/** Validated filesystem and commit authority for one sorted-dataset replacement. */
public record SortedPublicationContext(
        Path outputDir,
        Path stagingDir,
        PublishListener publishListener,
        StagingReconciliation ownedInputs,
        StagingReconciliation retainedOriginals,
        StagingReconciliation.DirectoryAuthority outputAuthority) {

    public SortedPublicationContext {
        Objects.requireNonNull(outputDir, "outputDir");
        Objects.requireNonNull(stagingDir, "stagingDir");
        Objects.requireNonNull(publishListener, "publishListener");
        Objects.requireNonNull(ownedInputs, "ownedInputs");
        Objects.requireNonNull(outputAuthority, "outputAuthority");
    }
}

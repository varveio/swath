/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Invoked by {@link SortTransform} <b>after all final files are renamed into place but before the
 * staging segments are deleted</b> — the seam where the publish commit point is inserted
 * (writing {@code manifest.json} last, §6). Because the manifest is the "final output
 * present" signal, a crash between the renames and the manifest write re-enters {@code MERGING} and
 * re-runs the merge; a crash after the manifest but before staging deletion leaves double data that
 * the PUBLISHED re-entry path cleans. {@link #NO_OP} publishes nothing.
 */
@FunctionalInterface
public interface PublishListener {

    /** Publishes nothing — the null-object implementation. */
    PublishListener NO_OP = (finalFiles, totalRows) -> { };

    void onPublished(List<Path> finalFiles, long totalRows) throws IOException;
}

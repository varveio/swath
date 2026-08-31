/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.sorted;

import io.varve.swath.sort.FinalPart;
import io.varve.swath.sort.SortTransform;
import java.io.IOException;
import java.util.List;

/**
 * Invoked by {@link SortTransform} <b>after all final files are renamed into place but before the
 * staging segments are deleted</b> — the seam where the publish commit point is inserted
 * (writing the authority artifacts with {@code _SUCCESS} last, §6). A crash between the renames and
 * the last marker re-enters {@code MERGING} and re-runs the merge; a crash or caught cleanup failure
 * after the listener returns leaves double data that the PUBLISHED re-entry path cleans without
 * relisting. {@link #NO_OP} publishes nothing.
 */
@FunctionalInterface
public interface PublishListener {

    /** Publishes nothing — the null-object implementation. */
    PublishListener NO_OP = (finalFiles, totalRows) -> { };

    void onPublished(List<FinalPart> finalParts, long totalRows) throws IOException;
}

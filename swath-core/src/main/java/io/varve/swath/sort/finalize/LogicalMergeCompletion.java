/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

import io.varve.swath.sort.SortedEntryCursor;

/** Package-local signal that a raw merge completed its caller's full logical range. */
interface LogicalMergeCompletion {

    /** Mark the wrapped raw merge complete after a successful logical-range cutoff. */
    void completeLogicalMerge();

    /** Delegate through a cursor wrapper when its inner cursor supports completion reporting. */
    static void complete(SortedEntryCursor cursor) {
        if (cursor instanceof LogicalMergeCompletion completion) {
            completion.completeLogicalMerge();
        }
    }
}

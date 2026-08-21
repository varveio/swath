/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol;

import java.util.List;

public record S3ListResult(
        S3ListRequest request,
        List<S3ResultEntry> entries,
        boolean truncated,
        String nextContinuationToken) {

    public int keyCount() {
        return entries.size();
    }
}

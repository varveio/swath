/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol.azure;

import io.varve.swath.replay.protocol.ListedObject;
import java.util.List;

/** Interleaved Azure Blob and BlobPrefix entries in name order. */
public record AzureListResult(AzureListRequest request, List<Entry> entries, String nextMarker) {
    public AzureListResult {
        entries = List.copyOf(entries);
    }

    public sealed interface Entry {
        record Blob(ListedObject object) implements Entry { }
        record BlobPrefix(byte[] name) implements Entry { }
    }
}

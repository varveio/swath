/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol.gcs;

import io.varve.swath.replay.protocol.ListedObject;
import java.util.List;

/** Result arrays retain their own order; pagination selection used one ordered stream. */
public record GcsPage(List<ListedObject> objects, List<byte[]> prefixes, String nextPageToken) {
    public GcsPage {
        objects = List.copyOf(objects);
        prefixes = List.copyOf(prefixes);
    }
}

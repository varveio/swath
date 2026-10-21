/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.conformance.provider;

import java.io.IOException;
import java.util.List;

/** Azure inventory projection from the strict field-policy parser. */
public final class AzureEvidence {
    private AzureEvidence() {
    }

    public static Page parse(byte[] body) throws IOException {
        return AzureProfileComparator.project(body);
    }

    public record Entry(String kind, String exactNameText, String encodedAttribute) {
    }

    public record Page(List<Entry> entries, String nextMarker, String serviceEndpoint, String containerName) {
    }
}

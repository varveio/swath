/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

/** One durably closed and renamed final sorted part, in deterministic publish order. */
public record FinalPart(Path path, Optional<FinalPartMetadata> metadata) {

    public FinalPart {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(metadata, "metadata");
    }
}

/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.dataset;

import java.io.IOException;
import java.nio.file.Path;

/** The only format-specific behavior required by the shared dataset writer pool. */
public interface DatasetFormat {
    String partSuffix();
    String manifestFormat();
    String manifestSchema();
    DatasetPartWriter openPart(Path path) throws IOException;
}

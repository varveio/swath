/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import java.io.IOException;

/**
 * Publishes the completed output atomically (e.g. an atomic rename of a staged temp file to its
 * destination). Invoked once, after the output writer has closed cleanly and before the run is
 * recorded complete; a throw aborts completion.
 */
@FunctionalInterface
public interface OutputPublisher {

    /** Publishes the staged output. */
    void publish() throws IOException;

    /** No-op publisher for destinations with nothing to publish (stdout). */
    OutputPublisher NONE = () -> { };
}

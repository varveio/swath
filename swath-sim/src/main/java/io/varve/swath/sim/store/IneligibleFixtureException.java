/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.store;

import java.nio.file.Path;
import java.util.List;

/**
 * Typed corpus classification for a forced backend rejecting a non-sorted-eligible fixture. The
 * full-path message is for logs; {@link #redactedMessage()} contains only file names for sweep records.
 */
public final class IneligibleFixtureException extends IllegalArgumentException {

    private static final long serialVersionUID = 1L;

    private final SimStoreBackend backend;
    private final String reason;
    /** Transient because {@link Path} is not serializable; path accessors are in-process only. */
    private final transient List<Path> files;

    IneligibleFixtureException(SimStoreBackend backend, String reason, List<Path> files) {
        super(message(backend, reason, files.stream().map(Path::toString).toList()));
        this.backend = backend;
        this.reason = reason;
        this.files = List.copyOf(files);
    }

    /** Machine-readable sorted-eligibility classification. */
    public String reason() {
        return reason;
    }

    /** Backend that declined the fixture. */
    public SimStoreBackend backend() {
        return backend;
    }

    /** Full paths of the resolved file set, in key order; unavailable after deserialization. */
    public List<Path> files() {
        return files;
    }

    /** Message with each file reduced to its name; unavailable after deserialization. */
    public String redactedMessage() {
        return message(backend, reason, files.stream().map(file -> file.getFileName().toString()).toList());
    }

    private static String message(SimStoreBackend backend, String reason, List<String> files) {
        return "backend " + backend + " requires a sorted-eligible fixture (reason=" + reason + "), use "
                + SimStoreBackend.PARQUET + " instead: " + files;
    }
}

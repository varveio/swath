/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Verifies the immutable current-format corpus metadata registered by the shell helper. */
final class BenchmarkCorpusRecord {

    static final String FORMAT =
            "swath-page-run-corpus-v" + PageRunFormat.CURRENT_FORMAT_VERSION;
    private static final Set<String> FIELDS = Set.of(
            "format", "corpus", "rows", "segments", "bytes", "corpus_id", "multiset",
            "created_by_head");

    private BenchmarkCorpusRecord() {
    }

    static void verify(String configuredPath, ParallelMergeBenchmark.CorpusCatalog corpus)
            throws IOException {
        if (configuredPath == null) {
            return;
        }
        if (configuredPath.isBlank()) {
            throw new IllegalArgumentException("swath.bench.corpus-record must not be blank");
        }
        Path path = Path.of(configuredPath).toAbsolutePath().normalize();
        Map<String, String> fields = read(path);
        if (!fields.keySet().equals(FIELDS)) {
            throw new IOException("benchmark corpus record fields disagree: expected="
                    + FIELDS + " actual=" + fields.keySet() + " record=" + path);
        }
        require(fields, "format", FORMAT, path);
        require(fields, "corpus",
                corpus.stagingDir().toAbsolutePath().normalize().toString(), path);
        require(fields, "rows", Long.toString(corpus.oracle().rows()), path);
        require(fields, "segments", Integer.toString(corpus.inputs().size()), path);
        long bytes = corpus.inputs().stream()
                .mapToLong(ParallelMergeBenchmark.CorpusInput::size)
                .sum();
        require(fields, "bytes", Long.toString(bytes), path);
        require(fields, "corpus_id", corpus.identity(), path);
        require(fields, "multiset", corpus.oracle().multisetDigest(), path);
        String createdByHead = fields.get("created_by_head");
        if (createdByHead == null || !createdByHead.matches("[0-9a-f]{40}")) {
            throw new IOException("benchmark corpus record has an invalid created_by_head: " + path);
        }
    }

    private static Map<String, String> read(Path path) throws IOException {
        if (!Files.isRegularFile(path)) {
            throw new IOException("benchmark corpus record is not a regular file: " + path);
        }
        Map<String, String> fields = new LinkedHashMap<>();
        for (String line : Files.readAllLines(path)) {
            int separator = line.indexOf('=');
            if (separator <= 0 || fields.putIfAbsent(
                    line.substring(0, separator), line.substring(separator + 1)) != null) {
                throw new IOException("benchmark corpus record has an invalid or duplicate field: "
                        + path);
            }
        }
        return Map.copyOf(fields);
    }

    private static void require(Map<String, String> fields, String key,
                                String expected, Path path) throws IOException {
        String actual = fields.get(key);
        if (!expected.equals(actual)) {
            throw new IOException("benchmark corpus record disagrees for " + key
                    + ": expected=" + expected + " actual=" + actual + " record=" + path);
        }
    }
}

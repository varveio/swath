/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

class ArchitectureTest {

    @Test
    void replayRuntimeDoesNotDependOnConformancePackage() throws IOException {
        Path sourceRoot = Path.of("src/main/java/io/varve/swath/replay");

        try (var files = Files.walk(sourceRoot)) {
            List<Path> offenders = files
                    .filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(".java"))
                    .filter(path -> !path.startsWith(sourceRoot.resolve("conformance")))
                    .filter(ArchitectureTest::mentionsConformancePackage)
                    .toList();

            assertThat(offenders)
                    .as("replay runtime sources must not import or reference io.varve.swath.replay.conformance")
                    .isEmpty();
        }
    }

    private static boolean mentionsConformancePackage(Path path) {
        try {
            String source = Files.readString(path);
            return source.contains("io.varve.swath.replay.conformance")
                    || source.contains("replay.conformance");
        } catch (IOException e) {
            throw new IllegalStateException("failed to read " + path, e);
        }
    }
}

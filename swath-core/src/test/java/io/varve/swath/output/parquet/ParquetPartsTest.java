/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ParquetPartsTest {

    @Test
    void resolvesOneFileOrDirectPartsInLexicalFileNameOrder(@TempDir Path dir) throws IOException {
        Path single = Files.createFile(dir.resolve("one.parquet"));
        assertThat(ParquetParts.resolve(single)).containsExactly(single);

        Path parts = Files.createDirectories(dir.resolve("parts"));
        Path second = Files.createFile(parts.resolve("part-00002.parquet"));
        Path first = Files.createFile(parts.resolve("part-00001.parquet"));
        Files.createFile(parts.resolve("ignored.txt"));
        Path nested = Files.createDirectories(parts.resolve("nested"));
        Files.createFile(nested.resolve("nested.parquet"));

        assertThat(ParquetParts.resolve(parts)).containsExactly(first, second);
    }

    @Test
    void resolvesAnEmptyDirectoryToAnEmptyList(@TempDir Path dir) throws IOException {
        assertThat(ParquetParts.resolve(dir)).isEmpty();
    }

    @Test
    void delegatesAMissingPathToTheFilesystem(@TempDir Path dir) {
        Path missing = dir.resolve("missing");

        assertThatThrownBy(() -> ParquetParts.resolve(missing))
                .isInstanceOf(NoSuchFileException.class)
                .hasMessage(missing.toString());
    }
}

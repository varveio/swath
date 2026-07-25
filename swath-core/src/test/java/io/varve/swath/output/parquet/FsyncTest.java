/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import static org.assertj.core.api.Assertions.assertThatCode;

import java.nio.file.FileSystemException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class FsyncTest {

    @Test
    void unsupportedDirectoryFsyncDoesNotThrow() {
        assertThatCode(() -> Fsync.directory(Path.of("/tmp/swath-dir"),
                dir -> {
                    throw new FileSystemException(dir.toString(), null, "directory fsync unsupported");
                }))
                .doesNotThrowAnyException();
    }

    @Test
    void unsupportedOperationOnDirectoryFsyncDoesNotThrow() {
        // Some platforms throw UnsupportedOperationException when a FileChannel can't force() a dir.
        assertThatCode(() -> Fsync.directory(Path.of("/tmp/swath-dir"),
                dir -> {
                    throw new UnsupportedOperationException("directory fsync unsupported on this FS");
                }))
                .doesNotThrowAnyException();
    }
}

/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.dataset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.FileSystemException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class DurableFilesTest {

    @Test
    void forcesFileBeforePublishingItsParentEntry() throws Exception {
        List<String> calls = new ArrayList<>();
        DurableFiles.fileAndParent(Path.of("/tmp/swath-dir/part"),
                file -> calls.add("file:" + file.getFileName()),
                directory -> calls.add("directory:" + directory.getFileName()));
        assertThat(calls)
                .containsExactly("file:part", "directory:swath-dir");
    }

    @Test
    void fileForceFailurePreventsDirectoryPublicationBarrier() {
        List<String> calls = new ArrayList<>();
        assertThatThrownBy(() -> DurableFiles.fileAndParent(Path.of("/tmp/swath-dir/part"),
                file -> {
                    calls.add("file");
                    throw new IOException("force failed");
                }, directory -> calls.add("directory")))
                .isInstanceOf(IOException.class)
                .hasMessage("force failed");
        assertThat(calls).containsExactly("file");
    }

    @Test
    void unsupportedDirectoryFsyncDoesNotThrow() {
        assertThatCode(() -> DurableFiles.directory(Path.of("/tmp/swath-dir"),
                dir -> {
                    throw new FileSystemException(dir.toString(), null,
                            "directory fsync unsupported");
                }))
                .doesNotThrowAnyException();
    }

    @Test
    void unsupportedOperationOnDirectoryFsyncDoesNotThrow() {
        assertThatCode(() -> DurableFiles.directory(Path.of("/tmp/swath-dir"),
                dir -> {
                    throw new UnsupportedOperationException(
                            "directory fsync unsupported on this FS");
                }))
                .doesNotThrowAnyException();
    }
}

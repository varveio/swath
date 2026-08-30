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
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystemException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
        var identity = new DurableFiles.FileSystemIdentity(
                "overlay", "fixture-unsupported", "provider");
        assertThatCode(() -> DurableFiles.directory(Path.of("/tmp/swath-dir"), identity,
                dir -> {
                    throw new DurableFiles.DirectoryFsyncFailure(
                            DurableFiles.DirectoryFsyncPhase.FORCE,
                            new FileSystemException(dir.toString(), null,
                                    "operation not supported"));
                }))
                .doesNotThrowAnyException();
    }

    @Test
    void unsupportedOperationOnDirectoryFsyncDoesNotThrow() {
        var identity = new DurableFiles.FileSystemIdentity("fixture-uoe", "test", "provider");
        assertThatCode(() -> DurableFiles.directory(Path.of("/tmp/swath-dir"), identity,
                dir -> {
                    throw new UnsupportedOperationException(
                            "directory fsync unsupported on this FS");
                }))
                .doesNotThrowAnyException();
    }

    @Test
    void unsupportedFilesystemIsProbedOnlyOnce() throws Exception {
        var identity = new DurableFiles.FileSystemIdentity("fixture-once", "test", "provider");
        AtomicInteger probes = new AtomicInteger();
        DurableFiles.DirectoryForcer unsupported = dir -> {
            probes.incrementAndGet();
            throw new UnsupportedOperationException("not supported");
        };

        DurableFiles.probe(Path.of("/tmp/swath-dir"), identity, unsupported);
        DurableFiles.probe(Path.of("/tmp/swath-dir"), identity, unsupported);
        DurableFiles.directory(Path.of("/tmp/swath-dir"), identity, unsupported);

        assertThat(probes).hasValue(1);
    }

    @Test
    void supportedProbeIsFollowedByABarrierForEachMutation() throws Exception {
        var identity = new DurableFiles.FileSystemIdentity("fixture-supported", "test", "provider");
        AtomicInteger forces = new AtomicInteger();
        DurableFiles.DirectoryForcer supported = dir -> forces.incrementAndGet();

        DurableFiles.probe(Path.of("/tmp/swath-dir"), identity, supported);
        DurableFiles.directory(Path.of("/tmp/swath-dir"), identity, supported);
        DurableFiles.directory(Path.of("/tmp/swath-dir"), identity, supported);

        assertThat(forces).hasValue(3);
    }

    @Test
    void overlayInvalidArgumentIsClassifiedAtTheProbe() {
        var identity = new DurableFiles.FileSystemIdentity(
                "overlay", "overlay-invalid", "LinuxProvider");
        assertThatCode(() -> DurableFiles.probe(Path.of("/tmp/swath-dir"), identity,
                dir -> {
                    throw new DurableFiles.DirectoryFsyncFailure(
                            DurableFiles.DirectoryFsyncPhase.FORCE,
                            new IOException("Invalid argument"));
                }))
                .doesNotThrowAnyException();
    }

    @Test
    void localizedForceFailureOnKnownUnsupportedFilesystemDegrades() {
        var identity = new DurableFiles.FileSystemIdentity(
                "overlay", "overlay-localized", "LinuxProvider");
        assertThatCode(() -> DurableFiles.probe(Path.of("/tmp/swath-dir"), identity,
                dir -> {
                    throw new DurableFiles.DirectoryFsyncFailure(
                            DurableFiles.DirectoryFsyncPhase.FORCE,
                            new IOException("Argument non valide"));
                }))
                .doesNotThrowAnyException();
    }

    @Test
    void windowsDirectoryOpenAccessDeniedIsClassifiedAsUnsupported() {
        var identity = new DurableFiles.FileSystemIdentity("NTFS", "disk", "WindowsProvider");
        assertThatCode(() -> DurableFiles.probe(Path.of("C:/swath-dir"), identity,
                dir -> {
                    throw new DurableFiles.DirectoryFsyncFailure(
                            DurableFiles.DirectoryFsyncPhase.OPEN,
                            new AccessDeniedException(dir.toString()));
                }))
                .doesNotThrowAnyException();
    }

    @Test
    void fileSystemIdentityIsResolvedOnlyOncePerDirectory(@TempDir Path dir) throws Exception {
        AtomicInteger reads = new AtomicInteger();

        DurableFiles.fileSystemIdentity(dir, path -> {
            reads.incrementAndGet();
            return java.nio.file.Files.getFileStore(path);
        });
        DurableFiles.fileSystemIdentity(dir.resolve(".").normalize(), path -> {
            reads.incrementAndGet();
            return java.nio.file.Files.getFileStore(path);
        });

        assertThat(reads).hasValue(1);
    }

    @Test
    void fileStoreDiscoveryFailureFallsBackAndIsMemoized(@TempDir Path dir) throws Exception {
        AtomicInteger reads = new AtomicInteger();

        DurableFiles.FileSystemIdentity first = DurableFiles.fileSystemIdentity(dir, path -> {
            reads.incrementAndGet();
            throw new IOException("mount table unavailable");
        });
        DurableFiles.FileSystemIdentity second = DurableFiles.fileSystemIdentity(dir, path -> {
            reads.incrementAndGet();
            throw new AssertionError("memoized fallback must avoid another lookup");
        });

        assertThat(first.type()).isEqualTo("unknown");
        assertThat(second).isEqualTo(first);
        assertThat(reads).hasValue(1);
    }

    @Test
    void sameTypeAndNameOnDifferentMountsHaveDistinctSupportIdentities() {
        String provider = "LinuxProvider";
        var first = new DurableFiles.FileSystemIdentity(
                "overlay", "overlay", provider, "/mnt/one");
        var second = new DurableFiles.FileSystemIdentity(
                "overlay", "overlay", provider, "/mnt/two");

        assertThat(first).isNotEqualTo(second);
    }

    @Test
    void oneMountIdentityIsSharedAcrossItsDirectories() {
        Object mount = new Object();
        var root = new DurableFiles.FileSystemIdentity(
                "overlay", "overlay", "LinuxProvider", mount);
        var child = new DurableFiles.FileSystemIdentity(
                "overlay", "overlay", "LinuxProvider", mount);

        assertThat(root).isEqualTo(child);
    }

    @Test
    void failedChildDiscoveryRemainsPathScopedAcrossUnknownMountBoundaries(@TempDir Path dir)
            throws Exception {
        Path child = java.nio.file.Files.createDirectory(dir.resolve("data"));
        DurableFiles.FileSystemIdentity root = DurableFiles.fileSystemIdentity(dir, path -> {
            throw new IOException("mount table unavailable");
        });

        DurableFiles.FileSystemIdentity nested = DurableFiles.fileSystemIdentity(child, path -> {
            throw new IOException("mount table unavailable");
        });

        assertThat(nested).isNotEqualTo(root);
    }

    @Test
    void realDirectoriesOnOneFileStoreShareSupportIdentity(@TempDir Path dir) throws Exception {
        Path child = java.nio.file.Files.createDirectory(dir.resolve("data"));

        DurableFiles.FileSystemIdentity root = DurableFiles.fileSystemIdentity(
                dir, java.nio.file.Files::getFileStore);
        DurableFiles.FileSystemIdentity nested = DurableFiles.fileSystemIdentity(
                child, java.nio.file.Files::getFileStore);

        assertThat(nested).isEqualTo(root);
    }

    @Test
    void realDirectoryProbeAndSteadyStateBarrierSucceed(@TempDir Path dir) {
        assertThatCode(() -> {
            DurableFiles.probeDirectoryFsync(dir);
            DurableFiles.directory(dir);
        }).doesNotThrowAnyException();
    }

    @Test
    void unexpectedDirectoryFsyncFailureRemainsFatal() {
        var identity = new DurableFiles.FileSystemIdentity("fixture-permission", "test", "provider");
        assertThatThrownBy(() -> DurableFiles.probe(Path.of("/tmp/swath-dir"), identity,
                dir -> {
                    throw new FileSystemException(dir.toString(), null, "permission denied");
                }))
                .isInstanceOf(FileSystemException.class)
                .hasMessageContaining("permission denied");
    }

    @Test
    void permissionFailureOnKnownUnsupportedFilesystemRemainsFatal() {
        var identity = new DurableFiles.FileSystemIdentity(
                "overlay", "overlay-permission", "LinuxProvider");
        assertThatThrownBy(() -> DurableFiles.probe(Path.of("/tmp/swath-dir"), identity,
                dir -> {
                    throw new DurableFiles.DirectoryFsyncFailure(
                            DurableFiles.DirectoryFsyncPhase.OPEN,
                            new AccessDeniedException(dir.toString()));
                }))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void plainFileSystemFailureIsNotBroadlySwallowedOnAnUnknownFilesystem() {
        var identity = new DurableFiles.FileSystemIdentity("ext4", "disk", "LinuxProvider");
        assertThatThrownBy(() -> DurableFiles.probe(Path.of("/tmp/swath-dir"), identity,
                dir -> {
                    throw new FileSystemException(dir.toString(), null, "Invalid argument");
                }))
                .isInstanceOf(FileSystemException.class)
                .hasMessageContaining("Invalid argument");
    }
}

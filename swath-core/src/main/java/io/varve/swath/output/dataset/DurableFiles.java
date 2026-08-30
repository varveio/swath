/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.dataset;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Crash-durability barriers shared by every local dataset format. */
public final class DurableFiles {
    private static final Logger log = LoggerFactory.getLogger(DurableFiles.class);
    private static final Map<FileSystemIdentity, DirectoryFsyncSupport> DIRECTORY_FSYNC_SUPPORT =
            new HashMap<>();
    private static final Map<Path, FileSystemIdentity> DIRECTORY_FILE_SYSTEM_IDENTITIES =
            new HashMap<>();

    private DurableFiles() {
    }

    /** Force a completed file's bytes and then the parent entry that names it. */
    public static void fileAndParent(Path file) throws IOException {
        fileAndParent(file, path -> forceFile(path, true), DurableFiles::directory);
    }

    private static void forceFile(Path file, boolean metadata) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
            channel.force(metadata);
        }
    }

    @FunctionalInterface
    interface FileForcer {
        void force(Path file) throws IOException;
    }

    static void fileAndParent(Path file, FileForcer fileForcer, DirectoryForcer directoryForcer)
            throws IOException {
        fileForcer.force(file);
        directoryForcer.force(file.getParent());
    }

    /**
     * Force directory-entry changes where supported. File-data failures remain fatal; unsupported
     * directory forcing is a documented debug-logged portability degradation.
     */
    public static void directory(Path directory) throws IOException {
        directory(directory, fileSystemIdentity(directory), DurableFiles::forceDirectory);
    }

    /** Probe directory-fsync support before output work begins, once per filesystem/provider. */
    public static void probeDirectoryFsync(Path directory) throws IOException {
        probe(directory, fileSystemIdentity(directory), DurableFiles::forceDirectory);
    }

    private static void forceDirectory(Path path) throws IOException {
        final FileChannel channel;
        try {
            channel = FileChannel.open(path);
        } catch (IOException e) {
            throw new DirectoryFsyncFailure(DirectoryFsyncPhase.OPEN, e);
        }
        try (channel) {
            try {
                channel.force(true);
            } catch (IOException e) {
                throw new DirectoryFsyncFailure(DirectoryFsyncPhase.FORCE, e);
            }
        }
    }

    @FunctionalInterface
    interface DirectoryForcer {
        void force(Path directory) throws IOException;
    }

    static void directory(Path directory, FileSystemIdentity identity, DirectoryForcer forcer)
            throws IOException {
        ProbeOutcome outcome = probe(directory, identity, forcer);
        if (outcome.support() == DirectoryFsyncSupport.SUPPORTED && !outcome.performed()) {
            // The startup probe proves support; every later directory mutation still needs a
            // barrier. A failure after that proof is real I/O failure and remains fatal.
            forcer.force(directory);
        }
    }

    static ProbeOutcome probe(Path directory, FileSystemIdentity identity, DirectoryForcer forcer)
            throws IOException {
        synchronized (DIRECTORY_FSYNC_SUPPORT) {
            DirectoryFsyncSupport known = DIRECTORY_FSYNC_SUPPORT.get(identity);
            if (known != null) {
                return new ProbeOutcome(known, false);
            }
            try {
                forcer.force(directory);
                DIRECTORY_FSYNC_SUPPORT.put(identity, DirectoryFsyncSupport.SUPPORTED);
                return new ProbeOutcome(DirectoryFsyncSupport.SUPPORTED, true);
            } catch (UnsupportedOperationException e) {
                return rememberUnsupported(directory, identity, e);
            } catch (IOException e) {
                if (!knownUnsupportedDirectoryFsync(e, identity)) {
                    throw directoryFsyncCause(e);
                }
                return rememberUnsupported(directory, identity, directoryFsyncCause(e));
            }
        }
    }

    private static ProbeOutcome rememberUnsupported(Path directory, FileSystemIdentity identity,
            Throwable failure) {
        DIRECTORY_FSYNC_SUPPORT.put(identity, DirectoryFsyncSupport.UNSUPPORTED);
        log.debug("directory fsync unsupported; continuing without directory barrier path={} "
                        + "fs_type={} fs_name={} provider={}",
                directory, identity.type(), identity.name(), identity.provider(), failure);
        return new ProbeOutcome(DirectoryFsyncSupport.UNSUPPORTED, true);
    }

    private static boolean knownUnsupportedDirectoryFsync(IOException failure,
            FileSystemIdentity identity) {
        DirectoryFsyncPhase phase = failure instanceof DirectoryFsyncFailure classified
                ? classified.phase
                : DirectoryFsyncPhase.UNKNOWN;
        IOException cause = directoryFsyncCause(failure);
        if (cause instanceof AccessDeniedException
                && phase == DirectoryFsyncPhase.OPEN
                && isWindowsFilesystem(identity)) {
            return true;
        }
        if (phase == DirectoryFsyncPhase.FORCE
                && isKnownUnsupportedFilesystem(identity)
                && !(cause instanceof AccessDeniedException)
                && !(cause instanceof NoSuchFileException)) {
            return true;
        }
        return false;
    }

    private static FileSystemIdentity fileSystemIdentity(Path directory) throws IOException {
        return fileSystemIdentity(directory, Files::getFileStore);
    }

    static FileSystemIdentity fileSystemIdentity(Path directory, FileStoreReader reader)
            throws IOException {
        Path normalized = directory.toAbsolutePath().normalize();
        synchronized (DIRECTORY_FSYNC_SUPPORT) {
            FileSystemIdentity known = DIRECTORY_FILE_SYSTEM_IDENTITIES.get(normalized);
            if (known != null) {
                return known;
            }
            String provider = normalized.getFileSystem().provider().getClass().getName();
            FileSystemIdentity discovered;
            try {
                FileStore store = reader.read(normalized);
                discovered = new FileSystemIdentity(
                        store.type(), store.name(), provider, store);
            } catch (IOException e) {
                // FileStore discovery is diagnostic input for support classification, not a
                // durability barrier of its own. Preserve the direct fsync attempt when mount-table
                // discovery is unavailable in a chroot/container.
                discovered = new FileSystemIdentity(
                        "unknown", normalized.toString(), provider, normalized.toString());
                log.debug("filesystem identity unavailable; probing directory fsync directly "
                                + "path={} provider={}",
                        normalized, provider, e);
            }
            DIRECTORY_FILE_SYSTEM_IDENTITIES.put(normalized, discovered);
            return discovered;
        }
    }

    private static IOException directoryFsyncCause(IOException failure) {
        return failure instanceof DirectoryFsyncFailure classified
                ? classified.failure
                : failure;
    }

    private static boolean isKnownUnsupportedFilesystem(FileSystemIdentity identity) {
        String type = identity.type().toLowerCase(java.util.Locale.ROOT);
        return type.equals("overlay") || type.startsWith("nfs")
                || type.equals("cifs") || type.equals("smbfs") || type.startsWith("fuse")
                || type.equals("9p") || type.equals("vboxsf") || type.equals("virtiofs")
                || type.equals("apfs") || isWindowsFilesystem(identity);
    }

    private static boolean isWindowsFilesystem(FileSystemIdentity identity) {
        String type = identity.type().toLowerCase(java.util.Locale.ROOT);
        String provider = identity.provider().toLowerCase(java.util.Locale.ROOT);
        return type.equals("ntfs") || type.equals("fat") || type.equals("fat32")
                || type.equals("exfat") || provider.contains("windows");
    }

    @FunctionalInterface
    interface FileStoreReader {
        FileStore read(Path directory) throws IOException;
    }

    enum DirectoryFsyncPhase {
        OPEN,
        FORCE,
        UNKNOWN
    }

    static final class DirectoryFsyncFailure extends IOException {
        private final DirectoryFsyncPhase phase;
        private final IOException failure;

        DirectoryFsyncFailure(DirectoryFsyncPhase phase, IOException failure) {
            super(failure.getMessage(), failure);
            this.phase = phase;
            this.failure = failure;
        }
    }

    record FileSystemIdentity(String type, String name, String provider, Object mountKey) {
        FileSystemIdentity(String type, String name, String provider) {
            this(type, name, provider, name);
        }
    }

    enum DirectoryFsyncSupport {
        SUPPORTED,
        UNSUPPORTED
    }

    record ProbeOutcome(DirectoryFsyncSupport support, boolean performed) {
    }
}

/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Merge-start disk-admission policy threaded into the core sorter.
 *
 * <p>The CLI uses {@link #enforced()} unless {@code sort.ignore-disk-check=on}, in which case it
 * uses {@link #bypassed()}. The probe is injectable so tests can exercise exact decisions without
 * filling a real filesystem. A failed filesystem query is represented by negative usable bytes and
 * deliberately fails open, matching {@link SortDiskGuard}'s existing behavior.
 */
public final class MergeDiskPolicy {

    private static final Object UNKNOWN_STORE = new Object();

    private final boolean bypassed;
    private final SpaceProbe probe;

    private MergeDiskPolicy(boolean bypassed, SpaceProbe probe) {
        this.bypassed = bypassed;
        this.probe = Objects.requireNonNull(probe, "probe");
    }

    /** Production policy: inspect the actual staging and final-output filesystems. */
    public static MergeDiskPolicy enforced() {
        return new MergeDiskPolicy(false, MergeDiskPolicy::probeReal);
    }

    /** Explicit {@code sort.ignore-disk-check=on} policy. */
    public static MergeDiskPolicy bypassed() {
        return new MergeDiskPolicy(true, ignored -> new Space(UNKNOWN_STORE, -1L));
    }

    /** Library/test compatibility policy for callers that do not opt into filesystem admission. */
    static MergeDiskPolicy disabled() {
        return bypassed();
    }

    /** Narrow deterministic test seam. Store identities compare by {@link Object#equals}. */
    static MergeDiskPolicy enforced(SpaceProbe probe) {
        return new MergeDiskPolicy(false, probe);
    }

    boolean bypassedByCaller() {
        return bypassed;
    }

    Snapshot snapshot(Path stagingDir, Path outputDir) {
        if (bypassed) {
            return new Snapshot(-1L, -1L, false);
        }
        Space staging = probeSafely(stagingDir);
        Space output = probeSafely(outputDir);
        boolean shared = staging.storeIdentity() != UNKNOWN_STORE
                && output.storeIdentity() != UNKNOWN_STORE
                && staging.storeIdentity().equals(output.storeIdentity());
        return new Snapshot(staging.usableBytes(), output.usableBytes(), shared);
    }

    private Space probeSafely(Path path) {
        try {
            Space space = probe.probe(path);
            return space == null ? new Space(UNKNOWN_STORE, -1L) : space;
        } catch (IOException | RuntimeException e) {
            return new Space(UNKNOWN_STORE, -1L);
        }
    }

    private static Space probeReal(Path path) throws IOException {
        FileStore store = Files.getFileStore(path);
        return new Space(store, store.getUsableSpace());
    }

    @FunctionalInterface
    interface SpaceProbe {
        Space probe(Path path) throws IOException;
    }

    record Space(Object storeIdentity, long usableBytes) {
        Space {
            Objects.requireNonNull(storeIdentity, "storeIdentity");
        }
    }

    record Snapshot(long stagingUsableBytes, long outputUsableBytes, boolean sharedStore) {
        long sharedUsableBytes() {
            if (stagingUsableBytes < 0) {
                return outputUsableBytes;
            }
            if (outputUsableBytes < 0) {
                return stagingUsableBytes;
            }
            return Math.min(stagingUsableBytes, outputUsableBytes);
        }
    }
}

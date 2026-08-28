/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/** Validated sorter-owned inputs and exact post-publish reconciliation of their staging directory. */
public final class StagingReconciliation {

    private final Path ownedStagingDir;
    private final List<String> originalNamesInOrder;
    private final Set<String> originalNames;

    private StagingReconciliation(Path ownedStagingDir, Collection<String> originalNames) {
        this.ownedStagingDir = ownedStagingDir;
        this.originalNamesInOrder = List.copyOf(originalNames);
        this.originalNames = Set.copyOf(originalNames);
    }

    /**
     * Build the exact retained set from paths already selected for a live merge. Every original must
     * be an immediate, ordinary page-run file under {@code stagingDir}; validation happens before
     * the merge publishes anything.
     */
    public static StagingReconciliation fromPaths(Path stagingDir, List<Path> originals)
            throws IOException {
        Path normalizedStaging = stagingDir.toAbsolutePath().normalize();
        LinkedHashSet<String> names = new LinkedHashSet<>();
        LinkedHashSet<Object> physicalIdentities = new LinkedHashSet<>();
        for (Path original : originals) {
            Path normalized = original.toAbsolutePath().normalize();
            if (!normalizedStaging.equals(normalized.getParent())) {
                throw new IOException("sort staging segment is not an immediate child of "
                        + stagingDir + ": " + original);
            }
            String name = normalized.getFileName().toString();
            requireSafePageRunName(name);
            if (!names.add(name)) {
                throw new IOException("duplicate sort staging segment: " + name);
            }
            BasicFileAttributes attributes = Files.readAttributes(
                    normalized, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!attributes.isRegularFile()) {
                throw new IOException(
                        "sort staging segment is missing or not an ordinary file: " + normalized);
            }
            Object physicalIdentity = attributes.fileKey();
            if (physicalIdentity != null && !physicalIdentities.add(physicalIdentity)) {
                throw new IOException("duplicate physical sort staging segment: " + normalized);
            }
        }
        StagingReconciliation reconciliation = new StagingReconciliation(normalizedStaging, names);
        reconciliation.requireOriginalFiles(normalizedStaging);
        return reconciliation;
    }

    /** Build the exact retained set from checkpoint-finalized page-run names. */
    public static StagingReconciliation fromNames(Collection<String> originals) throws IOException {
        LinkedHashSet<String> names = new LinkedHashSet<>();
        for (String name : originals) {
            requireSafePageRunName(name);
            if (!names.add(name)) {
                throw new IOException("duplicate retained sort staging segment: " + name);
            }
        }
        return new StagingReconciliation(null, names);
    }

    /** An empty exact set, used by owner-authorized fresh/default cleanup. */
    public static StagingReconciliation discardAll() {
        return new StagingReconciliation(null, Set.of());
    }

    /**
     * Return the path-backed originals in their caller-supplied order, after lexical aliases have
     * been collapsed to the validated absolute staging authority. Only {@link #fromPaths} creates
     * a path-backed reconciliation.
     */
    List<Path> ownedPaths() {
        if (ownedStagingDir == null) {
            throw new IllegalStateException(
                    "checkpoint-name reconciliation has no owned path authority");
        }
        return originalNamesInOrder.stream().map(ownedStagingDir::resolve).toList();
    }

    /** Delete only the normalized originals validated by {@link #fromPaths}. */
    void deleteOwnedOriginals() throws IOException {
        for (Path path : ownedPaths()) {
            Files.deleteIfExists(path);
        }
    }

    /**
     * Preserve exactly the validated originals and remove every other immediate staging entry.
     * Retained entries are all checked before the first deletion, so a missing file or planted
     * symlink never leaves a checkpoint pointing at a partially reconciled set. Non-retained child
     * trees are safe to remove recursively because {@code _staging} is sorter-owned; symbolic links
     * are unlinked and never followed by {@link Sweeps#deleteTree}.
     */
    public Result reconcile(Path stagingDir) throws IOException {
        Path normalizedStaging = requireAuthority(stagingDir);
        if (!Files.exists(normalizedStaging, LinkOption.NOFOLLOW_LINKS)) {
            if (originalNames.isEmpty()) {
                return new Result(0, 0);
            }
            throw new IOException("sort staging directory is missing: " + normalizedStaging);
        }
        if (!Files.isDirectory(normalizedStaging, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException(
                    "sort staging path is not an ordinary directory: " + normalizedStaging);
        }
        requireOriginalFiles(normalizedStaging);

        int removed = 0;
        try (Stream<Path> entries = Files.list(normalizedStaging)) {
            for (Path entry : entries.toList()) {
                if (originalNames.contains(entry.getFileName().toString())) {
                    continue;
                }
                Sweeps.deleteTree(entry);
                removed++;
            }
        }
        return new Result(originalNames.size(), removed);
    }

    private Path requireAuthority(Path stagingDir) throws IOException {
        Path normalized = stagingDir.toAbsolutePath().normalize();
        if (ownedStagingDir != null && !ownedStagingDir.equals(normalized)) {
            throw new IOException("sort staging authority changed after input validation: expected "
                    + ownedStagingDir + " but was " + normalized);
        }
        return normalized;
    }

    /** Sweep the sorter-owned final temporary namespace through its canonical glob. */
    public static void sweepFinalTemporaries(Path dir) throws IOException {
        Sweeps.sweep(dir, ignored -> { }, StagingNames.FINAL_TMP_GLOB);
    }

    private void requireOriginalFiles(Path stagingDir) throws IOException {
        for (String name : originalNames) {
            Path original = stagingDir.resolve(name);
            if (!Files.isRegularFile(original, LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("sort staging segment is missing or not an ordinary file: "
                        + original);
            }
        }
    }

    private static void requireSafePageRunName(String name) throws IOException {
        if (name == null || name.isBlank()) {
            throw new IOException("retained sort staging segment name is empty");
        }
        final Path parsed;
        try {
            parsed = Path.of(name);
        } catch (InvalidPathException e) {
            throw new IOException("invalid retained sort staging segment name: " + name, e);
        }
        if (parsed.isAbsolute() || parsed.getNameCount() != 1
                || !name.equals(parsed.getFileName().toString())
                || !name.endsWith(StagingNames.PAGE_RUN_SUFFIX)) {
            throw new IOException("unsafe retained sort staging segment name: " + name);
        }
    }

    /** Cheap reconciliation classification carried by the stable retention log marker. */
    public record Result(int retainedEntries, int removedEntries) {
    }
}

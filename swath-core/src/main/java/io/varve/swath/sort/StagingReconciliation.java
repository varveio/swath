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
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/** Exact post-publish reconciliation of sorter-owned staging against its durable originals. */
public final class StagingReconciliation {

    private final Set<String> originalNames;

    private StagingReconciliation(Set<String> originalNames) {
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
        for (Path original : originals) {
            Path normalized = original.toAbsolutePath().normalize();
            if (!normalizedStaging.equals(normalized.getParent())) {
                throw new IOException("retained sort staging segment is not an immediate child of "
                        + stagingDir + ": " + original);
            }
            String name = normalized.getFileName().toString();
            requireSafePageRunName(name);
            if (!names.add(name)) {
                throw new IOException("duplicate retained sort staging segment: " + name);
            }
        }
        StagingReconciliation reconciliation = new StagingReconciliation(names);
        reconciliation.requireOriginalFiles(stagingDir);
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
        return new StagingReconciliation(names);
    }

    /** An empty exact set, used by owner-authorized fresh/default cleanup. */
    public static StagingReconciliation discardAll() {
        return new StagingReconciliation(Set.of());
    }

    /**
     * Preserve exactly the validated originals and remove every other immediate staging entry.
     * Retained entries are all checked before the first deletion, so a missing file or planted
     * symlink never leaves a checkpoint pointing at a partially reconciled set. Non-retained child
     * trees are safe to remove recursively because {@code _staging} is sorter-owned; symbolic links
     * are unlinked and never followed by {@link Sweeps#deleteTree}.
     */
    public Result reconcile(Path stagingDir) throws IOException {
        if (!Files.exists(stagingDir, LinkOption.NOFOLLOW_LINKS)) {
            if (originalNames.isEmpty()) {
                return new Result(0, 0);
            }
            throw new IOException("sort staging directory is missing: " + stagingDir);
        }
        if (!Files.isDirectory(stagingDir, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("sort staging path is not an ordinary directory: " + stagingDir);
        }
        requireOriginalFiles(stagingDir);

        int removed = 0;
        try (Stream<Path> entries = Files.list(stagingDir)) {
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

    /** Sweep the sorter-owned final temporary namespace through its canonical glob. */
    public static void sweepFinalTemporaries(Path dir) throws IOException {
        Sweeps.sweep(dir, ignored -> { }, StagingNames.FINAL_TMP_GLOB);
    }

    private void requireOriginalFiles(Path stagingDir) throws IOException {
        for (String name : originalNames) {
            Path original = stagingDir.resolve(name);
            if (!Files.isRegularFile(original, LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("retained sort staging segment is missing or not an ordinary file: "
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

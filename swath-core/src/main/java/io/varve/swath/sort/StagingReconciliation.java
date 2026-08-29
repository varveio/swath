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

    private final DirectoryAuthority ownedStagingAuthority;
    private final List<String> originalNamesInOrder;
    private final Set<String> originalNames;

    private StagingReconciliation(DirectoryAuthority ownedStagingAuthority,
            Collection<String> originalNames) {
        this.ownedStagingAuthority = ownedStagingAuthority;
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
        return fromPaths(stagingDir, originals, path -> Files.readAttributes(
                path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS));
    }

    /** Deterministic attribute seam for physical-identity contract tests. */
    static StagingReconciliation fromPaths(Path stagingDir, List<Path> originals,
            AttributeReader attributeReader) throws IOException {
        DirectoryAuthority stagingAuthority = DirectoryAuthority.capture(
                stagingDir, "sort staging directory");
        Path normalizedStaging = stagingAuthority.normalizedPath();
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
            BasicFileAttributes attributes = attributeReader.read(normalized);
            if (!attributes.isRegularFile()) {
                throw new IOException(
                        "sort staging segment is missing or not an ordinary file: " + normalized);
            }
            Object physicalIdentity = attributes.fileKey();
            if (physicalIdentity == null) {
                throw new IOException("cannot establish physical identity for sort staging segment "
                        + "because the filesystem did not provide a file key: " + normalized);
            }
            if (!physicalIdentities.add(physicalIdentity)) {
                throw new IOException("duplicate physical sort staging segment: " + normalized);
            }
            Path realParent = normalized.toRealPath().getParent();
            if (!stagingAuthority.realPath().equals(realParent)) {
                throw new IOException("sort staging segment resolves outside the owned staging "
                        + "directory: " + normalized);
            }
        }
        StagingReconciliation reconciliation = new StagingReconciliation(stagingAuthority, names);
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
        if (ownedStagingAuthority == null) {
            throw new IllegalStateException(
                    "checkpoint-name reconciliation has no owned path authority");
        }
        return originalNamesInOrder.stream()
                .map(ownedStagingAuthority.normalizedPath()::resolve).toList();
    }

    /** Delete only the normalized originals validated by {@link #fromPaths}. */
    void deleteOwnedOriginals() throws IOException {
        requireAuthority(ownedStagingAuthority.normalizedPath());
        for (Path path : ownedPaths()) {
            requireAuthority(ownedStagingAuthority.normalizedPath());
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
        DirectoryAuthority phaseAuthority = ownedStagingAuthority != null
                ? ownedStagingAuthority
                : DirectoryAuthority.capture(normalizedStaging, "sort staging directory");
        requireOriginalFiles(normalizedStaging);

        int removed = 0;
        try (Stream<Path> entries = Files.list(normalizedStaging)) {
            for (Path entry : entries.toList()) {
                if (originalNames.contains(entry.getFileName().toString())) {
                    continue;
                }
                phaseAuthority.requireSame(normalizedStaging);
                Sweeps.deleteTree(entry);
                removed++;
            }
        }
        return new Result(originalNames.size(), removed);
    }

    private Path requireAuthority(Path stagingDir) throws IOException {
        Path normalized = stagingDir.toAbsolutePath().normalize();
        if (ownedStagingAuthority != null) {
            ownedStagingAuthority.requireSame(normalized);
        }
        return normalized;
    }

    /** Revalidate the retained staging directory immediately before a destructive phase. */
    void requireOwnedStagingAuthority(Path stagingDir) throws IOException {
        if (ownedStagingAuthority == null) {
            throw new IOException("checkpoint-name reconciliation has no owned staging authority");
        }
        requireAuthority(stagingDir);
    }

    /** Whether an immediate child belongs to the exact original input set. */
    boolean ownsImmediateChild(Path path) {
        if (ownedStagingAuthority == null) {
            return false;
        }
        Path normalized = path.toAbsolutePath().normalize();
        return ownedStagingAuthority.normalizedPath().equals(normalized.getParent())
                && originalNames.contains(normalized.getFileName().toString());
    }

    /** Delete one validated immediate-child working file, never an original input. */
    void deleteDisposable(Path path) throws IOException {
        if (ownedStagingAuthority == null) {
            throw new IOException("checkpoint-name reconciliation has no owned staging authority");
        }
        requireOwnedStagingAuthority(ownedStagingAuthority.normalizedPath());
        Path normalized = path.toAbsolutePath().normalize();
        if (!ownedStagingAuthority.normalizedPath().equals(normalized.getParent())) {
            throw new IOException("disposable sort path is outside the owned staging directory: "
                    + normalized);
        }
        if (originalNames.contains(normalized.getFileName().toString())) {
            throw new IOException("refusing to delete original sort staging segment as disposable: "
                    + normalized);
        }
        Files.deleteIfExists(normalized);
    }

    /** Sweep one owned working namespace after revalidating its directory authority. */
    void sweepDisposables(String glob) throws IOException {
        if (ownedStagingAuthority == null) {
            throw new IOException("checkpoint-name reconciliation has no owned staging authority");
        }
        requireOwnedStagingAuthority(ownedStagingAuthority.normalizedPath());
        try (var entries = Files.newDirectoryStream(ownedStagingAuthority.normalizedPath(), glob)) {
            for (Path entry : entries) {
                if (!ownsImmediateChild(entry)) {
                    requireOwnedStagingAuthority(ownedStagingAuthority.normalizedPath());
                    Files.deleteIfExists(entry);
                }
            }
        }
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
        if (name.startsWith("merge-")) {
            throw new IOException("sort staging segment name collides with the disposable cascade "
                    + "namespace: " + name);
        }
    }

    @FunctionalInterface
    interface AttributeReader {
        BasicFileAttributes read(Path path) throws IOException;
    }

    /** Canonical physical identity retained for destructive directory authority checks. */
    static final class DirectoryAuthority {
        private final Path normalizedPath;
        private final Path realPath;
        private final Object fileKey;
        private final String description;

        private DirectoryAuthority(Path normalizedPath, Path realPath, Object fileKey,
                String description) {
            this.normalizedPath = normalizedPath;
            this.realPath = realPath;
            this.fileKey = fileKey;
            this.description = description;
        }

        static DirectoryAuthority capture(Path path, String description) throws IOException {
            Path normalized = path.toAbsolutePath().normalize();
            BasicFileAttributes attributes = Files.readAttributes(
                    normalized, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!attributes.isDirectory()) {
                throw new IOException(description + " is missing, not an ordinary directory, or "
                        + "is a symbolic link: " + normalized);
            }
            Object fileKey = requireFileKey(attributes, normalized, description);
            Path real = normalized.toRealPath();
            BasicFileAttributes realAttributes = Files.readAttributes(
                    real, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!realAttributes.isDirectory()
                    || !fileKey.equals(requireFileKey(realAttributes, real, description))) {
                throw new IOException(description + " changed while its authority was captured: "
                        + normalized);
            }
            return new DirectoryAuthority(normalized, real, fileKey, description);
        }

        void requireSame(Path path) throws IOException {
            Path normalized = path.toAbsolutePath().normalize();
            if (!normalizedPath.equals(normalized)) {
                throw new IOException(description + " path changed after validation: expected "
                        + normalizedPath + " but was " + normalized);
            }
            BasicFileAttributes attributes = Files.readAttributes(
                    normalized, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!attributes.isDirectory()
                    || !fileKey.equals(requireFileKey(attributes, normalized, description))
                    || !realPath.equals(normalized.toRealPath())) {
                throw new IOException(description + " identity changed after validation: "
                        + normalized);
            }
        }

        Path normalizedPath() {
            return normalizedPath;
        }

        Path realPath() {
            return realPath;
        }

        private static Object requireFileKey(BasicFileAttributes attributes, Path path,
                String description) throws IOException {
            Object fileKey = attributes.fileKey();
            if (fileKey == null) {
                throw new IOException("cannot establish physical identity for " + description
                        + " because the filesystem did not provide a file key: " + path);
            }
            return fileKey;
        }
    }

    /** Cheap reconciliation classification carried by the stable retention log marker. */
    public record Result(int retainedEntries, int removedEntries) {
    }
}

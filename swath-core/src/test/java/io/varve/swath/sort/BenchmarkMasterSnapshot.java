/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;

/** Byte/content and directory-entry snapshot of an external retained benchmark master. */
final class BenchmarkMasterSnapshot {

    private final Path output;
    private final List<Entry> entries;

    private BenchmarkMasterSnapshot(Path output, List<Entry> entries) {
        this.output = output;
        this.entries = entries;
    }

    static BenchmarkMasterSnapshot capture(Path output) throws IOException {
        return new BenchmarkMasterSnapshot(output, scan(output));
    }

    void verifyUnchanged() throws IOException {
        List<Entry> after = scan(output);
        if (!entries.equals(after)) {
            throw new IOException("benchmark modified retained master tree: " + output);
        }
    }

    private static List<Entry> scan(Path output) throws IOException {
        List<Entry> result = new ArrayList<>();
        try (var paths = Files.walk(output)) {
            for (Path path : paths.sorted(Comparator.comparing(Path::toString)).toList()) {
                Path relative = output.relativize(path);
                BasicFileAttributes attributes = Files.readAttributes(
                        path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                Kind kind = attributes.isDirectory() ? Kind.DIRECTORY
                        : attributes.isRegularFile() ? Kind.FILE : Kind.OTHER;
                String relativeText = relative.toString();
                String digest = kind == Kind.FILE ? sha256(path) : "";
                result.add(new Entry(relativeText, kind,
                        kind == Kind.FILE ? attributes.size() : 0,
                        kind == Kind.FILE ? attributes.lastModifiedTime().toString() : "",
                        kind == Kind.FILE ? String.valueOf(attributes.fileKey()) : "", digest));
            }
        }
        return List.copyOf(result);
    }

    private static String sha256(Path path) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError("SHA-256 is required by the JDK", e);
        }
        byte[] buffer = new byte[64 * 1024];
        try (InputStream in = Files.newInputStream(path)) {
            int read;
            while ((read = in.read(buffer)) >= 0) {
                if (read > 0) {
                    digest.update(buffer, 0, read);
                }
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private enum Kind {
        DIRECTORY,
        FILE,
        OTHER
    }

    private record Entry(String relative, Kind kind, long size, String modified,
                         String fileKey, String sha256) {
    }
}

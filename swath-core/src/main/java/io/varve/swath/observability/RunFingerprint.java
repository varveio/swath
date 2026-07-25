/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * The process run-fingerprint fields for the JSON summary's {@code shape.fingerprint}
 * block — the SHA-256 of the running artifact and the build git SHA. Both are constant for the life
 * of the JVM, so they are computed once (lazily) and cached. Best-effort: an unavailable value is
 * {@code null} (rendered as JSON {@code null}), never a fabricated string.
 *
 * <p>{@code binary_sha256} is the digest of the swath jar on the classpath (the code source of this
 * class). In a dev/test run the code source is an exploded classes directory, not a single file, so
 * it is not hashed and the field is {@code null}. {@code git_sha} is read from the {@code swath.git.sha}
 * system property (which the build/container can inject) or, failing that, the jar manifest's
 * {@code Implementation-Version} — {@code null} when neither is present.
 */
final class RunFingerprint {

    private static final Object LOCK = new Object();
    private static boolean computed;
    private static String binarySha256;
    private static String gitSha;

    private RunFingerprint() {
    }

    static String binarySha256() {
        ensureComputed();
        return binarySha256;
    }

    static String gitSha() {
        ensureComputed();
        return gitSha;
    }

    private static void ensureComputed() {
        synchronized (LOCK) {
            if (computed) {
                return;
            }
            binarySha256 = computeBinarySha256();
            gitSha = computeGitSha();
            computed = true;
        }
    }

    private static String computeBinarySha256() {
        try {
            var codeSource = RunFingerprint.class.getProtectionDomain().getCodeSource();
            if (codeSource == null || codeSource.getLocation() == null) {
                return null;
            }
            Path path = Path.of(codeSource.getLocation().toURI());
            if (!Files.isRegularFile(path)) {
                return null;   // exploded classes dir (dev/test) — no single artifact to hash
            }
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream in = Files.newInputStream(path);
                 DigestInputStream dis = new DigestInputStream(in, digest)) {
                byte[] buf = new byte[1 << 16];
                while (dis.read(buf) != -1) {
                    // digest updated as a side effect of the read
                }
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (IOException | URISyntaxException | NoSuchAlgorithmException | RuntimeException e) {
            return null;
        }
    }

    private static String computeGitSha() {
        String prop = System.getProperty("swath.git.sha");
        if (prop != null && !prop.isBlank()) {
            return prop.trim();
        }
        Package pkg = RunFingerprint.class.getPackage();
        String version = pkg == null ? null : pkg.getImplementationVersion();
        return (version == null || version.isBlank()) ? null : version.trim();
    }
}

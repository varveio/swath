/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.fixture;

import io.varve.swath.output.parquet.sorted.SortedParquetStamp;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Stable restart identity for an immutable fixture's resolved file manifest. */
public final class FixtureIdentity {
    private FixtureIdentity() {
    }

    public static String of(Path fixture) throws IOException {
        List<Path> files = SortedFixtures.resolveFiles(fixture);
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
        digest.update("swath-replay-fixture-identity-v1".getBytes(StandardCharsets.US_ASCII));
        digest.update(ByteBuffer.allocate(4).putInt(files.size()).array());
        Path absoluteFixture = fixture.toAbsolutePath().normalize();
        Path root = Files.isDirectory(fixture) ? absoluteFixture : absoluteFixture.getParent();
        for (Path file : files) {
            // Relative part names keep the identity across a remount or a copy that preserves size,
            // mtime and stamp metadata. This is not a full content digest; fixtures stay immutable
            // while serving.
            field(digest, root.relativize(file.toAbsolutePath().normalize()).toString().replace('\\', '/'));
            BasicFileAttributes attributes = Files.readAttributes(file, BasicFileAttributes.class);
            digest.update(ByteBuffer.allocate(8).putLong(attributes.size()).array());
            digest.update(ByteBuffer.allocate(8).putLong(attributes.lastModifiedTime().to(TimeUnit.NANOSECONDS)).array());
            field(digest, SortedParquetStamp.read(file).map(Object::toString).orElse("unstamped"));
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static void field(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(4).putInt(bytes.length).array());
        digest.update(bytes);
    }
}

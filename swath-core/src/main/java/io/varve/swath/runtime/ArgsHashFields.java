/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

/**
 * The typed, canonical field set the {@code args_hash} is computed over:
 * <b>exactly the fields that change what is listed</b> — store
 * scheme + endpoint, bucket, prefix, recursive flag, {@code --all-versions}, the
 * user-supplied {@code --strategy} literal, hints-file contents, and the
 * inventory-manifest URI (+ delivery id). It deliberately <b>excludes</b>
 * concurrency, output format, filters, log level, and progress prefs — changing
 * any of those still resumes (§5).
 *
 * <p>A single canonical, typed field list lets a test pin the field set, so a
 * future field is added deliberately (and visibly flips the hash) rather than
 * by accident. Each field is emitted <b>label-tagged</b> so two distinct field
 * sets can never alias to the same canonical string.
 *
 * <p>v1.0 only wires {@code scheme}/{@code endpoint}/{@code bucket}/{@code prefix}
 * from live flags; the rest are dormant seams carrying their v1.0 defaults
 * ({@code recursive=true}, {@code allVersions=false}, {@code strategy="auto"}, no
 * hints, no inventory) so the hash is already stable across the phases that light
 * them up.
 */
public record ArgsHashFields(
        String scheme,
        String endpoint,
        String bucket,
        String prefix,
        boolean recursive,
        boolean allVersions,
        String strategy,
        String hintsContents,
        String inventoryManifestUri,
        String inventoryDeliveryId) {

    /** v1.0 defaults for the dormant seams (no {@code --recursive}/{@code --all-versions}/… flags yet). */
    public static ArgsHashFields forListing(String scheme, String endpoint, String bucket, String prefix) {
        return new ArgsHashFields(scheme, endpoint, bucket, prefix,
                true, false, "auto", "", null, null);
    }

    /** The label-tagged, fixed-order canonical encoding fed to the digest. */
    String[] canonical() {
        return new String[]{
                "scheme=", nz(scheme),
                "endpoint=", nz(endpoint),
                "bucket=", nz(bucket),
                "prefix=", nz(prefix),
                "recursive=", Boolean.toString(recursive),
                "all_versions=", Boolean.toString(allVersions),
                "strategy=", nz(strategy),
                "hints=", nz(hintsContents),
                "inventory_uri=", nz(inventoryManifestUri),
                "inventory_delivery_id=", nz(inventoryDeliveryId),
        };
    }

    /** SHA-256 over the canonical field set. */
    public String hash() {
        return ArgsHash.of(canonical());
    }

    private static String nz(String s) {
        return s == null ? "" : s;
    }
}

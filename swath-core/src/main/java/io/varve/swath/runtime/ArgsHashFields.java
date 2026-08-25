/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

/**
 * The typed, canonical field set used for the listing-scope {@code args_hash}.
 *
 * <p>The hash covers exactly the fields that change the source keyspace: store scheme and
 * endpoint, bucket, prefix, recursive flag, {@code --all-versions}, the user-supplied
 * {@code --strategy} literal, hints-file contents, and the inventory-manifest URI and
 * delivery ID.
 *
 * <p>Filters, output identity, and other resume-sensitive fields are deliberately not folded
 * into this digest. They are persisted and validated separately; excluding them from
 * {@code args_hash} does <b>not</b> mean they may change during resume. Operational settings
 * classified as free or restorable—such as concurrency, logging, and progress preferences—are
 * handled by their own resume classes.
 *
 * <p>A single typed field list lets tests pin the scope of the digest, so a future field is
 * added deliberately and visibly changes the hash. Each field is label-tagged, preventing two
 * different field sets from aliasing to the same canonical string.
 *
 * <p>The current CLI wires {@code scheme}, {@code endpoint}, {@code bucket}, and
 * {@code prefix}. The remaining fields carry their reserved defaults
 * ({@code recursive=true}, {@code allVersions=false}, {@code strategy="auto"}, no hints,
 * and no inventory) until the corresponding features ship.
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

    /** Defaults for the reserved listing-scope fields that the current CLI does not expose. */
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

    /** SHA-256 over the canonical listing-scope field set. */
    public String hash() {
        return ArgsHash.of(canonical());
    }

    private static String nz(String s) {
        return s == null ? "" : s;
    }
}

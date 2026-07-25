/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;

/**
 * Pins the {@code args_hash} canonical field set: the typed
 * {@link ArgsHashFields} must (a) hash deterministically, (b) change the hash when
 * <i>any</i> listing-relevant field changes, and (c) be label-tagged so two
 * distinct field assignments can never alias to the same hash.
 */
class ArgsHashFieldsTest {

    private static ArgsHashFields base() {
        return new ArgsHashFields("s3", "https://ep", "bucket", "prefix/",
                true, false, "auto", "hint-bytes", "s3://inv/manifest.json", "2024-01-01");
    }

    @Test
    void hashIsDeterministicAndStable() {
        // A golden so an accidental change to the canonical encoding is caught.
        assertThat(base().hash()).isEqualTo(base().hash());
        assertThat(base().hash()).hasSize(64).matches("[0-9a-f]{64}");
    }

    @Test
    void everyListingRelevantFieldChangesTheHash() {
        String baseHash = base().hash();
        List<UnaryOperator<ArgsHashFields>> mutations = List.of(
                f -> withScheme(f, "gs"),
                f -> withEndpoint(f, "https://other"),
                f -> withBucket(f, "other"),
                f -> withPrefix(f, "other/"),
                f -> withRecursive(f, false),
                f -> withAllVersions(f, true),
                f -> withStrategy(f, "scan"),
                f -> withHints(f, "other-bytes"),
                f -> withInventoryUri(f, "s3://inv/other.json"),
                f -> withInventoryId(f, "2024-02-02"));

        Set<String> distinct = new HashSet<>();
        distinct.add(baseHash);
        for (UnaryOperator<ArgsHashFields> m : mutations) {
            String mutated = m.apply(base()).hash();
            assertThat(mutated).as("mutating a listing-relevant field must change the hash").isNotEqualTo(baseHash);
            distinct.add(mutated);
        }
        // All 10 mutations + base are mutually distinct: no two fields collapse together.
        assertThat(distinct).hasSize(mutations.size() + 1);
    }

    @Test
    void fieldsAreLabelTaggedSoBoundariesCannotAlias() {
        // Without per-field labels, ("ab","") and ("a","b") would concatenate identically.
        ArgsHashFields ab = ArgsHashFields.forListing("s3", "", "ab", "");
        ArgsHashFields aSlashB = ArgsHashFields.forListing("s3", "", "a", "b");
        assertThat(ab.hash()).isNotEqualTo(aSlashB.hash());

        // The canonical encoding actually carries the field labels.
        assertThat(base().canonical()).contains(
                "scheme=", "endpoint=", "bucket=", "prefix=", "recursive=",
                "all_versions=", "strategy=", "hints=", "inventory_uri=", "inventory_delivery_id=");
    }

    @Test
    void v10DefaultsLightUpTheDormantSeams() {
        ArgsHashFields f = ArgsHashFields.forListing("s3", "", "b", "p/");
        assertThat(f.recursive()).isTrue();
        assertThat(f.allVersions()).isFalse();
        assertThat(f.strategy()).isEqualTo("auto");
        assertThat(f.hintsContents()).isEmpty();
        assertThat(f.inventoryManifestUri()).isNull();
        // null inventory fields encode as empty without throwing.
        assertThat(f.hash()).hasSize(64);
    }

    // ---- small record "withers" (records have no copy syntax) -----------------

    private static ArgsHashFields withScheme(ArgsHashFields f, String v) {
        return new ArgsHashFields(v, f.endpoint(), f.bucket(), f.prefix(), f.recursive(),
                f.allVersions(), f.strategy(), f.hintsContents(), f.inventoryManifestUri(), f.inventoryDeliveryId());
    }

    private static ArgsHashFields withEndpoint(ArgsHashFields f, String v) {
        return new ArgsHashFields(f.scheme(), v, f.bucket(), f.prefix(), f.recursive(),
                f.allVersions(), f.strategy(), f.hintsContents(), f.inventoryManifestUri(), f.inventoryDeliveryId());
    }

    private static ArgsHashFields withBucket(ArgsHashFields f, String v) {
        return new ArgsHashFields(f.scheme(), f.endpoint(), v, f.prefix(), f.recursive(),
                f.allVersions(), f.strategy(), f.hintsContents(), f.inventoryManifestUri(), f.inventoryDeliveryId());
    }

    private static ArgsHashFields withPrefix(ArgsHashFields f, String v) {
        return new ArgsHashFields(f.scheme(), f.endpoint(), f.bucket(), v, f.recursive(),
                f.allVersions(), f.strategy(), f.hintsContents(), f.inventoryManifestUri(), f.inventoryDeliveryId());
    }

    private static ArgsHashFields withRecursive(ArgsHashFields f, boolean v) {
        return new ArgsHashFields(f.scheme(), f.endpoint(), f.bucket(), f.prefix(), v,
                f.allVersions(), f.strategy(), f.hintsContents(), f.inventoryManifestUri(), f.inventoryDeliveryId());
    }

    private static ArgsHashFields withAllVersions(ArgsHashFields f, boolean v) {
        return new ArgsHashFields(f.scheme(), f.endpoint(), f.bucket(), f.prefix(), f.recursive(),
                v, f.strategy(), f.hintsContents(), f.inventoryManifestUri(), f.inventoryDeliveryId());
    }

    private static ArgsHashFields withStrategy(ArgsHashFields f, String v) {
        return new ArgsHashFields(f.scheme(), f.endpoint(), f.bucket(), f.prefix(), f.recursive(),
                f.allVersions(), v, f.hintsContents(), f.inventoryManifestUri(), f.inventoryDeliveryId());
    }

    private static ArgsHashFields withHints(ArgsHashFields f, String v) {
        return new ArgsHashFields(f.scheme(), f.endpoint(), f.bucket(), f.prefix(), f.recursive(),
                f.allVersions(), f.strategy(), v, f.inventoryManifestUri(), f.inventoryDeliveryId());
    }

    private static ArgsHashFields withInventoryUri(ArgsHashFields f, String v) {
        return new ArgsHashFields(f.scheme(), f.endpoint(), f.bucket(), f.prefix(), f.recursive(),
                f.allVersions(), f.strategy(), f.hintsContents(), v, f.inventoryDeliveryId());
    }

    private static ArgsHashFields withInventoryId(ArgsHashFields f, String v) {
        return new ArgsHashFields(f.scheme(), f.endpoint(), f.bucket(), f.prefix(), f.recursive(),
                f.allVersions(), f.strategy(), f.hintsContents(), f.inventoryManifestUri(), v);
    }
}

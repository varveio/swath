/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.cli.FilterSpecCodec.Decoded;
import io.varve.swath.error.CheckpointException;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The filter-spec encoding must be unambiguous and reversible. A naive
 * {@code name=value;} concatenation can let two different filter sets collide into the
 * same spec, which would bypass the resume-safety gate; the length-prefixed encoding
 * keeps them distinct and round-trips every value exactly.
 */
final class FilterSpecCodecTest {

    @Test
    void collidingFilterSets_produceDifferentSpecs() throws Exception {
        // Set A: a single include whose value happens to spell another field's delimiters.
        // Set B: a genuinely separate include + exclude. Under naive `name=value;`
        // concatenation both could read back identically; the length prefix prevents it.
        String specA = FilterSpecCodec.encode("a;exclude=b", null, null, null, null, null, null);
        String specB = FilterSpecCodec.encode("a", "b", null, null, null, null, null);
        assertThat(specA).isNotEqualTo(specB);

        // And each still decodes to its own true fields (no boundary shift).
        assertThat(FilterSpecCodec.decode(specA).include()).isEqualTo("a;exclude=b");
        assertThat(FilterSpecCodec.decode(specA).exclude()).isNull();
        assertThat(FilterSpecCodec.decode(specB).include()).isEqualTo("a");
        assertThat(FilterSpecCodec.decode(specB).exclude()).isEqualTo("b");
    }

    @Test
    void roundTrip_adversarialValues_restoreExactly() throws Exception {
        // Values stuffed with the encoding's own delimiters (; = :) must survive intact.
        String include = "key;with=odd:chars";
        String exclude = "a;b;c";
        String minSize = "1=2";
        String maxSize = "::";
        String modifiedAfter = "2020-01-01T00:00:00Z;drop=all";
        String modifiedBefore = null;   // null stays null
        List<String> storageClasses = List.of("STANDARD", "GLACIER");   // restored as the sorted list

        String spec = FilterSpecCodec.encode(include, exclude, minSize, maxSize,
                modifiedAfter, modifiedBefore, storageClasses);
        Decoded d = FilterSpecCodec.decode(spec);

        assertThat(d.include()).isEqualTo(include);
        assertThat(d.exclude()).isEqualTo(exclude);
        assertThat(d.minSize()).isEqualTo(minSize);
        assertThat(d.maxSize()).isEqualTo(maxSize);
        assertThat(d.modifiedAfter()).isEqualTo(modifiedAfter);
        assertThat(d.modifiedBefore()).isNull();
        assertThat(d.storageClasses()).containsExactly("GLACIER", "STANDARD");   // canonical sorted order
    }

    @Test
    void emptyStringField_roundTripsToEmptyString() throws Exception {
        // An empty-string value (e.g. --exclude "") must survive the codec unchanged.
        // It is behaviorally different from an absent field, so decode must not collapse it to null.
        String spec = FilterSpecCodec.encode("", "", "", "", "", "", List.of());
        Decoded d = FilterSpecCodec.decode(spec);
        assertThat(d.include()).isEqualTo("");
        assertThat(d.exclude()).isEqualTo("");
        assertThat(d.minSize()).isEqualTo("");
        assertThat(d.maxSize()).isEqualTo("");
        assertThat(d.modifiedAfter()).isEqualTo("");
        assertThat(d.modifiedBefore()).isEqualTo("");
        // storageClasses: null and empty-list are semantically equivalent → both decode to null.
        assertThat(d.storageClasses()).isNull();
    }

    @Test
    void absentField_roundTripsToNull() throws Exception {
        // A null field (option not supplied) must survive the codec as null.
        String spec = FilterSpecCodec.encode(null, null, null, null, null, null, null);
        Decoded d = FilterSpecCodec.decode(spec);
        assertThat(d.include()).isNull();
        assertThat(d.exclude()).isNull();
        assertThat(d.minSize()).isNull();
        assertThat(d.maxSize()).isNull();
        assertThat(d.modifiedAfter()).isNull();
        assertThat(d.modifiedBefore()).isNull();
        assertThat(d.storageClasses()).isNull();
    }

    @Test
    void absentAndEmpty_produceDifferentSpecs() {
        // null (option absent) and "" (option present but empty) must produce different
        // filter_spec strings so the resume gate detects the change.
        String specAbsent = FilterSpecCodec.encode(null, null, null, null, null, null, null);
        String specEmpty  = FilterSpecCodec.encode("", "", "", "", "", "", null);
        assertThat(specAbsent).isNotEqualTo(specEmpty);
    }

    @Test
    void nullOrEmptySpec_decodesToAllNull() throws Exception {
        // The old-checkpoint fallback: a missing/empty stored spec yields all-null fields.
        for (String spec : new String[] {null, ""}) {
            Decoded d = FilterSpecCodec.decode(spec);
            assertThat(d.include()).isNull();
            assertThat(d.exclude()).isNull();
            assertThat(d.minSize()).isNull();
            assertThat(d.maxSize()).isNull();
            assertThat(d.modifiedAfter()).isNull();
            assertThat(d.modifiedBefore()).isNull();
            assertThat(d.storageClasses()).isNull();
        }
    }

    @Test
    void malformedCheckpointSpecThrowsCheckpointException() {
        assertThatThrownBy(() -> FilterSpecCodec.decode("include=10:abc"))
                .isInstanceOf(CheckpointException.class)
                .satisfies(e -> assertThat(ExitCodes.forThrowable(e)).isEqualTo(1));
        assertThatThrownBy(() -> FilterSpecCodec.decode("include=x:abc"))
                .isInstanceOf(CheckpointException.class)
                .satisfies(e -> assertThat(ExitCodes.forThrowable(e)).isEqualTo(1));
    }

}

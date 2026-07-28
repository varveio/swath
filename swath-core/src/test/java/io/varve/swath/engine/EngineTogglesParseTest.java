/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.error.InvalidArgsException;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * {@code --engine-toggle NAME=on|off} + the {@code --no-owner-split} alias
 * parse/validation contract ({@link EngineToggles#parse}). Pure — no CLI/picocli involved, since
 * {@link io.varve.swath.cli.ListCommand#resolveEngineToggles} is a one-line delegation to this method.
 */
final class EngineTogglesParseTest {

    @Test
    void noArgsYieldsAllDefaults() throws InvalidArgsException {
        assertThat(EngineToggles.parse(null, false)).isEqualTo(EngineToggles.DEFAULT);
        assertThat(EngineToggles.parse(List.of(), false)).isEqualTo(EngineToggles.DEFAULT);
    }

    @Test
    void singleToggleOff() throws InvalidArgsException {
        EngineToggles toggles = EngineToggles.parse(List.of("structure_probes=off"), false);
        assertThat(toggles.structureProbes()).isFalse();
        assertThat(toggles.ownerSplit()).isTrue();
        assertThat(toggles.densityEwma()).isTrue();
        assertThat(toggles.radixBands()).isTrue();
        assertThat(toggles.farAhead()).isTrue();
        assertThat(toggles.alphabetPivots()).isTrue();
        assertThat(toggles.isDefault()).isFalse();
    }

    @Test
    void onIsAcceptedAndIsANoOpAgainstTheDefault() throws InvalidArgsException {
        EngineToggles toggles = EngineToggles.parse(List.of("far_ahead=on"), false);
        assertThat(toggles).isEqualTo(EngineToggles.DEFAULT);
    }

    @Test
    void reflectToggleOffParses() throws InvalidArgsException {
        EngineToggles toggles = EngineToggles.parse(List.of("reflect=off"), false);
        assertThat(toggles.reflect()).isFalse();
        assertThat(toggles.ownerSplit()).isTrue();
        assertThat(toggles.alphabetPivots()).isTrue();
        assertThat(toggles.isDefault()).isFalse();
        assertThat(toggles.disabledNames()).containsExactly("reflect");
    }

    @Test
    void repeatableAccumulatesAcrossOccurrences() throws InvalidArgsException {
        EngineToggles toggles = EngineToggles.parse(
                List.of("owner_split=off", "density_ewma=off", "radix_bands=off",
                        "structure_probes=off", "far_ahead=off", "alphabet_pivots=off", "reflect=off",
                        "confetti_feedback=off", "reflect_lift=off", "fanout_tiling=off"),
                false);
        assertThat(toggles.disabledNames()).isEqualTo(EngineToggles.NAMES);
    }

    @Test
    void valueIsCaseInsensitive() throws InvalidArgsException {
        EngineToggles toggles = EngineToggles.parse(List.of("owner_split=OFF"), false);
        assertThat(toggles.ownerSplit()).isFalse();
    }

    @Test
    void unknownNameIsRejected() {
        assertThatThrownBy(() -> EngineToggles.parse(List.of("not_a_real_toggle=off"), false))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("not_a_real_toggle")
                .hasMessageContaining("owner_split");   // the valid-names list is echoed
    }

    @Test
    void malformedNoEqualsSignIsRejected() {
        assertThatThrownBy(() -> EngineToggles.parse(List.of("owner_split"), false))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("NAME=VALUE");
    }

    @Test
    void malformedValueIsRejected() {
        assertThatThrownBy(() -> EngineToggles.parse(List.of("owner_split=maybe"), false))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("on|off");
    }

    @Test
    void contradictoryRepeatedValuesAreRejected() {
        assertThatThrownBy(() -> EngineToggles.parse(List.of("owner_split=off", "owner_split=on"), false))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("contradictory");
    }

    @Test
    void repeatedIdenticalValuesAreFine() throws InvalidArgsException {
        EngineToggles toggles = EngineToggles.parse(List.of("owner_split=off", "owner_split=off"), false);
        assertThat(toggles.ownerSplit()).isFalse();
    }

    // ---- --no-owner-split alias interplay --------------------------------------

    @Test
    void noOwnerSplitAloneIsTheSameAsOwnerSplitOff() throws InvalidArgsException {
        EngineToggles viaFlag = EngineToggles.parse(null, true);
        EngineToggles viaToggle = EngineToggles.parse(List.of("owner_split=off"), false);
        assertThat(viaFlag).isEqualTo(viaToggle);
    }

    @Test
    void noOwnerSplitPlusExplicitOwnerSplitOffAgree() throws InvalidArgsException {
        EngineToggles toggles = EngineToggles.parse(List.of("owner_split=off"), true);
        assertThat(toggles.ownerSplit()).isFalse();
    }

    @Test
    void noOwnerSplitConflictsWithExplicitOwnerSplitOn() {
        assertThatThrownBy(() -> EngineToggles.parse(List.of("owner_split=on"), true))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("--no-owner-split");
    }

    @Test
    void noOwnerSplitDoesNotDisturbOtherToggles() throws InvalidArgsException {
        EngineToggles toggles = EngineToggles.parse(List.of("far_ahead=off"), true);
        assertThat(toggles.ownerSplit()).isFalse();
        assertThat(toggles.farAhead()).isFalse();
        assertThat(toggles.densityEwma()).isTrue();
    }

    // ---- readahead: opt-in, default OFF, excluded from disabledNames() --------------------------

    @Test
    void readaheadDefaultsOffAndIsExcludedFromNames() throws InvalidArgsException {
        assertThat(EngineToggles.DEFAULT.readahead()).isFalse();
        // readahead is an OPT-IN new mechanism, not one of the ten ablation toggles: it is a
        // separately-accepted name, deliberately NOT in NAMES (which drives disabledNames()'s
        // ablated-mechanism set).
        assertThat(EngineToggles.NAMES).doesNotContain("readahead");
        assertThat(EngineToggles.parse(null, false)).isEqualTo(EngineToggles.DEFAULT);
        assertThat(EngineToggles.parse(List.of(), false)).isEqualTo(EngineToggles.DEFAULT);
    }

    @Test
    void readaheadOnParsesAndIsNotDefault() throws InvalidArgsException {
        EngineToggles toggles = EngineToggles.parse(List.of("readahead=on"), false);
        assertThat(toggles.readahead()).isTrue();
        assertThat(toggles.isDefault()).isFalse();
        // Enabling an opt-in mechanism must never appear in disabledNames() (the ABLATED-mechanism
        // set) — every ablation toggle is still at its supported default (all on).
        assertThat(toggles.disabledNames()).isEmpty();
        assertThat(toggles.ownerSplit()).isTrue();
    }

    @Test
    void readaheadOffExplicitlyParsesAndStaysExcludedFromDisabledNames() throws InvalidArgsException {
        EngineToggles toggles = EngineToggles.parse(List.of("readahead=off"), false);
        assertThat(toggles.readahead()).isFalse();
        assertThat(toggles).isEqualTo(EngineToggles.DEFAULT);
        // Explicitly redundant with the default OFF state — must not show up as an "ablated" toggle.
        assertThat(toggles.disabledNames()).isEmpty();
    }

    @Test
    void readaheadIsCaseInsensitiveAndComposesWithAblationToggles() throws InvalidArgsException {
        EngineToggles toggles = EngineToggles.parse(List.of("readahead=ON", "far_ahead=off"), false);
        assertThat(toggles.readahead()).isTrue();
        assertThat(toggles.farAhead()).isFalse();
        assertThat(toggles.disabledNames()).containsExactly("far_ahead");
    }

    @Test
    void readaheadUnknownValueIsRejected() {
        assertThatThrownBy(() -> EngineToggles.parse(List.of("readahead=maybe"), false))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("on|off");
    }

    @Test
    void readaheadContradictoryValuesAreRejected() {
        assertThatThrownBy(() -> EngineToggles.parse(List.of("readahead=on", "readahead=off"), false))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("contradictory");
    }

    @Test
    void readaheadRepeatedIdenticalValuesAreFine() throws InvalidArgsException {
        EngineToggles toggles = EngineToggles.parse(List.of("readahead=on", "readahead=on"), false);
        assertThat(toggles.readahead()).isTrue();
    }

    // ---- rate_anchored_sensing: opt-in, default OFF, excluded from disabledNames() ---------------

    @Test
    void rateAnchoredSensingDefaultsOffAndIsExcludedFromNames() throws InvalidArgsException {
        assertThat(EngineToggles.DEFAULT.rateAnchoredSensing()).isFalse();
        // Like readahead, an OPT-IN new mechanism rather than one of the ten ablation toggles:
        // separately accepted, deliberately NOT in NAMES (which drives disabledNames()'s
        // ablated-mechanism set) — nothing is turned OFF by leaving the shipped sensor installed.
        assertThat(EngineToggles.NAMES).doesNotContain(EngineToggles.RATE_ANCHORED_SENSING_NAME);
        assertThat(EngineToggles.parse(null, false).disabledNames()).isEmpty();
    }

    @Test
    void rateAnchoredSensingOnParsesAndIsNotDefault() throws InvalidArgsException {
        EngineToggles toggles = EngineToggles.parse(List.of("rate_anchored_sensing=on"), false);
        assertThat(toggles.rateAnchoredSensing()).isTrue();
        assertThat(toggles.isDefault()).isFalse();
        assertThat(toggles.disabledNames()).isEmpty();
        assertThat(toggles.readahead()).as("and it disturbs no other opt-in mechanism").isFalse();
    }

    @Test
    void rateAnchoredSensingOffExplicitlyParsesAndStaysExcludedFromDisabledNames() throws InvalidArgsException {
        EngineToggles toggles = EngineToggles.parse(List.of("rate_anchored_sensing=off"), false);
        assertThat(toggles.rateAnchoredSensing()).isFalse();
        assertThat(toggles).isEqualTo(EngineToggles.DEFAULT);
        assertThat(toggles.disabledNames()).isEmpty();
    }

    @Test
    void rateAnchoredSensingIsCaseInsensitiveAndComposesWithAblationToggles() throws InvalidArgsException {
        EngineToggles toggles =
                EngineToggles.parse(List.of("rate_anchored_sensing=ON", "far_ahead=off"), false);
        assertThat(toggles.rateAnchoredSensing()).isTrue();
        assertThat(toggles.farAhead()).isFalse();
        assertThat(toggles.disabledNames()).containsExactly("far_ahead");
    }

    @Test
    void rateAnchoredSensingUnknownValueIsRejected() {
        assertThatThrownBy(() -> EngineToggles.parse(List.of("rate_anchored_sensing=maybe"), false))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("on|off");
    }

    @Test
    void rateAnchoredSensingContradictoryValuesAreRejected() {
        assertThatThrownBy(() ->
                EngineToggles.parse(List.of("rate_anchored_sensing=on", "rate_anchored_sensing=off"), false))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("contradictory");
    }

    @Test
    void rateAnchoredSensingRepeatedIdenticalValuesAreFine() throws InvalidArgsException {
        EngineToggles toggles =
                EngineToggles.parse(List.of("rate_anchored_sensing=on", "rate_anchored_sensing=on"), false);
        assertThat(toggles.rateAnchoredSensing()).isTrue();
    }

    /** A name the parser accepts must be discoverable from the error a mistyped one produces. */
    @Test
    void theUnknownNameErrorListsTheOptInSensorToggleToo() {
        assertThatThrownBy(() -> EngineToggles.parse(List.of("rate_anchored=on"), false))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining(EngineToggles.RATE_ANCHORED_SENSING_NAME);
    }

    // ---- tail_floor: value-taking (not on|off), default `current`, excluded from disabledNames() --

    @Test
    void tailFloorDefaultsToTheShippedModeAndIsExcludedFromNames() throws InvalidArgsException {
        assertThat(EngineToggles.DEFAULT.tailFloor()).isEqualTo(TailFloorMode.CURRENT);
        // The only value-taking toggle: like the two opt-ins it turns nothing OFF, so it is
        // deliberately outside NAMES (which drives disabledNames()'s ablated-mechanism set).
        assertThat(EngineToggles.NAMES).doesNotContain(EngineToggles.TAIL_FLOOR_NAME);
        assertThat(EngineToggles.parse(null, false).disabledNames()).isEmpty();
    }

    @Test
    void tailFloorCurrentExplicitlyIsTheDefaultConfiguration() throws InvalidArgsException {
        EngineToggles toggles = EngineToggles.parse(List.of("tail_floor=current"), false);
        assertThat(toggles.tailFloor()).isEqualTo(TailFloorMode.CURRENT);
        assertThat(toggles).as("naming the shipped mode is bit-identical to not naming it")
                .isEqualTo(EngineToggles.DEFAULT);
        assertThat(toggles.isDefault()).isTrue();
    }

    @Test
    void tailFloorArmsParseAndAreNotDefault() throws InvalidArgsException {
        for (TailFloorMode arm : new TailFloorMode[] {TailFloorMode.EST_DIRECT, TailFloorMode.REACH_FLOORED}) {
            EngineToggles toggles = EngineToggles.parse(List.of("tail_floor=" + arm.code()), false);
            assertThat(toggles.tailFloor()).isEqualTo(arm);
            assertThat(toggles.isDefault()).isFalse();
            assertThat(toggles.disabledNames()).isEmpty();
            assertThat(toggles.rateAnchoredSensing())
                    .as("%s disturbs no other mechanism -- the race wants the factorial", arm).isFalse();
        }
    }

    @Test
    void tailFloorIsCaseInsensitiveAndComposesWithTheSensorToggle() throws InvalidArgsException {
        EngineToggles toggles =
                EngineToggles.parse(List.of("tail_floor=EST_DIRECT", "rate_anchored_sensing=on"), false);
        assertThat(toggles.tailFloor()).isEqualTo(TailFloorMode.EST_DIRECT);
        assertThat(toggles.rateAnchoredSensing()).isTrue();
    }

    @Test
    void tailFloorUnknownValueIsRejectedListingTheModes() {
        assertThatThrownBy(() -> EngineToggles.parse(List.of("tail_floor=on"), false))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("current|est_direct|reach_floored");
    }

    @Test
    void aNullTailFloorModeIsRejectedAtConstructionNotAtFirstDereference() {
        assertThatThrownBy(() -> EngineToggles.DEFAULT.withTailFloor(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("tailFloor");
    }

    @Test
    void tailFloorContradictoryValuesAreRejected() {
        assertThatThrownBy(() ->
                EngineToggles.parse(List.of("tail_floor=est_direct", "tail_floor=reach_floored"), false))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("contradictory");
    }

    @Test
    void tailFloorRepeatedIdenticalValuesAreFine() throws InvalidArgsException {
        EngineToggles toggles =
                EngineToggles.parse(List.of("tail_floor=reach_floored", "tail_floor=reach_floored"), false);
        assertThat(toggles.tailFloor()).isEqualTo(TailFloorMode.REACH_FLOORED);
    }

    @Test
    void theUnknownNameErrorListsTheTailFloorToggleToo() {
        assertThatThrownBy(() -> EngineToggles.parse(List.of("tail_floors=current"), false))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining(EngineToggles.TAIL_FLOOR_NAME);
    }
}

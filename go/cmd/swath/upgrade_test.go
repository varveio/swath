// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"slices"
	"testing"
)

// TestUpgradeRecorded: a command recorded by an earlier build replays
// with this build's flags: removed diagnostic switches are dropped in
// every spelling, and nothing after "--" is touched.
func TestUpgradeRecorded(t *testing.T) {
	got := upgradeRecorded([]string{"s3://b", "--seed", "--seed-descent=false", "-seed-descent", "--probe", "-probe=true", "--workers", "8", "--", "--seed-descent"})
	want := []string{"s3://b", "--seed", "--workers", "8", "--", "--seed-descent"}
	if !slices.Equal(got, want) {
		t.Fatalf("%q, want %q", got, want)
	}
}

// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"testing"

	"github.com/varveio/swath/go/internal/counters"
)

// TestCountersMatchIDs: one definition per ID.
func TestCountersMatchIDs(t *testing.T) {
	if counters.ID(len(Counters)) != numCounters {
		t.Fatalf("%d definitions for %d IDs", len(Counters), numCounters)
	}
}

// TestCounterTable: every counter ID has a definition with a distinct key
// (an ID without an entry would read as an empty one).
func TestCounterTable(t *testing.T) {
	seen := map[string]bool{}
	for id, d := range Counters {
		if d.Key == "" || seen[d.Key] {
			t.Fatalf("counter %d: key %q missing or repeated", id, d.Key)
		}
		seen[d.Key] = true
	}
}

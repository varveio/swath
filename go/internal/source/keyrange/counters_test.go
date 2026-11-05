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

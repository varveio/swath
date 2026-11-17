// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package live

import (
	"context"
	"testing"

	"github.com/varveio/swath/reader/listing"
)

// TestSummarizeUnknownTargetIncomplete: an exact target the lookup leaves
// unknown, with no bound to blame, counts nothing and is never complete:
// its Stop is empty, and the call's still completed.
func TestSummarizeUnknownTargetIncomplete(t *testing.T) {
	sums, ev, err := NewSource(answering{}).Summarize(context.Background(), []listing.Target{{Key: []byte("a")}}, listing.SummaryOptions{})
	if err != nil || ev.Stop != listing.StopCompleted {
		t.Fatalf("%+v, %v", ev, err)
	}
	if s := sums[0]; s.State != listing.LookupUnknown || s.Entries != 0 || s.Stop != "" {
		t.Errorf("an unknown target: %+v", s)
	}
}

// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"testing"

	"github.com/varveio/swath/go/internal/dataset"
)

// TestCommittedOutcome: the run.json a resume writes for a dataset an
// earlier attempt committed agrees with the command's --report for the same
// manifest: completed with exit 0 when the coverage is complete, incomplete
// with exit 1 when it is partial (a key limit), never completed with 0.
func TestCommittedOutcome(t *testing.T) {
	for _, c := range []struct {
		cov   dataset.Coverage
		class string
		code  int
	}{
		{dataset.Coverage{Complete: true, StopReason: "completed"}, ClassCompleted, 0},
		{dataset.Coverage{Complete: false, StopReason: "max_keys"}, ClassIncomplete, 1},
		{dataset.Coverage{Complete: false, StopReason: "unreported"}, ClassIncomplete, 1},
	} {
		o := Outcome{Status: StatusCommitted, Class: ClassCompleted, Complete: true, StopReason: "completed"}
		committedOutcome(&o, c.cov)
		if o.Class != c.class || o.ExitCode != c.code || o.Complete != c.cov.Complete || o.StopReason != c.cov.StopReason || o.Status != StatusCommitted {
			t.Fatalf("%+v: outcome %+v", c.cov, o)
		}
	}
}

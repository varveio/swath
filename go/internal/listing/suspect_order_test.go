// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/journal"
)

// TestSuspectOrderPastFiveDigits: with 100,000 records, a suspect left at
// ordinal 99,999 (its removal after the record failed, or was not synced)
// was reproduced and is removed, not taken for corruption because
// "part-99999.parquet" sorts after "part-100000.parquet" (review R2,
// 2026-11-06); one at ordinal 100,000 is still the suspect to reproduce.
// The journal state is built in memory: keepSuspect reads only the
// record count and the last record's Final.
func TestSuspectOrderPastFiveDigits(t *testing.T) {
	const n = 100_000
	st := &journal.State{Records: make([]journal.Record, n)}
	for _, c := range []struct {
		ordinal int
		suspect bool
	}{{n - 1, false}, {n, true}} {
		out := t.TempDir()
		dir := filepath.Join(out, dataset.Workspace, dataset.SuspectDir)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		left := filepath.Join(dir, dataset.PartName(c.ordinal))
		if err := os.WriteFile(left, []byte("a part set aside"), 0o644); err != nil {
			t.Fatal(err)
		}
		r, err := os.OpenRoot(out)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		sus, err := keepSuspect(verifier{ctx: context.Background()}, r, out, st)
		if err != nil {
			t.Fatalf("suspect %s with %d records: %v", dataset.PartName(c.ordinal), n, err)
		}
		_, serr := os.Stat(left)
		switch {
		case c.suspect && (sus == nil || sus.ordinal != n || serr != nil):
			t.Fatalf("suspect %s: got %+v (stat %v), want it kept as the suspect", dataset.PartName(c.ordinal), sus, serr)
		case !c.suspect && (sus != nil || serr == nil):
			t.Fatalf("suspect %s: got %+v (stat %v), want it removed as reproduced", dataset.PartName(c.ordinal), sus, serr)
		}
	}
}

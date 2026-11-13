// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/journal"
)

// TestSuspectOrderAtPartLimit: suspects are compared by ordinal at the
// part limit: a suspect left at an ordinal already recorded (its removal
// after the record failed, or was not synced) was reproduced and is
// removed, the one at the next ordinal is the suspect to reproduce, and a
// name past the five-digit ordinals is corruption (review R2: at 100,000
// records "part-99999.parquet" sorted after "part-100000.parquet" and a
// stale suspect was refused as corruption; the router now never opens
// part 100,000). The journal state is built in memory: keepSuspect reads
// only the record count and the last record's Final.
func TestSuspectOrderAtPartLimit(t *testing.T) {
	for _, c := range []struct {
		records int
		name    string
		want    string // removed, suspect or corrupt
	}{
		{dataset.MaxParts - 1, dataset.PartName(dataset.MaxParts - 2), "removed"},
		{dataset.MaxParts - 1, dataset.PartName(dataset.MaxParts - 1), "suspect"},
		{dataset.MaxParts, dataset.PartName(dataset.MaxParts - 1), "removed"},
		{dataset.MaxParts, "part-100000.parquet", "corrupt"},
	} {
		st := &journal.State{Records: make([]journal.Record, c.records)}
		out := t.TempDir()
		dir := filepath.Join(out, dataset.Workspace, dataset.SuspectDir)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		left := filepath.Join(dir, c.name)
		if err := os.WriteFile(left, []byte("a part set aside"), 0o644); err != nil {
			t.Fatal(err)
		}
		r, err := os.OpenRoot(out)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		sus, err := keepSuspect(verifier{ctx: context.Background()}, r, out, st)
		_, serr := os.Stat(left)
		switch got := c.want; {
		case got == "corrupt" && !errors.Is(err, journal.ErrCorrupt):
			t.Fatalf("suspect %s with %d records: %v, want corruption", c.name, c.records, err)
		case got != "corrupt" && err != nil:
			t.Fatalf("suspect %s with %d records: %v", c.name, c.records, err)
		case got == "suspect" && (sus == nil || sus.ordinal != c.records || serr != nil):
			t.Fatalf("suspect %s: got %+v (stat %v), want it kept as the suspect", c.name, sus, serr)
		case got == "removed" && (sus != nil || serr == nil):
			t.Fatalf("suspect %s: got %+v (stat %v), want it removed as reproduced", c.name, sus, serr)
		}
	}
}

// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/varveio/swath/go/internal/replay/legacy"
)

// TestSeekParquetPageStats: over a SeekParquet dataset the store's page
// counters and the skip-scan's meter count the module's work
// (seekparquet.ReadStats), as they do over a legacy fixture. Every part is
// checked when the store opens (Dataset.Verify), so no read opens one.
func TestSeekParquetPageStats(t *testing.T) {
	path := writeSorted(t, filepath.Join(t.TempDir(), "f"), fixtureKeys(10_000), 0)
	st, meta, err := Open(context.Background(), path, Options{Readers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if meta.Kind != KindSeekParquet {
		t.Fatalf("kind %s", meta.Kind)
	}
	s := st.(*Sorted)
	var rollups []legacy.Stats
	s.SetMeters(Meters{Rollup: func(stats legacy.Stats) { rollups = append(rollups, stats) }})
	ctx := context.Background()
	if rows, err := s.ReadRows(ctx, nil, true, nil, 1500, nil); err != nil || len(rows) != 1500 {
		t.Fatalf("read: %d rows, %v", len(rows), err)
	}
	c := s.Counters()
	if c.PageDecodes.Load() < 2 || c.RowsDecoded.Load() < 1500 {
		t.Fatalf("after a 1,500-row read: %d pages, %d keys decoded", c.PageDecodes.Load(), c.RowsDecoded.Load())
	}
	if _, _, err := s.RollupRows(ctx, nil, true, nil, nil, []byte("/"), 1000, nil, false); err != nil {
		t.Fatal(err)
	}
	if len(rollups) != 1 || rollups[0].GroupsOpened == 0 || rollups[0].KeysDecoded == 0 {
		t.Fatalf("rollup stats %+v", rollups)
	}
	if n := c.PartsOpened.Load(); n != 0 {
		t.Fatalf("%d parts opened by reads after Verify", n)
	}
}

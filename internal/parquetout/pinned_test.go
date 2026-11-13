// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/varveio/swath/internal/dataset"
)

// TestPinnedCalibratingPartCalibrates: a resume that reproduces the
// claimed first size-cut part (Options.Cut at ordinal 0, its row count)
// calibrates the size ratio from it as the cold run did, so every later
// boundary, and every part's bytes, are the cold run's. Before, the pinned
// part bypassed the size check, never calibrated, and part 1 was cut at
// the initial target.
func TestPinnedCalibratingPartCalibrates(t *testing.T) {
	rows := testRows(60_000, 3)
	cold := routeWith(t, rows, nil)
	if len(cold) < 4 {
		t.Fatalf("want >= 4 parts, got %d", len(cold))
	}
	for _, enc := range []int{1, 3} {
		got := routeWith(t, rows, func(o *Options) { o.Encoders, o.Cut = enc, &Cut{Ordinal: 0, Rows: cold[0].Rows} })
		if len(got) != len(cold) {
			t.Fatalf("encoders %d: %d parts after the pinned part 0, cold %d", enc, len(got), len(cold))
		}
		for i := range got {
			if got[i].Rows != cold[i].Rows || got[i].SHA256 != cold[i].SHA256 {
				t.Fatalf("encoders %d: part %d is %d rows (sha256 %s), cold %d rows (sha256 %s)", enc, i, got[i].Rows, got[i].SHA256, cold[i].Rows, cold[i].SHA256)
			}
		}
	}
}

// routeWith routes rows with one encoder and a 16 KiB part size, opt
// adjusting the options, and returns the parts.
func routeWith(t *testing.T, rows []row, opt func(*Options)) []dataset.Part {
	t.Helper()
	out := t.TempDir()
	if err := os.MkdirAll(filepath.Join(out, dataset.DataDir), 0o755); err != nil {
		t.Fatal(err)
	}
	o := Options{Out: out, PartSize: 16 << 10, Encoders: 1}
	if opt != nil {
		opt(&o)
	}
	r, err := NewRouter(o, &testSchema)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(rows); i += 1500 {
		if err := r.Write(context.Background(), [][]row{rows[i:min(i+1500, len(rows))]}); err != nil {
			t.Fatal(err)
		}
	}
	parts, err := r.Close()
	if err != nil {
		t.Fatal(err)
	}
	return parts
}

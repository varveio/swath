// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"testing"

	"github.com/varveio/swath/go/sortedparquet/sortedparquettest"
)

// openParts writes parts as one Java-era fixture and opens it.
func openParts(t *testing.T, opts Options, parts ...sortedparquettest.Part) *Sorted {
	t.Helper()
	dir := t.TempDir()
	for i := range parts {
		parts[i].Stamps = sortedparquettest.Stamps(i+1, i == len(parts)-1)
		writePart(t, filepath.Join(dir, partName(i)), parts[i])
	}
	s, _, err := Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s.(*Sorted)
}

func partName(i int) string { return fmt.Sprintf("part-%05d.parquet", i+1) }

// TestRollupMaxIntLimit asks for math.MaxInt entries. The rollup reads one
// entry past the limit to detect truncation; adding it to math.MaxInt
// wrapped negative and the rollup returned nothing.
func TestRollupMaxIntLimit(t *testing.T) {
	s := openParts(t, Options{}, sortedparquettest.Part{Rows: objects("a")})
	got, ok, err := s.DelimitedRollup(context.Background(), nil, true, nil, nil, []byte("/"), math.MaxInt, Projection{}, true)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	want := rangeWalk(storeWalk(t, s, nil), nil, true, nil, math.MaxInt, true)
	if len(want) != 1 {
		t.Fatalf("the range walk returned %d entries", len(want))
	}
	if d := diffEntries(got, want); d != "" {
		t.Fatal(d)
	}
}

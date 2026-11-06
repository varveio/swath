// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"testing"

	"github.com/varveio/swath/go/sortedparquet"
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

// TestDisorderReasonMatchesJava reads a stamped OBJECT-only group holding
// a, c, b, d through Rows and through the delimiter rollup: both fail with
// the replay server's row_group_disorder reason, naming the part, the
// group and the row, and each failure is counted.
func TestDisorderReasonMatchesJava(t *testing.T) {
	s := openParts(t, Options{}, sortedparquettest.Part{Rows: objects("a", "c", "b", "d")})
	check := func(how string, err error) {
		t.Helper()
		var disorder *sortedparquet.DisorderError
		if Refusal(err) != "row_group_disorder" || !errors.As(err, &disorder) || !errors.Is(err, sortedparquet.ErrCorrupt) {
			t.Fatalf("%s: %v (reason %q)", how, err, Refusal(err))
		}
		if filepath.Base(disorder.Part) != "part-00001.parquet" || disorder.RowGroup != 0 || disorder.Row != 2 {
			t.Fatalf("%s: located at %s group %d row %d", how, disorder.Part, disorder.RowGroup, disorder.Row)
		}
	}
	_, err := s.Rows(context.Background(), nil, true, nil, 10, Projection{})
	check("rows", err)
	_, ok, err := s.DelimitedRollup(context.Background(), nil, true, nil, nil, []byte("/"), 10, Projection{}, true)
	if !ok {
		t.Fatal("declined")
	}
	check("rollup", err)
	if n := s.Counters().DisorderRefusals.Load(); n != 2 {
		t.Fatalf("disorder refusals counted %d, want 2", n)
	}
}

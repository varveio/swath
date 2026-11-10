// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"runtime"
	"slices"
	"testing"

	"github.com/varveio/swath/go/internal/registry"
)

// TestReportFilters: --no-dirs leaves the directories' rows out, so the
// coverage records it among the filters; a listing of every entry records
// none.
func TestReportFilters(t *testing.T) {
	for _, tc := range []struct {
		opt  Options
		want []string
	}{
		{Options{}, nil},
		{Options{NoDirs: true}, []string{noDirsFilter}},
	} {
		p, err := New(t.TempDir(), tc.opt)
		if err != nil {
			t.Fatal(err)
		}
		if got := (&opened{p: p}).Report().Coverage.Filters; !slices.Equal(got, tc.want) {
			t.Fatalf("%+v: filters %q, want %q", tc.opt, got, tc.want)
		}
	}
}

// TestDefaultWorkers: a local tree's default --workers is 4 per processor, at
// most 64, and the registry carries it.
func TestDefaultWorkers(t *testing.T) {
	prev := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(prev)
	f, err := registry.Lookup(t.TempDir())
	if err != nil || f.Workers == nil {
		t.Fatalf("lookup: %v, Workers set %v", err, f.Workers != nil)
	}
	for procs, want := range map[int]int{1: 4, 2: 8, 4: 16, 16: 64, 32: 64} {
		runtime.GOMAXPROCS(procs)
		if got := f.Workers(); got != want {
			t.Errorf("GOMAXPROCS %d: %d workers, want %d", procs, got, want)
		}
	}
}

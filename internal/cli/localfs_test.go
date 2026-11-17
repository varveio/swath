// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"runtime"
	"testing"

	"github.com/varveio/swath/internal/source/localfs"
)

// TestLocalDefaultWorkers: the local factory carries a local tree's
// default --workers (localfs.DefaultWorkers, the provider's
// TestDefaultWorkers), known before the source is opened.
func TestLocalDefaultWorkers(t *testing.T) {
	prev := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(prev)
	f := factory(t, t.TempDir())
	if f.Workers == nil {
		t.Fatal("the local factory states no default --workers")
	}
	for _, procs := range []int{1, 32} {
		runtime.GOMAXPROCS(procs)
		if got, want := f.Workers(), localfs.DefaultWorkers(); got != want {
			t.Errorf("GOMAXPROCS %d: %d workers, want %d", procs, got, want)
		}
	}
}

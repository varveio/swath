// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/source"
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

// TestLocalCountRestoresProcs: a count through the command's local source
// runs with the processors its view raised (count_procs recorded: the
// provider's TestStatCountRaisesProcsAndRestores checks the factors), and
// GOMAXPROCS is restored when the run returns, at the stat and names tiers.
func TestLocalCountRestoresProcs(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "f"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	prev := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(prev)
	for _, tier := range []string{"stat", "names"} {
		src, err := openArgs(t, root, false, nil, "--metadata", tier)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := src.Run(context.Background(), registry.Env{Count: true, Order: order.Options{Workers: 64}}); err != nil {
			t.Fatal(err)
		}

		if n := runtime.GOMAXPROCS(0); n != 2 {
			t.Fatalf("%s: GOMAXPROCS %d after the run, want 2 restored", tier, n)
		}
		if got := src.(source.Reporter).Report().Stats.(*localfs.Stats).CountProcs; got <= 2 {
			t.Fatalf("%s: count_procs %d, want raised above 2", tier, got)
		}
	}
}

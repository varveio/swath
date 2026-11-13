// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath/internal/order"
)

// TestTmpfsSpillWarning: spill on tmpfs is memory, and the run says so at
// its first spill, once; a run on tmpfs that never spills says nothing,
// and its report still names the filesystem.
func TestTmpfsSpillWarning(t *testing.T) {
	base, err := os.MkdirTemp("/dev/shm", "swath-tmpfs-")
	if err != nil {
		t.Skipf("no /dev/shm: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(base) })
	if fs := order.FSType(base); !order.InMemory(fs) {
		t.Skipf("/dev/shm is %q, not tmpfs", fs)
	}
	for _, c := range []struct {
		name  string
		tree  *frontierTree
		spill bool
	}{
		{"no spill", &frontierTree{siblings: 2, rows: 3}, false},
		// Many fast siblings behind a slow first range, over the smallest
		// row budget: they spill.
		{"spill", &frontierTree{siblings: 100, rows: 100, delay: 500 * time.Millisecond}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			frontierMu.Lock()
			frontierNext = c.tree
			frontierMu.Unlock()
			var mu sync.Mutex
			var warned []string
			warn := func(m string) {
				mu.Lock()
				defer mu.Unlock()
				warned = append(warned, m)
			}
			out := filepath.Join(base, strings.ReplaceAll(c.name, " ", "-"))
			sum, err := Run(context.Background(), Config{Source: SourceConfig{URI: "frontier://x"}, Output: OutputConfig{Dir: out}, Warn: warn,
				Limits: Limits{RowBudget: minRowBudget, Encoders: 1, Workers: 16, PartSize: 256 << 10, Spill: SpillLimits{Max: -1}}})
			if err != nil {
				t.Fatal(err)
			}
			sp := sum.Resources.Spill
			if sp == nil || sp.FSType != "tmpfs" || (sp.Rows > 0) != c.spill {
				t.Fatalf("spill %+v, want rows spilled %v on tmpfs", sp, c.spill)
			}
			mu.Lock()
			defer mu.Unlock()
			n := 0
			for _, m := range warned {
				if strings.Contains(m, "tmpfs") {
					n++
				}
			}
			if want := map[bool]int{false: 0, true: 1}[c.spill]; n != want {
				t.Fatalf("%d tmpfs warnings, want %d: %q", n, want, warned)
			}
		})
	}
}

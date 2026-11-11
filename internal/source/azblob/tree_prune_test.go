// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/varveio/swath/internal/filter"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/source/azblob/azblobtest"
)

// Exercise registered Run, where filters become traversal predicates, and
// verify that excluding a subtree never turns its parent row into a skip.
func TestTreeRecursiveExcludes(t *testing.T) {
	for _, excludeRow := range []bool{false, true} {
		store := azblobtest.NewTreeStore([]string{"scope/drop/hidden", "scope/keep/visible"})
		o := openTree(t, store, "scope/", 4, 1)
		spec := filter.Spec{Base: "scope/", Exclude: []string{"drop/**"}}
		if excludeRow {
			spec.Exclude = append(spec.Exclude, "drop/")
		}
		var filtered atomic.Int64
		st, _, err := o.Run(context.Background(), registry.Env{
			Count: true, Filter: &spec, Filtered: &filtered, Order: order.Options{Workers: 4},
		})
		if err != nil {
			t.Fatal(err)
		}
		wantRows, wantFiltered := int64(3), int64(0)
		if excludeRow {
			wantRows, wantFiltered = 2, 1
		}
		if st.Rows != wantRows || filtered.Load() != wantFiltered || o.Listed() != 3 {
			t.Fatalf("excludeRow=%v rows=%d filtered=%d listed=%d", excludeRow, st.Rows, filtered.Load(), o.Listed())
		}
		for _, req := range store.Log() {
			if req.In.Prefix == "scope/drop/" {
				t.Fatal("listed excluded subtree")
			}
		}
		r := o.Report()
		stats := r.Stats.(*Stats)
		if stats.PrunedDirs != 1 || stats.Pruning == nil || stats.Pruning.Reason != "recursive_excludes" || stats.Pruning.RecursiveGlobs != 1 {
			t.Fatalf("pruning report: %+v", stats)
		}
		if !r.Coverage.Complete || r.Coverage.Skipped != 0 {
			t.Fatalf("pruning damaged coverage: %+v", r.Coverage)
		}
	}
}

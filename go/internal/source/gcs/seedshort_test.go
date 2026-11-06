// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"fmt"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/source/keyrange"
)

// TestSeedShortContinuingThroughClient: the seed descent's estimate for a
// continuing sample shorter than a nominal page is counted
// (seed.short_continuing_estimated) on a real provider, through the HTTP
// client and the JSON API fake, not only in the planner: 30 folders of 40
// subfolders of 3 objects, the descent weighs subfolders; when the service
// cuts every subfolder probe to one entry with a token, each such sample is
// estimated and counted; with whole pages none is. The listing is exact
// either way.
func TestSeedShortContinuingThroughClient(t *testing.T) {
	var keys []string
	for f := range 30 {
		for c := range 40 {
			for o := range 3 {
				keys = append(keys, fmt.Sprintf("f%02d/c%02d/o%d", f, c, o))
			}
		}
	}
	slices.Sort(keys)
	for _, short := range []bool{false, true} {
		f, ep := newFake(t, keys)
		if short {
			f.shortIf = func(q url.Values) bool { return strings.Count(q.Get("prefix"), "/") == 2 }
		}
		got, st := list(t, Options{Bucket: "bkt", Endpoint: ep, Policy: keyrange.Policy{Seed: true, SeedDescent: true}}, 64)
		if !slices.Equal(got, keys) {
			t.Fatalf("short=%v: %d keys, want %d", short, len(got), len(keys))
		}
		est := st.Counters.Value("seed.short_continuing_estimated")
		if !st.Counters.On("seed") || st.Counters.Value("seed.probes") == 0 {
			t.Fatalf("short=%v: the seed did not run", short)
		}
		if (est > 0) != short {
			t.Fatalf("short=%v: seed.short_continuing_estimated %d", short, est)
		}
		t.Logf("short=%v: %d requests, %d estimated samples", short, st.Counters.Value("requests"), est)
	}
}

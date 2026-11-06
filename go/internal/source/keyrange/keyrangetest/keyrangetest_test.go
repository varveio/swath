// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrangetest

import (
	"context"
	"slices"
	"testing"
)

// TestStartAfterSkipsCoveredPrefixes: like S3 (measured on
// noaa-hrrr-bdp-pds), a delimiter listing returns no common prefix at or
// below StartAfter: start-after "b/" and "b/2" list from "c/", "b" lists
// "b/".
func TestStartAfterSkipsCoveredPrefixes(t *testing.T) {
	f := NewSorted([]string{"a/1", "b", "b/1", "b/2", "b/3", "c/1", "d"}, false, 1)
	for after, want := range map[string][]string{"b/": {"c/"}, "b/2": {"c/"}, "b": {"b/", "c/"}, "": {"a/", "b/", "c/"}} {
		out, err := f.List(context.Background(), &ListInput{Delimiter: "/", StartAfter: after, MaxKeys: 10})
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(out.CommonPrefixes, want) {
			t.Fatalf("start-after %q: prefixes %q, want %q", after, out.CommonPrefixes, want)
		}
	}
}

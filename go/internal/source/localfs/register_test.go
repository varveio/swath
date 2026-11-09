// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"slices"
	"testing"
)

// TestReportFilters: --no-dirs leaves the directories' rows out, so the
// coverage records it among the filters, and a reader that needs the
// whole tree refuses the dataset; a listing of every entry records none.
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

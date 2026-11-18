// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"context"
	"runtime"
	"slices"
	"testing"

	"github.com/varveio/swath/internal/bind"
	"github.com/varveio/swath/internal/filter"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/reader"
)

// listOpened lists o once as the command's local source does
// (internal/cli): Begin with spec, then the bound provider unordered into
// no sink for a count, else in key order into sink.
func listOpened(ctx context.Context, o *Opened, spec *filter.Spec, count bool, env bind.Env, sink order.Sink[Entry]) (order.Stats, error) {
	end, err := o.Begin(spec, nil, count, &env.Order)
	if err != nil {
		return order.Stats{}, err
	}
	defer end()

	env.Filter = spec
	l, err := o.Bound().Prepare(env)
	if err != nil {
		return order.Stats{}, err
	}
	if count {
		return l.Count(ctx, nil)
	}
	return l.Run(ctx, sink)
}

// TestReportFilters: --no-dirs leaves the directories' rows out, so the
// coverage records it among the filters; a listing of every entry records
// none.
func TestReportFilters(t *testing.T) {
	for _, tc := range []struct {
		opt  Options
		want []reader.Filter
	}{
		{Options{}, nil},
		{Options{NoDirs: true}, []reader.Filter{noDirsFilter}},
	} {
		p, err := New(t.TempDir(), tc.opt)
		if err != nil {
			t.Fatal(err)
		}
		if got := (&Opened{p: p}).Report().Coverage.Filters; !slices.Equal(got, tc.want) {
			t.Fatalf("%+v: filters %q, want %q", tc.opt, got, tc.want)
		}
	}
}

// TestDefaultWorkers: a local tree's default --workers is 4 per processor,
// at most 64.
func TestDefaultWorkers(t *testing.T) {
	prev := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(prev)
	for procs, want := range map[int]int{1: 4, 2: 8, 4: 16, 16: 64, 32: 64} {
		runtime.GOMAXPROCS(procs)
		if got := DefaultWorkers(); got != want {
			t.Errorf("GOMAXPROCS %d: %d workers, want %d", procs, got, want)
		}
	}
}

// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import (
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/counters"
)

// TestLatencyBuckets: the buckets tile the durations without gaps, each
// at most a quarter of its lower edge wide past the exact ones, and a
// duration lands in the bucket whose edges hold it.
func TestLatencyBuckets(t *testing.T) {
	prev := int64(-1)
	for i := range latencyFinite {
		up := latencyUpper(i)
		lo := prev + 1
		if up < lo || (lo >= 4 && float64(up-lo+1) > float64(lo)/4) {
			t.Fatalf("bucket %d: [%d, %d]", i, lo, up)
		}
		for _, d := range []int64{lo, up} {
			if b := latencyBucket(d); b != i {
				t.Fatalf("%d ns in bucket %d, want %d", d, b, i)
			}
		}
		prev = up
	}
	if prev != 1<<latencyMaxBits-1 {
		t.Fatalf("the finite buckets end at %d, want 2^40-1", prev)
	}
	if latencyBucket(1<<latencyMaxBits) != latencyOverflow || latencyBucket(int64(time.Hour)) != latencyOverflow || latencyBucket(-1) != 0 {
		t.Fatal("out-of-range durations not clamped")
	}
	if latencyUpper(latencyOverflow) != LatencyOpen {
		t.Fatal("the overflow bucket has a finite edge")
	}
}

// TestLatencySaturation: at the histogram's boundary, 2^40-1 ns reports
// the last finite edge and no overflow; 2^40 ns and a 20-minute attempt
// land in the open bucket, a percentile falling there reads LatencyOpen
// (never the finite edge below it), the overflow count says how many,
// and Store carries both.
func TestLatencySaturation(t *testing.T) {
	var c Counters
	c.latency.record(1<<latencyMaxBits - 1)
	if n := c.Load(); n.P99Ns != 1<<latencyMaxBits-1 || n.LatencyOverflow != 0 {
		t.Fatalf("2^40-1 ns: p99 %d, overflow %d", n.P99Ns, n.LatencyOverflow)
	}
	c = Counters{}
	for range 98 {
		c.latency.record(10 * time.Millisecond)
	}
	c.latency.record(1 << latencyMaxBits)
	c.latency.record(20 * time.Minute)
	n := c.Load()
	if n.P50Ns >= int64(20*time.Millisecond) || n.P90Ns >= int64(20*time.Millisecond) || n.P99Ns != LatencyOpen || n.LatencyOverflow != 2 {
		t.Fatalf("p50 %d, p90 %d, p99 %d, overflow %d", n.P50Ns, n.P90Ns, n.P99Ns, n.LatencyOverflow)
	}
	c = Counters{}
	c.latency.record(20 * time.Minute)
	if n := c.Load(); n.P50Ns != LatencyOpen || n.P90Ns != LatencyOpen || n.P99Ns != LatencyOpen {
		t.Fatalf("one 20-minute attempt: p50 %d, p90 %d, p99 %d", n.P50Ns, n.P90Ns, n.P99Ns)
	}
	set := counters.New(CounterDefs("throttle", "-timeout"))
	n.Store(set, 0)
	if set.Load(counters.ID(CInflightP99)) != LatencyOpen || set.Load(counters.ID(CLatencyOverflow)) != 2 {
		t.Fatal("Store left saturation out")
	}
}

// TestLatencyQuantiles: the percentiles are the upper edges of the buckets
// holding them, and Store copies them to the tally.
func TestLatencyQuantiles(t *testing.T) {
	var c Counters
	if q := c.Load(); q.P50Ns != 0 || q.P99Ns != 0 {
		t.Fatalf("no samples: %+v", q)
	}
	for range 89 {
		c.latency.record(10 * time.Millisecond)
	}
	for range 10 {
		c.latency.record(100 * time.Millisecond)
	}
	c.latency.record(time.Second)
	n := c.Load()
	within := func(got int64, want time.Duration) bool {
		return got >= int64(want) && float64(got) <= 1.25*float64(want)
	}
	if !within(n.P50Ns, 10*time.Millisecond) || !within(n.P90Ns, 100*time.Millisecond) || !within(n.P99Ns, 100*time.Millisecond) {
		t.Fatalf("p50 %d, p90 %d, p99 %d", n.P50Ns, n.P90Ns, n.P99Ns)
	}
	set := counters.New(CounterDefs("throttle", "-timeout"))
	n.Store(set, 0)
	if set.Load(counters.ID(CInflightP50)) != n.P50Ns || set.Load(counters.ID(CInflightP90)) != n.P90Ns || set.Load(counters.ID(CInflightP99)) != n.P99Ns {
		t.Fatal("Store left the percentiles out")
	}
}

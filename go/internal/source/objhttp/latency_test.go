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
	for i := range latencyBuckets {
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
	if latencyBucket(int64(time.Hour)) != latencyBuckets-1 || latencyBucket(-1) != 0 {
		t.Fatal("out-of-range durations not clamped")
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

// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"fmt"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/objhttp/faulttest"
)

// proxied puts the fault proxy in front of a fake bucket.
func proxied(t *testing.T, keys []string, plan faulttest.Plan) (*faulttest.Proxy, string) {
	t.Helper()
	p, err := faulttest.New(fakeBucket(t, keys, 0), plan)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(p)
	t.Cleanup(srv.Close)
	return p, srv.URL
}

// TestThrottleStormLowersConcurrency: a window in which every request is
// answered 503 SlowDown neither fails the run nor changes its output; the
// limiter cuts the requests in flight during the storm and keeps them cut
// through the throttle-free window that follows. Without the limiter the
// same storm leaves concurrency at the worker count.
func TestThrottleStormLowersConcurrency(t *testing.T) {
	keys := testKeys()
	for i := range 8000 {
		keys = append(keys, fmt.Sprintf("logs/%03d/%05d", i%200, i))
	}
	slices.Sort(keys)
	storm := faulttest.Window{From: 300 * time.Millisecond, To: 800 * time.Millisecond, Fraction: 1, Fault: faulttest.Throttle}
	slow := faulttest.Window{Fraction: 1, Fault: faulttest.Latency, Delay: 60 * time.Millisecond}
	const workers = 16
	for _, on := range []bool{true, false} {
		px, ep := proxied(t, keys, faulttest.Plan{Seed: 1, Windows: []faulttest.Window{slow, storm}})
		rows, st := list(t, Options{Bucket: "bkt", Endpoint: ep, PathStyle: true, Workers: workers,
			Policy: keyrange.Policy{Seed: true, SeedDescent: true, Limiter: on}}, workers)
		if !slices.Equal(keysOf(rows), keys) {
			t.Fatalf("limiter %v: %d keys, want %d", on, len(rows), len(keys))
		}
		tl, s := px.Timeline(), px.Stats()
		if s.Throttled == 0 {
			t.Fatalf("limiter %v: the storm throttled nothing (%+v)", on, s)
		}
		after := int64(0) // most in flight from 200 ms after the storm to the end
		for i := int((storm.To + 200*time.Millisecond) / faulttest.Step); i < len(tl); i++ {
			after = max(after, tl[i].MaxActive)
		}
		v := st.Counters.Value
		t.Logf("limiter %v: %+v, in flight after the storm %d, decreases %d depth %d waits %d", on, s, after,
			v("limiter.decreases"), v("limiter.depth"), v("limiter.waits"))
		if !on {
			if st.Counters.On("limiter") || s.MaxActive < workers/2 {
				t.Fatalf("limiter off: feature %v, max in flight %d", st.Counters.On("limiter"), s.MaxActive)
			}
			continue
		}
		if !st.Counters.On("limiter") || v("limiter.decreases") == 0 || v("limiter.depth") < workers/2 || v("limiter.increases") != 0 {
			t.Fatalf("limiter counters: decreases %d depth %d increases %d", v("limiter.decreases"), v("limiter.depth"), v("limiter.increases"))
		}
		if after > workers-v("limiter.depth") {
			t.Fatalf("%d in flight after the storm, above the lowered limit %d", after, workers-v("limiter.depth"))
		}
	}
}

// TestOneAdmission: with one permit, the seed descent's prefetches, the
// structure probes sent alongside pages, split probes, retries and
// requeues never put two requests in flight at the server.
func TestOneAdmission(t *testing.T) {
	keys := testKeys()
	px, ep := proxied(t, keys, faulttest.Plan{Seed: 2, Windows: []faulttest.Window{
		{Fraction: 1, Fault: faulttest.Latency, Delay: 2 * time.Millisecond},
		{Fraction: 0.2, Fault: faulttest.Throttle, To: 300 * time.Millisecond},
		{Fraction: 0.1, Fault: faulttest.Reset, To: 300 * time.Millisecond}}})
	rows, st := list(t, Options{Bucket: "bkt", Endpoint: ep, PathStyle: true, Workers: 1,
		Policy: keyrange.Policy{Seed: true, SeedDescent: true, Structure: true, Probe: true, Limiter: true}}, 8)
	if !slices.Equal(keysOf(rows), keys) {
		t.Fatalf("%d keys, want %d", len(rows), len(keys))
	}
	s := px.Stats()
	if s.MaxActive != 1 || s.Throttled == 0 || s.Reset == 0 || st.Counters.Value("limiter.waits") == 0 {
		t.Fatalf("proxy %+v, waits %d", s, st.Counters.Value("limiter.waits"))
	}
}

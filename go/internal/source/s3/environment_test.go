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
	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/source/objhttp/faulttest"
	"github.com/varveio/swath/go/internal/source/s3/client"
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

// TestThrottleStormLowersConcurrency: a storm in which every request is
// answered 503 SlowDown neither fails the run nor changes its output; the
// limiter cuts the requests in flight during the storm and keeps them cut
// through the throttle-free window that follows. Without the limiter the
// same storm leaves concurrency at the worker count.
//
// The storm is requests 24 to 55 by arrival, not a span of time: on a
// loaded host a time span can hold no request at all. The limiter is at
// its floor after 5 throttles (16, 11, 7, 4, 2, 1), and the rest of the
// storm drains the requests it admitted before then. "After the storm"
// starts 200 ms after the last throttle and ends 10 s (the limiter's clean
// window) after the first: growth comes back only that long after the
// last real decrease, which no throttle precedes. Past it the limiter may
// grow, as it should, so increases are checked only for a run that ended
// inside it.
func TestThrottleStormLowersConcurrency(t *testing.T) {
	keys := testKeys()
	for i := range 8000 {
		keys = append(keys, fmt.Sprintf("logs%03d/%05d", i%200, i))
	}
	slices.Sort(keys)
	storm := faulttest.Window{N0: 24, N1: 56, Fraction: 1, Fault: faulttest.Throttle}
	slow := faulttest.Window{Fraction: 1, Fault: faulttest.Latency, Delay: 60 * time.Millisecond}
	const workers = 16
	const cleanWindow = 10 * time.Second // objhttp's throttle-free time before any growth
	for _, on := range []bool{true, false} {
		px, ep := proxied(t, keys, faulttest.Plan{Seed: 1, Windows: []faulttest.Window{slow, storm}})
		rows, st := list(t, Options{Bucket: "bkt", Endpoint: ep, PathStyle: true, Workers: workers,
			Policy: keyrange.Policy{Seed: true}, Admission: objhttp.Admission{Limiter: on}}, workers)
		if !slices.Equal(keysOf(rows), keys) {
			t.Fatalf("limiter %v: %d keys, want %d", on, len(rows), len(keys))
		}
		tl, s := px.Timeline(), px.Stats()
		if s.Throttled != int64(storm.N1-storm.N0) {
			t.Fatalf("limiter %v: the storm throttled %d requests, want %d (%+v)", on, s.Throttled, storm.N1-storm.N0, s)
		}
		first, last := -1, 0 // the buckets of the storm's first and last throttles
		for i, b := range tl {
			if b.Throttled > 0 && first < 0 {
				first = i
			}
			if b.Throttled > 0 {
				last = i
			}
		}
		from := last + int(200*time.Millisecond/faulttest.Step)
		to := min(len(tl), first+int(cleanWindow/faulttest.Step))
		after, arrived := int64(0), int64(0) // most in flight after the storm, and the requests then
		for i := from; i < to; i++ {
			after, arrived = max(after, tl[i].MaxActive), arrived+tl[i].Arrived
		}
		v := st.Counters.Value
		t.Logf("limiter %v: %+v, storm %d-%d ms, in flight after it %d (%d requests), decreases %d depth %d increases %d waits %d",
			on, s, first*int(faulttest.Step/time.Millisecond), last*int(faulttest.Step/time.Millisecond), after, arrived,
			v("limiter.decreases"), v("limiter.max_depth"), v("limiter.increases"), v("limiter.waits"))
		if !on {
			if st.Counters.On("limiter") || s.MaxActive < workers/2 {
				t.Fatalf("limiter off: feature %v, max in flight %d", st.Counters.On("limiter"), s.MaxActive)
			}
			continue
		}
		if arrived == 0 {
			t.Fatalf("no request arrived after the storm and inside the clean window:\n%s", faulttest.TSV(tl))
		}
		grew := v("limiter.increases") != 0 && len(tl) <= first+int(cleanWindow/faulttest.Step)
		if !st.Counters.On("limiter") || v("limiter.decreases") == 0 || v("limiter.max_depth") < workers/2 || grew {
			t.Fatalf("limiter counters: decreases %d depth %d increases %d", v("limiter.decreases"), v("limiter.max_depth"), v("limiter.increases"))
		}
		if after > workers-v("limiter.max_depth") {
			t.Fatalf("%d in flight after the storm, above the lowered limit %d:\n%s", after, workers-v("limiter.max_depth"), faulttest.TSV(tl))
		}
	}
}

// TestOneAdmission: with one permit, the seed page, the
// structure probes sent alongside pages, retries and
// requeues never put two requests in flight at the server. The faults
// span the whole run, so which requests get them depends on the request
// number alone (seed 2: throttles at 2 and 3, a reset at 5), not on how
// many requests a loaded host fits into a time window.
func TestOneAdmission(t *testing.T) {
	keys := testKeys()
	px, ep := proxied(t, keys, faulttest.Plan{Seed: 2, Windows: []faulttest.Window{
		{Fraction: 1, Fault: faulttest.Latency, Delay: 2 * time.Millisecond},
		{Fraction: 0.2, Fault: faulttest.Throttle},
		{Fraction: 0.1, Fault: faulttest.Reset}}})
	rows, st := list(t, Options{Bucket: "bkt", Endpoint: ep, PathStyle: true, Workers: 1,
		Policy: keyrange.Policy{Seed: true, Structure: keyrange.StructureOn}, Admission: objhttp.Admission{Limiter: true}}, 8)
	if !slices.Equal(keysOf(rows), keys) {
		t.Fatalf("%d keys, want %d", len(rows), len(keys))
	}
	s := px.Stats()
	if s.MaxActive != 1 || s.Throttled == 0 || s.Reset == 0 || st.Counters.Value("limiter.waits") == 0 {
		t.Fatalf("proxy %+v, waits %d", s, st.Counters.Value("limiter.waits"))
	}
}

// TestAttemptsAreRequestEconomics: the request economics count HTTP
// attempts. A call answered SlowDown twice, then a page, is 3 attempts and
// 1 logical request; a call exhausted by throttles (3 of 3) and requeued
// counts all 3 throttles, the last included (it was uncounted). In-flight
// attempts are measured at the send, apart from the active ranges.
func TestAttemptsAreRequestEconomics(t *testing.T) {
	keys := testKeys()[:50] // one page
	for _, c := range []struct {
		name       string
		fail       int
		extra, thr int64 // attempts beyond the logical request; throttles
	}{
		{"retried", 2, 2, 2},
		{"exhausted and requeued", 3, 3, 3}, // the requeue repeats the same logical request
	} {
		t.Run(c.name, func(t *testing.T) {
			ep := fakeBucket(t, keys, c.fail)
			cl, err := client.New(client.Config{Endpoint: ep, PathStyle: true, Anonymous: true, Region: "us-east-1",
				MaxAttempts: 3, BaseBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			rows, st := list(t, Options{Bucket: "bkt", Client: cl, Workers: 1}, 1)
			if !slices.Equal(keysOf(rows), keys) {
				t.Fatalf("%d keys, want %d", len(rows), len(keys))
			}
			v := st.Counters.Value
			if v("requests") != 1 || v("attempts") != v("requests")+c.extra || v("retries") != 2 ||
				v("throttles") != c.thr || v("inflight.max_active") != 1 || v("inflight.busy_ns") <= 0 {
				t.Fatalf("requests %d attempts %d retries %d throttles %d inflight max %d ns %d", v("requests"), v("attempts"),
					v("retries"), v("throttles"), v("inflight.max_active"), v("inflight.busy_ns"))
			}
		})
	}
}

// TestSlowStartStartsNarrow: with Admission.SlowStart the client's limiter
// starts at 4 and doubles on the first success, so the first 100 ms hold at
// most 8 requests in flight at the server; the ramp's counters are on.
func TestSlowStartStartsNarrow(t *testing.T) {
	keys := testKeys()
	for i := range 8000 {
		keys = append(keys, fmt.Sprintf("logs%03d/%05d", i%200, i))
	}
	slices.Sort(keys)
	slow := faulttest.Window{Fraction: 1, Fault: faulttest.Latency, Delay: 60 * time.Millisecond}
	const workers = 32
	px, ep := proxied(t, keys, faulttest.Plan{Seed: 1, Windows: []faulttest.Window{slow}})
	rows, st := list(t, Options{Bucket: "bkt", Endpoint: ep, PathStyle: true, Workers: workers,
		Policy: keyrange.Policy{Seed: true}, Admission: objhttp.Admission{Limiter: true, SlowStart: true}}, workers)
	if !slices.Equal(keysOf(rows), keys) {
		t.Fatalf("%d keys, want %d", len(rows), len(keys))
	}
	tl, v := px.Timeline(), st.Counters.Value
	if len(tl) == 0 || tl[0].MaxActive > 8 || v("slow_start.doubles") == 0 {
		t.Fatalf("first 100 ms: %+v; doubles %d", tl[:min(1, len(tl))], v("slow_start.doubles"))
	}
}

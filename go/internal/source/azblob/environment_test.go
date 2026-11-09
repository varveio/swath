// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"context"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/go/internal/source/azblob/client"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/source/objhttp/faulttest"
)

// TestFaultsRequeued: 503 ServerBusy throttles on most of the first 60
// listing requests, attempts past the request timeout and connection resets,
// through a client with few attempts, neither fail a run nor change its
// output: exhausted pages are requeued, and the report counts throttles,
// timeouts and requeues. Admission refuses on any failure
// (TestAdmission), so each arm's proxy leaves its first request (Get
// Account Information) alone.
func TestFaultsRequeued(t *testing.T) {
	defer keyrange.SetRequeueBackoff(5*time.Millisecond, 20*time.Millisecond)()
	keys := azblobtest.Keys()
	r := azblobtest.Serve(t, keys)
	var timeouts, delayed, reset int64
	for _, pol := range []keyrange.Policy{{Sequential: true, PageSize: 500}, {Seed: true, PageSize: 100}} {
		workers := 16
		if pol.Sequential {
			workers = 1
		}
		px, err := faulttest.New(base(r), faulttest.Plan{Seed: 5, Dialect: "azure", Windows: []faulttest.Window{
			{N0: 1, N1: 60, Fraction: 0.8, Fault: faulttest.Throttle},
			{N0: 1, Fraction: 0.05, Fault: faulttest.Latency, Delay: 600 * time.Millisecond},
			{N0: 1, Fraction: 0.05, Fault: faulttest.Reset}}})
		if err != nil {
			t.Fatal(err)
		}
		srv := httptest.NewServer(px)
		t.Cleanup(srv.Close)
		lim, _ := objhttp.Admission{Limiter: true}.New(workers)
		c, err := client.New(context.Background(), client.Config{Endpoint: srv.URL, Account: r.Account, Anonymous: true, Workers: workers,
			MaxAttempts: 3, BaseBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond, RequestTimeout: 300 * time.Millisecond,
			Limiter: lim})
		if err != nil {
			t.Fatal(err)
		}
		rows, st := list(t, Options{Account: r.Account, Container: r.Container, Client: c, Policy: pol}, workers)
		if !slices.Equal(keysOf(rows), keys) {
			t.Fatalf("%+v: %d keys, want %d", pol, len(rows), len(keys))
		}
		v := st.Counters.Value
		timeouts += v("timeouts")
		delayed, reset = delayed+px.Stats().Delayed, reset+px.Stats().Reset
		if v("throttles") == 0 || v("requeues") == 0 || v("retries") == 0 {
			t.Fatalf("%+v: throttles %d requeues %d retries %d timeouts %d (proxy %+v)", pol, v("throttles"), v("requeues"), v("retries"), v("timeouts"), px.Stats())
		}
		t.Logf("%+v: throttles %d timeouts %d retries %d requeues %d", pol, v("throttles"), v("timeouts"), v("retries"), v("requeues"))
	}
	if timeouts == 0 || delayed == 0 || reset == 0 {
		t.Fatalf("timeouts %d, delayed %d, reset %d: a fault never engaged", timeouts, delayed, reset)
	}
}

// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/source/objhttp/faulttest"
)

// TestRateLimitStormLowersConcurrency: a window of 429 rateLimitExceeded
// answers, with connection resets throughout, neither fails a GCS run nor changes its
// output, and the limiter lowers the requests in flight.
func TestRateLimitStormLowersConcurrency(t *testing.T) {
	keys := testKeys()
	_, ep := newFake(t, keys)
	base, path, _ := strings.Cut(strings.TrimPrefix(ep, "http://"), "/")
	px, err := faulttest.New("http://"+base, faulttest.Plan{Seed: 3, Dialect: "gcs", Windows: []faulttest.Window{
		{Fraction: 1, Fault: faulttest.Latency, Delay: 30 * time.Millisecond},
		{To: 300 * time.Millisecond, Fraction: 0.7, Fault: faulttest.Throttle},
		{Fraction: 0.15, Fault: faulttest.Reset}}})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(px)
	t.Cleanup(srv.Close)
	got, st := list(t, Options{Bucket: "bkt", Endpoint: srv.URL + "/" + path,
		Policy: keyrange.Policy{Seed: true, SeedDescent: true}, Admission: objhttp.Admission{Limiter: true}}, 16)
	if !slices.Equal(got, keys) {
		t.Fatalf("%d keys, want %d", len(got), len(keys))
	}
	s, v := px.Stats(), st.Counters.Value
	if s.Throttled == 0 || s.Reset == 0 || v("limiter.decreases") == 0 || v("limiter.depth") == 0 || v("throttles") == 0 {
		t.Fatalf("proxy %+v, decreases %d depth %d throttles %d", s, v("limiter.decreases"), v("limiter.depth"), v("throttles"))
	}
}

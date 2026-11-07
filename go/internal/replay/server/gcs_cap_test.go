// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/varveio/swath/go/internal/replay/gcs"
	"github.com/varveio/swath/go/internal/replay/metrics"
	"github.com/varveio/swath/go/internal/replay/store/memstore"
)

// TestGCSResponseLimitsAreEnforcedBeforeAllocating is the review's case:
// 1,000 names of about 1 KiB each, asked for in one page, under an
// 8 KiB response cap and an 8 KiB budget. The page would be about
// 1.1 MB; the request must stop at the cap with response_too_large
// without ever charging more than the cap or allocating the body, and a
// request running beside it must still get its budget.
func TestGCSResponseLimitsAreEnforcedBeforeAllocating(t *testing.T) {
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%04d/%s", i, strings.Repeat("x", 1018))
	}
	cfg := gcsConfig(memstore.Keys(memstore.Options{}, keys...), ProtocolGCS)
	cfg.MaxResponseBytes, cfg.ResponseBufferBudget = 8192, 8192
	s := start(t, cfg)
	const body = 1_000 * 1_024

	// One request served in process, so the allocation measured is the
	// request's own. Reading the page (the store's rows and the page's
	// items; the names are not copied) is measured alone first; the
	// request may allocate that plus a small, page-size-independent
	// remainder (the capped output and the error), never the body.
	serve := func(target string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, target, nil)
		req.RequestURI = target
		s.ServeHTTP(rec, req)
		return rec
	}
	serve(gcsList + "?maxResults=1") // warm the route's one-time state
	measure := func(f func()) uint64 {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		f()
		runtime.ReadMemStats(&after)
		return after.TotalAlloc - before.TotalAlloc
	}
	req, err := gcs.Parse("bucket", "maxResults=1000")
	if err != nil {
		t.Fatal(err)
	}
	reading := measure(func() {
		if _, err := s.routes.gcs.List(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	})
	var rec *httptest.ResponseRecorder
	allocated := measure(func() { rec = serve(gcsList + "?maxResults=1000") })
	if rec.Code != 500 || strings.Join(rec.Header()["x-swath-replay-error"], ",") != "response_too_large" {
		t.Fatalf("%d %v %s", rec.Code, rec.Header(), rec.Body)
	}
	if peak := s.budget.Peak(); peak > int64(cfg.MaxResponseBytes) {
		t.Errorf("peak charge %d over the %d-byte cap", peak, cfg.MaxResponseBytes)
	}
	if allocated > reading+64<<10 {
		t.Errorf("the refused request allocated %d bytes, %d beyond reading its page; the body is %d", allocated, allocated-reading, body)
	}
	if s.budget.Charged() != 0 {
		t.Errorf("%d bytes still charged", s.budget.Charged())
	}

	// Beside a stream of refused full pages, small pages keep getting the
	// budget: each refusal returns its charge as it stops.
	var wg sync.WaitGroup
	stop := make(chan struct{})
	var refused, served, busy int
	var mu sync.Mutex
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				if r := get(t, s, gcsList+"?maxResults=1000"); r.status != 500 || r.header.Get("x-swath-replay-error") != "response_too_large" {
					if r.header.Get("x-swath-replay-error") != "response_budget_exhausted" {
						t.Errorf("full page: %d %v", r.status, r.header)
					}
				}
				mu.Lock()
				refused++
				mu.Unlock()
			}
		})
	}
	for range 200 {
		r := get(t, s, gcsList+"?maxResults=2")
		mu.Lock()
		switch {
		case r.status == 200 && strings.Contains(r.body, `"name":"k0001/`):
			served++
		case r.status == 503 && r.header.Get("x-swath-replay-error") == "response_budget_exhausted":
			busy++
		default:
			t.Errorf("small page: %d %v", r.status, r.header)
		}
		mu.Unlock()
	}
	close(stop)
	wg.Wait()
	if served == 0 || refused == 0 {
		t.Errorf("%d small pages served, %d refused for the budget, %d full pages refused", served, busy, refused)
	}
	if peak := s.budget.Peak(); peak > int64(cfg.ResponseBufferBudget) || s.budget.Charged() != 0 {
		t.Errorf("peak %d, charged %d", peak, s.budget.Charged())
	}
	if count(s, metrics.ResponseAdmissionRefused, "gcs", "response_too_large") < 1 {
		t.Error("no response_too_large refusal counted")
	}
	t.Logf("refused request allocated %d bytes (reading the page %d); beside it %d small pages served, %d refused for the budget, %d full pages refused", allocated, reading, served, busy, refused)
}

// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/varveio/swath/go/internal/replay/metrics"
	"github.com/varveio/swath/go/internal/replay/store/memstore"
)

// apostropheNames are 5,000 names of 1,020 apostrophes and a four-digit
// id: each within the 1,024-unit limit, each six times longer escaped,
// about 30 MiB for one page.
func apostropheNames() []string {
	names := make([]string, 5000)
	for i := range names {
		names[i] = strings.Repeat("'", 1020) + fmt.Sprintf("%04d", i)
	}
	return names
}

// allocatedDuring is the bytes the process allocated while f ran.
func allocatedDuring(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// TestAzureEscapingHeavyPageStopsAtTheCap pins the per-response cap on
// growth during rendering: a page whose escaped body is about 30 MiB, with
// a 4 KiB cap, is refused (500 response_too_large) without the body being
// built, and the budget is whole again afterwards.
func TestAzureEscapingHeavyPageStopsAtTheCap(t *testing.T) {
	cfg := config(memstore.Keys(memstore.Options{}, apostropheNames()...))
	cfg.Protocols, cfg.AzureAccount, cfg.MaxResponseBytes = []string{"azure"}, DefaultAzureAccount, 4096
	s := start(t, cfg)
	var r response
	alloc := allocatedDuring(func() {
		r = azureDo(t, s, "GET", "/replay/bucket?restype=container&comp=list&maxresults=5000", "x-ms-version", "2026-06-06")
	})
	if r.status != 500 || r.header.Get("x-swath-replay-error") != "response_too_large" {
		t.Fatalf("%d %s", r.status, r.header.Get("x-swath-replay-error"))
	}
	// The rows themselves are about 5 MiB; the escaped body would be 30.
	if alloc > 16<<20 {
		t.Errorf("allocated %d bytes to refuse a page over a 4 KiB cap", alloc)
	}
	waitFor(t, "the budget to come back", func() bool { return s.budget.Charged() == 0 })
}

// TestAzureConcurrentBudgetExhaustion sends escaping-heavy pages at once
// against a small budget: requests beyond it are refused 503 while
// rendering, the charged bytes never exceed the budget, and the budget is
// whole again afterwards.
func TestAzureConcurrentBudgetExhaustion(t *testing.T) {
	cfg := config(memstore.Keys(memstore.Options{}, apostropheNames()[:200]...))
	cfg.Protocols, cfg.AzureAccount = []string{"azure"}, DefaultAzureAccount
	cfg.ResponseBufferBudget, cfg.MaxResponseBytes = 2<<20, 2<<20
	s := start(t, cfg)
	statuses := map[int]int{}
	var mu sync.Mutex
	alloc := allocatedDuring(func() {
		var wg sync.WaitGroup
		for range 16 {
			wg.Go(func() {
				r := azureDo(t, s, "GET", "/replay/bucket?restype=container&comp=list&maxresults=200", "x-ms-version", "2026-06-06")
				mu.Lock()
				statuses[r.status]++
				mu.Unlock()
			})
		}
		wg.Wait()
	})
	// A served page is about 1.2 MiB rendered, and the test's client
	// copies it twice more while reading it; a refused one must stop
	// growing at the refusal rather than render in full first (before the
	// bounded sink, 16 requests allocated about 136 MiB here).
	if limit := uint64(16<<20 + statuses[200]*(8<<20)); alloc > limit {
		t.Errorf("allocated %d bytes, over %d for %d served pages", alloc, limit, statuses[200])
	}
	if statuses[200]+statuses[503] != 16 || statuses[200] == 0 {
		t.Errorf("statuses %v", statuses)
	}
	if peak := s.budget.Peak(); peak > cfg.ResponseBufferBudget {
		t.Errorf("peak %d over the budget %d", peak, cfg.ResponseBufferBudget)
	}
	waitFor(t, "the budget to come back", func() bool { return s.budget.Charged() == 0 })
	if statuses[503] > 0 && count(s, metrics.ResponseAdmissionRefused, ProtocolAzure, "response_budget_exhausted") != int64(statuses[503]) {
		t.Error("refusals not counted")
	}
}

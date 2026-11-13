// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath/internal/replay/metrics"
	"github.com/varveio/swath/internal/replay/store/memstore"
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
	if n := s.budget.Charged(); n != 0 {
		t.Errorf("%d bytes still charged", n)
	}
}

// TestAzureConcurrentBudgetExhaustion holds one escaping-heavy page
// charged (its client sends the request and never reads, so the write
// blocks with the rendered chunks charged), then sends competing pages:
// each is refused 503 response_budget_exhausted while rendering, the
// refusals are counted exactly, the peak charge stays within the budget,
// and once the held connection closes every byte is released.
func TestAzureConcurrentBudgetExhaustion(t *testing.T) {
	const pageNames = 3300 // about 20 MiB escaped
	cfg := config(memstore.Keys(memstore.Options{}, apostropheNames()[:pageNames]...))
	cfg.Protocols, cfg.AzureAccount = []string{"azure"}, DefaultAzureAccount
	cfg.ResponseBufferBudget, cfg.MaxResponseBytes = 24<<20, 24<<20
	s := start(t, cfg)
	target := fmt.Sprintf("/replay/bucket?restype=container&comp=list&maxresults=%d", pageNames)

	held, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", s.Port()))
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	if tc, ok := held.(*net.TCPConn); ok {
		tc.SetReadBuffer(4096)
	}
	fmt.Fprintf(held, "GET %s HTTP/1.1\r\nHost: x\r\nx-ms-version: 2026-06-06\r\n\r\n", target)
	deadline := time.Now().Add(20 * time.Second)
	// Wait until the held page is rendered (its render stage recorded):
	// from then on it is only writing, with its chunks charged.
	for count(s, metrics.RequestStageLatency, ProtocolAzure, "render") < 1 {
		if time.Now().After(deadline) {
			t.Fatalf("the held page never rendered: %d bytes charged", s.budget.Charged())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if held := s.budget.Charged(); held < 16<<20 {
		t.Fatalf("the held page holds only %d bytes", held)
	}

	const competing = 3
	var wg sync.WaitGroup
	statuses := make([]response, competing)
	for i := range competing {
		wg.Go(func() { statuses[i] = azureDo(t, s, "GET", target, "x-ms-version", "2026-06-06") })
	}
	wg.Wait()
	for i, r := range statuses {
		if r.status != 503 || r.header.Get("x-swath-replay-error") != "response_budget_exhausted" ||
			r.header.Get("x-ms-error-code") != "ServerBusy" {
			t.Errorf("competing page %d: %d %s (%d bytes, charge now %d)", i, r.status, r.header.Get("x-swath-replay-error"), len(r.body), s.budget.Charged())
		}
	}
	if n := count(s, metrics.ResponseAdmissionRefused, ProtocolAzure, "response_budget_exhausted"); n != competing {
		t.Errorf("%d refusals counted, want %d", n, competing)
	}
	if peak := s.budget.Peak(); peak > cfg.ResponseBufferBudget {
		t.Errorf("peak %d over the budget %d", peak, cfg.ResponseBufferBudget)
	}
	held.Close()
	for s.budget.Charged() != 0 {
		if time.Now().After(deadline.Add(10 * time.Second)) {
			t.Fatalf("%d bytes still charged after the held connection closed", s.budget.Charged())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

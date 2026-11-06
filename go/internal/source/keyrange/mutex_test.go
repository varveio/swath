// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestDecisionMutexCounters: the decision mutex's wait and longest hold
// move under contention (the scale gate's "decision-mutex wait"): one
// goroutine holds it 50 ms while four others wait, so the summed wait is
// at least 4 × 40 ms and the longest hold at least 50 ms, in Contention
// and in the counter table (mutex.wait_ns, mutex.max_hold_ns); a real
// listing also records them. Before, nothing recorded either.
func TestDecisionMutexCounters(t *testing.T) {
	p, err := New(Options{Workers: 4}, Description[Object]{Target: "test", Listing: Listing}, fetchFrom(newSorted(keysN(3000), false, 1)))
	if err != nil {
		t.Fatal(err)
	}
	const hold = 50 * time.Millisecond
	p.lock()
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.lock()
			p.unlock()
		}()
	}
	time.Sleep(hold)
	p.unlock()
	wg.Wait()
	wait, maxHold := p.Contention()
	if wait < 4*(hold-10*time.Millisecond) || maxHold < hold {
		t.Fatalf("wait %s, max hold %s", wait, maxHold)
	}
	st := p.Stats() // takes the mutex itself: the counters can only have grown
	if time.Duration(st.Get(cMutexWait)) < wait || time.Duration(st.Get(cMutexMaxHold)) < maxHold {
		t.Fatalf("counters %d %d, Contention %s %s", st.Get(cMutexWait), st.Get(cMutexMaxHold), wait, maxHold)
	}
	// A listing takes it on every page: the hold counter moves on its own.
	q, err := New(Options{Workers: 4}, Description[Object]{Target: "test", Listing: Listing}, fetchFrom(newSorted(keysN(3000), false, 1)))
	if err != nil {
		t.Fatal(err)
	}
	if err := q.expandRange(context.Background(), Range{open: true}, &direct{&collect{}}); err != nil {
		t.Fatal(err)
	}
	if _, h := q.Contention(); h <= 0 {
		t.Fatalf("a listing recorded no hold: %s", h)
	}
}

func keysN(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%06d", i)
	}
	return keys
}

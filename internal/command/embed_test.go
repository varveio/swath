// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath/internal/inventory"
)

// TestSuperviseOffersNoEnd: the watchdog offers a progress callback the
// snapshots of a run that goes on, but not a done one: the run's end
// reaches the callback once the command has ended (deliver), never from a
// tick between inventory.Run's return and the report. The stall warning,
// due at half the stall timeout, is the barrier: ticks have offered by then.
func TestSuperviseOffersNoEnd(t *testing.T) {
	for _, ended := range []bool{false, true} {
		pr := &inventory.Progress{}
		if ended {
			pr.End(nil)
		}
		var mu sync.Mutex
		var states []string
		e := &embedding{ctx: context.Background(), progress: func(s inventory.ProgressSnapshot) {
			mu.Lock()
			defer mu.Unlock()
			states = append(states, s.State)
		}}
		e.start()
		warned := make(chan struct{})
		var once sync.Once
		stop := supervise(context.Background(), func(error) {}, pr, "", time.Second, nil,
			func(string, string) { once.Do(func() { close(warned) }) }, nil, e)
		select {
		case <-warned:
		case <-time.After(30 * time.Second): // a guard
			t.Fatal("no stall warning: the watchdog did not tick")
		}
		stop()
		e.stopped(pr)
		<-e.fed
		mu.Lock()
		got := states
		mu.Unlock()
		switch {
		case !ended && len(got) == 0:
			t.Error("a run that goes on: no snapshot offered")
		case ended && len(got) != 0:
			t.Errorf("a done run: offered %v", got)
		}
	}
}

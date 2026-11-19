// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build swathtest

package command

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/varveio/swath/internal/faults"
	"github.com/varveio/swath/internal/order"
)

// testHooks installs the test hooks the environment asks for, in a build
// with the swathtest tag only (the tests that run the real process build
// one; the release binary reads none of them): on the
// crash points (package faults), in this order, SWATH_TEST_SERIAL_SEALS
// (serialSeals), then SWATH_TEST_KILL_AT (the process SIGKILLs itself) and
// SWATH_TEST_HANG_AT (the goroutine writes hangMarker and blocks forever,
// as on stuck I/O), each
// "point:n": the n-th time the process reaches crash point point, and
// SWATH_TEST_SLOW_AT (slowAt); and on
// the order stage's events (order.SetDurableEvents), serialSeals',
// SWATH_TEST_LAST_RANGE_FILE (lastRangeFile) and
// SWATH_TEST_DRAIN_AFTER_LISTING (drainAfterListing). SWATH_TEST_GRACE, a
// duration, replaces shutdownGrace: a forced-exit test holds the run at a
// hang point and lets a short grace fire instead of waiting out 15 s.
func testHooks() {
	if d, err := time.ParseDuration(os.Getenv("SWATH_TEST_GRACE")); err == nil {
		shutdownGrace = d
	}
	var hooks, events []func(string)
	if os.Getenv("SWATH_TEST_SERIAL_SEALS") != "" {
		h, e := serialSeals()
		hooks, events = append(hooks, h), append(events, e)
	}
	if path := os.Getenv("SWATH_TEST_LAST_RANGE_FILE"); path != "" {
		events = append(events, lastRangeFile(path))
	}
	if os.Getenv("SWATH_TEST_DRAIN_AFTER_LISTING") != "" {
		events = append(events, drainAfterListing())
	}
	if len(events) > 0 {
		order.SetDurableEvents(func(e string) {
			for _, f := range events {
				f(e)
			}
		})
	}
	killAt := os.Getenv("SWATH_TEST_KILL_AT")
	killed := func() {
		fmt.Fprintln(os.Stderr, killMarker(os.Getenv("SWATH_TEST_KILL_ID"), killAt)) // the harness tells this kill from its own
		kill()
	}
	for _, h := range []func(string){atNth(killAt, killed), atNth(os.Getenv("SWATH_TEST_HANG_AT"), func() {
		fmt.Fprintln(os.Stderr, hangMarker(os.Getenv("SWATH_TEST_HANG_AT")))
		select {}
	}), slowAt(os.Getenv("SWATH_TEST_SLOW_AT"))} {
		if h != nil {
			hooks = append(hooks, h)
		}
	}
	if len(hooks) == 0 {
		return
	}
	faults.Set(func(p string) error {
		for _, h := range hooks {
			h(p)
		}
		return nil
	})
}

// atNth is a hook running act the n-th time the process reaches point,
// for spec "point:n" (nil for another spec).
func atNth(spec string, act func()) func(string) {
	point, n, ok := strings.Cut(spec, ":")
	if !ok {
		return nil
	}
	nth, err := strconv.ParseInt(n, 10, 64)
	if err != nil {
		return nil
	}
	var seen atomic.Int64
	return func(p string) {
		if p == point && seen.Add(1) == nth {
			act()
		}
	}
}

// slowAt is SWATH_TEST_SLOW_AT, "point:duration": each time the process
// reaches crash point point, the goroutine sleeps that long (slow work that
// is still work: this package's TestSegmentsReplayDurableFiles slows each
// certificate's scan past the stall timeout). Nil for another spec.
func slowAt(spec string) func(string) {
	point, d, ok := strings.Cut(spec, ":")
	if !ok {
		return nil
	}
	dur, err := time.ParseDuration(d)
	if err != nil {
		return nil
	}
	return func(p string) {
		if p == point {
			time.Sleep(dur)
		}
	}
}

// serialSeals is SWATH_TEST_SERIAL_SEALS, the real-process ledger barrier
// (this package's SIGKILL matrix): each durable seal waits at its crash point
// until the flusher has taken every earlier seal's record in a cycle of
// its own (order's "cycle-records" events), so seal n's record is the
// n-th ledger append and the matrix's fourth-cycle arms are reached by
// construction, not by how the seals fell in the flusher's 100 ms
// windows. A wait over 10 s gives up, and the arm it served then fails as
// unreached (the same gate as listing's in-process sealGate). It returns
// the crash-point hook and the order-event hook it counts cycles with.
func serialSeals() (hook, events func(string)) {
	var cycles atomic.Int64
	events = func(e string) {
		if e == "cycle-records" {
			cycles.Add(1)
		}
	}
	var mu sync.Mutex
	var seals int64 // seals past their crash point (mu)
	hook = func(p string) {
		if p != "spill-seal" {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		for deadline := time.Now().Add(10 * time.Second); cycles.Load() < seals && time.Now().Before(deadline); {
			time.Sleep(time.Millisecond)
		}
		seals++
	}
	return hook, events
}

// lastRangeFile is SWATH_TEST_LAST_RANGE_FILE: the file is created once a
// range is done with one left (order's "one-range-open") and a durable
// file has been sealed ("sealed"), in whichever order they come. A test
// that holds one range's listing from outside the process waits for the
// file, a signal from the run's own state, instead of for the fake's
// requests to go quiet (this package's TestSegmentsReplayDurableFiles).
func lastRangeFile(path string) func(string) {
	var alone, sealed, made atomic.Bool
	return func(e string) {
		switch e {
		case "one-range-open":
			alone.Store(true)
		case "sealed":
			sealed.Store(true)
		default:
			return
		}
		if alone.Load() && sealed.Load() && made.CompareAndSwap(false, true) {
			if f, err := os.Create(path); err == nil {
				f.Close()
			}
		}
	}
}

// drainAfterListing is SWATH_TEST_DRAIN_AFTER_LISTING: the drainer starts
// ("drain-start", on its goroutine) only once every range is listed
// ("listing-done"), so a resume's reused certificates are left to the
// segments, none replayed by the drainer while the gaps are listed
// (this package's TestSegmentsReplayDurableFiles), except those it reaches
// before the router can state its cut rule (a resume whose interrupted run
// published no part: until its own part 0 has closed by size). A wait over
// 30 s gives up.
func drainAfterListing() func(string) {
	listed := make(chan struct{})
	var once sync.Once
	return func(e string) {
		switch e {
		case "listing-done":
			once.Do(func() { close(listed) })
		case "drain-start":
			select {
			case <-listed:
			case <-time.After(30 * time.Second):
			}
		}
	}
}

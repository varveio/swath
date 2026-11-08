// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/dataset/datasettest"
)

// TestRequestRateFlag: --request-rate reaches the client: a GCS run at 20
// requests per second takes at least (attempts-1)/20, and its report
// carries the cap's counters with the feature on and its waits counted
// (at a lower rate if the run was too slow to need one).
func TestRequestRateFlag(t *testing.T) {
	datasettest.RequireJournal(t)
	s := newRowlessService(t)
	// The cap holds at every rate (the attempts take at least their
	// spacing) and engages (waits) at a rate the run can outpace: a run
	// slow enough (the race detector on a loaded runner) never to need a
	// wait at 20/s proves the cap without engaging it, so a lower rate is
	// tried then, never a weaker assertion.
	engaged := false
	for _, rate := range []float64{20, 5, 2} {
		dir := filepath.Join(t.TempDir(), "ds")
		report := filepath.Join(t.TempDir(), "report.json")
		start := time.Now()
		if err := run(context.Background(), s.args(dir, "--request-rate", fmt.Sprint(rate), "--report", report), io.Discard, false); err != nil {
			t.Fatal(err)
		}
		el := time.Since(start)
		var rep struct {
			Source struct {
				Stats struct {
					Counters struct {
						Features map[string]bool `json:"features"`
						Values   map[string]any  `json:"values"` // numbers, and flags (the seed's) as booleans
					} `json:"counters"`
				} `json:"stats"`
			} `json:"source"`
		}
		readJSON(t, report, &rep)
		c := rep.Source.Stats.Counters
		attempts, _ := c.Values["attempts"].(float64)
		n := int64(attempts)
		if n < 10 || el < time.Duration(float64(n-1)*float64(time.Second)/rate) || !c.Features["request_rate"] {
			t.Fatalf("%d attempts in %s at %v/s; features %v", n, el, rate, c.Features)
		}
		if w, _ := c.Values["rate.waits"].(float64); w > 0 {
			engaged = true
			break
		}
		t.Logf("%d attempts in %s at %v/s without a wait: slower than the cap", n, el, rate)
	}
	if !engaged {
		t.Fatal("the rate cap never engaged (rate.waits 0 at every rate)")
	}
	// B4: a rate whose spacing exceeds half the stall timeout is refused
	// (before, the run started and the watchdog stopped it as stuck while
	// an attempt waited for its token); without a stall timeout it is not.
	var u usageError
	if err := run(context.Background(), s.args(filepath.Join(t.TempDir(), "z"), "--request-rate", "0.2", "--stall-timeout", "3s"), io.Discard, false); !errors.As(err, &u) {
		t.Fatalf("a rate slower than the stall timeout: %v, want a usage error", err)
	}
	if err := rateFitsStall(newFlagsWith(t, "--request-rate", "0.001", "--stall-timeout", "0"), 0); err != nil {
		t.Fatalf("no stall timeout: %v", err)
	}
	if err := rateFitsStall(newFlagsWith(t, "--request-rate", "0.001"), 10*time.Minute); err == nil {
		t.Fatal("the minimum rate was accepted with the default stall timeout")
	}
	if err := run(context.Background(), s.args(filepath.Join(t.TempDir(), "x"), "--request-rate", "-1"), io.Discard, false); err == nil {
		t.Fatal("a negative rate was accepted")
	}
	// A positive rate too small for a time.Duration interval (it wrapped
	// to no cap at all) is refused.
	if err := run(context.Background(), s.args(filepath.Join(t.TempDir(), "y"), "--request-rate", "1e-12"), io.Discard, false); err == nil {
		t.Fatal("a rate below the minimum was accepted")
	}
}

// newFlagsWith is the list flag set parsed from args.
func newFlagsWith(t *testing.T, args ...string) *flag.FlagSet {
	t.Helper()
	var o options
	f := newFlags(&o, false)
	if err := f.Parse(args); err != nil {
		t.Fatal(err)
	}
	return f
}

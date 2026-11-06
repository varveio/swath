// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"path/filepath"
	"testing"
	"time"
)

// TestRequestRateFlag: --request-rate reaches the client: a GCS run at 20
// requests per second takes at least (attempts-1)/20, and its report
// carries the cap's counters with the feature on.
func TestRequestRateFlag(t *testing.T) {
	s := newRowlessService(t)
	dir := filepath.Join(t.TempDir(), "ds")
	report := filepath.Join(t.TempDir(), "report.json")
	start := time.Now()
	if err := run(context.Background(), s.args(dir, "--request-rate", "20", "--report", report), io.Discard, false); err != nil {
		t.Fatal(err)
	}
	el := time.Since(start)
	var rep struct {
		Source struct {
			Stats struct {
				Counters struct {
					Features map[string]bool  `json:"features"`
					Values   map[string]int64 `json:"values"`
				} `json:"counters"`
			} `json:"stats"`
		} `json:"source"`
	}
	readJSON(t, report, &rep)
	c := rep.Source.Stats.Counters
	n := c.Values["attempts"]
	if n < 10 || el < time.Duration(n-1)*time.Second/20 || !c.Features["request_rate"] || c.Values["rate.waits"] == 0 {
		t.Fatalf("%d attempts in %s at 20/s; features %v, rate.waits %d", n, el, c.Features, c.Values["rate.waits"])
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

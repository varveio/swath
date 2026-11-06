// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
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
	if err := run(context.Background(), s.args(filepath.Join(t.TempDir(), "x"), "--request-rate", "-1"), io.Discard, false); err == nil {
		t.Fatal("a negative rate was accepted")
	}
}

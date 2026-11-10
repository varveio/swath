// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"testing"
	"time"
)

// TestParseDuration: Go's durations with d and w besides, and every
// duration flag reads them.
func TestParseDuration(t *testing.T) {
	const day = 24 * time.Hour
	for in, want := range map[string]time.Duration{"90s": 90 * time.Second, "1h30m": 90 * time.Minute, "2d": 2 * day, "1w": 7 * day, "1d12h": 36 * time.Hour,
		"1.5d": 36 * time.Hour, "1w2d3h4m5s": 9*day + 3*time.Hour + 4*time.Minute + 5*time.Second, "0": 0, "0d": 0, "100ms1d": day + 100*time.Millisecond} {
		if got, err := ParseDuration(in); err != nil || got != want {
			t.Errorf("%s: %v %v", in, got, err)
		}
	}
	for _, bad := range []string{"", "d", "2", "2dd", "-1d", "1d-1h", "2x", "1.d.5", "99999999w", "200000d1h"} {
		if got, err := ParseDuration(bad); err == nil {
			t.Errorf("%q accepted: %v", bad, got)
		}
	}
	var d time.Duration
	if err := Duration(&d).Set("2d"); err != nil || d != 2*day {
		t.Errorf("--max-duration 2d: %v %v", d, err)
	}
	if err := Duration(&d).Set("-1h"); err == nil {
		t.Error("a negative duration accepted")
	}
}

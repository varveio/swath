// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"testing"
	"time"
)

// TestDurationFlag: a duration flag reads source.ParseDuration's syntax
// (days and weeks besides Go's units) and refuses a negative duration.
func TestDurationFlag(t *testing.T) {
	var d time.Duration
	if err := Duration(&d).Set("2d"); err != nil || d != 48*time.Hour {
		t.Errorf("--max-duration 2d: %v %v", d, err)
	}
	if err := Duration(&d).Set("-1h"); err == nil {
		t.Error("a negative duration accepted")
	}
}

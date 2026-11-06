// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestNegativeDurationsRefused (R9): every duration flag refuses a negative
// value with exit 2 before the source is opened; 0 still disables
// --max-duration and --stall-timeout.
func TestNegativeDurationsRefused(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{src, "--count-only", "--max-duration=-1s"},
		{src, "--count-only", "--stall-timeout=-1s"},
		{src, "--count-only", "--progress-interval=-1s"},
		{src, "--count-only", "--part-interval=-1s"},
		{"s3://bucket", "--count-only", "--request-timeout=-1s"},
		{"gs://bucket", "--count-only", "--gcs-request-timeout=-1s"},
	} {
		var buf bytes.Buffer
		err := run(context.Background(), args, &buf, false)
		if exitOf(err) != exitUsage || !strings.Contains(err.Error(), "negative") {
			t.Errorf("%v: exit %d (%v), want %d", args, exitOf(err), err, exitUsage)
		}
	}
	var buf bytes.Buffer
	if err := run(context.Background(), []string{src, "--count-only", "--max-duration=0", "--stall-timeout=0", "--no-stats"}, &buf, false); err != nil {
		t.Fatalf("zero durations: %v", err)
	}
}

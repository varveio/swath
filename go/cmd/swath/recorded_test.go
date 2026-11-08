// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

// TestResumeRefusesRemovedFlag: a journal whose recorded command names a
// flag this build no longer has (an earlier build's: --probe,
// --seed-descent, --memory before --row-budget) is refused on resume
// as a usage error, and the interrupted run is left resumable by the build
// that started it: before the first release no build promises to resume
// another's runs.
func TestResumeRefusesRemovedFlag(t *testing.T) {
	captureStderr(t)
	url := slowBucket(t)
	ctx := context.Background()
	for _, removed := range []string{"--probe", "--seed-descent=false", "--memory=1MiB"} {
		t.Run(removed, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "ds")
			args := []string{"s3://bkt", "--endpoint", url, "--anonymous", "--workers", "2", "--part-size", "4KiB", "-o", dir}
			// The command an earlier build recorded: the same run with a
			// flag since removed.
			recorded := append([]string{"diag", "list", removed}, args...)
			err := runWith(ctx, command{diag: true}, append(args, "--max-duration", "200ms"), io.Discard, recorded, false, nil)
			if c := exitCode(t, err); c != exitDeadline {
				t.Fatalf("first attempt: exit %d", c)
			}
			err = resume(ctx, []string{dir}, io.Discard)
			var u usageError
			if !errors.As(err, &u) || !strings.Contains(err.Error(), strings.TrimLeft(strings.SplitN(removed, "=", 2)[0], "-")) {
				t.Fatalf("resume: %v, want a usage refusal naming %s", err, removed)
			}
			if dataset.Committed(dir) || !dataset.Resumable(dir) {
				t.Fatal("the refused resume changed the interrupted run")
			}
		})
	}
}

// slowBucket is an S3 fake of 3,000 keys in 7 folders whose every answer
// takes 50 ms, so a 200 ms deadline interrupts a run with parts published.
func slowBucket(t *testing.T) string {
	return s3test.Serve(t, s3test.Delayed(s3test.Handler(s3test.Backend(t, s3test.Keys(3000, 7))), 50*time.Millisecond))
}

// TestResumeTakesDiagnosticDiskFlags: a swath list run resumes with the
// diagnostic disk overrides (--spill-reserve, --spill-compress), and the
// report states the reserve in effect.
func TestResumeTakesDiagnosticDiskFlags(t *testing.T) {
	captureStderr(t)
	url := slowBucket(t)
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "ds")
	args := []string{"s3://bkt", "--endpoint", url, "--anonymous", "--workers", "2", "--part-size", "4KiB", "-o", dir}
	if c := exitCode(t, run(ctx, append(args, "--max-duration", "200ms"), io.Discard, false)); c != exitDeadline {
		t.Fatalf("first attempt: exit %d", c)
	}
	var out bytes.Buffer
	if err := resume(ctx, []string{dir, "--spill-reserve", "3MiB", "--spill-compress", "--max-duration", "0", "--report-format", "json"}, &out); err != nil {
		t.Fatalf("resume: %v", err)
	}
	var r struct {
		Resources struct {
			Spill *struct {
				ReserveBytes int64 `json:"reserve_bytes"`
			}
		}
	}
	if err := json.Unmarshal(out.Bytes(), &r); err != nil || r.Resources.Spill == nil || r.Resources.Spill.ReserveBytes != 3<<20 {
		t.Fatalf("report: %v %s", err, out.String())
	}
	if !dataset.Committed(dir) {
		t.Fatal("not committed")
	}
}

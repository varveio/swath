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
	"github.com/varveio/swath/go/internal/dataset/datasettest"
	"github.com/varveio/swath/go/internal/listing"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

// TestResumeRefusesRemovedFlag: a journal whose recorded command names a
// flag this build no longer has (an earlier build's: --probe,
// --seed-descent, --memory before --sort-buffer, the prefixed GCS and Azure
// store flags before the shared --endpoint-url and --request-timeout) is
// refused on resume as a usage error naming it, and the interrupted run is left resumable by the build
// that started it: before the first release no build promises to resume
// another's runs.
func TestResumeRefusesRemovedFlag(t *testing.T) {
	datasettest.RequireJournal(t)
	captureStderr(t)
	url := slowBucket(t)
	ctx := context.Background()
	for _, removed := range []string{"--probe", "--seed-descent=false", "--memory=1MiB", "--gcs-endpoint=http://127.0.0.1:1/storage/v1/",
		"--azblob-request-timeout=5s", "--endpoint=http://127.0.0.1:1", "--limiter=false", "--report-format=json"} {
		t.Run(removed, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "ds")
			args := []string{"s3://bkt", "--endpoint-url", url, "--anonymous", "--workers", "2", "--part-size", "4KiB", "-o", dir}
			// The command an earlier build recorded (canonical: the flags,
			// then SOURCE): the same run with a flag since removed.
			recorded := []string{"diag", "list", "--endpoint-url=" + url, removed, "--anonymous", "--workers=2", "--part-size=4KiB", "--output=" + dir, "s3://bkt"}
			err := runWith(ctx, command{diag: true}, append(args, "--max-duration", "200ms"), io.Discard, recorded, false, nil)
			if c := exitCode(t, err); c != exitDeadline {
				t.Fatalf("first attempt: exit %d", c)
			}
			err = resume(ctx, []string{dir}, io.Discard)
			var u usageError
			if !errors.As(err, &u) || !strings.Contains(err.Error(), strings.SplitN(removed, "=", 2)[0]+", which this build does not take") {
				t.Fatalf("resume: %v, want a usage refusal naming %s", err, removed)
			}
			if dataset.Committed(dir) || !dataset.Resumable(dir) {
				t.Fatal("the refused resume changed the interrupted run")
			}
		})
	}
}

// TestResumeRenamedFlag: a journal that recorded a renamed flag
// (--row-budget, now --sort-buffer) resumes with the recorded value under
// the new name, and a --sort-buffer given to swath resume wins over it.
func TestResumeRenamedFlag(t *testing.T) {
	datasettest.RequireJournal(t)
	captureStderr(t)
	url := slowBucket(t)
	ctx := context.Background()
	for name, tc := range map[string]struct {
		override []string
		want     int64
	}{
		"recorded":   {nil, 3 << 20},
		"overridden": {[]string{"--sort-buffer", "5MiB"}, 5 << 20},
	} {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "ds")
			args := []string{"s3://bkt", "--endpoint-url", url, "--anonymous", "--workers", "2", "--part-size", "4KiB", "--sort-buffer", "3MiB", "-o", dir}
			// The command a build before the rename recorded.
			recorded := []string{"diag", "list", "--endpoint-url=" + url, "--anonymous", "--workers=2", "--part-size=4KiB", "--row-budget=3MiB", "--output=" + dir, "s3://bkt"}
			err := runWith(ctx, command{diag: true}, append(args, "--max-duration", "200ms"), io.Discard, recorded, false, nil)
			if c := exitCode(t, err); c != exitDeadline {
				t.Fatalf("first attempt: exit %d", c)
			}
			var out bytes.Buffer
			if err := resume(ctx, append([]string{dir, "--max-duration", "0", "--report", "-"}, tc.override...), &out); err != nil {
				t.Fatalf("resume: %v", err)
			}
			var r struct {
				Invocation struct {
					SortBufferBytes int64 `json:"sort_buffer_bytes"`
					SortBuffer      struct {
						Reason string `json:"reason"`
					} `json:"sort_buffer"`
				}
			}
			if err := json.Unmarshal(out.Bytes(), &r); err != nil || r.Invocation.SortBufferBytes != tc.want || r.Invocation.SortBuffer.Reason != listing.BudgetExplicit {
				t.Fatalf("report: %v %+v, want %d bytes, explicit", err, r.Invocation, tc.want)
			}
			if !dataset.Committed(dir) {
				t.Fatal("not committed")
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
	datasettest.RequireJournal(t)
	captureStderr(t)
	url := slowBucket(t)
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "ds")
	args := []string{"s3://bkt", "--endpoint-url", url, "--anonymous", "--workers", "2", "--part-size", "4KiB", "-o", dir}
	if c := exitCode(t, run(ctx, append(args, "--max-duration", "200ms"), io.Discard, false)); c != exitDeadline {
		t.Fatalf("first attempt: exit %d", c)
	}
	var out bytes.Buffer
	if err := resume(ctx, []string{dir, "--spill-reserve", "3MiB", "--spill-compress", "--max-duration", "0", "--report", "-"}, &out); err != nil {
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

// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/varveio/swath/go/internal/dataset"
)

// TestResumeRefusesRemovedFlag: a journal whose recorded command names a
// flag this build no longer has (an earlier build's: --probe,
// --seed-descent, --memory before --row-budget) is refused on resume
// as a usage error, and the interrupted run is left resumable by the build
// that started it: before the first release no build promises to resume
// another's runs.
func TestResumeRefusesRemovedFlag(t *testing.T) {
	captureStderr(t)
	b := s3mem.New()
	b.CreateBucket("bkt")
	for i := range 3000 {
		k := fmt.Sprintf("p%02d/obj-%06d", i%7, i)
		b.PutObject("bkt", k, nil, bytes.NewReader([]byte(k)), int64(len(k)), nil)
	}
	h := gofakes3.New(b).Server()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	ctx := context.Background()
	for _, removed := range []string{"--probe", "--seed-descent=false", "--memory=1MiB"} {
		t.Run(removed, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "ds")
			args := []string{"s3://bkt", "--endpoint", srv.URL, "--anonymous", "--workers", "2", "--part-size", "4KiB", "-o", dir}
			// The command an earlier build recorded: the same run with a
			// flag since removed.
			recorded := append([]string{"diag", "list", removed}, args...)
			err := runWith(ctx, append(args, "--max-duration", "200ms"), io.Discard, true, recorded, false, nil)
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

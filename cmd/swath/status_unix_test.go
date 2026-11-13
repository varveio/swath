// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"regexp"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/varveio/swath/internal/source/gcs/gcstest"
)

var statusOnce sync.Once

// TestStatusSignal (G4): SIGUSR1 to a running list command prints one
// progress record on stderr, without -v or --progress, in the log's
// format: a plain record in text, a progress event in json.
func TestStatusSignal(t *testing.T) {
	statusOnce.Do(statusOnSignal)
	hung := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	t.Cleanup(hung.Close)
	for _, c := range []struct{ format, want string }{{"text", `(?m)^  \w+ · `}, {"json", `(?m)^\{.*"event":"progress"`}} {
		want := regexp.MustCompile(c.want)
		stderr := captureSyncStderr(t)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- run(ctx, append([]string{"gs://bkt", "--endpoint-url", hung.URL + gcstest.Path, "--anonymous", "--stall-timeout", "3s",
				"--log-format", c.format, "-o", filepath.Join(t.TempDir(), "ds")}, unjournaled...), io.Discard, len(unjournaled) > 0)
		}()
		deadline := time.Now().Add(10 * time.Second)
		for statusNow.Load() == nil && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if err := syscall.Kill(syscall.Getpid(), syscall.SIGUSR1); err != nil {
			t.Fatal(err)
		}
		for !want.MatchString(stderr.String()) && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		got := stderr.String()
		cancel()
		<-done
		if !want.MatchString(got) {
			t.Fatalf("%s: no progress record after SIGUSR1: %q", c.format, got)
		}
	}
}

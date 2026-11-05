// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

// TestExitCodes: every outcome has its exit code and, for a run that did
// not complete in an output this tool owns, an error.json with the class,
// the cause and whether swath resume can continue it.
func TestExitCodes(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary")
	}
	bin := filepath.Join(t.TempDir(), "swath")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	b := s3mem.New()
	if err := b.CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}
	for i := range 3000 {
		k := fmt.Sprintf("p%02d/obj-%06d", i%11, i)
		if _, err := b.PutObject("bkt", k, nil, bytes.NewReader([]byte(k)), int64(len(k)), nil); err != nil {
			t.Fatal(err)
		}
	}
	h := gofakes3.New(b).Server()
	var hang atomic.Bool // after the first request, hold every request (no progress)
	var served atomic.Int64
	var slow atomic.Bool // 300 ms per request: a run long enough to signal
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hang.Load() && served.Add(1) > 1 {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(30 * time.Second):
			}
		}
		if slow.Load() {
			time.Sleep(300 * time.Millisecond)
		}
		time.Sleep(20 * time.Millisecond)
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	s3args := func(bucket, out string, extra ...string) []string {
		return append([]string{"list", "s3://" + bucket, "--endpoint", srv.URL, "--path-style", "--anonymous", "--workers", "4",
			"--part-size", "4KiB", "-o", out}, extra...)
	}
	code := func(err error) int {
		if err == nil {
			return 0
		}
		if ee, ok := err.(*exec.ExitError); ok {
			return ee.ExitCode()
		}
		t.Fatal(err)
		return -1
	}
	outcome := func(dir string) outcome {
		var o outcome
		b, err := os.ReadFile(filepath.Join(dir, errorName))
		if err != nil {
			t.Fatalf("%s: %v", dir, err)
		}
		if err := json.Unmarshal(b, &o); err != nil {
			t.Fatal(err)
		}
		return o
	}
	src := t.TempDir()
	os.WriteFile(filepath.Join(src, "a"), nil, 0o644)

	// 0: completed; no error.json.
	ok := filepath.Join(t.TempDir(), "ok")
	if c := code(exec.Command(bin, "list", src, "-o", ok).Run()); c != exitCompleted {
		t.Fatalf("completed: exit %d", c)
	}
	if _, err := os.Stat(filepath.Join(ok, errorName)); !os.IsNotExist(err) {
		t.Fatal("error.json after a completed run")
	}
	// 2: usage.
	if c := code(exec.Command(bin, "list").Run()); c != exitUsage {
		t.Fatalf("usage: exit %d", c)
	}
	// 1: fatal, with the provider's cause.
	fatal := filepath.Join(t.TempDir(), "fatal")
	if c := code(exec.Command(bin, s3args("nobucket", fatal)...).Run()); c != exitFatal {
		t.Fatalf("fatal: exit %d", c)
	}
	if o := outcome(fatal); o.Class != "fatal" || o.Resumable || o.Cause != "no_such_bucket" {
		t.Fatalf("fatal: %+v", o)
	}
	// 124: the deadline, resumable.
	dl := filepath.Join(t.TempDir(), "deadline")
	if c := code(exec.Command(bin, s3args("bkt", dl, "--max-duration", "50ms")...).Run()); c != exitDeadline {
		t.Fatalf("deadline: exit %d", c)
	}
	if o := outcome(dl); o.Class != "deadline" || !o.Resumable {
		t.Fatalf("deadline: %+v", o)
	}
	if out, err := exec.Command(bin, "resume", dl, "--max-duration", "0").CombinedOutput(); err != nil {
		t.Fatalf("resume after the deadline: %v\n%s", err, out)
	}
	if _, err := os.Stat(filepath.Join(dl, errorName)); !os.IsNotExist(err) {
		t.Fatal("error.json left after the resume completed")
	}
	// 143 and 130: signals, graceful, resumable.
	slow.Store(true)
	for sig, want := range map[syscall.Signal]int{syscall.SIGTERM: exitTerminated, syscall.SIGINT: exitInterrupted} {
		dir := filepath.Join(t.TempDir(), "sig")
		cmd := exec.Command(bin, s3args("bkt", dir)...)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(700 * time.Millisecond)
		cmd.Process.Signal(sig)
		if c := code(cmd.Wait()); c != want {
			t.Fatalf("%v: exit %d, want %d", sig, c, want)
		}
		if o := outcome(dir); o.ExitCode != want || !o.Resumable {
			t.Fatalf("%v: %+v", sig, o)
		}
	}
	// 75: stuck, when nothing progresses for the stall timeout.
	slow.Store(false)
	hang.Store(true)
	stuck := filepath.Join(t.TempDir(), "stuck")
	if c := code(exec.Command(bin, s3args("bkt", stuck, "--stall-timeout", "1s")...).Run()); c != exitStuck {
		t.Fatalf("stuck: exit %d", c)
	}
	if o := outcome(stuck); o.Class != "stuck" || !o.Resumable {
		t.Fatalf("stuck: %+v", o)
	}
	if b, err := os.ReadFile(filepath.Join(stuck, progressName)); err != nil || !bytes.Contains(b, []byte(`"advance"`)) {
		t.Fatalf("progress file: %v %s", err, b)
	}
}

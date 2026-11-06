// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/journal"
	"github.com/varveio/swath/go/internal/listing"
	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
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
	// 1: a committed dataset with partial coverage (a key limit) never exits 0.
	part := filepath.Join(t.TempDir(), "partial")
	pargs := append([]string{"diag"}, s3args("bkt", part, "--sequential", "--max-keys", "10")...)
	if c := code(exec.Command(bin, pargs...).Run()); c != exitFatal {
		t.Fatalf("partial: exit %d", c)
	}
	if o := outcome(part); o.Class != "incomplete" || o.Resumable {
		t.Fatalf("partial: %+v", o)
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

// TestGCSOutageThenResume: a GCS bucket that starts answering 503 to
// every request three quarters through a run stops the run as stuck (75,
// resumable: retry exhaustion requeues, it never fails the run); once the
// outage ends, swath resume completes it with the dataset of an
// uninterrupted run byte for byte.
func TestGCSOutageThenResume(t *testing.T) {
	h := gcstest.Handler(gcstest.Keys(20000), 20*time.Millisecond)
	var served, failed, after atomic.Int64 // after: the outage begins after this many requests (0: never)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n := served.Add(1); after.Load() > 0 && n > after.Load() {
			failed.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(503)
			fmt.Fprint(w, `{"error":{"code":503,"message":"Backend Error"}}`)
			return
		}
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	args := func(out string) []string {
		return []string{"gs://bkt", "--gcs-endpoint", srv.URL + gcstest.Path, "--gcs-anonymous", "--workers", "4",
			"--part-size", "4KiB", "--memory", "1MiB", "--stall-timeout", "1s", "-o", out}
	}
	ctx := context.Background()
	ref := filepath.Join(t.TempDir(), "ref")
	if err := run(ctx, args(ref), io.Discard, false); err != nil {
		t.Fatal(err)
	}
	total := served.Load()
	// The outage begins three quarters through the requests a run makes.
	served.Store(0)
	after.Store(total * 3 / 4)
	out := filepath.Join(t.TempDir(), "ds")
	err := run(ctx, args(out), io.Discard, false)
	var ee *exitError
	if !errors.As(err, &ee) || ee.code != exitStuck {
		t.Fatalf("outage: %v, want exit %d", err, exitStuck)
	}
	var o outcome
	b, _ := os.ReadFile(filepath.Join(out, errorName))
	if json.Unmarshal(b, &o) != nil || o.Class != "stuck" || !o.Resumable {
		t.Fatalf("outage outcome: %s", b)
	}
	if _, err := os.Stat(filepath.Join(out, "manifest.json")); err == nil {
		t.Skip("the run finished before the outage")
	}
	if failed.Load() == 0 {
		t.Fatal("no request met the outage")
	}
	retained, _ := filepath.Glob(filepath.Join(out, "data", "*.parquet"))
	if len(retained) == 0 {
		t.Fatal("no part was published before the outage")
	}
	after.Store(0)
	if err := resume(ctx, []string{out}, io.Discard); err != nil {
		t.Fatalf("resume after the outage: %v", err)
	}
	sameParts(t, "after the outage", ref, out)
	if _, err := os.Stat(filepath.Join(out, errorName)); !os.IsNotExist(err) {
		t.Fatal("error.json left after the resume completed")
	}
	t.Logf("a run makes %d requests; %d met the outage; %d parts retained", total, failed.Load(), len(retained))
}

// sameParts requires out's data/ to hold ref's parts byte for byte.
func sameParts(t *testing.T, at, ref, out string) {
	t.Helper()
	parts, _ := filepath.Glob(filepath.Join(ref, "data", "*.parquet"))
	got, _ := filepath.Glob(filepath.Join(out, "data", "*.parquet"))
	if len(parts) == 0 || len(got) != len(parts) {
		t.Fatalf("%s: %d parts, want %d", at, len(got), len(parts))
	}
	for _, p := range parts {
		a, _ := os.ReadFile(p)
		g, _ := os.ReadFile(filepath.Join(out, "data", filepath.Base(p)))
		if !bytes.Equal(a, g) {
			t.Fatalf("%s: %s differs", at, filepath.Base(p))
		}
	}
}

// TestQueuedHealthyWorkIsNotStuck: requests the service answers keep the
// stall clock alive although no row moves; requests that fail or never
// return do not. With the limiter at one permit (four workers, three
// throttle answers first) the root range's first pages come back empty
// with a continuation for longer than twice the stall timeout before any
// row: the run completes (before, it stopped as stuck with 75 while the
// service answered every request). A source that hangs, or answers only
// 503, still stops as stuck (Sol, fifth review, Part 2).
func TestQueuedHealthyWorkIsNotStuck(t *testing.T) {
	const stall = time.Second
	h := gcstest.Handler(gcstest.Keys(2000), 0)
	var mode atomic.Value // "healthy", "hung" or "503"
	var served atomic.Int64
	var firstEmpty, lastEmpty atomic.Int64 // unix nanoseconds
	const empties = 50
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := served.Add(1)
		m := mode.Load().(string)
		if m == "hung" {
			<-r.Context().Done()
			return
		}
		if m == "503" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(503)
			fmt.Fprint(w, `{"error":{"code":503,"message":"Backend Error"}}`)
			return
		}
		if n <= 3 { // three throttles: the limit falls 4 -> 2 -> 1 -> 1
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(429)
			fmt.Fprint(w, `{"error":{"code":429,"message":"rate limit exceeded"}}`)
			return
		}
		q := r.URL.Query()
		tok, k := q.Get("pageToken"), 0
		if rest, ok := strings.CutPrefix(tok, "empty-"); ok {
			k, _ = strconv.Atoi(rest)
		} else if tok == "" && firstEmpty.Load() == 0 {
			firstEmpty.Store(time.Now().UnixNano())
		} else {
			h.ServeHTTP(w, r)
			return
		}
		if k > 0 {
			time.Sleep(50 * time.Millisecond)
		}
		if k < empties {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"kind":"storage#objects","nextPageToken":"empty-%d"}`, k+1)
			return
		}
		lastEmpty.Store(time.Now().UnixNano())
		q.Del("pageToken") // the chain's real first page
		r.URL.RawQuery = q.Encode()
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	ctx := context.Background()
	for _, m := range []string{"healthy", "hung", "503"} {
		mode.Store(m)
		served.Store(0)
		dir := filepath.Join(t.TempDir(), "ds")
		var out bytes.Buffer
		err := run(ctx, []string{"gs://bkt", "--gcs-endpoint", srv.URL + gcstest.Path, "--gcs-anonymous", "--limiter", "--workers", "4",
			"--stall-timeout", stall.String(), "--report-format", "json", "-o", dir}, &out, true)
		if m == "healthy" {
			if err != nil {
				t.Fatalf("healthy work at one permit: %v", err)
			}
			if !strings.Contains(out.String(), `"limiter.decreases": 2`) {
				t.Fatalf("the limit did not fall to one: %s", out.String())
			}
			if d := time.Duration(lastEmpty.Load() - firstEmpty.Load()); d < 2*stall {
				t.Fatalf("the rowless stretch lasted %s, not over twice the stall timeout", d)
			}
			continue
		}
		var ee *exitError
		if !errors.As(err, &ee) || ee.code != exitStuck {
			t.Fatalf("%s source: %v, want exit %d", m, err, exitStuck)
		}
	}
}

// TestGCSCappedRunIsIncomplete: a GCS run capped by --max-keys commits a
// dataset whose coverage says incomplete, exits 1 with class incomplete
// (not resumable), keeps no journal, and swath resume refuses it as
// complete and leaves it as it is (S3: TestExitCodes, partial; Sol P2-8).
func TestGCSCappedRunIsIncomplete(t *testing.T) {
	captureStderr(t)
	ep := gcstest.Bucket(t, gcstest.Keys(3000), 0)
	dir := filepath.Join(t.TempDir(), "ds")
	ctx := context.Background()
	err := run(ctx, []string{"gs://bkt", "--gcs-endpoint", ep, "--gcs-anonymous", "--sequential", "--max-keys", "10",
		"--part-size", "4KiB", "-o", dir}, io.Discard, true)
	if c := exitCode(t, err); c != exitFatal {
		t.Fatalf("capped run: exit %d (%v)", c, err)
	}
	var o outcome
	if b, err := os.ReadFile(filepath.Join(dir, errorName)); err != nil || json.Unmarshal(b, &o) != nil || o.Class != "incomplete" || o.Resumable {
		t.Fatalf("capped run: error.json %v %+v", err, o)
	}
	var m dataset.Manifest
	if b, err := os.ReadFile(filepath.Join(dir, dataset.ManifestName)); err != nil || json.Unmarshal(b, &m) != nil {
		t.Fatalf("capped run: no manifest: %v", err)
	}
	var rows int64
	for _, p := range m.Parts {
		rows += p.Rows
	}
	if m.Coverage.Complete || rows != 10 {
		t.Fatalf("capped run: coverage complete %v, %d rows", m.Coverage.Complete, rows)
	}
	if _, err := os.Stat(journal.Path(dir)); !os.IsNotExist(err) {
		t.Fatalf("capped run kept a journal: %v", err)
	}
	want := dirTree(t, dir)
	if err := resume(ctx, []string{dir}, io.Discard); !errors.Is(err, listing.ErrComplete) {
		t.Fatalf("resume of a capped run: %v", err)
	}
	if got := dirTree(t, dir); !maps.Equal(want, got) {
		t.Fatalf("resume changed the capped dataset:\nbefore %v\nafter  %v", want, got)
	}
}

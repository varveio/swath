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

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/dataset/datasettest"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/journal"
	"github.com/varveio/swath/go/internal/listing"
	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

// TestExitCodes: every outcome has its exit code and, for a run that did
// not complete in an output this tool owns, an error.json with the class,
// the cause and whether swath resume can continue it.
func TestExitCodes(t *testing.T) {
	datasettest.RequireJournal(t)
	if testing.Short() {
		t.Skip("builds the binary")
	}
	bin := filepath.Join(t.TempDir(), "swath")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	h := s3test.Handler(s3test.Backend(t, s3test.Keys(3000, 11)))
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
		return append([]string{"list", "s3://" + bucket, "--endpoint-url", srv.URL, "--s3-path-style", "--anonymous", "--workers", "4",
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
	// 1: fatal; an unlabeled service refusal retains its status and code.
	fatal := filepath.Join(t.TempDir(), "fatal")
	if c := code(exec.Command(bin, s3args("nobucket", fatal)...).Run()); c != exitFatal {
		t.Fatalf("fatal: exit %d", c)
	}
	if o := outcome(fatal); o.Class != "fatal" || o.Resumable || o.Cause != "" || !strings.Contains(o.Message, "404") || !strings.Contains(o.Message, "NoSuchBucket") {
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
	if b, err := os.ReadFile(filepath.Join(stuck, progressName)); err != nil || !bytes.Contains(b, []byte(`"advance"`)) || !bytes.Contains(b, []byte(`"mutex_max_hold_ms"`)) {
		t.Fatalf("progress file: %v %s", err, b)
	}
}

// TestGCSOutageThenResume: a GCS bucket that starts answering 503 to
// every request three quarters through a run stops the run as stuck (75,
// resumable: retry exhaustion requeues, it never fails the run); once the
// outage ends, swath resume completes it with the dataset of an
// uninterrupted run byte for byte.
func TestGCSOutageThenResume(t *testing.T) {
	datasettest.RequireJournal(t)
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
		return []string{"gs://bkt", "--endpoint-url", srv.URL + gcstest.Path, "--anonymous", "--workers", "4",
			"--part-size", "4KiB", "--sort-buffer", "512KiB", "--stall-timeout", "1s", "-o", out}
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

// TestDiskFullExits74: a full disk on a part, a journal record or the
// manifest exits 74 (class disk_full, cause no_space, resumable), in the
// exit code, error.json and the report, so a runner can retry with a
// larger workspace on the code alone; swath resume then completes the
// dataset of an uninterrupted run (before: exit 1, class fatal).
func TestDiskFullExits74(t *testing.T) {
	datasettest.RequireJournal(t)
	srv := httptest.NewServer(gcstest.Handler(gcstest.Keys(6000), 0))
	t.Cleanup(srv.Close)
	args := func(out string, extra ...string) []string {
		return append([]string{"gs://bkt", "--endpoint-url", srv.URL + gcstest.Path, "--anonymous", "--workers", "4",
			"--part-size", "4KiB", "--sort-buffer", "512KiB", "--part-interval", "0", "-o", out}, extra...)
	}
	ctx := context.Background()
	ref := filepath.Join(t.TempDir(), "ref")
	if err := run(ctx, args(ref), io.Discard, true); err != nil {
		t.Fatal(err)
	}
	for target, match := range map[string]func(string) bool{
		"part":     func(n string) bool { return strings.HasSuffix(n, ".parquet") },
		"journal":  func(n string) bool { return filepath.Base(n) == "journal" },
		"manifest": func(n string) bool { return dataset.TempOf(dataset.ManifestName, filepath.Base(n)) },
	} {
		out := filepath.Join(t.TempDir(), "ds")
		report := filepath.Join(t.TempDir(), "report.json")
		nth := map[string]int64{"part": 4, "journal": 3, "manifest": 1}[target]
		var n atomic.Int64
		restore := faults.SetIO(func(op, file string, b []byte) (int, error) {
			if op == "write" && match(file) && n.Add(1) == nth {
				return len(b) / 2, syscall.ENOSPC
			}
			return 0, nil
		})
		err := run(ctx, args(out, "--report", report), io.Discard, true)
		restore()
		if c := exitCode(t, err); c != exitDiskFull {
			t.Fatalf("%s: exit %d (%v), want %d", target, c, err, exitDiskFull)
		}
		var o outcome
		readJSON(t, filepath.Join(out, errorName), &o)
		if o.Class != "disk_full" || o.ExitCode != exitDiskFull || o.Cause != "no_space" || !o.Resumable {
			t.Fatalf("%s: error.json %+v", target, o)
		}
		var rep listing.Report
		readJSON(t, report, &rep)
		if rep.Outcome.Class != "disk_full" || rep.Outcome.ExitCode != exitDiskFull || rep.Outcome.Cause != "no_space" || !rep.Outcome.Resumable {
			t.Fatalf("%s: report outcome %+v", target, rep.Outcome)
		}
		if err := resume(ctx, []string{out}, io.Discard); err != nil {
			t.Fatalf("%s: resume: %v", target, err)
		}
		sameParts(t, target, ref, out)
	}
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

// stuckEvidence is what a run stopped as stuck left in dir, for a failure
// message: the progress at the stop and the stacks of the goroutines inside
// the source. The dump goes with the test's temporary directory otherwise,
// and a stall that fires once on a loaded runner leaves nothing to read.
func stuckEvidence(dir string) string {
	b, err := os.ReadFile(filepath.Join(dir, dataset.Workspace, "stacks-stuck.txt"))
	if err != nil {
		return fmt.Sprintf("no goroutine dump: %v", err)
	}
	blocks := strings.Split(string(b), "\n\n")
	kept := blocks[:1] // the header: the cause, the time and the progress snapshot
	for _, g := range blocks[1:] {
		if strings.Contains(g, "/internal/source/") {
			kept = append(kept, g)
		}
	}
	return strings.Join(kept, "\n\n")
}

// TestQueuedHealthyWorkIsNotStuck: requests the service answers keep the
// stall clock alive although no row moves; requests that fail or never
// return do not. With the limiter at one permit (one worker; the windowed
// limiter no longer falls to one on three throttle answers) the root range's first pages come back empty
// with a continuation for longer than twice the stall timeout before any
// row (--no-seed: the seed would stop following them at its budgets,
// TestSeedChargesEmptyPages): the run completes (before, it stopped as stuck with 75 while the
// service answered every request). A source that hangs, or answers only
// 503, still stops as stuck (durability review).
func TestQueuedHealthyWorkIsNotStuck(t *testing.T) {
	const stall = time.Second
	h := gcstest.Handler(gcstest.Keys(2000), 0)
	var mode atomic.Value // "healthy", "hung" or "503"
	var served atomic.Int64
	var firstEmpty, lastEmpty atomic.Int64 // unix nanoseconds
	const empties = 50
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served.Add(1)
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
		start := time.Now()
		err := run(ctx, append([]string{"gs://bkt", "--endpoint-url", srv.URL + gcstest.Path, "--anonymous", "--adaptive-limit", "--workers", "1", "--no-seed",
			"--stall-timeout", stall.String(), "--report", "-", "-o", dir}, unjournaled...), &out, true)
		if m == "healthy" {
			if err != nil {
				t.Fatalf("healthy work at one permit: %v (after %s, %d requests served)\n%s\n%s",
					err, time.Since(start).Round(time.Millisecond), served.Load(), out.String(), stuckEvidence(dir))
			}
			if !strings.Contains(out.String(), `"limiter.limit": 1`) {
				t.Fatalf("the limit was not one: %s", out.String())
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
// complete and leaves it as it is (S3: TestExitCodes, partial; durability P2-8).
func TestGCSCappedRunIsIncomplete(t *testing.T) {
	captureStderr(t)
	ep := gcstest.Bucket(t, gcstest.Keys(3000), 0)
	dir := filepath.Join(t.TempDir(), "ds")
	ctx := context.Background()
	err := run(ctx, []string{"gs://bkt", "--endpoint-url", ep, "--anonymous", "--sequential", "--max-keys", "10",
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

// TestProtocolViolationIsUnresumable: a GCS service that answers, three
// quarters through a run, with pages whose first two keys are swapped (out
// of order: the engine cannot recover) fails the run with exit 1, cause
// protocol, not resumable, after parts were published; the journal holds
// the terminal unresumable mark (Java's markRunUnresumable), and swath
// resume refuses with the same cause, the parts and journal unchanged, as
// does a resumed run and swath show names the mark. Before, nothing in the
// journal marked the run and the resume continued.
func TestProtocolViolationIsUnresumable(t *testing.T) {
	datasettest.RequireJournal(t)
	captureStderr(t)
	h := gcstest.Handler(gcstest.Keys(20000), 0)
	var served, after, swapped atomic.Int64 // after: invalid pages come after this many requests (0: never)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n := served.Add(1); after.Load() == 0 || n <= after.Load() {
			h.ServeHTTP(w, r)
			return
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		var page map[string]any
		if json.Unmarshal(rec.Body.Bytes(), &page) == nil {
			if items, _ := page["items"].([]any); len(items) >= 2 {
				items[0], items[1] = items[1], items[0]
				swapped.Add(1)
			}
		}
		b, _ := json.Marshal(page)
		w.Header().Set("Content-Type", "application/json")
		w.Write(b)
	}))
	t.Cleanup(srv.Close)
	args := func(out string, extra ...string) []string {
		return append([]string{"gs://bkt", "--endpoint-url", srv.URL + gcstest.Path, "--anonymous", "--workers", "4",
			"--part-size", "4KiB", "--sort-buffer", "512KiB", "--part-interval", "0", "-o", out}, extra...)
	}
	ctx := context.Background()
	// The number of requests a run makes, to place the invalid pages.
	if err := run(ctx, args(filepath.Join(t.TempDir(), "count")), io.Discard, true); err != nil {
		t.Fatal(err)
	}
	total := served.Load()
	served.Store(0)
	after.Store(total * 3 / 4)
	out := filepath.Join(t.TempDir(), "ds")
	report := filepath.Join(t.TempDir(), "report.json")
	err := run(ctx, args(out, "--report", report), io.Discard, true)
	if swapped.Load() == 0 {
		t.Fatal("no invalid page was served")
	}
	if c := exitCode(t, err); c != exitFatal {
		t.Fatalf("invalid page: exit %d (%v)", c, err)
	}
	check := func(how string, class string, code int, cause string, resumable bool) {
		t.Helper()
		if class != "fatal" || code != exitFatal || cause != "protocol" || resumable {
			t.Fatalf("%s: class %s exit %d cause %q resumable %v", how, class, code, cause, resumable)
		}
	}
	var o outcome
	readJSON(t, filepath.Join(out, errorName), &o)
	check("error.json", o.Class, o.ExitCode, o.Cause, o.Resumable)
	var rep listing.Report
	readJSON(t, report, &rep)
	check("report", rep.Outcome.Class, rep.Outcome.ExitCode, rep.Outcome.Cause, rep.Outcome.Resumable)
	if retained, _ := filepath.Glob(filepath.Join(out, "data", "*.parquet")); len(retained) == 0 {
		t.Fatal("no part was published before the invalid page")
	}
	st, err := journal.Read(out)
	if err != nil || st.Unresumable == nil || st.Unresumable.Cause != "protocol" || !strings.Contains(st.Unresumable.Reason, "out of order") {
		t.Fatalf("journal: %+v %v", st, err)
	}
	// The service is healthy again: the resume still refuses.
	after.Store(0)
	tree := func() map[string]string {
		m := dirTree(t, out)
		delete(m, errorName) // the refusal records its own outcome
		return m
	}
	before := tree()
	report = filepath.Join(t.TempDir(), "report.json")
	err = resume(ctx, []string{"--report", report, out}, io.Discard)
	var ue *journal.UnresumableError
	if c := exitCode(t, err); c != exitFatal || !errors.As(err, &ue) {
		t.Fatalf("resume: exit %d (%v)", c, err)
	}
	readJSON(t, filepath.Join(out, errorName), &o)
	check("resume error.json", o.Class, o.ExitCode, o.Cause, o.Resumable)
	rep = listing.Report{}
	readJSON(t, report, &rep)
	check("resume report", rep.Outcome.Class, rep.Outcome.ExitCode, rep.Outcome.Cause, rep.Outcome.Resumable)
	// A resumed run (the race the command's early check cannot close)
	// refuses under the lock.
	if err := runWith(ctx, command{diag: true}, args(out), io.Discard, nil, true, nil); !errors.As(err, &ue) {
		t.Fatalf("resumed run: %v", err)
	}
	if got := tree(); !maps.Equal(before, got) {
		t.Fatalf("the refused resumes changed the output:\nbefore %v\nafter  %v", before, got)
	}
	var shown bytes.Buffer
	if err := show([]string{out}, &shown); err != nil || !strings.Contains(shown.String(), "marked unresumable (protocol)") {
		t.Fatalf("show: %v %q", err, shown.String())
	}
}

// TestDirectoryBucketRefused: an S3 directory bucket (Express One Zone,
// a name ending in --x-s3) is refused before any request, as Java: exit 2,
// class refused, a message naming it, and the output left uncreated.
// Before, the run went to the service and failed there.
func TestDirectoryBucketRefused(t *testing.T) {
	captureStderr(t)
	var requests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "", 500)
	}))
	t.Cleanup(srv.Close)
	out := filepath.Join(t.TempDir(), "ds")
	report := filepath.Join(t.TempDir(), "report.json")
	err := run(context.Background(), []string{"s3://orders--use1-az4--x-s3/prefix", "--endpoint-url", srv.URL, "--s3-path-style", "--anonymous",
		"-o", out, "--report", report}, io.Discard, false)
	if c := exitCode(t, err); c != exitUsage || !strings.Contains(err.Error(), "directory bucket") {
		t.Fatalf("exit %d: %v", c, err)
	}
	var rep listing.Report
	readJSON(t, report, &rep)
	if rep.Outcome.Class != "refused" || rep.Outcome.ExitCode != exitUsage || rep.Outcome.Resumable {
		t.Fatalf("report outcome %+v", rep.Outcome)
	}
	if n := requests.Load(); n != 0 {
		t.Fatalf("%d requests to the service", n)
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatalf("the output exists: %v", err)
	}
}

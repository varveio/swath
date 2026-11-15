// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/dataset/datasettest"
	"github.com/varveio/swath/internal/faults"
	"github.com/varveio/swath/internal/listing"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/gcs/gcstest"
	"github.com/varveio/swath/internal/source/s3/s3test"
)

// closedPort is a local address nothing listens on: connections to it
// are refused.
func closedPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	host := ln.Addr().String()
	ln.Close()
	return host
}

// shortGrace shortens the clients' fail-fast grace for one test.
func shortGrace(t *testing.T, d time.Duration) {
	g := source.UnreachableGrace
	source.UnreachableGrace = d
	t.Cleanup(func() { source.UnreachableGrace = g })
}

// unreachableReport is the part of the report these tests read.
type unreachableReport struct {
	Outcome     listing.Outcome `json:"outcome"`
	Diagnostics struct {
		Source struct {
			Stats struct {
				Counters struct {
					Values map[string]any `json:"values"`
				} `json:"counters"`
			} `json:"stats"`
		} `json:"source"`
	} `json:"diagnostics"`
}

func (r *unreachableReport) value(name string) int64 {
	f, _ := r.Diagnostics.Source.Stats.Counters.Values[name].(float64)
	return int64(f)
}

// TestUnreachableEndpoint: an endpoint override that refuses every
// connection, nothing in the run ever answering, stops the run as stuck
// (exit 75, cause unreachable) once the grace has passed, long before the
// stall timeout, for S3, GCS and Azure Blob alike: the message names the
// endpoint and the cause, the report counts the fail-fast, and a text run
// is not resumable.
func TestUnreachableEndpoint(t *testing.T) {
	stderr := captureSyncStderr(t)
	s3test.NoCredentials(t)
	t.Setenv("STORAGE_EMULATOR_HOST", "")
	shortGrace(t, time.Second)
	host := closedPort(t)
	ep := "http://" + host
	for _, c := range []struct {
		name string
		args []string
	}{
		{"s3", []string{"s3://bkt/p/", "--endpoint-url", ep, "--s3-path-style", "--s3-region", "us-east-1"}},
		{"gcs", []string{"gs://bkt/p/", "--endpoint-url", ep + gcstest.Path}},
		{"azure", []string{"az://acct/c/p/", "--endpoint-url", ep, "--azure-assume-flat"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			report := filepath.Join(t.TempDir(), "report.json")
			args := append(c.args, "--anonymous", "--workers", "2", "--stall-timeout", "2m", "--report", report)
			t0 := time.Now()
			err := run(context.Background(), args, io.Discard, false)
			took := time.Since(t0)
			if code := exitCode(t, err); code != exitStuck {
				t.Fatalf("exit %d after %s (%v)", code, took, err)
			}
			// Generous: Windows takes about 2 s to refuse a local connection.
			if took > 30*time.Second {
				t.Fatalf("failed after %s, not within the grace", took)
			}
			if !strings.Contains(err.Error(), "endpoint "+ep+" cannot be reached") || !strings.Contains(err.Error(), "check the endpoint address") {
				t.Fatalf("the error does not name the endpoint: %v", err)
			}
			var rep unreachableReport
			readJSON(t, report, &rep)
			o := rep.Outcome
			if o.Class != listing.ClassStuck || o.ExitCode != exitStuck || o.Cause != source.CauseUnreachable || o.Resumable || strings.Contains(o.Error, "every request was failing") {
				t.Fatalf("outcome %+v", o)
			}
			if rep.value("unreachable.stops") < 1 || rep.value("unreachable.attempts") < 2 {
				t.Fatalf("the fail-fast is not in the report: %v", rep.Diagnostics.Source.Stats.Counters.Values)
			}
		})
	}
	// The persistent-failure warning, when it came first, said how long.
	if strings.Contains(stderr.String(), "(retrying;") && !strings.Contains(stderr.String(), "nothing has answered: the run stops as unreachable 1s after the first attempt)") {
		t.Fatalf("the warning does not say how long retrying lasts: %s", stderr.String())
	}
}

// TestUnreachableGraceRecovers: an endpoint that starts answering inside
// the grace (an emulator still starting) is listed as if it had been up.
func TestUnreachableGraceRecovers(t *testing.T) {
	captureSyncStderr(t)
	s3test.NoCredentials(t)
	shortGrace(t, 20*time.Second)
	host := closedPort(t)
	srv := &http.Server{Handler: s3test.Handler(s3test.Backend(t, s3test.Keys(5, 1)))}
	t.Cleanup(func() { srv.Close() })
	started := make(chan error, 1)
	time.AfterFunc(time.Second, func() {
		ln, err := net.Listen("tcp", host)
		started <- err
		if err == nil {
			go srv.Serve(ln)
		}
	})
	var out strings.Builder
	err := run(context.Background(), []string{"s3://bkt/", "--endpoint-url", "http://" + host, "--s3-path-style", "--s3-region", "us-east-1", "--anonymous",
		"--columns", "key", "--format", "tsv"}, &out, false)
	if lerr := <-started; lerr != nil {
		t.Skipf("the port was taken meanwhile: %v", lerr)
	}
	if err != nil {
		t.Fatalf("a server that started inside the grace: %v", err)
	}
	if n := strings.Count(out.String(), "\n"); n != 6 { // the header and five keys
		t.Fatalf("%d lines: %q", n, out.String())
	}
}

// TestUnreachableAfterAnswer: an endpoint that answered once and then
// refuses is an outage, not a wrong address: the run keeps retrying until
// the stall timeout stops it as stuck (75), whatever the grace.
func TestUnreachableAfterAnswer(t *testing.T) {
	captureSyncStderr(t)
	s3test.NoCredentials(t)
	shortGrace(t, 100*time.Millisecond)
	h := s3test.Handler(s3test.Backend(t, s3test.Keys(5, 1)))
	var n atomic.Int64
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) > 1 {
			panic(http.ErrAbortHandler) // a connection still open: cut
		}
		w.Header().Set("Connection", "close")
		h.ServeHTTP(w, r)
		srv.Listener.Close() // every later connection is refused
	}))
	t.Cleanup(srv.Close)
	report := filepath.Join(t.TempDir(), "report.json")
	err := run(context.Background(), []string{"s3://bkt/", "--endpoint-url", srv.URL, "--s3-path-style", "--s3-region", "us-east-1", "--anonymous",
		"--sequential", "--page-size", "1", "--stall-timeout", "2s", "--report", report}, io.Discard, true)
	if code := exitCode(t, err); code != exitStuck {
		t.Fatalf("exit %d (%v)", code, err)
	}
	var rep unreachableReport
	readJSON(t, report, &rep)
	if rep.value("unreachable.stops") != 0 || rep.Outcome.Class != listing.ClassStuck || rep.Outcome.Cause == source.CauseUnreachable {
		t.Fatalf("outcome %+v, counters %v", rep.Outcome, rep.Diagnostics.Source.Stats.Counters.Values)
	}
}

// TestUnreachableLeavesNoRun: a dataset run against an endpoint that never
// answers (a mistyped --endpoint-url, a service not up) stops as
// unreachable (exit 75) and removes its journal and work files: nothing was
// published, the outcome is not resumable and the report says why it is in
// no error.json, and the corrected command runs on the same -o. Before, the
// directory held an interrupted run that refused the corrected command,
// and a resume, which refuses another endpoint, could not fix it (C-S2). A
// resume of a run with parts keeps its journal when the endpoint stops
// answering: it is still resumable.
func TestUnreachableLeavesNoRun(t *testing.T) {
	datasettest.RequireJournal(t)
	captureSyncStderr(t)
	s3test.NoCredentials(t)
	shortGrace(t, 500*time.Millisecond)
	host := closedPort(t)
	dir := filepath.Join(t.TempDir(), "ds")
	report := filepath.Join(t.TempDir(), "report.json")
	args := func(ep string, extra ...string) []string {
		return append([]string{"s3://bkt/", "--endpoint-url", ep, "--s3-path-style", "--s3-region", "us-east-1", "--anonymous", "-o", dir}, extra...)
	}
	err := run(context.Background(), args("http://"+host, "--report", report), io.Discard, false)
	if code := exitCode(t, err); code != exitStuck {
		t.Fatalf("exit %d (%v)", code, err)
	}
	var rep unreachableReport
	readJSON(t, report, &rep)
	if o := rep.Outcome; o.Class != listing.ClassStuck || o.Cause != source.CauseUnreachable || o.Resumable || o.Unrecorded != listing.UnrecordedDiscarded {
		t.Fatalf("outcome %+v", o)
	}
	if ents := dirNames(t, dir); len(ents) != 1 || ents[0] != dataset.Workspace || dataset.Resumable(dir) {
		t.Fatalf("the unanswered run left %v", ents)
	}
	if ws := dirNames(t, filepath.Join(dir, dataset.Workspace)); len(ws) != 1 || ws[0] != dataset.LockName {
		t.Fatalf("the unanswered run left %v in the workspace", ws)
	}
	// The corrected command runs on the same output.
	srv := httptest.NewServer(s3test.Handler(s3test.Backend(t, s3test.Keys(3000, 11))))
	if err := run(context.Background(), args(srv.URL, "--part-size", "4KiB", "--part-interval", "0"), io.Discard, false); err != nil {
		t.Fatalf("the corrected command: %v", err)
	}
	// A run with parts: stopped after them, then resumed while the
	// endpoint refuses connections. Its journal stays: resumable.
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	var n atomic.Int64
	restore := faults.SetIO(func(op, file string, b []byte) (int, error) {
		if op == "write" && filepath.Base(file) == "journal" && n.Add(1) == 3 { // the header and a record before it
			return len(b) / 2, syscall.ENOSPC
		}
		return 0, nil
	})
	err = run(context.Background(), args(srv.URL, "--part-size", "4KiB", "--part-interval", "0"), io.Discard, false)
	restore()
	if code := exitCode(t, err); code != exitDiskFull {
		t.Fatalf("exit %d (%v)", code, err)
	}
	srv.Close()
	err = dispatch(context.Background(), []string{"resume", dir, "--report", report}, io.Discard)
	if code := exitCode(t, err); code != exitStuck {
		t.Fatalf("resume: exit %d (%v)", code, err)
	}
	rep = unreachableReport{}
	readJSON(t, report, &rep)
	if o := rep.Outcome; o.Cause != source.CauseUnreachable || !o.Resumable || !dataset.Resumable(dir) {
		t.Fatalf("resume with parts: outcome %+v, resumable dir %v", o, dataset.Resumable(dir))
	}
}

// dirNames is dir's entries' names.
func dirNames(t *testing.T, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(ents))
	for i, e := range ents {
		names[i] = e.Name()
	}
	return names
}

// TestOverwriteKeepsThePreviousDataset: --overwrite of a dataset with a
// command that cannot run (an endpoint nothing answers) leaves the previous
// dataset as it was: swath show and swath du --from-dataset read it, and
// the failed attempt leaves nothing to continue. One stopped by its
// deadline leaves it too, beside its own run, which swath show names and
// swath resume finishes and installs. Before, --overwrite removed the
// previous dataset first, and either failure left the directory without
// one (C-S1).
func TestOverwriteKeepsThePreviousDataset(t *testing.T) {
	datasettest.RequireJournal(t)
	captureSyncStderr(t)
	s3test.NoCredentials(t)
	shortGrace(t, 500*time.Millisecond)
	tree := t.TempDir()
	mk := func(name string) {
		if err := os.WriteFile(filepath.Join(tree, name), []byte("12"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	mk("a")
	mk("b")
	dir := filepath.Join(t.TempDir(), "ds")
	if err := run(context.Background(), []string{tree, "-o", dir}, io.Discard, false); err != nil {
		t.Fatal(err)
	}
	var was strings.Builder
	if err := show([]string{dir}, &was); err != nil {
		t.Fatal(err)
	}
	s3args := func(ep string, extra ...string) []string {
		return append([]string{"s3://bkt/", "--endpoint-url", ep, "--s3-path-style", "--s3-region", "us-east-1", "--anonymous", "-o", dir, "--overwrite"}, extra...)
	}
	err := run(context.Background(), s3args("http://"+closedPort(t)), io.Discard, false)
	if code := exitCode(t, err); code != exitStuck {
		t.Fatalf("exit %d (%v)", code, err)
	}
	var now strings.Builder
	if err := show([]string{dir}, &now); err != nil || now.String() != was.String() {
		t.Fatalf("show after the failed overwrite: %v\n%s\nwas\n%s", err, now.String(), was.String())
	}
	if got, err := duRun(t, "--from-dataset", dir, "--format", "tsv"); err != nil || !strings.Contains(got, "\ntotal\t0\t2\t") {
		t.Fatalf("du --from-dataset after the failed overwrite: %v\n%q", err, got)
	}
	if dataset.Staged(dir) {
		t.Fatal("the unanswered overwrite left a run")
	}
	// Stopped by its deadline: the previous dataset stands beside the
	// replacement's run, which a resume finishes and installs.
	var slow atomic.Bool
	slow.Store(true)
	h := s3test.Handler(s3test.Backend(t, s3test.Keys(3000, 11)))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if slow.Load() {
			time.Sleep(400 * time.Millisecond)
		}
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	err = run(context.Background(), s3args(srv.URL, "--workers", "1", "--part-size", "4KiB", "--part-interval", "0", "--max-duration", "1s"), io.Discard, false)
	if code := exitCode(t, err); code != exitDeadline {
		t.Fatalf("exit %d (%v)", code, err)
	}
	now.Reset()
	if err := show([]string{dir}, &now); err != nil || !strings.HasPrefix(now.String(), was.String()) || !strings.Contains(now.String(), "swath resume "+dir+" continues it") {
		t.Fatalf("show after the stopped overwrite: %v\n%s", err, now.String())
	}
	slow.Store(false)
	report := filepath.Join(t.TempDir(), "report.json")
	if err := dispatch(context.Background(), []string{"resume", dir, "--report", report}, io.Discard); err != nil {
		t.Fatalf("resume: %v", err)
	}
	var rep listing.Report
	readJSON(t, report, &rep)
	now.Reset()
	if err := show([]string{dir}, &now); err != nil || rep.Outcome.Class != listing.ClassOK || rep.Invocation.Out != dir ||
		!strings.Contains(now.String(), "source s3://bkt/") || !strings.Contains(now.String(), "3000 rows") || dataset.Staged(dir) {
		t.Fatalf("after the resume: %v %+v\n%s", err, rep.Outcome, now.String())
	}
}

// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/journal"
	"github.com/varveio/swath/go/internal/listing"
	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
)

// TestOutcomeIsCurrent: a stuck run leaves error.json with 75; a resume
// that then fails before it owns the output (broken Application Default
// Credentials), or before it can run at all (a damaged journal), replaces
// it with its own outcome under the output lock, so a supervisor never
// resumes on a stale 75 forever. An output another process owns keeps its
// outcome, and the invocation's goes to stderr; a failed write of the
// outcome is reported, on stderr and in the report (Sol, fourth review,
// minimum change 4).
func TestOutcomeIsCurrent(t *testing.T) {
	var hang atomic.Bool // every request waits: no progress
	hang.Store(true)
	gcs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hang.Load() {
			<-r.Context().Done()
			return
		}
		fmt.Fprint(w, `{"kind":"storage#objects","items":[{"name":"a","size":"1"}]}`)
	}))
	t.Cleanup(gcs.Close)
	good, broken := fakeADC(t)
	stderr := captureStderr(t)
	code := func(err error) int { return exitCode(t, err) }
	dir := filepath.Join(t.TempDir(), "ds")
	errorFile := filepath.Join(dir, errorName)
	read := func() outcome {
		t.Helper()
		var o outcome
		b, err := os.ReadFile(errorFile)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(b, &o); err != nil {
			t.Fatal(err)
		}
		return o
	}
	ctx := context.Background()
	err := run(ctx, []string{"gs://bkt", "--gcs-endpoint", gcs.URL + "/storage/v1/", "--stall-timeout", "1s", "-o", dir}, io.Discard, false)
	if c := code(err); c != exitStuck {
		t.Fatalf("stuck run: exit %d (%v)", c, err)
	}
	if o := read(); o.Class != "stuck" || !o.Resumable {
		t.Fatalf("stuck run: %+v", o)
	}
	stale, _ := os.ReadFile(errorFile)

	// Broken ADC: the source does not open; the stuck outcome is replaced.
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", broken)
	if c := code(resume(ctx, []string{dir}, io.Discard)); c != exitFatal {
		t.Fatalf("broken ADC: exit %d", c)
	}
	if o := read(); o.Class != "fatal" || o.Cause != "auth" {
		t.Fatalf("broken ADC left %+v", o)
	}

	// A damaged journal: the resume refuses before it runs.
	os.WriteFile(errorFile, stale, 0o644)
	jpath := journal.Path(dir)
	intact, _ := os.ReadFile(jpath)
	os.WriteFile(jpath, append(append([]byte(nil), intact...), 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0), 0o644)
	if c := code(resume(ctx, []string{dir}, io.Discard)); c != exitFatal {
		t.Fatalf("corrupt journal: exit %d", c)
	}
	if o := read(); o.Class != "fatal" || o.Resumable || !strings.Contains(o.Message, journal.ErrCorrupt.Error()) {
		t.Fatalf("corrupt journal left %+v", o)
	}
	os.WriteFile(jpath, intact, 0o644)

	// Another process owns the output: its outcome stays, ours is on stderr.
	os.WriteFile(errorFile, []byte("owner"), 0o644)
	lock, err := journal.TakeLock(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range []struct {
		name string
		adc  string
		want string
	}{{"broken ADC", broken, `"cause":"auth"`}, {"locked", good, "another process owns the output"}} {
		t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", step.adc)
		stderr.Reset()
		if resume(ctx, []string{dir}, io.Discard) == nil {
			t.Fatalf("%s: a resume ran over another owner", step.name)
		}
		if b, _ := os.ReadFile(errorFile); string(b) != "owner" {
			t.Fatalf("%s: the competing owner's error.json became %q", step.name, b)
		}
		if s := stderr.String(); !strings.Contains(s, "swath: outcome: {") || !strings.Contains(s, step.want) {
			t.Fatalf("%s: stderr %q", step.name, s)
		}
	}
	lock.Release()

	// The outcome cannot be written (error.json is a non-empty directory,
	// so the rename fails): reported on stderr and in the report.
	os.Remove(errorFile)
	os.MkdirAll(filepath.Join(errorFile, "x"), 0o755)
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", broken)
	stderr.Reset()
	var out bytes.Buffer
	if c := code(resume(ctx, []string{dir, "--report-format", "json"}, &out)); c != exitFatal {
		t.Fatalf("failed write: exit %d", c)
	}
	var rep listing.Report
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil || !strings.Contains(rep.Outcome.Unrecorded, "write "+errorName) {
		t.Fatalf("failed write: report %v %+v", err, rep.Outcome)
	}
	if s := stderr.String(); !strings.Contains(s, "is not recorded") || !strings.Contains(s, `"cause":"auth"`) {
		t.Fatalf("failed write: stderr %q", s)
	}
	if _, err := os.Stat(errorFile + ".tmp"); err == nil {
		t.Fatal("failed write left its temporary file")
	}
	// The same through a refusal before the run.
	os.WriteFile(jpath, append(append([]byte(nil), intact...), 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0), 0o644)
	stderr.Reset()
	if resume(ctx, []string{dir}, io.Discard) == nil || !strings.Contains(stderr.String(), "is not recorded") {
		t.Fatalf("failed write on a refusal: stderr %q", stderr.String())
	}
	os.WriteFile(jpath, intact, 0o644)

	// Credentials and the source are back: the resume completes and the
	// outcome file goes.
	os.RemoveAll(errorFile)
	os.WriteFile(errorFile, stale, 0o644)
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", good)
	hang.Store(false)
	if err := resume(ctx, []string{dir}, io.Discard); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if _, err := os.Stat(errorFile); !os.IsNotExist(err) {
		t.Fatal("error.json after the resume completed")
	}
}

// fakeADC points Application Default Credentials at a fake token server:
// good is an authorized-user file that works, broken one that does not
// parse (the source does not open). good is set.
func fakeADC(t *testing.T) (good, broken string) {
	t.Helper()
	tok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"access_token":"tok","token_type":"Bearer","expires_in":3600}`)
	}))
	t.Cleanup(tok.Close)
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("APPDATA", home)
	good, broken = filepath.Join(home, "adc.json"), filepath.Join(home, "broken.json")
	os.WriteFile(good, fmt.Appendf(nil, `{"type":"authorized_user","client_id":"c","client_secret":"s","refresh_token":"rt","token_uri":%q}`, tok.URL), 0o600)
	os.WriteFile(broken, []byte(`{"type":"authorized_user",`), 0o600)
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", good)
	return good, broken
}

// captureStderr collects what the command reports on stderr.
func captureStderr(t *testing.T) *bytes.Buffer {
	var b bytes.Buffer
	errOut = &b
	t.Cleanup(func() { errOut = os.Stderr })
	return &b
}

func exitCode(t *testing.T, err error) int {
	t.Helper()
	var ee *exitError
	if errors.As(err, &ee) {
		return ee.code
	}
	t.Fatalf("not an exit: %v", err)
	return 0
}

// dirTree is every entry under dir: directories, and files with their size
// and digest.
func dirTree(t *testing.T, dir string) map[string]string {
	t.Helper()
	m := map[string]string{}
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, p)
		if d.IsDir() {
			m[rel] = "dir"
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		m[rel] = fmt.Sprintf("%d/%x", len(b), sha256.Sum256(b))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// crashRun runs args in process and crashes it at the nth recorded part.
func crashRun(t *testing.T, args []string, nth int64) {
	t.Helper()
	var n atomic.Int64
	restore := faults.Set(func(p string) error {
		if p == "recorded" && n.Add(1) == nth {
			return faults.ErrCrash
		}
		return nil
	})
	defer restore()
	if err := run(context.Background(), args, io.Discard, false); !errors.Is(err, faults.ErrCrash) {
		t.Fatalf("crash not injected: %v", err)
	}
}

// TestOutcomeSettlementAfterConcurrentCommit: an invocation that fails
// before it owns the output (a resume refused on a damaged journal, or a
// resume whose source does not open) settles its outcome only if the run
// it saw is still there once it holds the lock. Paused just before the
// lock while another process completes the run, or replaces it with a new
// interrupted one, and then continued, it must leave the directory exactly
// as the other process left it; its outcome goes to stderr, and the report
// (--report) names this attempt with the reason in outcome.unrecorded (Sol,
// fifth review, M4; sixth review: the source that does not open gave no
// reason).
func TestOutcomeSettlementAfterConcurrentCommit(t *testing.T) {
	good, broken := fakeADC(t)
	stderr := captureStderr(t)
	ep := gcstest.Bucket(t, gcstest.Keys(3000), 0)
	args := func(dir string, extra ...string) []string {
		return append([]string{"gs://bkt", "--gcs-endpoint", ep, "--part-size", "4KiB", "--memory", "1MiB", "-o", dir}, extra...)
	}
	ctx := context.Background()
	for _, failing := range []string{"damaged journal", "source does not open"} {
		for _, other := range []string{"completes", "replaces"} {
			name := failing + ", another process " + other
			dir := filepath.Join(t.TempDir(), "ds")
			t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", good)
			crashRun(t, args(dir), 2)
			if failing == "damaged journal" {
				jb, _ := os.ReadFile(journal.Path(dir))
				os.WriteFile(journal.Path(dir), append(jb, 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0), 0o644)
			} else {
				t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", broken)
			}
			paused, cont := make(chan struct{}), make(chan struct{})
			restore := faults.Set(func(p string) error {
				if p == "settle-lock" {
					close(paused)
					<-cont
				}
				return nil
			})
			report := filepath.Join(t.TempDir(), "report.json")
			done := make(chan error, 1)
			go func() { done <- resume(ctx, []string{dir, "--report", report}, io.Discard) }()
			select {
			case <-paused:
			case err := <-done:
				restore()
				t.Fatalf("%s: the failing resume never reached the lock: %v", name, err)
			case <-time.After(time.Minute):
				t.Fatalf("%s: the failing resume never reached the lock", name)
			}
			restore()
			t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", good)
			if other == "completes" {
				if err := run(ctx, args(dir, "--overwrite"), io.Discard, false); err != nil {
					t.Fatalf("%s: %v", name, err)
				}
			} else {
				crashRun(t, args(dir, "--overwrite"), 1)
			}
			want := dirTree(t, dir)
			stderr.Reset()
			close(cont)
			if c := exitCode(t, <-done); c != exitFatal {
				t.Fatalf("%s: exit %d", name, c)
			}
			if got := dirTree(t, dir); !maps.Equal(want, got) {
				t.Fatalf("%s: the directory changed:\nbefore %v\nafter  %v", name, want, got)
			}
			if !strings.Contains(stderr.String(), "swath: outcome: {") || !strings.Contains(stderr.String(), listing.UnrecordedChanged) {
				t.Fatalf("%s: the outcome and its reason are not on stderr: %q", name, stderr.String())
			}
			// The report is this attempt's, and says why error.json does
			// not hold it; an error.json the other process left names
			// another attempt.
			var rep listing.Report
			if b, err := os.ReadFile(report); err != nil || json.Unmarshal(b, &rep) != nil {
				t.Fatalf("%s: report: %v %q", name, err, b)
			}
			if rep.Attempt == nil || rep.Attempt.ID == "" || rep.Outcome.ExitCode != exitFatal || rep.Outcome.Unrecorded != listing.UnrecordedChanged {
				t.Fatalf("%s: report %+v %+v", name, rep.Attempt, rep.Outcome)
			}
			if b, err := os.ReadFile(filepath.Join(dir, errorName)); err == nil {
				var o outcome
				if json.Unmarshal(b, &o) != nil || o.Attempt == nil || o.Attempt.ID == rep.Attempt.ID {
					t.Fatalf("%s: error.json %q names this attempt", name, b)
				}
			}
		}
	}
}

// TestNothingToResumeLeavesDirectoryUntouched: `swath resume` on a
// directory without a journal (an empty workspace, one holding only the
// lock, or none) refuses with ErrNothingToResume and changes nothing: no
// lock is created and error.json is not replaced. The same through a run
// asked to resume directly (the library path the command does not reach).
func TestNothingToResumeLeavesDirectoryUntouched(t *testing.T) {
	captureStderr(t)
	ep := gcstest.Bucket(t, gcstest.Keys(100), 0)
	ctx := context.Background()
	for _, layout := range []string{"empty workspace", "lock only", "no workspace"} {
		dir := filepath.Join(t.TempDir(), "ds")
		os.MkdirAll(dir, 0o755)
		os.WriteFile(filepath.Join(dir, errorName), []byte(`{"class":"stuck","exit_code":75,"resumable":true}`), 0o644)
		if layout != "no workspace" {
			os.MkdirAll(filepath.Join(dir, dataset.Workspace), 0o755)
		}
		if layout == "lock only" {
			os.WriteFile(filepath.Join(dir, dataset.Workspace, dataset.LockName), nil, 0o644)
		}
		want := dirTree(t, dir)
		if err := resume(ctx, []string{dir}, io.Discard); !errors.Is(err, listing.ErrNothingToResume) {
			t.Fatalf("%s: resume: %v", layout, err)
		}
		if got := dirTree(t, dir); !maps.Equal(want, got) {
			t.Fatalf("%s: resume changed the directory:\nbefore %v\nafter  %v", layout, want, got)
		}
		err := runWith(ctx, []string{"gs://bkt", "--gcs-endpoint", ep, "--gcs-anonymous", "--output", dir}, io.Discard, false, nil, true, nil)
		if !errors.Is(err, listing.ErrNothingToResume) {
			t.Fatalf("%s: resumed run: %v", layout, err)
		}
		if got := dirTree(t, dir); !maps.Equal(want, got) {
			t.Fatalf("%s: a resumed run changed the directory:\nbefore %v\nafter  %v", layout, want, got)
		}
	}
}

// TestAttemptIdentifiesTheOutcome: error.json and the report a supervisor
// names (--report) carry the same attempt. When a later attempt cannot
// write error.json (its temporary file cannot be created), the old
// error.json stays, with the same class and exit code as the new outcome,
// but the later attempt's report names another attempt, so a supervisor
// that compares them never takes the old outcome for the current one; the
// same for a refusal before the run (Sol, fifth review, M4).
func TestAttemptIdentifiesTheOutcome(t *testing.T) {
	gcs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done() // no progress: every attempt is stuck
	}))
	t.Cleanup(gcs.Close)
	captureStderr(t)
	dir := filepath.Join(t.TempDir(), "ds")
	reports := t.TempDir()
	errorFile := filepath.Join(dir, errorName)
	readOutcome := func() outcome {
		t.Helper()
		var o outcome
		b, err := os.ReadFile(errorFile)
		if err != nil || json.Unmarshal(b, &o) != nil || o.Attempt == nil || o.Attempt.ID == "" {
			t.Fatalf("error.json: %v %q", err, b)
		}
		return o
	}
	readReport := func(path string) listing.Report {
		t.Helper()
		var r listing.Report
		b, err := os.ReadFile(path)
		if err != nil || json.Unmarshal(b, &r) != nil || r.Attempt == nil || r.Attempt.ID == "" {
			t.Fatalf("report %s: %v %q", path, err, b)
		}
		return r
	}
	ctx := context.Background()
	r1 := filepath.Join(reports, "1.json")
	err := run(ctx, []string{"gs://bkt", "--gcs-endpoint", gcs.URL + "/storage/v1/", "--gcs-anonymous", "--stall-timeout", "1s", "-o", dir, "--report", r1}, io.Discard, false)
	if c := exitCode(t, err); c != exitStuck {
		t.Fatalf("first attempt: exit %d (%v)", c, err)
	}
	old, rep := readOutcome(), readReport(r1)
	if rep.Attempt.ID != old.Attempt.ID || !rep.Attempt.Started.Equal(old.Attempt.Started) || rep.Outcome.ExitCode != exitStuck || old.ExitCode != exitStuck {
		t.Fatalf("first attempt: error.json %+v and report %+v %+v disagree", old, rep.Attempt, rep.Outcome)
	}
	stale, _ := os.ReadFile(errorFile)

	os.MkdirAll(filepath.Join(errorFile+".tmp", "x"), 0o755)
	r2 := filepath.Join(reports, "2.json")
	if c := exitCode(t, resume(ctx, []string{dir, "--report", r2}, io.Discard)); c != exitStuck {
		t.Fatalf("second attempt: exit %d", c)
	}
	if b, _ := os.ReadFile(errorFile); !bytes.Equal(b, stale) {
		t.Fatalf("error.json was written: %q", b)
	}
	rep = readReport(r2)
	if rep.Attempt.ID == old.Attempt.ID || rep.Outcome.ExitCode != exitStuck || !strings.Contains(rep.Outcome.Unrecorded, "write "+errorName) {
		t.Fatalf("second attempt: report %+v %+v against the stale %+v", rep.Attempt, rep.Outcome, old.Attempt)
	}
	if rep.Attempt.Started.Before(old.Attempt.Started) {
		t.Fatalf("second attempt started %v, before the first %v", rep.Attempt.Started, old.Attempt.Started)
	}

	// A refusal before the run (a damaged journal), its write failing too.
	jb, _ := os.ReadFile(journal.Path(dir))
	os.WriteFile(journal.Path(dir), append(jb, 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0), 0o644)
	r3 := filepath.Join(reports, "3.json")
	if c := exitCode(t, resume(ctx, []string{dir, "--report", r3}, io.Discard)); c != exitFatal {
		t.Fatalf("refusal: exit %d", c)
	}
	if b, _ := os.ReadFile(errorFile); !bytes.Equal(b, stale) {
		t.Fatalf("refusal: error.json was written: %q", b)
	}
	rep = readReport(r3)
	if rep.Attempt.ID == old.Attempt.ID || rep.Outcome.ExitCode != exitFatal || rep.Outcome.Class != "fatal" || !strings.Contains(rep.Outcome.Unrecorded, "write "+errorName) {
		t.Fatalf("refusal: report %+v %+v", rep.Attempt, rep.Outcome)
	}
}

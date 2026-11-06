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
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/varveio/swath/go/internal/journal"
	"github.com/varveio/swath/go/internal/listing"
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
	tok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"access_token":"tok","token_type":"Bearer","expires_in":3600}`)
	}))
	t.Cleanup(tok.Close)
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
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("APPDATA", home)
	good, broken := filepath.Join(home, "adc.json"), filepath.Join(home, "broken.json")
	os.WriteFile(good, fmt.Appendf(nil, `{"type":"authorized_user","client_id":"c","client_secret":"s","refresh_token":"rt","token_uri":%q}`, tok.URL), 0o600)
	os.WriteFile(broken, []byte(`{"type":"authorized_user",`), 0o600)
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", good)
	var stderr bytes.Buffer
	errOut = &stderr
	t.Cleanup(func() { errOut = os.Stderr })

	code := func(err error) int {
		var ee *exitError
		if errors.As(err, &ee) {
			return ee.code
		}
		t.Fatalf("not an exit: %v", err)
		return 0
	}
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

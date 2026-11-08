// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/dataset/datasettest"
	"github.com/varveio/swath/go/internal/journal"
	"github.com/varveio/swath/go/internal/source/azblob"
	"github.com/varveio/swath/go/internal/source/azblob/azblobtest"
)

// azEndpoint is a replay's --azblob-endpoint: without the account path
// segment, which the client takes from the root.
func azEndpoint(r *azblobtest.Replay) string { return strings.TrimSuffix(r.Endpoint, "/"+r.Account) }

// TestAzblobStagedRefusalOutcome: a run that meets an Azure name at
// U+E000 and above stops as refused (exit 2) with cause unsupported, never
// as a protocol violation: its journal is not marked unresumable, the
// outcome says resumable, and a resume (by this build) is refused the same
// way, the journal still intact.
func TestAzblobStagedRefusalOutcome(t *testing.T) {
	datasettest.RequireJournal(t)
	captureStderr(t)
	r := azblobtest.Serve(t, azblobtest.Keys())
	out := filepath.Join(t.TempDir(), "ds")
	ctx := context.Background()
	check := func(at string, err error) {
		t.Helper()
		if c := exitCode(t, err); c != exitUsage {
			t.Fatalf("%s: exit %d (%v), want %d", at, c, err, exitUsage)
		}
		var o outcome
		b, _ := os.ReadFile(filepath.Join(out, errorName))
		if json.Unmarshal(b, &o) != nil || o.Class != "refused" || o.Cause != "unsupported" || !o.Resumable ||
			!strings.Contains(o.Message, "names at U+E000 and above are not supported yet") {
			t.Fatalf("%s: outcome %s", at, b)
		}
		st, err := journal.Read(out)
		if err != nil || st.Resumable() != nil {
			t.Fatalf("%s: journal %v, resumable %v", at, err, st.Resumable())
		}
		if dataset.Committed(out) {
			t.Fatalf("%s: committed", at)
		}
	}
	check("run", run(ctx, []string{"az://replay/corpus", "--azblob-endpoint", azEndpoint(r), "--azblob-anonymous", "--workers", "8",
		"--part-size", "4KiB", "-o", out}, io.Discard, false))
	check("resume", resume(ctx, []string{out}, io.Discard))
}

// TestAzblobSASNeverRecorded: a SAS given in the root or by --azblob-sas
// reaches no recorded file (journal, outcome, report); a resume of a run
// whose root held one needs a SAS again, takes another one (credentials
// are not identity) and completes; the endpoint stays frozen.
func TestAzblobSASNeverRecorded(t *testing.T) {
	datasettest.RequireJournal(t)
	captureStderr(t)
	t.Setenv(azblob.EnvSAS, "")
	t.Setenv(azblob.EnvKey, "")
	keys := slices.DeleteFunc(azblobtest.Keys(), func(k string) bool { return strings.ContainsFunc(k, func(r rune) bool { return r >= 0xE000 }) })
	r := azblobtest.Serve(t, keys)
	u, _ := url.Parse(azEndpoint(r))
	px := httputil.NewSingleHostReverseProxy(u)
	var mu sync.Mutex // seed and probe requests run beside the worker's
	sigs := map[string]int{}
	sent := func(sig string) int {
		mu.Lock()
		defer mu.Unlock()
		return sigs[sig]
	}
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		sigs[req.URL.Query().Get("sig")]++
		mu.Unlock()
		time.Sleep(30 * time.Millisecond)
		px.ServeHTTP(w, req)
	}))
	t.Cleanup(slow.Close)
	ctx := context.Background()
	out := filepath.Join(t.TempDir(), "ds")
	report := filepath.Join(t.TempDir(), "report.json")
	args := []string{"az://replay/corpus?sv=2026-10-06&sig=SECRET1", "--azblob-endpoint", slow.URL, "--workers", "1",
		"--page-size", "50", "--part-size", "4KiB", "--report", report, "--report-format", "json", "-o", out}
	if c := exitCode(t, run(ctx, append(args, "--max-duration", "300ms"), io.Discard, true)); c != exitDeadline {
		t.Fatalf("first attempt: exit %d", c)
	}
	if sent("SECRET1") == 0 {
		t.Fatalf("the root's SAS was not sent: %v", sigs)
	}
	noSecret := func(at string) {
		t.Helper()
		files := []string{report}
		filepath.Walk(out, func(p string, fi os.FileInfo, err error) error {
			if err == nil && !fi.IsDir() && !strings.HasSuffix(p, ".parquet") {
				files = append(files, p)
			}
			return nil
		})
		for _, f := range files {
			if b, _ := os.ReadFile(f); bytes.Contains(b, []byte("SECRET")) {
				t.Fatalf("%s: %s holds a SAS", at, f)
			}
		}
	}
	noSecret("interrupted")
	st, err := journal.Read(out)
	if err != nil || !slices.Contains(st.Header.Argv, "az://replay/corpus?"+azblob.RedactedSAS) {
		t.Fatalf("recorded command %q (%v)", st.Header.Argv, err)
	}
	// The SAS must be given again.
	if err := resume(ctx, []string{out, "--max-duration", "0"}, io.Discard); err == nil || !strings.Contains(err.Error(), "never recorded") {
		t.Fatalf("resume without a SAS: %v", err)
	}
	// The endpoint is frozen: not an operational flag.
	var ue usageError
	if err := resume(ctx, []string{out, "--azblob-endpoint", "http://127.0.0.1:1"}, io.Discard); !errors.As(err, &ue) {
		t.Fatalf("resume with another endpoint: %v, want a usage refusal", err)
	}
	mu.Lock()
	clear(sigs)
	mu.Unlock()
	if err := resume(ctx, []string{out, "--azblob-sas", "sv=2026-10-06&sig=SECRET2", "--max-duration", "0"}, io.Discard); err != nil {
		t.Fatalf("resume with another SAS: %v", err)
	}
	if !dataset.Committed(out) || sent("SECRET2") == 0 || sent("SECRET1") != 0 {
		t.Fatalf("committed %v, signatures sent %d %d", dataset.Committed(out), sent("SECRET1"), sent("SECRET2"))
	}
	noSecret("resumed")
}

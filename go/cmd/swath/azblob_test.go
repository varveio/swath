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
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/dataset/datasettest"
	"github.com/varveio/swath/go/internal/journal"
	"github.com/varveio/swath/go/internal/source/azblob"
	"github.com/varveio/swath/go/internal/source/azblob/azblobtest"
)

// azEndpoint is a replay's --endpoint: without the account path
// segment, which the client takes from the root.
func azEndpoint(r *azblobtest.Replay) string { return strings.TrimSuffix(r.Endpoint, "/"+r.Account) }

// TestAzblobHostileCorpus: a run over the hostile corpus (names at U+E000
// and above, which the staged refusal used to stop at) completes and
// commits every key once in byte order, the adapter's counters in the
// report.
func TestAzblobHostileCorpus(t *testing.T) {
	datasettest.RequireJournal(t)
	captureStderr(t)
	keys := azblobtest.Keys()
	r := azblobtest.Serve(t, keys)
	out := filepath.Join(t.TempDir(), "ds")
	if err := run(context.Background(), []string{"az://replay/corpus", "--endpoint", azEndpoint(r), "--anonymous", "--workers", "8",
		"--part-size", "4KiB", "-o", out}, io.Discard, false); err != nil {
		t.Fatal(err)
	}
	if !dataset.Committed(out) {
		t.Fatal("not committed")
	}
	if got := datasetKeys(t, out); !slices.Equal(got, keys) {
		t.Fatalf("%d keys, want %d in byte order", len(got), len(keys))
	}
}

// sasReplay serves the hostile corpus through a slow proxy that counts the signatures (sig) requests carry.
type sasReplay struct {
	url  string
	mu   sync.Mutex // seed and probe requests run beside the worker's
	sigs map[string]int
}

func newSASReplay(t *testing.T) *sasReplay {
	t.Helper()
	r := azblobtest.Serve(t, azblobtest.Keys())
	u, _ := url.Parse(azEndpoint(r))
	px := httputil.NewSingleHostReverseProxy(u)
	s := &sasReplay{sigs: map[string]int{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		s.mu.Lock()
		s.sigs[req.URL.Query().Get("sig")]++
		s.mu.Unlock()
		time.Sleep(30 * time.Millisecond)
		px.ServeHTTP(w, req)
	}))
	t.Cleanup(srv.Close)
	s.url = srv.URL
	return s
}

func (s *sasReplay) sent(sig string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sigs[sig]
}

func (s *sasReplay) reset() {
	s.mu.Lock()
	clear(s.sigs)
	s.mu.Unlock()
}

// noSecret fails if stderr or any file of out but its parts, or of the
// extra files, holds "SECRET".
func noSecret(t *testing.T, at string, stderr *bytes.Buffer, out string, extra ...string) {
	t.Helper()
	if bytes.Contains(stderr.Bytes(), []byte("SECRET")) {
		t.Fatalf("%s: stderr holds a SAS:\n%s", at, stderr)
	}
	files := extra
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

// TestAzblobSASNeverRecorded: a SAS given in the root reaches no recorded
// file (journal, outcome, report) and no line on stderr, -v's start line
// included; a resume of a run whose root held one needs a SAS again, takes
// another one (credentials are not identity) and completes; the endpoint
// stays frozen.
func TestAzblobSASNeverRecorded(t *testing.T) {
	datasettest.RequireJournal(t)
	stderr := captureStderr(t)
	t.Setenv(azblob.EnvSAS, "")
	t.Setenv(azblob.EnvKey, "")
	rp := newSASReplay(t)
	ctx := context.Background()
	out := filepath.Join(t.TempDir(), "ds")
	report := filepath.Join(t.TempDir(), "report.json")
	args := []string{"az://replay/corpus?sv=2026-10-06&sig=SECRET1", "--endpoint", rp.url, "--workers", "1", "-v",
		"--page-size", "50", "--part-size", "4KiB", "--report", report, "--report-format", "json", "-o", out}
	if c := exitCode(t, run(ctx, append(args, "--max-duration", "300ms"), io.Discard, true)); c != exitDeadline {
		t.Fatalf("first attempt: exit %d", c)
	}
	if rp.sent("SECRET1") == 0 {
		t.Fatal("the root's SAS was not sent")
	}
	if !strings.Contains(stderr.String(), "listing az://replay/corpus?"+azblob.RedactedSAS) {
		t.Fatalf("-v printed no redacted start line:\n%s", stderr)
	}
	noSecret(t, "interrupted", stderr, out, report)
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
	if err := resume(ctx, []string{out, "--endpoint", "http://127.0.0.1:1"}, io.Discard); !errors.As(err, &ue) {
		t.Fatalf("resume with another endpoint: %v, want a usage refusal", err)
	}
	rp.reset()
	if err := resume(ctx, []string{out, "--azblob-sas", "sv=2026-10-06&sig=SECRET2", "--max-duration", "0", "-v"}, io.Discard); err != nil {
		t.Fatalf("resume with another SAS: %v", err)
	}
	if !dataset.Committed(out) || rp.sent("SECRET2") == 0 || rp.sent("SECRET1") != 0 {
		t.Fatalf("committed %v, signatures sent %d %d", dataset.Committed(out), rp.sent("SECRET1"), rp.sent("SECRET2"))
	}
	noSecret(t, "resumed", stderr, out, report)
}

// TestAzblobSASFlagThenEnvironment: a run started with --azblob-sas
// resumes with a rotated SAS from AZURE_STORAGE_SAS_TOKEN alone (the
// provider reads it when the flag is absent); without either the resume
// is refused, naming both.
func TestAzblobSASFlagThenEnvironment(t *testing.T) {
	datasettest.RequireJournal(t)
	stderr := captureStderr(t)
	t.Setenv(azblob.EnvSAS, "")
	t.Setenv(azblob.EnvKey, "")
	rp := newSASReplay(t)
	ctx := context.Background()
	out := filepath.Join(t.TempDir(), "ds")
	args := []string{"az://replay/corpus", "--endpoint", rp.url, "--azblob-sas", "sv=2026-10-06&sig=SECRET1", "--workers", "1",
		"--page-size", "50", "--part-size", "4KiB", "-o", out}
	if c := exitCode(t, run(ctx, append(args, "--max-duration", "300ms"), io.Discard, true)); c != exitDeadline {
		t.Fatalf("first attempt: exit %d", c)
	}
	if err := resume(ctx, []string{out, "--max-duration", "0"}, io.Discard); err == nil || !strings.Contains(err.Error(), azblob.EnvSAS) {
		t.Fatalf("resume without a SAS: %v", err)
	}
	rp.reset()
	t.Setenv(azblob.EnvSAS, "sv=2026-10-06&sig=SECRET4")
	if err := resume(ctx, []string{out, "--max-duration", "0"}, io.Discard); err != nil {
		t.Fatalf("resume with the SAS in the environment: %v", err)
	}
	if !dataset.Committed(out) || rp.sent("SECRET4") == 0 || rp.sent("SECRET1") != 0 {
		t.Fatalf("committed %v, signatures sent %d %d", dataset.Committed(out), rp.sent("SECRET1"), rp.sent("SECRET4"))
	}
	noSecret(t, "resumed", stderr, out)
}

// TestAzblobAdmissionRefusalRecordsNoSAS: a run refused at admission (Get
// Account Information refused, no Data Lake host on a custom endpoint)
// with -v and --report records the root without its SAS, even though the
// source never opened; so does a root whose SAS query does not parse.
func TestAzblobAdmissionRefusalRecordsNoSAS(t *testing.T) {
	stderr := captureStderr(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-ms-error-code", "AuthorizationFailure")
		w.WriteHeader(403)
	}))
	t.Cleanup(srv.Close)
	for _, root := range []string{"az://acct/ctr?sv=2026-10-06&sig=SECRET3", "az://acct/ctr?sig=SECRET5&x=%ZZ", "az://acct?sig=SECRET6&x=%ZZ"} {
		out := filepath.Join(t.TempDir(), "ds")
		report := filepath.Join(t.TempDir(), "report.json")
		err := run(context.Background(), []string{root, "--endpoint", srv.URL, "-v", "--report", report, "--report-format", "json", "-o", out}, io.Discard, false)
		// The admission refusal is exit 2; a root that does not parse fails
		// at open, as any malformed root does.
		if c := exitCode(t, err); c == exitCompleted || strings.Contains(root, "sv=") && c != exitUsage {
			t.Fatalf("%s: exit %d (%v)", azblob.RedactRoot(root), c, err)
		}
		if strings.Contains(err.Error(), "SECRET") {
			t.Fatalf("the error quotes the SAS: %v", err)
		}
		b, rerr := os.ReadFile(report)
		if rerr != nil || !bytes.Contains(b, []byte(`"source"`)) {
			t.Fatalf("report %v: %s", rerr, b)
		}
		noSecret(t, azblob.RedactRoot(root), stderr, out, report)
	}
}

// TestAzureSDKLogSuppressed: with AZURE_SDK_GO_LOGGING=all, which makes
// azcore write its own log (request URLs, a SAS included) to stderr, a run
// in a fresh process writes no line of it: the command turns it off at
// startup. The child's requests are answered with an error that quotes the
// query, and one endpoint refuses connections, the two cases azcore logs.
func TestAzureSDKLogSuppressed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAzureSDKLogProcess$", "-test.v")
	cmd.Env = append(os.Environ(), "SWATH_TEST_AZLOG=1", "AZURE_SDK_GO_LOGGING=all")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.Output()
	if err != nil || !bytes.Contains(stdout, []byte("--- PASS: TestAzureSDKLogProcess")) {
		t.Fatalf("child: %v\nstdout: %s\nstderr: %s", err, stdout, stderr.String())
	}
	for _, line := range strings.Split(stderr.String(), "\n") {
		// azcore's listener writes "[time] Event: message"
		for _, ev := range []string{"] Request: ", "] Response: ", "] Retry: ", "] ResponseError: "} {
			if strings.Contains(line, ev) {
				t.Errorf("the SDK logged: %s", line)
			}
		}
	}
	if bytes.Contains(stderr.Bytes(), []byte("SECRET")) || bytes.Contains(stdout, []byte("SECRET")) {
		t.Errorf("the signature reached the output:\nstdout: %s\nstderr: %s", stdout, stderr.String())
	}
}

// TestAzureSDKLogProcess is run only by TestAzureSDKLogSuppressed's child,
// so azcore reads AZURE_SDK_GO_LOGGING in a fresh process.
func TestAzureSDKLogProcess(t *testing.T) {
	if os.Getenv("SWATH_TEST_AZLOG") != "1" {
		t.Skip("subprocess helper for TestAzureSDKLogSuppressed")
	}
	var requests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("x-ms-error-code", "AuthorizationFailure")
		w.WriteHeader(403)
		fmt.Fprintf(w, "\xef\xbb\xbf<Error><Code>AuthorizationFailure</Code><Message>%s</Message></Error>", url.QueryEscape(r.URL.RawQuery))
	}))
	t.Cleanup(srv.Close)
	down := httptest.NewServer(http.NotFoundHandler())
	down.Close() // a connection refused
	for _, ep := range []string{srv.URL, down.URL} {
		err := run(context.Background(), []string{"az://acct/c?sv=2026-10-06&sig=SECRET9", "--endpoint", ep, "--workers", "1",
			"--request-timeout", "2s", "-o", filepath.Join(t.TempDir(), "ds")}, io.Discard, false)
		if err == nil {
			t.Fatalf("%s: the run did not fail", ep)
		}
	}
	if requests.Load() == 0 {
		t.Fatal("no request reached the server")
	}
}

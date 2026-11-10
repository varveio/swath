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
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/listing"
	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
	"github.com/varveio/swath/go/internal/source/objhttp"
)

// syncBuffer is stderr for a test whose run writes it from several
// goroutines (the display, the request log, a signal).
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func captureSyncStderr(t *testing.T) *syncBuffer {
	b := &syncBuffer{}
	errOut = b
	t.Cleanup(func() { errOut = os.Stderr; logs.Store(nil) })
	return b
}

// TestEventGolden pins --log-format json: each event once, as
// docs/cli.md's "Log events" block shows it (UPDATE=1 rewrites the block).
func TestEventGolden(t *testing.T) {
	stderr := captureSyncStderr(t)
	at := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	logClock = func() time.Time { return at }
	t.Cleanup(func() { logClock = time.Now })
	setLog(termOptions{verbose: 3, logFormat: logJSON}, "5f0c2a9e4b1d4c3a8e7f6a5b4c3d2e1f")
	l := elog()
	l.start("s3://bucket/logs/", "to stdout")
	l.progress(listing.ProgressSnapshot{Objects: 120000, Advance: 120000, Answered: 121, Frontier: 121, Requests: 122, Retries: 1, Inflight: 8, Limit: 64,
		StartedAt: at.Add(-time.Minute), AsOf: at, DurationMs: 60000, PartAgeMs: 60000}, "listing · 120,000 objects · 2,000 keys/s (avg 2,000)")
	w := newFailureWatch("s3")
	refused := objhttp.Exchange{Method: http.MethodGet, URL: &url.URL{Scheme: "http", Host: "127.0.0.1:9", Path: "/bucket"}, Err: syscall.ECONNREFUSED}
	for i := range 14 {
		w.exchange(refused, at.Add(time.Duration(i)*time.Second))
	}
	sas, _ := url.Parse("https://acct.blob.core.windows.net/c?restype=container&comp=list&prefix=logs%2F&marker=TOKEN&sv=2022-11-02&sig=SECRET")
	l.request(objhttp.Exchange{Method: http.MethodGet, URL: sas, Status: http.StatusOK, Latency: 42 * time.Millisecond})
	l.retry(objhttp.Failed{Attempt: 2, Retry: true, Wait: 400 * time.Millisecond,
		Failure: &objhttp.Failure{Transient: true, Throttle: true, Status: 503, Code: "SlowDown", Class: objhttp.ClassThrottle, Cause: errors.New("503 SlowDown: Please reduce your request rate.")}})
	o := outcome{Class: listing.ClassStuck, ExitCode: exitStuck, Resumable: true, Cause: stuckNoRows, Message: errStuck.Error() + " (" + stuckNoRows + ")"}
	if c := conclude(&exitError{error: errors.New(o.Message), code: o.ExitCode, out: o}); c != exitStuck {
		t.Fatalf("exit %d", c)
	}
	got := stderr.String()
	if strings.Contains(got, "SECRET") || strings.Contains(got, "TOKEN") || strings.Contains(got, "\x1b") {
		t.Fatalf("a credential or an escape on stderr: %s", got)
	}
	events := map[string]bool{}
	for line := range strings.Lines(got) {
		var e map[string]any
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("not one JSON object: %q", line)
		}
		events[fmt.Sprint(e["event"])] = true
	}
	for _, e := range []string{evStart, evProgress, evWarning, evRequest, evRetry, evOutcome} {
		if !events[e] {
			t.Fatalf("no %s event in %s", e, got)
		}
	}
	const begin, end = "<!-- events:begin -->\n", "<!-- events:end -->"
	doc, err := os.ReadFile("../../docs/cli.md")
	if err != nil {
		t.Fatal(err)
	}
	s := string(doc)
	i, j := strings.Index(s, begin), strings.Index(s, end)
	if i < 0 || j < i {
		t.Fatal("docs/cli.md lacks the events markers")
	}
	want := s[:i+len(begin)] + "```json\n" + got + "```\n" + s[j:]
	if want == s {
		return
	}
	if os.Getenv("UPDATE") != "" {
		if err := os.WriteFile("../../docs/cli.md", []byte(want), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	t.Fatalf("docs/cli.md's log events are stale; run UPDATE=1 go test ./cmd/swath\n%s", got)
}

// TestFailureWatch: one cause failing every request warns once it has
// lasted persistFor, then at most once per persistEvery; an answer ends
// it, and a new failure starts over; the end of a run names the cause
// outstanding.
func TestFailureWatch(t *testing.T) {
	stderr := captureSyncStderr(t)
	setLog(termOptions{logFormat: logText}, "")
	w := newFailureWatch("s3")
	at := time.Unix(1_000_000, 0)
	u := &url.URL{Scheme: "http", Host: "127.0.0.1:9", Path: "/b"}
	refused := objhttp.Exchange{Method: http.MethodGet, URL: u, Err: &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}}
	fail := func(d time.Duration) { w.exchange(refused, at.Add(d)) }
	for d := time.Duration(0); d < persistFor; d += time.Second {
		fail(d)
	}
	if stderr.String() != "" || w.outstanding(at.Add(persistFor-time.Millisecond)) != "" {
		t.Fatalf("warned before %s: %q", persistFor, stderr.String())
	}
	fail(persistFor)
	if want := "swath: warning: s3 127.0.0.1:9: connection refused (retrying; 11 attempts)\n"; stderr.String() != want {
		t.Fatalf("stderr %q, want %q", stderr.String(), want)
	}
	for d := persistFor; d < persistFor+persistEvery; d += time.Second {
		fail(d)
	}
	if n := strings.Count(stderr.String(), "\n"); n != 1 || w.warnings.Load() != 1 {
		t.Fatalf("%d warnings within %s: %q", n, persistEvery, stderr.String())
	}
	fail(persistFor + persistEvery)
	if w.warnings.Load() != 2 {
		t.Fatalf("no repeat after %s: %q", persistEvery, stderr.String())
	}
	if o := w.outstanding(at.Add(persistFor + persistEvery)); !strings.HasPrefix(o, "s3 127.0.0.1:9: connection refused (") {
		t.Fatalf("outstanding %q", o)
	}
	// An answer ends the failure; a 404 is an answer that does not.
	end := at.Add(2 * persistEvery)
	w.exchange(objhttp.Exchange{URL: u, Status: http.StatusNotFound}, end)
	if w.outstanding(end) == "" {
		t.Fatal("a 404 ended the failure")
	}
	w.exchange(objhttp.Exchange{URL: u, Status: http.StatusOK}, end)
	if o := w.outstanding(end); o != "" {
		t.Fatalf("outstanding after an answer: %q", o)
	}
	for d := time.Duration(0); d <= persistFor; d += time.Second {
		w.exchange(objhttp.Exchange{URL: u, Status: http.StatusServiceUnavailable}, end.Add(d))
	}
	if w.warnings.Load() != 3 || !strings.HasSuffix(stderr.String(), "s3 127.0.0.1:9: 503 Service Unavailable (retrying; 11 attempts)\n") {
		t.Fatalf("a new failure after an answer: %q", stderr.String())
	}
	// -q: no warnings.
	setLog(termOptions{quiet: 1, logFormat: logText}, "")
	n := len(stderr.String())
	for d := time.Duration(0); d <= persistFor; d += time.Second {
		w.exchange(objhttp.Exchange{URL: &url.URL{Host: "other:1"}, Status: http.StatusBadGateway}, end.Add(d))
	}
	if len(stderr.String()) != n {
		t.Fatalf("-q printed %q", stderr.String()[n:])
	}
}

// TestPersistentFailureRun (D2): a GCS endpoint that refuses connections
// warns without -v while the run retries, the run ends stuck with its
// statistics, its error naming the cause, and the report counts the
// warnings.
func TestPersistentFailureRun(t *testing.T) {
	stderr := captureSyncStderr(t)
	persistFor = 300 * time.Millisecond
	t.Cleanup(func() { persistFor = 10 * time.Second })
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	host := ln.Addr().String()
	ln.Close() // connections to it are refused
	dir := filepath.Join(t.TempDir(), "ds")
	report := filepath.Join(t.TempDir(), "report.json")
	err = run(context.Background(), append([]string{"gs://bkt", "--endpoint-url", "http://" + host + gcstest.Path, "--anonymous", "--stall-timeout", "2s",
		"--report", report, "-o", dir}, unjournaled...), io.Discard, len(unjournaled) > 0)
	if exitCode(t, err) != exitStuck {
		t.Fatalf("exit %v", err)
	}
	warning := "swath: warning: gs " + host + ": connection refused (retrying; "
	if s := stderr.String(); strings.Count(s, warning) != 1 || !strings.Contains(s, "INCOMPLETE (stuck)") {
		t.Fatalf("stderr %q", s)
	}
	if !strings.Contains(err.Error(), "every request was failing: gs "+host+": connection refused (") {
		t.Fatalf("the final error does not name the cause: %v", err)
	}
	var rep listing.Report
	readJSON(t, report, &rep)
	if rep.Requests == nil || rep.Requests.FailureWarnings != 1 {
		t.Fatalf("report requests %+v", rep.Requests)
	}
}

// flakyGCS is a GCS endpoint whose first listing request answers 503.
func flakyGCS(t *testing.T) string {
	var n atomic.Int64
	h := gcstest.Handler(gcstest.Keys(3), 0)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) == 1 {
			http.Error(w, `{"error":{"code":503,"message":"backend"}}`, http.StatusServiceUnavailable)
			return
		}
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv.URL + gcstest.Path
}

// TestVerbosity (G2): by default no start line and no request log; -v the
// start line saying where the rows go; -vv each failed request and retry
// with its cause, not the answered ones; -vvv every request.
func TestVerbosity(t *testing.T) {
	stderr := captureSyncStderr(t)
	start := regexp.MustCompile(`(?m)^swath: listing gs://bkt to stdout \(attempt [0-9a-f]{32}\)$`)
	failed := regexp.MustCompile(`(?m)^swath: request GET 127\.0\.0\.1:\d+/storage/v1/b/bkt/o\?\S*: 503 \(`)
	retry := regexp.MustCompile(`(?m)^swath: attempt 1 failed \(\w+\): .*; retrying in `)
	answered := regexp.MustCompile(`(?m)^swath: request GET 127\.0\.0\.1:\d+/storage/v1/b/bkt/o\?\S*: 200 \(`)
	for _, c := range []struct {
		flag                           string
		start, failed, retry, answered bool
	}{
		{"", false, false, false, false},
		{"-v", true, false, false, false},
		{"-vv", true, true, true, false},
		{"-vvv", true, true, true, true},
	} {
		args := []string{"gs://bkt", "--endpoint-url", flakyGCS(t), "--anonymous", "--workers", "1", "--no-progress", "--no-stats"}
		if c.flag != "" {
			args = append(args, c.flag)
		}
		stderr.mu.Lock()
		stderr.b.Reset()
		stderr.mu.Unlock()
		var out bytes.Buffer
		if err := run(context.Background(), args, &out, false); err != nil {
			t.Fatalf("%q: %v", c.flag, err)
		}
		s := stderr.String()
		if start.MatchString(s) != c.start || failed.MatchString(s) != c.failed || retry.MatchString(s) != c.retry || answered.MatchString(s) != c.answered {
			t.Fatalf("%q: stderr\n%s", c.flag, s)
		}
	}
	// A dataset run's start line names the directory.
	dir := filepath.Join(t.TempDir(), "ds")
	if err := run(context.Background(), append([]string{"gs://bkt", "--endpoint-url", flakyGCS(t), "--anonymous", "-v", "--no-stats", "-o", dir}, unjournaled...), io.Discard, len(unjournaled) > 0); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stderr.String(), "swath: listing gs://bkt to "+dir+" (attempt ") {
		t.Fatalf("stderr %s", stderr)
	}
}

// TestRequestPathRedacted: the request log shows a listing's own query
// parameters and redacts every other value (an Azure SAS, a continuation
// token).
func TestRequestPathRedacted(t *testing.T) {
	u, _ := url.Parse("https://acct.blob.core.windows.net/c?restype=container&comp=list&prefix=a%2Fb&marker=TOKEN&sv=2022-11-02&sig=SECRET&se=2030")
	want := "/c?comp=list&marker=[redacted]&prefix=a%2Fb&restype=container&se=[redacted]&sig=[redacted]&sv=[redacted]"
	if got := requestPath(u); got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	if got := requestPath(&url.URL{Path: "/b"}); got != "/b" {
		t.Fatalf("no query: %s", got)
	}
}

// TestOneLineFailures (D1, D4): a refusal and a failure before any
// listing request was answered print the error line alone on stderr,
// with no statistics, INCOMPLETE, outcome line or internal prefix; -v adds
// the outcome line, --log-format json prints only events. The report is
// written either way.
func TestOneLineFailures(t *testing.T) {
	stderr := captureSyncStderr(t)
	ctx := context.Background()
	src := t.TempDir()
	os.WriteFile(filepath.Join(src, "f"), []byte("x"), 0o644)
	full := t.TempDir()
	os.WriteFile(filepath.Join(full, "other"), nil, 0o644)
	missing := filepath.Join(t.TempDir(), "nosuchdir")
	nobucket := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":{"code":404,"message":"The specified bucket does not exist."}}`, http.StatusNotFound)
	}))
	t.Cleanup(nobucket.Close)
	for _, c := range []struct {
		name string
		args []string
		line string // the whole of stderr; "" checks one line
	}{
		{"not empty", []string{"list", src, "-o", full}, "swath: " + full + ": output directory is not empty (use --overwrite to replace a previous dataset)\n"},
		{"no such directory", []string{"du", missing}, "swath: " + missing + ": no such file or directory\n"},
		{"no such directory, a dataset", []string{"list", missing, "-o", filepath.Join(t.TempDir(), "ds")}, "swath: " + missing + ": no such file or directory\n"},
		{"no such bucket", []string{"list", "gs://bkt", "--endpoint-url", nobucket.URL + gcstest.Path, "--anonymous", "-o", filepath.Join(t.TempDir(), "ds")}, ""},
	} {
		for _, mode := range []string{"", "-v", "--log-format=json"} {
			stderr.mu.Lock()
			stderr.b.Reset()
			stderr.mu.Unlock()
			report := filepath.Join(t.TempDir(), "report.json")
			args := append(append([]string{}, c.args...), "--report", report)
			if mode != "" {
				args = append(args, mode)
			}
			code := conclude(dispatch(ctx, args, io.Discard))
			var rep listing.Report
			readJSON(t, report, &rep)
			if code == exitCompleted || rep.Outcome.ExitCode != code {
				t.Fatalf("%s %s: exit %d, report %+v", c.name, mode, code, rep.Outcome)
			}
			s := stderr.String()
			switch mode {
			case "":
				if (c.line != "" && s != c.line) || strings.Count(s, "\n") != 1 || !strings.HasPrefix(s, "swath: ") {
					t.Fatalf("%s: stderr %q, want %q", c.name, s, c.line)
				}
			case "-v":
				if !strings.Contains(s, "swath: outcome: {") || (c.line != "" && !strings.HasSuffix(s, c.line)) || strings.Contains(s, "INCOMPLETE") {
					t.Fatalf("%s -v: stderr %q", c.name, s)
				}
			default:
				for line := range strings.Lines(s) {
					var e map[string]any
					if json.Unmarshal([]byte(line), &e) != nil {
						t.Fatalf("%s json: not an event: %q", c.name, line)
					}
				}
				if !strings.Contains(s, `"event":"outcome"`) || !strings.Contains(s, fmt.Sprintf(`"exit_code":%d`, code)) {
					t.Fatalf("%s json: stderr %q", c.name, s)
				}
			}
			for _, bad := range []string{"prepare output", "root: ", "0 entries", "peak RSS"} {
				if strings.Contains(s, bad) {
					t.Fatalf("%s %s: %q on stderr: %q", c.name, mode, bad, s)
				}
			}
		}
	}
	// --log-format before the command word, and a usage error under it.
	stderr.mu.Lock()
	stderr.b.Reset()
	stderr.mu.Unlock()
	if code := conclude(dispatch(ctx, []string{"--log-format", "json", "list"}, io.Discard)); code != exitUsage ||
		!strings.Contains(stderr.String(), `"event":"outcome","msg":"missing SOURCE","class":"usage","exit_code":2`) {
		t.Fatalf("exit %d, stderr %q", code, stderr.String())
	}
	if err := dispatch(ctx, []string{"list", src, "--log-format", "xml"}, io.Discard); !strings.Contains(fmt.Sprint(err), `--log-format "xml": want text or json`) {
		t.Fatalf("--log-format xml: %v", err)
	}
}

// TestJSONProgress: under --log-format json a run's progress records are
// events, whatever the terminal, and no statistics block is printed.
func TestJSONProgress(t *testing.T) {
	stderr := captureSyncStderr(t)
	was := stderrTerminal
	stderrTerminal = func() (bool, int) { return true, 120 }
	t.Cleanup(func() { stderrTerminal = was })
	src := t.TempDir()
	os.WriteFile(filepath.Join(src, "f"), []byte("x"), 0o644)
	if code := conclude(dispatch(context.Background(), []string{"--log-format=json", "du", src, "--progress-interval", "1s", "--stats"}, io.Discard)); code != exitCompleted {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	s := stderr.String()
	for line := range strings.Lines(s) {
		var e map[string]any
		if json.Unmarshal([]byte(line), &e) != nil || strings.Contains(line, "\x1b") || strings.Contains(line, "\r") {
			t.Fatalf("not an event: %q", line)
		}
	}
	if !strings.Contains(s, `"event":"outcome","attempt":"`) || !strings.Contains(s, `"class":"completed","exit_code":0`) {
		t.Fatalf("stderr %q", s)
	}
}

// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/listing"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

func snap() listing.ProgressSnapshot {
	usd := 0.0125
	return listing.ProgressSnapshot{Objects: 1234567, Retained: 1000, Parts: 3, Requests: 2500, Inflight: 17, DurationMs: 10_000,
		StartedAt: time.Unix(1, 0), CostUSD: &usd, CostRatePer1kUSD: 0.005}
}

// TestProgressRecord: Java's listing record (phase, objects with the
// recovered rows apart, live and average rates, in flight, elapsed, API
// calls, the labelled estimate), its trailing fields dropped whole to fit
// a terminal, and colour only when asked.
func TestProgressRecord(t *testing.T) {
	parts := progressParts(snap(), 75*time.Second, 2000)
	want := []string{"listing", "1,234,567 objects (+1,000 recovered)", "2,000 keys/s (avg 123,457)", "3 parts", "17 in flight",
		"1m15s elapsed", "2,500 API calls", "~$0.013 (est. @ $0.005/1k LIST)"}
	if strings.Join(parts, "|") != strings.Join(want, "|") {
		t.Fatalf("parts\n got %q\nwant %q", parts, want)
	}
	if got := frame(parts, 0, false); got != "  "+strings.Join(want, " · ") {
		t.Fatalf("plain frame %q", got)
	}
	narrow := frame(parts, 60, false)
	if len([]rune(narrow)) > 60 || !strings.HasSuffix(narrow, "recovered)") {
		t.Fatalf("narrow frame %q", narrow)
	}
	if got := frame(parts, 8, false); got != "  listin" {
		t.Fatalf("too narrow for the phase: %q", got)
	}
	if c := frame(parts, 0, true); !strings.Contains(c, "\x1b[36m1,234,567 objects") || !strings.Contains(c, "\x1b[2m") {
		t.Fatalf("coloured frame %q", c)
	}
	v := snap()
	v.Objects, v.Requests, v.Verified = 0, 0, 5_000_000
	if p := progressParts(v, time.Second, 0); p[0] != "verifying" || p[1] != "5.0 MB verified" {
		t.Fatalf("verification record %q", p)
	}
}

// TestProgressRedrawAndAppend: on a terminal a record overwrites the last
// (carriage return, erase to the end of the line, no newline) and another
// stderr write erases and repaints it; elsewhere each record is one plain
// line without control sequences. Records are spaced by the interval.
func TestProgressRedrawAndAppend(t *testing.T) {
	var tty bytes.Buffer
	c := &stderrCoord{w: &tty}
	d := &display{out: c, redraw: true, width: func() int { return 200 }, interval: time.Second, started: time.Now()}
	now := time.Now()
	d.render(snap(), now)
	d.render(snap(), now.Add(100*time.Millisecond)) // not due
	_, _ = c.Write([]byte("swath: warning: x\n"))
	d.render(snap(), now.Add(time.Second))
	c.finish()
	got := tty.String()
	if strings.Count(got, "\r\x1b[2K  listing") != 2 || !strings.Contains(got, "\r\x1b[2Kswath: warning: x\n  listing") || strings.Count(got, "\n") != 2 ||
		!strings.HasSuffix(got, "\n") {
		t.Fatalf("redraw output %q", got)
	}
	var log bytes.Buffer
	d = &display{out: &stderrCoord{w: &log}, interval: 30 * time.Second, started: time.Now()}
	d.render(snap(), now)
	d.render(snap(), now.Add(time.Second))
	d.render(snap(), now.Add(31*time.Second))
	if got := log.String(); strings.Count(got, "\n") != 2 || strings.ContainsAny(got, "\r\x1b") || !strings.HasPrefix(got, "  listing · ") {
		t.Fatalf("appended output %q", got)
	}
}

// TestProgressGating: Java's decision ladder and cadence.
func TestProgressGating(t *testing.T) {
	type c struct {
		opt          termOptions
		tty          bool
		width        int
		term         string
		show, redraw bool
	}
	on, off := tri{true, true}, tri{true, false}
	for i, tc := range []c{
		{termOptions{}, true, 80, "xterm", true, true},
		{termOptions{}, false, 0, "", false, false},
		{termOptions{quiet: 1}, true, 80, "xterm", false, false},
		{termOptions{verbose: 1}, true, 80, "xterm", true, false},
		{termOptions{progress: on}, false, 0, "", true, false},
		{termOptions{progress: on, quiet: 1}, true, 80, "xterm", true, true},
		{termOptions{progress: off}, true, 80, "xterm", false, false},
		{termOptions{interval: 5 * time.Second}, false, 0, "", true, false},
		{termOptions{}, true, 80, "dumb", true, false},
		{termOptions{}, true, 20, "xterm", true, false},
	} {
		// A redraw also needs escape sequences to act on stderr: on Windows
		// a console, which a test's stderr is not.
		show, redraw := progressPrefs(tc.opt, tc.tty, tc.width, tc.term)
		if tc.redraw && !enableVT() {
			tc.redraw = false
		}
		if show != tc.show || redraw != tc.redraw {
			t.Errorf("%d: show %v redraw %v, want %v %v", i, show, redraw, tc.show, tc.redraw)
		}
	}
	if progressInterval(termOptions{}, true) != time.Second || progressInterval(termOptions{}, false) != 30*time.Second ||
		progressInterval(termOptions{interval: 100 * time.Millisecond}, false) != time.Second {
		t.Fatal("cadence")
	}
}

func TestColorResolution(t *testing.T) {
	env := func(m map[string]string) func(string) (string, bool) {
		return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
	}
	for i, tc := range []struct {
		mode string
		env  map[string]string
		tty  bool
		want bool
	}{
		{"auto", nil, true, true},
		{"auto", nil, false, false},
		{"auto", map[string]string{"NO_COLOR": "1"}, true, false},
		{"auto", map[string]string{"NO_COLOR": ""}, true, true},
		{"auto", map[string]string{"TERM": "dumb"}, true, false},
		{"auto", map[string]string{"CLICOLOR_FORCE": "1"}, false, true},
		{"always", map[string]string{"NO_COLOR": "1"}, false, true},
		{"never", nil, true, false},
	} {
		if got := colorEnabled(tc.mode, env(tc.env), tc.tty); got != tc.want {
			t.Errorf("%d: %v", i, got)
		}
	}
}

// TestStatsBlock: Java's gating and lines from the report.
func TestStatsBlock(t *testing.T) {
	rss := int64(200_000_000)
	per := 1.5
	usd := 0.01
	r := &listing.Report{Timings: listing.Timings{WallMs: 12_300, SessionMs: 14_000, ListingMs: 12_000},
		Output:     listing.Output{RowsEmitted: 2_000_000, PartsWritten: 2, OutputBytes: 3_500_000},
		Resources:  listing.Resources{PeakRSSBytes: &rss},
		Requests:   &listing.Requests{HTTPAttempts: 2000},
		Cost:       &listing.Cost{CostUSD: &usd, Basis: &listing.CostBasis{RatePer1kUSD: 0.005, Source: "aws-list-reference-rate"}},
		Efficiency: &listing.Efficiency{KeysPerSec: 166_667, HTTPAttemptsPer1kRows: &per}}
	got := statsLines(r, false)
	want := []string{"2,000,000 objects in 12.3s · 166,667 keys/s · 14.0s total", "2,000 API calls · 1.50 per 1k objects · in flight avg 0.00 · peak 0",
		"~$0.010 (est. @ $0.005/1k LIST)", "2 files · 3.5 MB written · peak RSS 200.0 MB"}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("lines\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	r.Outcome = listing.Outcome{Class: "deadline", ExitCode: exitDeadline, Resumable: true}
	r.Invocation.Out = "/data/ds"
	if l := statsLines(r, false); l[0] != "INCOMPLETE (deadline) — resume: swath resume /data/ds" {
		t.Fatalf("disposition %q", l[0])
	}
	short := &listing.Report{Timings: listing.Timings{SessionMs: 100}}
	switch {
	case showStats(termOptions{}, short, false):
		t.Fatal("a short complete run without output prints statistics")
	case !showStats(termOptions{}, r, false):
		t.Fatal("an incomplete run prints no statistics")
	case showStats(termOptions{quiet: 1}, r, false), showStats(termOptions{}, r, true):
		t.Fatal("quiet or a closed pipe prints statistics")
	case !showStats(termOptions{stats: tri{true, true}, quiet: 1}, short, true):
		t.Fatal("--stats does not force them")
	case showStats(termOptions{stats: tri{true, false}}, r, false):
		t.Fatal("--no-stats prints them")
	}
}

// TestReportContinuity: a run's report carries requests.http_attempts
// (every HTTP attempt) with the labelled estimate only on a known tariff, efficiency,
// session_ms and args_hash (the same for the same identity, another for
// another prefix); --stats prints the block on stderr, the summary line
// stays on stdout, and progress.json carries the request account.
func TestReportContinuity(t *testing.T) {
	url := s3test.Bucket(t, []string{"a/1", "a/2", "b/1"})
	stderr := captureStderr(t)
	type rep struct {
		ArgsHash string `json:"args_hash"`
		Requests *struct {
			HTTPAttempts int64 `json:"http_attempts"`
		}
		Cost *struct {
			CostUSD *float64 `json:"cost_usd"`
			Basis   *struct{ Source string }
		}
		Efficiency *struct {
			KeysPerSec float64  `json:"keys_per_sec"`
			Per1k      *float64 `json:"http_attempts_per_1k_rows"`
		}
		Timings struct {
			SessionMs int64 `json:"session_ms"`
			WallMs    int64 `json:"wall_ms"`
		}
	}
	list := func(src string, extra ...string) (rep, string) {
		t.Helper()
		var out bytes.Buffer
		file := filepath.Join(t.TempDir(), "r.json")
		args := append([]string{src, "--endpoint", url, "--path-style", "--anonymous", "-o", filepath.Join(t.TempDir(), "ds"), "--report", file}, extra...)
		args = append(args, unjournaled...)
		if err := run(context.Background(), args, &out, diagList); err != nil {
			t.Fatal(err)
		}
		var r rep
		readJSON(t, file, &r)
		return r, out.String()
	}
	r, out := list("s3://bkt", "--stats")
	if r.Requests == nil || r.Requests.HTTPAttempts < 1 || r.Cost == nil || r.Cost.CostUSD != nil || r.Cost.Basis != nil {
		t.Fatalf("cost on a custom endpoint: %+v", r.Cost)
	}
	if r.Efficiency == nil || r.Efficiency.Per1k == nil || *r.Efficiency.Per1k <= 0 || len(r.ArgsHash) != 64 || r.Timings.SessionMs < r.Timings.WallMs {
		t.Fatalf("report %+v", r)
	}
	if !strings.Contains(out, " rows=3 ") || !strings.Contains(stderr.String(), "3 objects in ") || !strings.Contains(stderr.String(), "API calls") {
		t.Fatalf("stdout %q stderr %q", out, stderr.String())
	}
	again, _ := list("s3://bkt")
	other, _ := list("s3://bkt/a/")
	if again.ArgsHash != r.ArgsHash || other.ArgsHash == r.ArgsHash {
		t.Fatalf("args_hash %s %s %s", r.ArgsHash, again.ArgsHash, other.ArgsHash)
	}
	// progress.json's request fields.
	s, _ := json.Marshal(snap())
	for _, k := range []string{`"requests":2500`, `"inflight":17`, `"limit":0`, `"retries":0`, `"rows_retained":1000`, `"cost_rate_per_1k_usd":0.005`} {
		if !bytes.Contains(s, []byte(k)) {
			t.Fatalf("progress.json lacks %s: %s", k, s)
		}
	}
}

// TestStopSource: the outcome names what stopped a run.
func TestStopSource(t *testing.T) {
	err := errors.New("stopped")
	for cause, want := range map[error]string{errDeadline: "timebox", &stuckError{stuckNoRows}: "liveness_watchdog",
		&signalError{syscall.SIGTERM}: "sigterm", &signalError{os.Interrupt}: "sigint"} {
		if got := classify(err, cause, "").StopSource; got != want {
			t.Errorf("%v: %q, want %q", cause, got, want)
		}
	}
	if classify(err, nil, "").StopSource != "" {
		t.Error("a failure names a stop source")
	}
}

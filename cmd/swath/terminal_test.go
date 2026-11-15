// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/varveio/swath/internal/jsontime"
	"github.com/varveio/swath/internal/listing"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/s3/s3test"
	"github.com/varveio/swath/internal/totals"
)

func snap() listing.ProgressSnapshot {
	usd := 0.0125
	return listing.ProgressSnapshot{Rows: 1234567, Retained: 1000, Parts: 3, Requests: 2500, Inflight: 17, DurationMs: 10_000,
		StartedAt: jsontime.Of(time.Unix(1, 0)), CostUSD: &usd, CostRatePer1kUSD: 0.005}
}

// TestProgressRecord: the listing record (phase, rows with the recovered
// ones apart, bytes so far, elapsed, live and average rates, the parts of
// a dataset, then in flight, API calls and the labelled estimate), its
// trailing fields dropped whole to fit a terminal, and colour only when
// asked. The rate takes the rows' noun; a run without a dataset shows no
// parts, a source without requests no request fields; while every request
// fails the cause retried takes the rate's place.
func TestProgressRecord(t *testing.T) {
	store := rowNouns(false, false)
	s := snap()
	s.ObjectBytes = 5 << 30
	parts := progressParts(s, 75*time.Second, 2000, store, true, "")
	want := []string{"listing", "1,234,567 objects (+1,000 recovered)", "5.00 GiB", "1m15s elapsed", "2,000 keys/s (avg 123,457)", "3 parts", "17 in flight",
		"2,500 API calls", "~$0.013 (est. @ $0.005/1k LIST)"}
	if strings.Join(parts, "|") != strings.Join(want, "|") {
		t.Fatalf("parts\n got %q\nwant %q", parts, want)
	}
	if got := frame(parts, 0, false); got != "  "+strings.Join(want, " · ") {
		t.Fatalf("plain frame %q", got)
	}
	narrow := frame(parts, 60, false)
	if len([]rune(narrow)) > 60 || !strings.HasSuffix(narrow, "5.00 GiB") {
		t.Fatalf("narrow frame %q", narrow)
	}
	if got := frame(parts, 8, false); got != "  listin" {
		t.Fatalf("too narrow for the phase: %q", got)
	}
	if c := frame(parts, 0, true); !strings.Contains(c, "\x1b[36m1,234,567 objects") || !strings.Contains(c, "\x1b[2m") {
		t.Fatalf("coloured frame %q", c)
	}
	if p := progressParts(snap(), time.Second, 0, store, true, "connection refused"); p[2] != "1.0s elapsed" || p[3] != "retrying: connection refused" {
		t.Fatalf("failing record %q", p)
	}
	v := snap()
	v.Rows, v.Requests, v.Verified = 0, 0, 5_000_000
	if p := progressParts(v, time.Second, 0, store, true, ""); p[0] != "verifying" || p[1] != "4.77 MiB verified" || p[2] != "1.0s elapsed" {
		t.Fatalf("verification record %q", p)
	}
	v = snap()
	v.Parts = 1
	if p := progressParts(v, time.Second, 0, store, true, ""); p[4] != "1 part" {
		t.Fatalf("one part: %q", p)
	}
	l := snap()
	l.Requests, l.Inflight, l.CostUSD = 0, 0, nil
	local := progressParts(l, time.Second, 2000, rowNouns(true, false), false, "")
	if strings.Join(local, "|") != "listing|1,234,567 entries (+1,000 recovered)|1.0s elapsed|2,000 entries/s (avg 123,457)" {
		t.Fatalf("local text run: %q", local)
	}
	if p := progressParts(snap(), time.Second, 2000, rowNouns(false, true), false, ""); p[1] != "1,234,567 rows (+1,000 recovered)" || p[3] != "2,000 rows/s (avg 123,457)" {
		t.Fatalf("versions: %q", p)
	}
}

// TestAllVersionsNouns: the command sees --all-versions, which names the
// terminal's rows (rowNouns).
func TestAllVersionsNouns(t *testing.T) {
	var o options
	f := newFlags(&o, false)
	if o.allVersions == nil || o.versions() {
		t.Fatal("--all-versions unseen or on by default")
	}
	if err := f.Parse([]string{"--all-versions"}); err != nil || !o.versions() {
		t.Fatalf("%v: --all-versions unseen", err)
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
	// Rows that follow on the same terminal (swath du's table) find the
	// line cleared, the cursor at its start, whichever path stops it.
	tty.Reset()
	d = &display{out: &stderrCoord{w: &tty}, redraw: true, erase: true, width: func() int { return 200 }, interval: time.Second, started: time.Now(),
		samples: make(chan listing.ProgressSnapshot, 1), stopped: make(chan struct{})}
	d.render(snap(), now)
	d.stop()
	d.render(snap(), now.Add(time.Second))
	if got := tty.String(); !strings.HasSuffix(got, "\r\x1b[2K") || strings.Contains(got, "\n") || strings.Count(got, "listing") != 1 {
		t.Fatalf("erased output %q", got)
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
		tty, rows    bool
		width        int
		term         string
		show, redraw bool
	}
	on, off := tri{true, true}, tri{true, false}
	for i, tc := range []c{
		{termOptions{}, true, false, 80, "xterm", true, true},
		{termOptions{}, false, false, 0, "", false, false},
		{termOptions{quiet: 1}, true, false, 80, "xterm", false, false},
		{termOptions{verbose: 1}, true, false, 80, "xterm", true, false},
		{termOptions{progress: on}, false, false, 0, "", true, false},
		{termOptions{progress: on, quiet: 1}, true, false, 80, "xterm", true, true},
		{termOptions{progress: off}, true, false, 80, "xterm", false, false},
		{termOptions{interval: 5 * time.Second}, false, false, 0, "", true, false},
		{termOptions{}, true, false, 80, "dumb", true, false},
		{termOptions{}, true, false, 20, "xterm", true, false},
		// stdout a terminal of rows: no automatic progress among them;
		// --progress still draws it.
		{termOptions{}, true, true, 80, "xterm", false, false},
		{termOptions{progress: on}, true, true, 80, "xterm", true, true},
		// -v's appended records follow the same rule.
		{termOptions{verbose: 1}, true, true, 80, "xterm", false, false},
		{termOptions{verbose: 1, progress: on}, true, true, 80, "xterm", true, false},
	} {
		// A redraw also needs escape sequences to act on stderr: on Windows
		// a console, which a test's stderr is not.
		show, redraw := progressPrefs(tc.opt, tc.tty, tc.rows, tc.width, tc.term)
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

// TestStatsBlock: the closing lines from the report (the verdict, the
// count, the bytes, the time and its rate, where the output went, what to
// run next), the statistics only with detail, and the gating.
func TestStatsBlock(t *testing.T) {
	rss := int64(200_000_000)
	usd := 0.01
	objBytes := int64(5 << 40)
	r := &listing.Report{Timings: listing.Timings{WallMs: 12_300, SessionMs: 14_000, ListingMs: 12_000},
		Invocation: listing.Invocation{Out: "ds1"},
		Outcome:    listing.Outcome{Status: listing.StatusCommitted, Complete: true, Class: listing.ClassOK},
		Output: listing.Output{RowsFetched: 2_000_000, RowsEmitted: 2_000_000, RowsCommitted: 2_000_000, PartsWritten: 2, OutputBytes: 3_500_000,
			CommittedObjectBytes: &objBytes},
		Requests: &listing.Requests{HTTPAttempts: 2000, Retries: 3},
		Cost:     &listing.Cost{CostUSD: &usd, Basis: &listing.CostBasis{RatePer1kUSD: 0.005, Source: "aws-list-reference-rate"}},
		Process:  listing.Process{PeakRSSBytes: &rss}}
	store := rowNouns(false, false)
	check := func(got []string, want ...string) {
		t.Helper()
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Fatalf("lines\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
		}
	}
	check(statsLines(r, false, store, false), "complete: 2,000,000 objects, 5.00 TiB in 14.0s · 142,857 keys/s → ds1 (2 parts, 3.34 MiB)",
		"next: swath show ds1 · swath du --from-dataset ds1")
	// --stats: the request account, the retries (no throttles: none), the
	// estimate and the peak RSS.
	check(statsLines(r, false, store, true)[2:], "2,000 API calls · in flight avg 0.00 · peak 0", "retried 3", "~$0.010 (est. @ $0.005/1k LIST)", "peak RSS 191 MiB")
	// A short run gives no rate; a resume names the rows from earlier
	// attempts; filtered rows follow the kept ones.
	short := *r
	short.Timings.SessionMs = 1500
	short.Output.RowsRetained, short.Output.RowsFiltered = 7_003, 12
	if l := statsLines(&short, false, store, false); l[0] != "complete: 2,000,000 objects (7,003 from earlier attempts) kept, 12 filtered out, 5.00 TiB in 1.5s → ds1 (2 parts, 3.34 MiB)" {
		t.Fatalf("short resume %q", l[0])
	}
	// A text file, a stream, a discarded file.
	text := listing.Report{Timings: r.Timings, Outcome: listing.Outcome{Status: listing.StatusWritten, Complete: true},
		Output: listing.Output{RowsFetched: 1, RowsEmitted: 1, Text: &listing.TextOutput{Destination: "out.tsv", Bytes: 2048}}}
	check(statsLines(&text, false, rowNouns(true, false), false), "complete: 1 entry in 14.0s · 0 entries/s → out.tsv (2.00 KiB)")
	text.Output.Text.Destination = "-"
	check(statsLines(&text, false, store, false), "complete: 1 object in 14.0s · 0 keys/s")
	text.Output.Text.Destination = "out.tsv"
	text.Outcome = listing.Outcome{Status: listing.StatusFailed, Class: listing.ClassFatal, ExitCode: exitFatal}
	check(statsLines(&text, false, store, false), "failed: 1 object in 14.0s · 0 keys/s, no file written")
	// A stopped dataset run: what it saved for swath resume, what it listed.
	r.Outcome = listing.Outcome{Status: listing.StatusFailed, Class: "deadline", ExitCode: exitDeadline, Resumable: true}
	r.Output = listing.Output{RowsFetched: 12_003, RowsEmitted: 9_000, RowsDurable: 7_003, PartsWritten: 1, OutputBytes: 2048}
	check(statsLines(r, false, store, false), "stopped: --max-duration reached; 7,003 saved, 12,003 listed in 14.0s · 857 keys/s → ds1 (1 part, 2.00 KiB)",
		"next: swath resume ds1")
	// swath du: the total row's counts and size.
	size := int64(410 << 10)
	du := listing.Report{Timings: listing.Timings{SessionMs: 100}, Outcome: listing.Outcome{Complete: true},
		Output: listing.Output{RowsFetched: 16, RowsEmitted: 16, Totals: &totals.Stats{Objects: 7, Dirs: 9, Size: &size}}}
	check(statsLines(&du, false, rowNouns(true, false), false), "complete: 7 files, 9 directories, 410 KiB in 0.1s")
	quick := &listing.Report{Timings: listing.Timings{SessionMs: 100}}
	switch {
	case showStats(termOptions{}, quick, false):
		t.Fatal("a short complete run without output prints statistics")
	case !showStats(termOptions{}, r, false):
		t.Fatal("an incomplete run prints no statistics")
	case showStats(termOptions{quiet: 1}, r, false), showStats(termOptions{}, r, true):
		t.Fatal("quiet or a closed pipe prints statistics")
	case !showStats(termOptions{stats: tri{true, true}, quiet: 1}, quick, true):
		t.Fatal("--stats does not force them")
	case showStats(termOptions{stats: tri{true, false}}, r, false):
		t.Fatal("--no-stats prints them")
	}
}

// TestReportContinuity: a run's report carries requests.http_attempts
// (every HTTP attempt) with the labelled estimate only on a known tariff,
// diagnostics.efficiency, session_ms, args_hash (the same for the same
// identity, another for another prefix) and the run's id; --stats prints
// the block on stderr, the summary line stays on stdout, and progress.json
// carries the request account and, once the run ended, its final counts.
func TestReportContinuity(t *testing.T) {
	url := s3test.Bucket(t, []string{"a/1", "a/2", "b/1"})
	stderr := captureStderr(t)
	type rep struct {
		ArgsHash string              `json:"args_hash"`
		Attempt  struct{ ID string } `json:"attempt"`
		Run      *struct{ ID string }
		Requests *struct {
			HTTPAttempts int64 `json:"http_attempts"`
		}
		Cost *struct {
			CostUSD *float64 `json:"cost_usd"`
			Basis   *struct{ Source string }
		}
		Diagnostics struct {
			Efficiency *struct {
				KeysPerSec float64  `json:"keys_per_sec"`
				Per1k      *float64 `json:"http_attempts_per_1k_rows"`
			}
		}
		Output struct {
			RowsFetched int64 `json:"rows_fetched"`
		}
		Timings struct {
			StartedAt string `json:"started_at"`
			SessionMs int64  `json:"session_ms"`
			WallMs    int64  `json:"wall_ms"`
		}
	}
	list := func(src string, extra ...string) (rep, string, string) {
		t.Helper()
		var out bytes.Buffer
		file, ds := filepath.Join(t.TempDir(), "r.json"), filepath.Join(t.TempDir(), "ds")
		args := append([]string{src, "--endpoint-url", url, "--s3-path-style", "--anonymous", "-o", ds, "--report", file}, extra...)
		args = append(args, unjournaled...)
		if err := run(context.Background(), args, &out, diagList); err != nil {
			t.Fatal(err)
		}
		var r rep
		readJSON(t, file, &r)
		return r, out.String(), ds
	}
	r, out, ds := list("s3://bkt", "--stats")
	if r.Requests == nil || r.Requests.HTTPAttempts < 1 || r.Cost == nil || r.Cost.CostUSD != nil || r.Cost.Basis != nil {
		t.Fatalf("cost on a custom endpoint: %+v", r.Cost)
	}
	e := r.Diagnostics.Efficiency
	if e == nil || e.Per1k == nil || *e.Per1k <= 0 || len(r.ArgsHash) != 64 || r.Timings.SessionMs < r.Timings.WallMs {
		t.Fatalf("report %+v", r)
	}
	// The run's id is its journal's (without one, the attempt's); every
	// time has six fraction digits.
	if r.Run == nil || len(r.Run.ID) != 32 || (r.Run.ID == r.Attempt.ID) != (len(unjournaled) > 0) || !regexp.MustCompile(`^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z$`).MatchString(r.Timings.StartedAt) {
		t.Fatalf("run %+v, attempt %+v, started_at %q", r.Run, r.Attempt, r.Timings.StartedAt)
	}
	// The progress file's last snapshot: the run's final counts, done.
	var p listing.ProgressSnapshot
	readJSON(t, filepath.Join(ds, progressName), &p)
	if p.ProgressVersion != listing.ProgressVersion || p.State != listing.StateDone || p.AttemptID != r.Attempt.ID || p.RunID != r.Run.ID ||
		p.Rows != r.Output.RowsFetched || p.Requests != r.Requests.HTTPAttempts {
		t.Fatalf("progress.json %+v, report %+v", p, r)
	}
	if out != "" || !strings.Contains(stderr.String(), "complete: 3 objects, 9 B in ") || !strings.Contains(stderr.String(), "API calls") {
		t.Fatalf("stdout %q stderr %q", out, stderr.String())
	}
	again, _, _ := list("s3://bkt")
	other, _, _ := list("s3://bkt/a/")
	if again.ArgsHash != r.ArgsHash || other.ArgsHash == r.ArgsHash {
		t.Fatalf("args_hash %s %s %s", r.ArgsHash, again.ArgsHash, other.ArgsHash)
	}
	// progress.json's request fields.
	s, _ := json.Marshal(snap())
	for _, k := range []string{`"http_attempts":2500`, `"inflight":17`, `"limit":0`, `"retries":0`, `"rows_retained":1000`, `"rate_per_1k_usd":0.005`} {
		if !bytes.Contains(s, []byte(k)) {
			t.Fatalf("progress.json lacks %s: %s", k, s)
		}
	}
}

// TestStopSource: the outcome names what stopped a run.
func TestStopSource(t *testing.T) {
	err := errors.New("stopped")
	for cause, want := range map[error]string{errDeadline: "timebox", &stuckError{stuckNoRows}: "liveness_watchdog", &spillWaitError{"spill cap too small"}: "spill_wait",
		&signalError{syscall.SIGTERM}: "sigterm", &signalError{os.Interrupt}: "sigint"} {
		if got := classify(err, cause, "").StopSource; got != want {
			t.Errorf("%v: %q, want %q", cause, got, want)
		}
	}
	if classify(err, nil, "").StopSource != "" {
		t.Error("a failure names a stop source")
	}
}

// TestRetrying: the progress line's failing cause (failureWatch.retrying)
// is the cause every request fails with: at once while nothing was ever
// answered, after retryingFor once something was, and none after an
// answer; its host is left out.
func TestRetrying(t *testing.T) {
	w := newFailureWatch("S3", 0)
	at := time.Now()
	refused := source.Exchange{URL: &url.URL{Host: "127.0.0.1:1"}, Err: &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}}
	w.exchange(refused, at)
	if got := w.retrying(at); got != syscall.ECONNREFUSED.Error() {
		t.Fatalf("nothing answered: %q", got)
	}
	w.exchange(source.Exchange{URL: refused.URL, Status: 200}, at)
	if got := w.retrying(at); got != "" {
		t.Fatalf("after an answer: %q", got)
	}
	w.exchange(refused, at)
	if got := w.retrying(at.Add(time.Second)); got != "" {
		t.Fatalf("a moment's failure after answers: %q", got)
	}
	if got := w.retrying(at.Add(retryingFor)); got != syscall.ECONNREFUSED.Error() {
		t.Fatalf("a lasting failure after answers: %q", got)
	}
}

// TestDuDatasetHint: swath du DIR on a dataset directory totals DIR's
// files, and says on stderr how to total what the dataset lists.
func TestDuDatasetHint(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	ds := filepath.Join(t.TempDir(), "ds")
	if err := run(context.Background(), []string{"-o", ds, src}, io.Discard, false); err != nil {
		t.Fatal(err)
	}
	stderr := captureStderr(t)
	var out bytes.Buffer
	if err := runDu(context.Background(), []string{ds}, &out); err != nil {
		t.Fatal(err)
	}
	if want := "hint: " + ds + " is a swath dataset; to total what it lists: swath du --from-dataset " + shellOperand(ds) + "\n"; !strings.Contains(stderr.String(), want) || out.Len() == 0 {
		t.Fatalf("stderr %q", stderr.String())
	}
}

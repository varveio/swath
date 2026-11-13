// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/internal/listing"
	"github.com/varveio/swath/internal/source"
)

// The event log is every line swath writes to stderr outside the redrawn
// progress line and the statistics block: warnings, the start line,
// progress records, the request log (-vv, -vvv) and the command's end.
// --log-format text prints them as prose, json as one object per line
// (docs/reference/log-events.md).

// The --log-format values.
const (
	logText = "text"
	logJSON = "json"
)

// Events and levels.
const (
	evStart    = "start"
	evProgress = "progress"
	evWarning  = "warning"
	evRequest  = "request"
	evRetry    = "retry"
	evOutcome  = "outcome"

	lvDebug = "debug"
	lvInfo  = "info"
	lvWarn  = "warn"
	lvError = "error"
)

// eventLog renders events in one format at one verbosity.
type eventLog struct {
	json           bool
	verbose, quiet int
	attempt        string // the run's attempt id ("" before a run has one)
}

var logs atomic.Pointer[eventLog]

// logClock is the events' time (tests fix it).
var logClock = time.Now

// tsLayout is an event's ts: RFC 3339 in UTC, to the millisecond.
const tsLayout = "2006-01-02T15:04:05.000Z07:00"

// elog is the log in force: text at the default verbosity until a
// command line sets it (setLog).
func elog() *eventLog {
	if l := logs.Load(); l != nil {
		return l
	}
	return &eventLog{}
}

// setLog sets the log from the terminal flags; attempt is the run's id.
func setLog(t termOptions, attempt string) {
	logs.Store(&eventLog{json: t.logFormat == logJSON, verbose: t.verbose, quiet: t.quiet, attempt: attempt})
}

// shows says whether an event at level is printed: errors unless -qq,
// warnings unless -q, info from -v, debug from -vv.
func (l *eventLog) shows(level string) bool {
	switch level {
	case lvError:
		return l.quiet < 2
	case lvWarn:
		return l.quiet == 0
	case lvInfo:
		return l.quiet == 0 && l.verbose >= 1
	}
	return l.quiet == 0 && l.verbose >= 2
}

// event prints an event its level shows.
func (l *eventLog) event(level, event, msg string, fields any) {
	if l.shows(level) {
		l.print(level, event, msg, fields)
	}
}

// warn prints a warning.
func (l *eventLog) warn(msg string) { l.event(lvWarn, evWarning, msg, nil) }

// print writes an event: in text "swath: MSG" ("swath: warning: MSG" for
// a warning), in json its object (line).
func (l *eventLog) print(level, event, msg string, fields any) {
	send := note
	if level == lvWarn || level == lvError {
		send = noteKept // a warning or an error outlasts the request log
	}
	if l.json {
		send(errOut, l.line(level, event, msg, fields))
		return
	}
	p := "swath: "
	if level == lvWarn {
		p += "warning: "
	}
	send(errOut, p+msg+"\n")
}

// line is an event as one JSON object: ts, level, event, attempt, msg,
// then the members of fields (a struct or map marshalled as an object).
func (l *eventLog) line(level, event, msg string, fields any) string {
	str := func(b []byte, k, v string) []byte {
		return append(append(b, `,"`+k+`":`...), marshal(v)...)
	}
	b := []byte(`{"ts":"` + logClock().UTC().Format(tsLayout) + `"`)
	b = str(b, "level", level)
	b = str(b, "event", event)
	if l.attempt != "" {
		b = str(b, "attempt", l.attempt)
	}
	b = str(b, "msg", msg)
	if f := marshal(fields); len(f) > 2 && f[0] == '{' {
		return string(append(append(b, ','), f[1:]...)) + "\n"
	}
	return string(append(b, '}')) + "\n"
}

// marshal is v as JSON, with & < > as they are (a query, a path).
func marshal(v any) []byte {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if enc.Encode(v) != nil {
		return nil
	}
	return bytes.TrimSuffix(b.Bytes(), []byte("\n"))
}

// The start event's modes: what a run writes.
const (
	modeRows    = "rows"    // text rows on stdout
	modeFile    = "file"    // text rows into a file
	modeDataset = "dataset" // a dataset directory
	modeTotals  = "totals"  // swath du's totals on stdout
	modeCount   = "count"   // nothing: swath diag count
)

// startFields are a start event's: the source, what the run writes (mode)
// and where (destination: a path, "stdout", or none).
type startFields struct {
	Source      string `json:"source"`
	Mode        string `json:"mode"`
	Destination string `json:"destination,omitempty"`
}

// words are where the rows go as the text start line says it.
func (f startFields) words() string {
	switch f.Mode {
	case modeTotals:
		return "as totals"
	case modeCount:
		return "as a count"
	}
	return "to " + f.Destination
}

// start prints the start line (-v): the source, where its rows go (to
// stdout, to DIR, to FILE, as totals, as a count) and the attempt.
func (l *eventLog) start(f startFields) {
	if !l.shows(lvInfo) {
		return
	}
	msg := "listing " + f.Source + " " + f.words()
	if l.json {
		l.print(lvInfo, evStart, msg, f)
		return
	}
	note(errOut, "swath: "+msg+" (attempt "+l.attempt+")\n")
}

// startOf is a run's start event: src, and what it writes where.
func startOf(src string, o options, dir string) startFields {
	switch {
	case o.totals != nil:
		return startFields{src, modeTotals, "stdout"}
	case o.countOnly:
		return startFields{src, modeCount, ""}
	case o.text != nil && o.text.Path == "":
		return startFields{src, modeRows, "stdout"}
	case o.text != nil:
		return startFields{src, modeFile, o.text.Path}
	}
	return startFields{src, modeDataset, dir}
}

// progress prints a progress record: the snapshot's fields after msg,
// the record's text.
func (l *eventLog) progress(s listing.ProgressSnapshot, msg string) {
	l.print(lvInfo, evProgress, msg, s)
}

// outcomeFields are an outcome event's.
type outcomeFields struct {
	Class     string `json:"class"`
	ExitCode  int    `json:"exit_code"`
	Resumable bool   `json:"resumable"`
	Cause     string `json:"cause,omitempty"`
	Message   string `json:"message,omitempty"`
}

// conclude ends a command on stderr and returns its exit code. In text a
// failure prints "swath: MESSAGE" (and a usage error's help hint), with
// -v the outcome as one JSON line before it; in json the outcome event
// ends every run, and any failure. -qq prints nothing.
func conclude(err error) int {
	if errors.Is(err, pflag.ErrHelp) {
		err = nil
	}
	l := elog()
	o := outcome{Class: listing.ClassCompleted, ExitCode: exitCompleted}
	var ee *exitError
	var u usageError
	switch {
	case err == nil:
	case errors.As(err, &ee):
		o = ee.out
	case errors.As(err, &u):
		o = outcome{Class: listing.ClassUsage, ExitCode: exitUsage, Message: err.Error()}
	default:
		o = outcome{Class: listing.ClassFatal, ExitCode: exitFatal, Message: err.Error()}
	}
	if cr := (*credentialRefusal)(nil); errors.As(err, &cr) && !l.shows(lvInfo) {
		o.Message = cr.Error() // the cause is -v's
	}
	if l.quiet >= 2 {
		return o.ExitCode
	}
	if l.json {
		if err != nil || l.attempt != "" {
			level, msg := lvInfo, o.Class
			if err != nil {
				level, msg = lvError, err.Error()
			}
			noteFinal(errOut, l.line(level, evOutcome, msg, outcomeFields{o.Class, o.ExitCode, o.Resumable, o.Cause, o.Message}))
		}
		return o.ExitCode
	}
	if err == nil {
		return o.ExitCode
	}
	if ee != nil && l.shows(lvInfo) {
		b, _ := json.Marshal(o)
		noteFinal(errOut, fmt.Sprintf("swath: outcome: %s\n", b))
	}
	noteFinal(errOut, "swath: "+err.Error()+"\n")
	var sh seeHelp
	if errors.As(err, &sh) {
		noteFinal(errOut, sh.see()+"\n")
	}
	return o.ExitCode
}

// persistFor is how long requests must fail with one cause, none
// answered, before the warning; persistEvery spaces its repeats while the
// failure lasts (tests shorten both).
var persistFor, persistEvery = 10 * time.Second, time.Minute

// failureWatch warns of a persistent request failure (D2 of the CLI
// review): one transport or service cause failing every request for
// persistFor names itself and the endpoint host, without -v.
type failureWatch struct {
	provider string       // the registry's Title: S3, GCS, Azure Blob
	answers  atomic.Int64 // answered exchanges: a streak lasts while none comes
	warnings atomic.Int64 // warnings printed

	mu      sync.Mutex
	streaks map[string]*streak // by "HOST: CAUSE"
}

// streak is one cause's failures since the last answered exchange.
type streak struct {
	first, warned time.Time
	n             int64
	answers       int64 // failureWatch.answers when it began
}

// maxStreaks bounds the causes a watch holds at once.
const maxStreaks = 64

func newFailureWatch(provider string) *failureWatch {
	return &failureWatch{provider: provider, streaks: map[string]*streak{}}
}

// exchangeCause is a failed exchange's cause ("" for an answer): the
// innermost transport error, or a 5xx or 429 status.
func exchangeCause(e source.Exchange) string {
	switch {
	case e.Err != nil:
		err := e.Err
		for u := errors.Unwrap(err); u != nil; u = errors.Unwrap(err) {
			err = u
		}
		return err.Error()
	case e.Status >= 500 || e.Status == http.StatusTooManyRequests:
		return fmt.Sprintf("%d %s", e.Status, http.StatusText(e.Status))
	}
	return ""
}

// exchange takes one exchange at now: an answer (2xx) ends every streak,
// a failure extends its cause's and warns when it is due.
func (w *failureWatch) exchange(e source.Exchange, now time.Time) {
	if e.Err != nil && errors.Is(e.Err, context.Canceled) {
		return // the run stopping
	}
	cause := exchangeCause(e)
	if cause == "" {
		if e.Err == nil && e.Status/100 == 2 {
			w.answers.Add(1)
		}
		return
	}
	host := ""
	if e.URL != nil {
		host = e.URL.Host
	}
	key := host + ": " + cause
	w.mu.Lock()
	answers := w.answers.Load()
	s := w.streaks[key]
	if s == nil || s.answers != answers {
		if len(w.streaks) >= maxStreaks {
			clear(w.streaks)
		}
		s = &streak{first: now, answers: answers}
		w.streaks[key] = s
	}
	s.n++
	due := now.Sub(s.first) >= persistFor && (s.warned.IsZero() || now.Sub(s.warned) >= persistEvery)
	if due {
		s.warned = now
	}
	n := s.n
	w.mu.Unlock()
	if l := elog(); due && l.shows(lvWarn) {
		w.warnings.Add(1)
		l.event(lvWarn, evWarning, fmt.Sprintf("%s %s (retrying; %d attempts)", w.provider, key, n),
			failureFields{host, cause, n})
	}
}

// failureFields are a persistent-failure warning's.
type failureFields struct {
	Host     string `json:"host"`
	Cause    string `json:"cause"`
	Attempts int64  `json:"attempts"`
}

// outstanding names the cause that has failed every request for at least
// persistFor up to now, or for any time when no request was ever
// answered, the one with the most attempts ("" when none).
func (w *failureWatch) outstanding(now time.Time) string {
	if w == nil {
		return ""
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	answers := w.answers.Load()
	best, n := "", int64(0)
	for k, s := range w.streaks {
		if s.answers == answers && (answers == 0 || now.Sub(s.first) >= persistFor) && s.n > n {
			best, n = k, s.n
		}
	}
	if best == "" {
		return ""
	}
	return fmt.Sprintf("%s %s (%d attempts)", w.provider, best, n)
}

// requestFields are a request event's.
type requestFields struct {
	Op        string  `json:"op"`
	Host      string  `json:"host"`
	Path      string  `json:"path"`
	Status    int     `json:"status,omitempty"`
	Cause     string  `json:"cause,omitempty"`
	LatencyMs float64 `json:"latency_ms"`
}

// request logs one exchange: every one with -vvv, a failed one (no
// answer, or a status of 400 or more) with -vv.
func (l *eventLog) request(e source.Exchange) {
	failed := e.Err != nil || e.Status >= 400
	if l.quiet > 0 || l.verbose < 2 || (l.verbose == 2 && !failed) {
		return
	}
	f := requestFields{Op: e.Method, Status: e.Status, LatencyMs: float64(e.Latency.Microseconds()) / 1000}
	if e.URL != nil {
		f.Host, f.Path = e.URL.Host, requestPath(e.URL)
	}
	what := fmt.Sprint(e.Status)
	if e.Err != nil {
		f.Cause = exchangeCause(e)
		what = f.Cause
	}
	l.print(lvDebug, evRequest, fmt.Sprintf("request %s %s%s: %s (%s)", f.Op, f.Host, f.Path, what, e.Latency.Round(time.Millisecond)), f)
}

// retryFields are a retry event's: the request's host and path (redacted
// as a request event's) match it to its request event.
type retryFields struct {
	Host   string `json:"host,omitempty"`
	Path   string `json:"path,omitempty"`
	Try    int    `json:"try"` // the call's attempt, from 1 (attempt is the run's id)
	Class  string `json:"class"`
	Status int    `json:"status,omitempty"`
	Code   string `json:"code,omitempty"`
	Cause  string `json:"cause"`
	Retry  bool   `json:"retry"`
	WaitMs int64  `json:"wait_ms,omitempty"`
}

// retry logs a failed attempt (-vv): retried, throttled or the last.
func (l *eventLog) retry(f source.Failed) {
	if !l.shows(lvDebug) {
		return
	}
	r := retryFields{Try: f.Attempt, Class: f.Class, Status: f.Status, Code: f.Code, Retry: f.Retry, WaitMs: f.Wait.Milliseconds()}
	if f.Cause != nil {
		r.Cause = f.Cause.Error()
	}
	on := ""
	if f.URL != nil {
		r.Host, r.Path = f.URL.Host, requestPath(f.URL)
		on = " " + r.Host + r.Path
	}
	next := "not retried"
	if f.Retry {
		next = "retrying in " + f.Wait.Round(time.Millisecond).String()
	}
	l.print(lvDebug, evRetry, fmt.Sprintf("attempt %d%s failed (%s): %s; %s", f.Attempt, on, r.Class, r.Cause, next), r)
}

// watchRequests installs the request log and the persistent-failure
// watch on every client (source.SetWatch) for one run; stop removes them.
func watchRequests(w *failureWatch) (stop func()) {
	source.SetWatch(&source.Watch{
		Exchange: func(e source.Exchange) {
			w.exchange(e, time.Now())
			elog().request(e)
		},
		Failed: func(f source.Failed) { elog().retry(f) },
	})
	return func() { source.SetWatch(nil) }
}

// shownQuery are the query parameters a request line shows: where and how
// much a listing asks for. Any other (a token, a credential, an Azure SAS,
// a continuation token) is [redacted], name and value.
var shownQuery = []string{"comp", "delimiter", "endOffset", "include", "list-type", "max-keys", "maxResults", "maxresults", "prefix", "restype", "start-after", "startOffset", "versions"}

// requestPath is a request's path and query as the request log shows it.
func requestPath(u *url.URL) string {
	p := u.EscapedPath()
	q := u.Query()
	if len(q) == 0 {
		return p
	}
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	var b strings.Builder
	for i, k := range keys {
		sep := "&"
		if i == 0 {
			sep = "?"
		}
		for _, v := range q[k] {
			if slices.Contains(shownQuery, k) {
				b.WriteString(sep + url.QueryEscape(k) + "=" + url.QueryEscape(v))
			} else {
				b.WriteString(sep + redacted) // name and value: a malformed SAS (?zSECRET) is all name
			}
			sep = "&"
		}
	}
	return p + b.String()
}

// unreadableShown is how many unreadable entries a run names on stderr.
const unreadableShown = 5

// unreadableFields are an unreadable-entries warning's.
type unreadableFields struct {
	Unreadable int64    `json:"unreadable"`
	Samples    []string `json:"samples,omitempty"`
}

// reportUnreadable warns of the entries a listing could not read: their
// count, then up to unreadableShown of them with their cause (the
// provider's samples) and the count of the rest.
func reportUnreadable(r *listing.Report, du bool) {
	n := r.Outcome.Unreadable
	l := elog()
	if n == 0 || !l.shows(lvWarn) {
		return
	}
	var samples []string
	if sr := r.SourceReport(); sr != nil {
		if s, ok := sr.Stats.(interface{ ErrorSamples() []string }); ok {
			samples = s.ErrorSamples()
		}
	}
	samples = samples[:min(len(samples), unreadableShown)]
	what, out := "entries", "the output leaves them out"
	if n == 1 {
		what = "entry"
	}
	if du {
		out = "the totals leave them out"
	}
	msg := fmt.Sprintf("%s %s could not be read: %s", count(n), what, out)
	if len(samples) == 0 {
		msg += " (the report's source counters say why)"
	}
	if l.json {
		l.print(lvWarn, evWarning, msg, unreadableFields{n, samples})
		return
	}
	l.warn(msg)
	for _, s := range samples {
		noteKept(errOut, indent+s+"\n")
	}
	if rest := n - int64(len(samples)); rest > 0 && len(samples) > 0 {
		noteKept(errOut, indent+"and "+count(rest)+" more (the report's source counters say why)\n")
	}
}

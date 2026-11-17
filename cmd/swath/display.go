// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/pflag"
	"golang.org/x/term"

	"github.com/varveio/swath/internal/cli"
	"github.com/varveio/swath/internal/inventory"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/textout"
)

// The terminal surface on stderr, as Java swath's: a live progress record
// (redrawn in place on a terminal, appended as lines elsewhere), the
// closing lines (with --stats the run's statistics), verbosity (-v, -q)
// and colour.
// Everything here renders the snapshot the progress file is written from,
// or the final report: nothing reads the listing workers' state.

// tri is a flag pair's value: --x, --no-x or neither (auto).
type tri struct{ set, on bool }

// triFlag is one side of a --x/--no-x pair.
type triFlag struct {
	t   *tri
	neg bool
}

func (f triFlag) Type() string { return "bool" }

func (f triFlag) String() string {
	if f.t == nil || !f.t.set {
		return ""
	}
	return strconv.FormatBool(f.t.on != f.neg)
}

func (f triFlag) Set(v string) error {
	b, err := strconv.ParseBool(v)
	if err != nil {
		return err
	}
	f.t.set, f.t.on = true, b != f.neg
	return nil
}

// termOptions are the terminal flags.
type termOptions struct {
	progress, stats tri
	interval        time.Duration
	verbose, quiet  int
	color           string
	logFormat       string // --log-format: logText or logJSON
}

// termFlags are the terminal flags' names: runtime flags, which swath resume
// accepts again.
var termFlags = []string{"progress", "no-progress", "progress-interval", "stats", "no-stats", "verbose", "quiet", "color", "log-format"}

// globalFlags are the terminal flags also taken before the command word
// (swath -v list ...).
var globalFlags = []string{"verbose", "quiet", "color", "log-format"}

func (t *termOptions) register(f *pflag.FlagSet) {
	g := cli.GroupReporting
	cli.Define(f, cli.Flag{Name: "verbose", Short: "v", Value: cli.Counter(&t.verbose), Group: g,
		Usage: "a start line, progress records; -vv, -vvv: the request log",
		Long:  "-vv adds each failed request and retry with its cause, -vvv every request (credentials and tokens redacted)."})
	cli.Define(f, cli.Flag{Name: "quiet", Short: "q", Value: cli.Counter(&t.quiet), Group: g,
		Usage: "no warnings, no automatic progress or closing lines (-qq: no errors)",
		Long:  "With -qq the exit code and the reports stand alone."})
	cli.Define(f, cli.Flag{Name: "progress", Value: triFlag{&t.progress, false}, Group: g,
		Usage: "show progress on stderr (default: a terminal that rows do not share)",
		Long:  "Not by default with -q or -v (which appends progress records), nor while rows go to the same terminal, where swath list refuses it; swath du's table comes after the line is erased."})
	cli.Define(f, cli.Flag{Name: "no-progress", Value: triFlag{&t.progress, true}, Group: g, Usage: "no live progress on stderr"})
	cli.Define(f, cli.Flag{Name: "progress-interval", Value: cli.Duration(&t.interval), Metavar: "DURATION", Group: g,
		Usage: "how often progress is printed (at least 1s); implies --progress",
		Long:  "Default 1s while the line redraws on a terminal, 30s for appended records."})
	cli.Define(f, cli.Flag{Name: "stats", Value: triFlag{&t.stats, false}, Group: g,
		Usage: "print the closing lines on stderr, with the run's statistics",
		Long:  "The closing lines (the verdict, the count, the time, where the output went, what to run next) print by default for runs over 1.5 s, runs that write output, and runs that do not complete; not with -q, nor after a closed downstream pipe. --stats prints them always, followed by the requests, in flight, throttles, retries, the estimate and the peak RSS."})
	cli.Define(f, cli.Flag{Name: "no-stats", Value: triFlag{&t.stats, true}, Group: g, Usage: "no closing lines on stderr"})
	t.color = "auto"
	cli.Define(f, cli.Flag{Name: "color", Value: cli.Text(&t.color), Metavar: "auto|always|never", Default: "auto", Group: g,
		Usage: "color progress, closing lines, the help and du's view",
		Long:  "auto: on a terminal, without NO_COLOR or TERM=dumb; CLICOLOR_FORCE forces it."})
	t.logFormat = logText
	cli.Define(f, cli.Flag{Name: "log-format", Value: cli.Text(&t.logFormat), Metavar: logText + "|" + logJSON, Default: logText, Group: g,
		Usage: "stderr as text, or json: one JSON object per line",
		Long:  "json: warnings, the start line, progress records, the request log and the outcome as events, no redrawn line or closing lines."})
}

// registerGlobal defines the global flags alone (globalFlags), for a
// command that takes no other terminal flag (show, completion).
func (t *termOptions) registerGlobal(f *pflag.FlagSet) {
	all := pflag.NewFlagSet("", pflag.ContinueOnError)
	t.register(all)
	all.VisitAll(func(fl *pflag.Flag) {
		if slices.Contains(globalFlags, fl.Name) {
			f.AddFlag(fl)
		}
	})
}

func (t *termOptions) validate() error {
	switch t.color {
	case "auto", "always", "never":
	default:
		return fmt.Errorf("--color %q: want auto, always or never", t.color)
	}
	if t.logFormat != logText && t.logFormat != logJSON {
		return fmt.Errorf("--log-format %q: want text or json", t.logFormat)
	}
	if t.interval < 0 {
		return fmt.Errorf("--progress-interval %s: negative", t.interval)
	}
	return nil
}

// minUsableWidth is the narrowest terminal a redrawn line is drawn on (Java's).
const minUsableWidth = 24

// stderrTerminal reports whether fd 2 is a terminal, and its width (0 when
// unknown).
var stderrTerminal = func() (bool, int) {
	fd := int(os.Stderr.Fd())
	if !term.IsTerminal(fd) {
		return false, 0
	}
	w, _, err := term.GetSize(fd)
	if err != nil {
		return true, 0
	}
	return true, w
}

// stdoutTerminal reports whether fd 1 is a terminal (the --format auto choice).
var stdoutTerminal = func() bool { return term.IsTerminal(int(os.Stdout.Fd())) }

// stdoutWidth is the width of the terminal on fd 1, 0 when it is none.
func stdoutWidth() int {
	if !stdoutTerminal() {
		return 0
	}
	w, _, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		return 0
	}
	return w
}

// colorEnabled is Java's resolution: always and never decide; auto is off
// with a non-empty NO_COLOR (no-color.org) or TERM=dumb, on with CLICOLOR_FORCE, else on a terminal.
// env is os.LookupEnv (tests substitute one).
func colorEnabled(mode string, env func(string) (string, bool), tty bool) bool {
	switch mode {
	case "always":
		return true
	case "never":
		return false
	}
	if v, _ := env("NO_COLOR"); v != "" {
		return false
	}
	if t, _ := env("TERM"); t == "dumb" {
		return false
	}
	if _, ok := env("CLICOLOR_FORCE"); ok {
		return true
	}
	return tty
}

// palette styles text when colour is on.
type palette bool

func (p palette) style(code, s string) string {
	if !p || s == "" {
		return s
	}
	return "\x1b[" + code + "m" + s + "\x1b[0m"
}
func (p palette) dim(s string) string    { return p.style("2", s) }
func (p palette) accent(s string) string { return p.style("36", s) }
func (p palette) red(s string) string    { return p.style("31", s) }

const (
	indent = "  "
	sep    = " · "
)

// count formats n with thousands separators, as Java's %,d.
func count(n int64) string {
	s := strconv.FormatInt(n, 10)
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
	var b strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	if neg {
		return "-" + b.String()
	}
	return b.String()
}

// elapsed is Java's duration text: 12.3s, 4m05s, 2h03m.
func elapsed(d time.Duration) string {
	s := int64(d / time.Second)
	switch {
	case s < 60:
		return fmt.Sprintf("%.1fs", float64(d.Milliseconds())/1000)
	case s < 3600:
		return fmt.Sprintf("%dm%02ds", s/60, s%60)
	}
	return fmt.Sprintf("%dh%02dm", s/3600, (s%3600)/60)
}

// plural is the noun for n of it: one or many.
func plural[N int | int64](n N, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

// costText is Java's labelled estimate.
func costText(usd, ratePer1k float64) string {
	a := fmt.Sprintf("~$%.3f", usd)
	if usd < 0.001 {
		a = "<$0.001"
	}
	return a + " (est. @ $" + strconv.FormatFloat(ratePer1k, 'f', -1, 64) + "/1k LIST)"
}

// frame joins a record's fields, the second (the headline figure)
// accented, dropping whole trailing fields that would pass width (0: no
// bound): a wrapped line strands its tail under an in-place redraw.
func frame(parts []string, width int, p palette) string {
	var b strings.Builder
	b.WriteString(indent)
	cols := len(indent)
	for i, f := range parts {
		s := ""
		if i > 0 {
			s = sep
		}
		n := len([]rune(s)) + len([]rune(f))
		if width > 0 && cols+n > width {
			break
		}
		cols += n
		b.WriteString(p.dim(s))
		if i == 1 {
			b.WriteString(p.accent(f))
		} else {
			b.WriteString(p.dim(f))
		}
	}
	if cols == len(indent) && len(parts) > 0 {
		// Too narrow for the phase name: cut it, never wrap.
		r := []rune(indent + parts[0])
		return p.dim(string(r[:min(len(r), max(width, 0))]))
	}
	return b.String()
}

// nouns are what the terminal calls a source's rows and their rate: a
// local tree's are entries (its files, directories and other kinds), an
// object store's objects, listed in keys a second; with --all-versions an
// object store's are rows (versions and delete markers).
type nouns struct{ row, rows, rate string }

func rowNouns(local, versions bool) nouns {
	switch {
	case local:
		return nouns{"entry", "entries", "entries/s"}
	case versions:
		return nouns{"row", "rows", "rows/s"}
	}
	return nouns{"object", "objects", "keys/s"}
}

// of is n rows, counted and named.
func (n nouns) of(rows int64) string { return count(rows) + " " + plural(rows, n.row, n.rows) }

// progressParts is one record's fields, most important first: the phase,
// the rows listed (called by n; a resume's recovered rows apart) and their
// bytes, the elapsed time, the rate (or, while every request fails, the
// cause retried: failing), the parts published for a run that writes a
// dataset, then the requests in flight and sent, and the estimate, for a
// source that makes requests. No percentage or ETA: a listing has no
// honest denominator. No key text.
func progressParts(s inventory.ProgressSnapshot, session time.Duration, live float64, n nouns, dataset bool, failing string) []string {
	phase := "listing"
	switch {
	case s.StartedAt.IsZero():
		phase = "starting"
	case s.Verified > 0 && s.Rows == 0 && s.Requests == 0:
		phase = "verifying"
	}
	parts := []string{phase}
	if phase == "verifying" {
		parts = append(parts, textout.Size(s.Verified)+" verified", elapsed(session)+" elapsed")
	} else {
		rows := n.of(s.Rows)
		if s.Retained > 0 {
			rows += " (+" + count(s.Retained) + " recovered)"
		}
		parts = append(parts, rows)
		if b := s.ObjectBytes; b != nil && *b > 0 {
			parts = append(parts, textout.Size(*b))
		}
		parts = append(parts, elapsed(session)+" elapsed")
		if failing != "" {
			parts = append(parts, "retrying: "+failing)
		} else {
			avg := 0.0
			if s.DurationMs > 0 {
				avg = float64(s.Rows) * 1000 / float64(s.DurationMs)
			}
			parts = append(parts, fmt.Sprintf("%s %s (avg %s)", count(int64(live+0.5)), n.rate, count(int64(avg+0.5))))
		}
		if dataset {
			parts = append(parts, count(s.Parts)+" "+plural(s.Parts, "part", "parts"))
		}
	}
	if s.Requests > 0 {
		parts = append(parts, count(s.Inflight)+" in flight", count(s.Requests)+" "+plural(s.Requests, "API call", "API calls"))
	}
	if s.CostUSD != nil && s.Requests > 0 && s.CostRatePer1kUSD > 0 {
		parts = append(parts, costText(*s.CostUSD, s.CostRatePer1kUSD))
	}
	return parts
}

// display is the live progress record. Samples are offered without
// blocking (a full slot drops the older one), and one goroutine renders
// them, so a stderr nobody drains never holds the watchdog.
type display struct {
	out      *stderrCoord
	redraw   bool
	width    func() int
	pal      palette
	interval time.Duration
	started  time.Time // the invocation's start: the elapsed time shown
	noun     nouns
	dataset  bool // the run writes a dataset: its parts are shown
	erase    bool // rows follow on the same terminal: stop clears the line
	// failing is the cause every request fails with at a moment
	// (failureWatch.retrying), shown in place of the rate; nil: none.
	failing func(time.Time) string

	samples chan inventory.ProgressSnapshot
	stopped chan struct{}
	done    sync.WaitGroup
	once    sync.Once

	last     time.Time // the last record's time
	prevObj  int64
	prevAt   time.Time
	renderMu sync.Mutex
}

// progressPrefs decides whether a run shows live progress and how, as
// Java's: an explicit --progress/--no-progress decides; an explicit
// --progress-interval opts in; otherwise on when stderr is a terminal and
// neither -q nor -v is given, and stdout is not a terminal that carries
// rows (rows): the two share the screen, and a line would land among the
// rows. -v alone shows it as appended records (the verbose log surface),
// under the same rows rule. Lines redraw only on a terminal of usable width
// whose TERM is not dumb.
func progressPrefs(t termOptions, tty, rows bool, width int, termEnv string) (show, redraw bool) {
	switch {
	case t.progress.set:
		show = t.progress.on
	case t.interval > 0:
		show = true
	case t.verbose > 0:
		show = t.quiet == 0 && !rows
	default:
		show = tty && t.quiet == 0 && !rows
	}
	redraw = show && tty && termEnv != "dumb" && width >= minUsableWidth && t.verbose == 0 && enableVT()
	return show, redraw
}

// progressForced says the command line asks for live progress
// (--progress, or --progress-interval without --no-progress), whatever
// shares the terminal.
func (t termOptions) progressForced() bool {
	return t.progress.set && t.progress.on || !t.progress.set && t.interval > 0
}

// progressInterval is the record cadence: the flag, else 1 s redrawing and
// 30 s appended (Java's), never under 1 s.
func progressInterval(t termOptions, redraw bool) time.Duration {
	d := t.interval
	if d == 0 {
		d = 30 * time.Second
		if redraw {
			d = time.Second
		}
	}
	return max(d, time.Second)
}

func newDisplay(out *stderrCoord, redraw bool, width func() int, pal palette, interval time.Duration, started time.Time, noun nouns, dataset bool, failing func(time.Time) string) *display {
	d := &display{out: out, redraw: redraw, width: width, pal: pal, interval: interval, started: started, noun: noun, dataset: dataset, failing: failing,
		samples: make(chan inventory.ProgressSnapshot, 1), stopped: make(chan struct{})}
	d.done.Add(1)
	go d.loop()
	return d
}

// retrying is the cause every request fails with at now ("" when none).
func (d *display) retrying(now time.Time) string {
	if d.failing == nil {
		return ""
	}
	return d.failing(now)
}

// offer hands the display a sample; it never blocks.
func (d *display) offer(s inventory.ProgressSnapshot) {
	if d == nil {
		return
	}
	for {
		select {
		case d.samples <- s:
			return
		default:
		}
		select {
		case <-d.samples: // drop the older sample
		default:
		}
	}
}

func (d *display) loop() {
	defer d.done.Done()
	for {
		select {
		case <-d.stopped:
			return
		case s := <-d.samples:
			d.render(s, time.Now())
		}
	}
}

// render writes a record if one is due at now.
func (d *display) render(s inventory.ProgressSnapshot, now time.Time) {
	d.renderMu.Lock()
	defer d.renderMu.Unlock()
	if !d.last.IsZero() && now.Sub(d.last) < d.interval-d.interval/20 {
		return
	}
	live := 0.0
	if !d.prevAt.IsZero() && now.After(d.prevAt) {
		live = float64(s.Rows-d.prevObj) / now.Sub(d.prevAt).Seconds()
	} else if s.DurationMs > 0 {
		live = float64(s.Rows) * 1000 / float64(s.DurationMs)
	}
	d.last, d.prevObj, d.prevAt = now, s.Rows, now
	parts := progressParts(s, now.Sub(d.started), max(live, 0), d.noun, d.dataset, d.retrying(now))
	if l := elog(); l.json {
		l.progress(s, strings.Join(parts, sep))
		return
	}
	if d.redraw {
		d.out.frame(frame(parts, d.width(), d.pal))
	} else {
		d.out.record(frame(parts, 0, d.pal) + "\n")
	}
}

// stop ends the display: the last redrawn line stays, ended by a newline,
// so what follows on stderr (the statistics, an error) starts on a clean
// line; with erase, rows follow on the same terminal (swath du's table)
// and the line is cleared instead.
func (d *display) stop() {
	if d == nil {
		return
	}
	d.once.Do(func() {
		close(d.stopped)
		d.done.Wait()
		if d.erase {
			d.out.erase()
		} else {
			d.out.finish()
		}
	})
}

// eraseLine clears the whole terminal line (ESC [2K); a frame is drawn
// after a carriage return.
const eraseLine = "\x1b[2K"

// stderrCoord serializes stderr: a redrawn progress line is erased before
// any other write and painted again after it, so warnings never land in
// the middle of a frame. Every write is bounded (note).
type stderrCoord struct {
	w    io.Writer
	mu   sync.Mutex
	line string // the frame on screen ("" when none)
	done bool   // progress ended: no frame is drawn again
}

// Write writes p as a disposable line (send); the command's own lines
// reach send with their class, which the coordinator keeps (put).
func (c *stderrCoord) Write(p []byte) (int, error) {
	c.put(string(p), disposable)
	return len(p), nil
}

// put writes s, of class, around the frame on screen.
func (c *stderrCoord) put(s string, class lineClass) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.line != "" {
		s = "\r" + eraseLine + s + c.line
	}
	send(c.w, s, class)
}

// frame draws a redrawn progress line in place.
func (c *stderrCoord) frame(line string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.done {
		return
	}
	c.line = line
	note(c.w, "\r"+eraseLine+line)
}

// record appends a progress record.
func (c *stderrCoord) record(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.done {
		note(c.w, s)
	}
}

// finish ends the progress line for good: the last record stays, and a
// newline follows it.
func (c *stderrCoord) finish() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.line != "" {
		note(c.w, "\n")
	}
	c.line, c.done = "", true
}

// erase ends the progress line for good and clears it, the cursor at the
// start of the empty line.
func (c *stderrCoord) erase() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.line != "" {
		note(c.w, "\r"+eraseLine)
	}
	c.line, c.done = "", true
}

// autoStatsMin is Java's threshold: a run longer than this prints its
// statistics without --stats.
const autoStatsMin = 1500 * time.Millisecond

// showStats is Java's gating: --stats/--no-stats decide; otherwise not with
// -q nor after a closed downstream pipe, and on for a run over 1.5 s, one
// that wrote output, or one that did not complete.
func showStats(t termOptions, r *inventory.Report, brokenPipe bool) bool {
	switch {
	case t.stats.set:
		return t.stats.on
	case t.quiet > 0 || brokenPipe:
		return false
	}
	return time.Duration(r.Timings.SessionMs)*time.Millisecond > autoStatsMin ||
		r.Output.PartsWritten+r.Output.PartsRetained > 0 || (r.Output.Text != nil && r.Output.Text.Destination != "-" && r.Output.Text.Bytes > 0) ||
		r.Outcome.ExitCode != exitOK
}

// resumableStops are the stops whose closing lines end with resume advice.
var resumableStops = map[string]bool{"interrupted": true, "terminated": true, "deadline": true, "stuck": true, "disk_full": true}

// coverageVerdict is the verdict on an output's coverage, in the one
// vocabulary a person reads a run or a dataset in (the closing block,
// swath show, swath du --from-dataset): "complete" when nothing is
// missing, else "partial: " and what is.
func coverageVerdict(complete bool, stopReason string, unreadable int64) string {
	switch {
	case unreadable > 0:
		return "partial: " + plural(unreadable, "1 unreadable entry", count(unreadable)+" unreadable entries")
	case complete:
		return "complete"
	}
	switch stopReason {
	case source.StopMaxKeys:
		return "partial: --max-keys reached"
	case inventory.StopDownstreamClosed:
		return "partial: the reader closed the output"
	case "", source.StopEndOfScope:
		return "partial: the listing did not reach the end of its scope"
	}
	return "partial: the listing stopped early (" + stopReason + ")"
}

// runVerdict is a run's verdict line: its coverage when it wrote its
// output, else how it stopped, with the cause.
func runVerdict(o inventory.Outcome) string {
	switch o.Class {
	case inventory.ClassOK, inventory.ClassIncomplete:
		return coverageVerdict(o.Complete, o.StopReason, o.Unreadables())
	case inventory.ClassDeadline:
		return "stopped: --max-duration reached"
	case inventory.ClassStuck:
		if o.Cause == source.CauseUnreachable {
			return "stopped: the endpoint cannot be reached"
		}
		return "stopped: no progress within --stall-timeout"
	case inventory.ClassUnresolved:
		return "stopped: the bucket commit is unresolved"
	case inventory.ClassDiskFull:
		if o.Cause == causeSpillBudget {
			return "stopped: --spill-wait-timeout reached"
		}
		return "stopped: the disk is full"
	case inventory.ClassInterrupted:
		return "interrupted"
	case inventory.ClassTerminated:
		return "interrupted: " + strings.ToUpper(strings.TrimSuffix(o.StopSource, "+forced_exit"))
	case inventory.ClassRefused, inventory.ClassUsage:
		return "refused"
	}
	return "failed"
}

// rateMin is the shortest run whose closing line gives a rate: below it
// the rate is mostly the setup's and the clock's resolution.
const rateMin = 2 * time.Second

// statsLines is the closing block: the result line, then a next line
// (closingNext), then with detail (--stats) the run's statistics
// (closingDetail). The result line is the verdict, what the run counted
// (closingCount) and the bytes listed when known, the time the
// invocation took and, from rateMin on, the rate over that time, and
// where the output went (closingDest).
func statsLines(r *inventory.Report, brokenPipe bool, n nouns, detail bool) []string {
	o := r.Outcome
	verdict := ""
	switch {
	case brokenPipe:
		verdict = coverageVerdict(false, inventory.StopDownstreamClosed, 0)
	case o.ExitCode == exitOK && (o.Class == "" || o.Class == inventory.ClassOK):
		verdict = coverageVerdict(o.Complete, o.StopReason, o.Unreadables())
	default:
		verdict = runVerdict(o)
		if o.Class == inventory.ClassRefused || o.Class == inventory.ClassUsage {
			// Refused before it ran: the verdict is the whole record.
			return []string{verdict}
		}
	}
	took := time.Duration(r.Timings.SessionMs) * time.Millisecond
	if took == 0 {
		took = time.Duration(r.Timings.WallMs) * time.Millisecond
	}
	head := closingCount(r, n) + " in " + elapsed(took)
	if fetched := r.Output.RowsFetched; took >= rateMin && fetched > 0 {
		head += sep + count(int64(float64(fetched)/took.Seconds()+0.5)) + " " + n.rate
	}
	joiner := ": "
	if strings.Contains(verdict, ":") {
		joiner = "; "
	}
	lines := []string{verdict + joiner + head + closingDest(r)}
	if next := closingNext(r); next != "" {
		lines = append(lines, "next: "+next)
	}
	if detail {
		lines = append(lines, closingDetail(r)...)
	}
	return lines
}

// saved says a dataset run stopped with parts kept for swath resume.
func saved(r *inventory.Report) bool {
	o := r.Outcome
	return r.Output.Text == nil && r.Invocation.Out != "" && o.Status != inventory.StatusCommitted && o.Resumable && resumableStops[o.Class]
}

// closingCount is what a run counted, for the result line: under swath du
// the total row's files and directories (or objects), for a dataset run
// that stopped the rows saved for swath resume beside those listed, for a
// resume the dataset's rows with those from earlier attempts apart;
// otherwise the rows listed. The rows the filters dropped follow the ones
// kept, and the bytes listed (a dataset's, or the total row's size) when
// they are known.
func closingCount(r *inventory.Report, n nouns) string {
	out := &r.Output
	if saved(r) {
		return count(out.RowsDurable) + " saved, " + count(out.RowsFetched+out.RowsRetained) + " listed"
	}
	var s string
	var bytes *int64
	switch t := out.Totals; {
	case t != nil && n == rowNouns(true, false): // a local tree's, as its table counts them
		s = count(t.Objects) + " " + plural(t.Objects, "file", "files")
		if t.Dirs > 0 {
			s += ", " + count(t.Dirs) + " " + plural(t.Dirs, "directory", "directories")
		}
	case t != nil:
		s = n.of(t.Objects)
	case out.RowsCommitted > 0:
		s = n.of(out.RowsCommitted)
	default:
		s = n.of(out.RowsEmitted + out.RowsRetained)
	}
	// Bytes only where every size counted is known: a sum short of an
	// unknown size is no total.
	switch t := out.Totals; {
	case t != nil:
		if t.Size != nil && t.SizeMissing == 0 {
			bytes = t.Size
		}
	case r.Outcome.Status == inventory.StatusCommitted:
		bytes = out.CommittedObjectBytes
	case out.RowsRetained == 0:
		bytes = out.ObjectBytes
	}
	if out.RowsRetained > 0 {
		s += " (" + count(out.RowsRetained) + " from earlier attempts)"
	}
	switch {
	case out.RowsFiltered > 0 && out.Totals != nil:
		// The totals count files once however many names they have: the
		// filters' account is in rows.
		s += " (" + n.of(out.RowsEmitted) + " kept, " + count(out.RowsFiltered) + " filtered out)"
	case out.RowsFiltered > 0:
		s += " kept, " + count(out.RowsFiltered) + " filtered out"
	}
	if bytes != nil {
		s += ", " + textout.Size(*bytes)
	}
	return s
}

// closingDest is where the output went: " → " a dataset with its parts and
// their bytes, or a text file with its bytes; nothing for stdout, swath
// du's rows and a count. A text file that a stopped run discarded says so.
func closingDest(r *inventory.Report) string {
	out, o := &r.Output, r.Outcome
	if t := out.Text; t != nil {
		switch {
		case t.Destination == "-":
			return ""
		case o.Status == inventory.StatusWritten:
			return " → " + escapeControl(t.Destination) + " (" + textout.Size(t.Bytes) + ")"
		}
		return ", no file written"
	}
	dir := r.Invocation.Out
	if dir == "" || o.Status != inventory.StatusCommitted && !saved(r) {
		return ""
	}
	if rem := out.Remote; rem != nil && o.Status == inventory.StatusCommitted {
		dir = rem.Destination
	}
	parts := int64(out.PartsWritten + out.PartsRetained)
	if parts == 0 {
		return " → " + escapeControl(dir)
	}
	return " → " + escapeControl(dir) + " (" + count(parts) + " " + plural(parts, "part", "parts") + ", " + textout.Size(out.OutputBytes) + ")"
}

// closingNext is the commands a person runs next: swath resume for a
// stopped run that can resume, swath show and swath du --from-dataset
// for a committed dataset (swath show alone for one in a bucket, given
// its work directory); "" otherwise.
func closingNext(r *inventory.Report) string {
	o, dir := r.Outcome, r.Invocation.Out
	switch {
	case dir == "" || r.Output.Text != nil:
		return ""
	case o.Resumable && resumableStops[o.Class]:
		return "swath resume " + shellOperand(dir)
	case o.Status != inventory.StatusCommitted:
		return ""
	case r.Output.Remote != nil:
		return "swath show " + shellOperand(dir)
	}
	return "swath show " + shellOperand(dir) + sep + "swath du --from-dataset " + shellOperand(dir)
}

// closingDetail is the run's statistics (--stats): the requests and the
// mean and peak in flight, the throttles and retries when any, the
// estimate, and the peak RSS.
func closingDetail(r *inventory.Report) []string {
	var lines []string
	q := r.Live()
	if r.Requests != nil && r.Requests.HTTPAttempts > 0 {
		avg := 0.0
		if ms := r.Timings.ListingMs; ms > 0 {
			avg = float64(q.InflightNs) / float64(ms*1e6)
		}
		n := r.Requests.HTTPAttempts
		lines = append(lines, fmt.Sprintf("%s %s%sin flight avg %.2f%speak %s",
			count(n), plural(n, "API call", "API calls"), sep, avg, sep, count(q.InflightMax)))
	}
	var faults []string
	if rq := r.Requests; rq != nil && rq.Throttles > 0 {
		faults = append(faults, "throttled "+count(rq.Throttles))
	}
	if rq := r.Requests; rq != nil && rq.Retries > 0 {
		faults = append(faults, "retried "+count(rq.Retries))
	}
	if len(faults) > 0 {
		lines = append(lines, strings.Join(faults, sep))
	}
	if c := r.Cost; c != nil && c.CostUSD != nil && c.Basis != nil && r.Requests != nil && r.Requests.HTTPAttempts > 0 {
		lines = append(lines, costText(*c.CostUSD, c.Basis.RatePer1kUSD))
	}
	if rss := r.Process.PeakRSSBytes; rss != nil && *rss > 0 {
		lines = append(lines, "peak RSS "+textout.Size(*rss))
	}
	return lines
}

// writeStats prints the block, indented: the result line red when the run
// failed or its output is partial (alarm), else accented; the rest dim.
func writeStats(w io.Writer, lines []string, pal palette, alarm bool) {
	var b strings.Builder
	for i, l := range lines {
		s := pal.dim(l)
		switch {
		case i == 0 && alarm:
			s = pal.red(l)
		case i == 0:
			s = pal.accent(l)
		}
		b.WriteString(indent + s + "\n")
	}
	noteKept(w, b.String()) // the run's account, which a partial or failed run needs read
}

// termUI is one run's stderr surface: the progress display (nil when
// off) and the statistics' preferences.
type termUI struct {
	opt     termOptions
	pal     palette
	disp    *display
	prev    io.Writer // errOut before the run
	coord   *stderrCoord
	noun    nouns
	dataset bool                   // the run writes a dataset (its progress shows parts)
	began   time.Time              // the invocation's start
	failing func(time.Time) string // display.failing
	tty     bool                   // stderr is a terminal (and not --log-format json)
	// saveHint, for a live swath du, is the commands that save its source
	// once and total the saved rows (savedInventory): the statistics end
	// with them after a large listing (hintRows).
	saveHint []string
}

// startTerminal resolves the terminal preferences for a run and starts its
// progress display; -v prints a start line, saying where the rows go (to).
// rows says stdout is a terminal that carries the run's rows
// (progressPrefs). Under --log-format json nothing is redrawn or coloured:
// progress records are appended events.
func startTerminal(t termOptions, att *inventory.Attempt, start startFields, dataset bool, noun nouns, rows bool, failing func(time.Time) string) *termUI {
	tty, width := stderrTerminal()
	json := t.logFormat == logJSON
	if json {
		tty, width = false, 0
	}
	tt := &termUI{opt: t, pal: palette(!json && colorEnabled(t.color, os.LookupEnv, tty)), prev: errOut, noun: noun, dataset: dataset, began: att.StartedAt.Time, tty: tty, failing: failing}
	elog().start(start)
	show, redraw := progressPrefs(t, tty, rows, width, os.Getenv("TERM"))
	if !show {
		return tt
	}
	tt.coord = &stderrCoord{w: errOut}
	if redraw {
		errOut = tt.coord // other stderr writers erase and repaint the line
	}
	widthNow := func() int {
		_, w := stderrTerminal()
		return w
	}
	tt.disp = newDisplay(tt.coord, redraw, widthNow, tt.pal, progressInterval(t, redraw), att.StartedAt.Time, noun, dataset, failing)
	return tt
}

// stats prints the end-of-run block when the preferences call for it, and
// reports whether it did; under --log-format json the outcome event stands
// for it.
func (tt *termUI) stats(r *inventory.Report, brokenPipe bool) bool {
	if tt == nil || elog().json || !showStats(tt.opt, r, brokenPipe) {
		return false
	}
	lines := statsLines(r, brokenPipe, tt.noun, tt.opt.stats.set && tt.opt.stats.on)
	failed := brokenPipe || r.Outcome.ExitCode != exitOK || r.Outcome.Class == inventory.ClassIncomplete
	writeStats(errOut, lines, tt.pal, failed && !brokenPipe)
	if n := r.Output.RowsEmitted; tt.saveHint != nil && tt.tty && tt.opt.quiet == 0 && !failed && n >= hintRows {
		var b strings.Builder
		b.WriteString(tt.pal.dim("hint: this run listed "+approx(n)+" "+tt.noun.rows+"; to explore them again without relisting, save an inventory once and total that:") + "\n")
		for _, c := range tt.saveHint {
			b.WriteString(tt.pal.dim("hint:   "+c) + "\n")
		}
		noteKept(errOut, b.String())
	}
	return true
}

// hintRows is the rows a live swath du lists before its statistics suggest
// saving an inventory (termUI.saveHint); a var so that tests can lower it.
var hintRows int64 = 1_000_000

// approx is a count of rows as a person reads a large one: 12.4M, 1.3B;
// under a million, exactly.
func approx(n int64) string {
	switch {
	case n >= 1_000_000_000:
		return fmt.Sprintf("%.1fB", float64(n)/1e9)
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1e6)
	}
	return count(n)
}

// status prints one progress record of s now, whatever the display's
// gating, in the log's format (SIGUSR1, SIGINFO).
func (tt *termUI) status(s inventory.ProgressSnapshot) {
	avg := 0.0
	if s.DurationMs > 0 {
		avg = float64(s.Rows) * 1000 / float64(s.DurationMs)
	}
	now := time.Now()
	failing := ""
	if tt.failing != nil {
		failing = tt.failing(now)
	}
	parts := progressParts(s, now.Sub(tt.began), avg, tt.noun, tt.dataset, failing)
	if l := elog(); l.json {
		l.progress(s, strings.Join(parts, sep))
		return
	}
	note(errOut, frame(parts, 0, tt.pal)+"\n")
}

// close restores stderr.
func (tt *termUI) close() {
	if tt == nil {
		return
	}
	tt.disp.stop()
	errOut = tt.prev
}

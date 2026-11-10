// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/pflag"
	"golang.org/x/term"

	"github.com/varveio/swath/go/internal/listing"
	"github.com/varveio/swath/go/internal/source"
)

// The terminal surface on stderr, as Java swath's: a live progress record
// (redrawn in place on a terminal, appended as lines elsewhere), the
// end-of-run statistics block (--stats), verbosity (-v, -q) and colour.
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
}

// termFlags are the terminal flags' names: runtime flags, which swath resume
// accepts again.
var termFlags = []string{"progress", "no-progress", "progress-interval", "stats", "no-stats", "verbose", "quiet", "color"}

// globalFlags are the terminal flags also taken before the command word
// (swath -v list ...).
var globalFlags = []string{"verbose", "quiet", "color"}

func (t *termOptions) register(f *pflag.FlagSet) {
	g := source.GroupReporting
	source.Define(f, source.Flag{Name: "verbose", Short: "v", Value: source.Counter(&t.verbose), Group: g,
		Usage: "a start line, and progress as appended records"})
	source.Define(f, source.Flag{Name: "quiet", Short: "q", Value: source.Counter(&t.quiet), Group: g,
		Usage: "no warnings, no automatic progress or statistics (-qq: no errors)",
		Long:  "With -qq the exit code and the reports stand alone."})
	source.Define(f, source.Flag{Name: "progress", Value: triFlag{&t.progress, false}, Group: g,
		Usage: "print live progress on stderr (default: on a terminal, without -q, -v)"})
	source.Define(f, source.Flag{Name: "no-progress", Value: triFlag{&t.progress, true}, Group: g, Usage: "no live progress on stderr"})
	source.Define(f, source.Flag{Name: "progress-interval", Value: source.Duration(&t.interval), Metavar: "DURATION", Group: g,
		Usage: "how often progress is printed (at least 1s); implies --progress",
		Long:  "Default 1s while the line redraws on a terminal, 30s for appended records."})
	source.Define(f, source.Flag{Name: "stats", Value: triFlag{&t.stats, false}, Group: g,
		Usage: "print the end-of-run statistics on stderr",
		Long:  "Default: for runs over 1.5 s, runs that write output, and runs that do not complete; not with -q, nor after a closed downstream pipe."})
	source.Define(f, source.Flag{Name: "no-stats", Value: triFlag{&t.stats, true}, Group: g, Usage: "no end-of-run statistics on stderr"})
	t.color = "auto"
	source.Define(f, source.Flag{Name: "color", Value: source.Text(&t.color), Metavar: "auto|always|never", Default: "auto", Group: g,
		Usage: "color the progress line, the statistics and the help",
		Long:  "auto: on a terminal, without NO_COLOR or TERM=dumb; CLICOLOR_FORCE forces it."})
}

func (t *termOptions) validate() error {
	switch t.color {
	case "auto", "always", "never":
	default:
		return fmt.Errorf("--color %q: want auto, always or never", t.color)
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

// byteText is Java's decimal byte text: 512 B, 1.5 MB.
func byteText(n int64) string {
	units := []string{"B", "KB", "MB", "GB", "TB", "PB"}
	v, u := float64(n), 0
	for v >= 1000 && u < len(units)-1 {
		v /= 1000
		u++
	}
	if u == 0 {
		return count(n) + " B"
	}
	return fmt.Sprintf("%.1f %s", v, units[u])
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

// rowNoun is what the terminal calls a source's rows: a local tree's are
// entries (its files, directories and other kinds), an object store's
// objects.
func rowNoun(local bool) string {
	if local {
		return "entries"
	}
	return "objects"
}

// progressParts is one record's fields, most important first: the phase,
// its counters (rows called noun), then elapsed time, API calls and the
// estimate. No percentage or ETA: a listing has no honest denominator. No
// key text.
func progressParts(s listing.ProgressSnapshot, session time.Duration, live float64, noun string) []string {
	phase := "listing"
	switch {
	case s.StartedAt.IsZero():
		phase = "starting"
	case s.Verified > 0 && s.Objects == 0 && s.Requests == 0:
		phase = "verifying"
	}
	parts := []string{phase}
	if phase == "verifying" {
		parts = append(parts, byteText(s.Verified)+" verified")
	} else {
		objects := count(s.Objects) + " " + noun
		if s.Retained > 0 {
			objects += " (+" + count(s.Retained) + " recovered)"
		}
		avg := 0.0
		if s.DurationMs > 0 {
			avg = float64(s.Objects) * 1000 / float64(s.DurationMs)
		}
		parts = append(parts, objects, fmt.Sprintf("%s keys/s (avg %s)", count(int64(live+0.5)), count(int64(avg+0.5))),
			count(s.Parts)+" parts", count(s.Inflight)+" in flight")
	}
	parts = append(parts, elapsed(session)+" elapsed", count(s.Requests)+" API calls")
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
	noun     string    // the rows' name (rowNoun)

	samples chan listing.ProgressSnapshot
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
// neither -q nor -v is given. -v alone shows it as appended records (the
// verbose log surface). Lines redraw only on a terminal of usable width
// whose TERM is not dumb.
func progressPrefs(t termOptions, tty bool, width int, termEnv string) (show, redraw bool) {
	switch {
	case t.progress.set:
		show = t.progress.on
	case t.interval > 0:
		show = true
	case t.verbose > 0:
		show = t.quiet == 0
	default:
		show = tty && t.quiet == 0
	}
	redraw = show && tty && termEnv != "dumb" && width >= minUsableWidth && t.verbose == 0 && enableVT()
	return show, redraw
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

func newDisplay(out *stderrCoord, redraw bool, width func() int, pal palette, interval time.Duration, started time.Time, noun string) *display {
	d := &display{out: out, redraw: redraw, width: width, pal: pal, interval: interval, started: started, noun: noun,
		samples: make(chan listing.ProgressSnapshot, 1), stopped: make(chan struct{})}
	d.done.Add(1)
	go d.loop()
	return d
}

// offer hands the display a sample; it never blocks.
func (d *display) offer(s listing.ProgressSnapshot) {
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
func (d *display) render(s listing.ProgressSnapshot, now time.Time) {
	d.renderMu.Lock()
	defer d.renderMu.Unlock()
	if !d.last.IsZero() && now.Sub(d.last) < d.interval-d.interval/20 {
		return
	}
	live := 0.0
	if !d.prevAt.IsZero() && now.After(d.prevAt) {
		live = float64(s.Objects-d.prevObj) / now.Sub(d.prevAt).Seconds()
	} else if s.DurationMs > 0 {
		live = float64(s.Objects) * 1000 / float64(s.DurationMs)
	}
	d.last, d.prevObj, d.prevAt = now, s.Objects, now
	parts := progressParts(s, now.Sub(d.started), max(live, 0), d.noun)
	if d.redraw {
		d.out.frame(frame(parts, d.width(), d.pal))
	} else {
		d.out.record(frame(parts, 0, d.pal) + "\n")
	}
}

// stop ends the display; a redrawn line is erased, so what follows on
// stderr (the statistics, an error) starts on a clean line.
func (d *display) stop() {
	if d == nil {
		return
	}
	d.once.Do(func() {
		close(d.stopped)
		d.done.Wait()
		d.out.finish()
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

func (c *stderrCoord) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := string(p)
	if c.line != "" {
		s = "\r" + eraseLine + s + c.line
	}
	note(c.w, s)
	return len(p), nil
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

// autoStatsMin is Java's threshold: a run longer than this prints its
// statistics without --stats.
const autoStatsMin = 1500 * time.Millisecond

// showStats is Java's gating: --stats/--no-stats decide; otherwise not with
// -q nor after a closed downstream pipe, and on for a run over 1.5 s, one
// that wrote output, or one that did not complete.
func showStats(t termOptions, r *listing.Report, brokenPipe bool) bool {
	switch {
	case t.stats.set:
		return t.stats.on
	case t.quiet > 0 || brokenPipe:
		return false
	}
	return time.Duration(r.Timings.SessionMs)*time.Millisecond > autoStatsMin ||
		r.Output.PartsWritten+r.Output.PartsRetained > 0 || (r.Output.Text != nil && r.Output.Text.Destination != "-" && r.Output.Text.Bytes > 0) ||
		r.Outcome.ExitCode != exitCompleted
}

// resumableStops are the stops whose statistics end with resume advice.
var resumableStops = map[string]bool{"interrupted": true, "terminated": true, "deadline": true, "stuck": true, "disk_full": true}

// statsLines is the end-of-run block, Java's lines from the report: a
// disposition for a run that did not complete, the headline (objects,
// time, keys/s), the request economy, faults when any, the estimate, and
// the output.
func statsLines(r *listing.Report, brokenPipe bool, noun string) []string {
	var lines []string
	o := r.Outcome
	switch {
	case brokenPipe:
		lines = append(lines, "stopped early — downstream closed")
	case o.ExitCode != exitCompleted || o.Class == "incomplete":
		d := "INCOMPLETE (" + o.Class + ")"
		if o.Resumable && resumableStops[o.Class] && r.Invocation.Out != "" {
			d += " — resume: swath resume " + r.Invocation.Out
		}
		lines = append(lines, d)
		if o.Class == "refused" || o.Class == "usage" {
			// Refused before it ran: the disposition is the whole record,
			// as Java's refused resume (a body would be a block of zeros).
			return lines
		}
	}
	run := time.Duration(r.Timings.WallMs) * time.Millisecond
	session := time.Duration(r.Timings.SessionMs) * time.Millisecond
	kps := 0.0
	if r.Efficiency != nil {
		kps = r.Efficiency.KeysPerSec
	} else if r.Timings.ListingMs > 0 {
		kps = float64(r.Output.RowsEmitted) * 1000 / float64(r.Timings.ListingMs)
	}
	head := count(r.Output.RowsEmitted) + " " + noun + " in " + elapsed(run) + sep + count(int64(kps+0.5)) + " keys/s"
	if session-run > time.Second {
		head += sep + elapsed(session) + " total"
	}
	lines = append(lines, head)
	q := r.Live()
	if r.Requests != nil {
		per := 0.0
		if r.Efficiency != nil && r.Efficiency.HTTPAttemptsPer1kRows != nil {
			per = *r.Efficiency.HTTPAttemptsPer1kRows
		}
		avg := 0.0
		if ms := r.Timings.ListingMs; ms > 0 {
			avg = float64(q.InflightNs) / float64(ms*1e6)
		}
		lines = append(lines, fmt.Sprintf("%s API calls%s%.2f per 1k objects%sin flight avg %.2f%speak %s",
			count(r.Requests.HTTPAttempts), sep, per, sep, avg, sep, count(q.InflightMax)))
	}
	if q.Throttles > 0 || q.Retries > 0 || o.EntryErrors > 0 {
		lines = append(lines, "throttled "+count(q.Throttles)+sep+"retried "+count(q.Retries)+sep+"errors "+count(o.EntryErrors))
	}
	if c := r.Cost; c != nil && c.CostUSD != nil && c.Basis != nil && r.Requests != nil && r.Requests.HTTPAttempts > 0 {
		lines = append(lines, costText(*c.CostUSD, c.Basis.RatePer1kUSD))
	}
	var out []string
	files := int64(r.Output.PartsWritten + r.Output.PartsRetained)
	written := r.Output.OutputBytes
	if t := r.Output.Text; t != nil && t.Destination != "-" {
		files, written = 1, t.Bytes
	}
	if files > 0 {
		f := " files"
		if files == 1 {
			f = " file"
		}
		out = append(out, count(files)+f)
	}
	if written > 0 {
		out = append(out, byteText(written)+" written")
	}
	if rss := r.Resources.PeakRSSBytes; rss != nil && *rss > 0 {
		out = append(out, "peak RSS "+byteText(*rss))
	}
	if len(out) > 0 {
		lines = append(lines, strings.Join(out, sep))
	}
	return lines
}

// writeStats prints the block, indented and styled as Java's: the
// disposition red when INCOMPLETE, the headline accented, the rest dim.
func writeStats(w io.Writer, lines []string, pal palette, hasDisposition bool) {
	var b strings.Builder
	for i, l := range lines {
		s := pal.dim(l)
		switch {
		case hasDisposition && i == 0 && strings.HasPrefix(l, "INCOMPLETE"):
			s = pal.red(l)
		case (hasDisposition && i == 1) || (!hasDisposition && i == 0):
			s = pal.accent(l)
		}
		b.WriteString(indent + s + "\n")
	}
	note(w, b.String())
}

// termUI is one run's stderr surface: the progress display (nil when
// off) and the statistics' preferences.
type termUI struct {
	opt   termOptions
	pal   palette
	disp  *display
	prev  io.Writer // errOut before the run
	coord *stderrCoord
	noun  string // the rows' name (rowNoun)
}

// startTerminal resolves the terminal preferences for a run and starts its
// progress display; -v prints a start line.
func startTerminal(t termOptions, att *listing.Attempt, src, out, noun string) *termUI {
	tty, width := stderrTerminal()
	tt := &termUI{opt: t, pal: palette(colorEnabled(t.color, os.LookupEnv, tty)), prev: errOut, noun: noun}
	if t.verbose > 0 && t.quiet == 0 {
		dest := out
		if dest == "" {
			dest = "(no dataset)"
		}
		note(errOut, fmt.Sprintf("swath: listing %s to %s (attempt %s)\n", src, dest, att.ID))
	}
	show, redraw := progressPrefs(t, tty, width, os.Getenv("TERM"))
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
	tt.disp = newDisplay(tt.coord, redraw, widthNow, tt.pal, progressInterval(t, redraw), att.Started, noun)
	return tt
}

// stats prints the end-of-run block when the preferences call for it.
func (tt *termUI) stats(r *listing.Report, brokenPipe bool) {
	if tt == nil || !showStats(tt.opt, r, brokenPipe) {
		return
	}
	lines := statsLines(r, brokenPipe, tt.noun)
	writeStats(errOut, lines, tt.pal, brokenPipe || r.Outcome.ExitCode != exitCompleted || r.Outcome.Class == "incomplete")
}

// close restores stderr.
func (tt *termUI) close() {
	if tt == nil {
		return
	}
	tt.disp.stop()
	errOut = tt.prev
}

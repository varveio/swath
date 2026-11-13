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
	"os"
	"path/filepath"
	"runtime/pprof"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/journal"
	"github.com/varveio/swath/internal/listing"
	"github.com/varveio/swath/internal/source"
)

// Exit codes: the outcome a supervising process acts on without reading
// stderr (error.json carries the same, with the cause).
const (
	exitCompleted   = 0   // the dataset is committed (or a dry run finished)
	exitFatal       = 1   // a cause that a resume will not fix
	exitUsage       = 2   // a malformed command line, or a refusal that is the caller's to fix
	exitIncomplete  = 3   // finished, but the output is incomplete: unreadable entries, or a key limit
	exitDiskFull    = 74  // a write found its filesystem full (EX_IOERR); resumable when the journal is intact
	exitStuck       = 75  // no progress for the stall timeout, or a bucket commit unresolved; resumable when the journal is intact
	exitDeadline    = 124 // --max-duration reached with the output unfinished; resumable
	exitInterrupted = 130 // SIGINT
	exitTerminated  = 143 // SIGTERM, a graceful stop; resumable
)

// Causes the command cancels a run with.
var (
	errStuck    = errors.New("stuck: no progress within the stall timeout")
	errDeadline = errors.New("deadline: --max-duration reached")
	// errIncomplete: the dataset is committed, but its coverage is partial.
	errIncomplete = errors.New("the dataset is committed with partial coverage (see the manifest's coverage)")
)

// incompleteError is a run that finished with an output that does not hold
// its whole scope (exit 3): the message says why.
type incompleteError struct{ msg string }

func (e *incompleteError) Error() string        { return e.msg }
func (e *incompleteError) Is(target error) bool { return target == errIncomplete }

// unreadableError is a run that left out n entries it could not read.
func unreadableError(n int64) error {
	if n == 1 {
		return &incompleteError{"1 entry could not be read and is missing from the output"}
	}
	return &incompleteError{count(n) + " entries could not be read and are missing from the output"}
}

// Stuck causes (error.json and the report's outcome cause on 75): what
// rose while the run's liveness (listing.ProgressSnapshot.Liveness) did
// not.
const (
	// stuckEmptyChain: the service answered only empty continuing pages,
	// each with a fresh token, past the first keyrange.EmptyChainLive of
	// their run: up, but the listing does not move.
	stuckEmptyChain = "stuck_empty_chain"
	stuckThrottle   = "stuck_throttle" // the service answered only with throttles
	stuckTimeouts   = "stuck_timeouts" // attempts ran past the request timeout (no throttle)
	stuckNoRows     = "stuck_no_rows"  // none of these: failing or hung requests, or a pipeline that stopped
)

// stuckError is errStuck with its cause.
type stuckError struct{ cause string }

func (e *stuckError) Error() string        { return errStuck.Error() + " (" + e.cause + ")" }
func (e *stuckError) Is(target error) bool { return target == errStuck }

// stuckCause names what rose in the stalled window: from the counts at
// the last progress (at) to now.
func stuckCause(at, now listing.ProgressSnapshot) string {
	switch {
	case now.EmptyChain > at.EmptyChain:
		return stuckEmptyChain
	case now.Throttles > at.Throttles:
		return stuckThrottle
	case now.Timeouts > at.Timeouts:
		return stuckTimeouts
	}
	return stuckNoRows
}

// signalError is a run stopped by a signal.
type signalError struct{ sig os.Signal }

func (e *signalError) Error() string { return "stopped by " + e.sig.String() }

// outcome is how a command ended: its class and exit code.
type outcome struct {
	Class     string `json:"class"` // listing.Class*: completed, fatal, incomplete, usage, refused, disk_full, stuck, deadline, interrupted, terminated
	ExitCode  int    `json:"exit_code"`
	Message   string `json:"message,omitempty"`
	Resumable bool   `json:"resumable"` // swath resume DIR can continue it
	// Cause is the provider's error class when one is known (S3: auth,
	// access_denied, throttle, ...).
	Cause string `json:"cause,omitempty"`
	// StopSource is what stopped the run, when something did (stopSource).
	StopSource string `json:"stop_source,omitempty"`
	// Attempt is the invocation this outcome belongs to; the report
	// (--report) carries the same one.
	Attempt *listing.Attempt `json:"attempt,omitempty"`
}

// classify maps a command's error (and the cause its context was
// cancelled with) to an outcome. dir is the output directory, if any.
func classify(err, cause error, dir string) outcome {
	if err == nil {
		return outcome{Class: listing.ClassCompleted, ExitCode: exitCompleted}
	}
	resumable := dir != "" && !dataset.Committed(dir) && hasJournal(dir)
	o := outcome{Message: err.Error(), Resumable: resumable, StopSource: stopSource(cause)}
	var u usageError
	var se *signalError
	var ue *journal.UnresumableError
	switch {
	case errors.As(err, &u), errors.Is(err, pflag.ErrHelp):
		o.Class, o.ExitCode, o.Resumable = listing.ClassUsage, exitUsage, false
	case refusal(err):
		// The caller's to fix (another output, --overwrite, the original
		// settings): nothing ran, and the output is as it was, so
		// Resumable still says whether swath resume can continue a run
		// there. A source refused mid-run (source.Unsupported) keeps its
		// journal, so it stays resumable by a build that lists it; the
		// provider's class is its cause.
		o.Class, o.ExitCode = listing.ClassRefused, exitUsage
		var ce interface{ Class() string }
		if errors.As(err, &ce) {
			o.Cause = ce.Class()
		}
	case errors.Is(err, source.ErrProtocol), errors.As(err, &ue):
		// The service broke the listing contract: never resumable, and
		// it outranks a stop cause (a resumable 75, 124, 130 or 143), as
		// Java's; the journal marks the run unresumable.
		o.Class, o.ExitCode, o.Resumable, o.Cause = listing.ClassFatal, exitFatal, false, "protocol"
	case errors.Is(err, dataset.ErrUnsafeLayout):
		// The output needs a person: a stop cause selected meanwhile (a
		// stall, the deadline, a signal) must not make it resumable.
		o.Class, o.ExitCode, o.Resumable, o.Cause = listing.ClassFatal, exitFatal, false, "unsafe_layout"
	case errors.Is(err, dataset.ErrPartLimit):
		// The part size and row cap are frozen in the identity, so a
		// resume would reach the limit again; it outranks a stop cause.
		o.Class, o.ExitCode, o.Resumable, o.Cause = listing.ClassFatal, exitFatal, false, "part_limit"
	case errors.Is(err, dataset.ErrCandidateMismatch):
		// An earlier attempt's manifest may still appear: a person settles
		// it, never a resume.
		o.Class, o.ExitCode, o.Resumable, o.Cause = listing.ClassFatal, exitFatal, false, "candidate_mismatch"
	case errors.Is(err, dataset.ErrCommitUnresolved):
		// Nothing was removed: a resume creates the same manifest again.
		// It outranks a stop cause, which may be what cut the commit short.
		o.Class, o.ExitCode, o.Cause = listing.ClassUnresolved, exitStuck, "commit_unresolved"
	case errors.Is(err, dataset.ErrLostRace):
		o.Class, o.ExitCode, o.Resumable, o.Cause = listing.ClassFatal, exitFatal, false, "lost_race"
	case errors.Is(err, dataset.ErrRemoteAltered):
		o.Class, o.ExitCode, o.Resumable, o.Cause = listing.ClassFatal, exitFatal, false, "remote_altered"
	case errors.Is(err, errIncomplete):
		o.Class, o.ExitCode, o.Resumable = listing.ClassIncomplete, exitIncomplete, false
	case errors.Is(err, source.ErrUnreachable):
		// Nothing in the run answered and every connection failed
		// deterministically for the grace (objhttp's fail-fast): no
		// progress and nothing reachable, the stall reached early.
		// Resumable as the journal says, once the endpoint answers.
		o.Class, o.ExitCode, o.Cause = listing.ClassStuck, exitStuck, source.CauseUnreachable
	case errors.Is(cause, errStuck):
		o.Class, o.ExitCode = listing.ClassStuck, exitStuck
		var st *stuckError
		if errors.As(cause, &st) {
			o.Cause = st.cause
		}
	case errors.Is(cause, errDeadline):
		o.Class, o.ExitCode = listing.ClassDeadline, exitDeadline
	case errors.As(cause, &se) && se.sig == syscall.SIGTERM:
		o.Class, o.ExitCode = listing.ClassTerminated, exitTerminated
	case errors.As(cause, &se):
		o.Class, o.ExitCode = listing.ClassInterrupted, exitInterrupted
	case errors.Is(err, listing.ErrComplete):
		// swath resume on a committed dataset has nothing to do: it is
		// idempotent, exit 0, as Java's, when the dataset covers its
		// scope; one committed with partial coverage stays incomplete.
		o.Resumable = false
		if m, ok := committedManifest(dir); ok && m.Coverage.Complete && m.Coverage.Unreadable == 0 {
			o.Class, o.ExitCode = listing.ClassCompleted, exitCompleted
		} else {
			o.Class, o.ExitCode = listing.ClassIncomplete, exitIncomplete
		}
	case errors.Is(err, syscall.ENOSPC):
		// A full disk stops the run (a part, a journal append, spill or
		// the manifest cannot be written) but loses nothing durable:
		// resumable once there is space, when the journal is intact. Its
		// own code lets a runner retry with a larger workspace.
		o.Class, o.ExitCode, o.Cause = listing.ClassDiskFull, exitDiskFull, "no_space"
	default:
		o.Class, o.ExitCode, o.Resumable = listing.ClassFatal, exitFatal, false
		var ce interface{ Class() string }
		if errors.As(err, &ce) {
			o.Cause = ce.Class()
		}
	}
	return o
}

// stopSource names what cancelled a run (Java's stop_source tags): a
// signal, the liveness watchdog (--stall-timeout) or the timebox
// (--max-duration); "" when nothing did.
func stopSource(cause error) string {
	var se *signalError
	switch {
	case cause == nil:
		return ""
	case errors.Is(cause, errStuck):
		return "liveness_watchdog"
	case errors.Is(cause, errDeadline):
		return "timebox"
	case errors.As(cause, &se) && se.sig == syscall.SIGTERM:
		return "sigterm"
	case errors.As(cause, &se):
		return "sigint"
	}
	return ""
}

// refusal tells a refusal that is the caller's fault, as Java's exit 2
// (its guarded refusals): an output that is not this tool's to write
// without --overwrite (not empty, another tool's, an interrupted run) or
// that overlaps the listed root, a resume whose identity changed, and a
// resume with nothing to resume; and a source refused before any request
// (an S3 directory bucket).
func refusal(err error) bool {
	var mm *journal.MismatchError
	for _, r := range []error{dataset.ErrNotEmpty, dataset.ErrNotDataset, dataset.ErrOutIsRoot, dataset.ErrInsideRoot, listing.ErrInterrupted, listing.ErrNothingToResume, listing.ErrPrior, source.ErrUnsupported,
		listing.ErrDestinationExists, listing.ErrOverlap} {
		if errors.Is(err, r) {
			return true
		}
	}
	return errors.As(err, &mm)
}

// committedManifest is dir's manifest, or for the work directory of a
// dataset in a bucket its committed candidate (ok false without a readable
// one).
func committedManifest(dir string) (*dataset.Manifest, bool) {
	var m dataset.Manifest
	b, err := os.ReadFile(filepath.Join(dir, dataset.ManifestName))
	if errors.Is(err, os.ErrNotExist) {
		b, err = dataset.CommittedCandidate(dir) // a bucket dataset's work directory
	}
	if err != nil || json.Unmarshal(b, &m) != nil {
		return nil, false
	}
	return &m, true
}

// account applies the frozen classification o to a report's outcome (its
// message is the report's error, unless the run completed). The
// completion fields follow it: a run that did not commit is not complete
// and its stop reason is its class (fatal, stuck, deadline, ...), whatever
// the source reported (a source's coverage says complete once its listing
// loop ends, also after a failure); a committed run keeps the source's,
// which the manifest's coverage holds too (an incomplete one included).
func account(r *listing.Outcome, o outcome) {
	r.Class, r.ExitCode, r.Resumable, r.Cause, r.StopSource = o.Class, o.ExitCode, o.Resumable, o.Cause, o.StopSource
	if o.ExitCode != exitCompleted && o.Message != "" {
		r.Error = o.Message
	}
	if o.ExitCode != exitCompleted && o.Class != listing.ClassIncomplete {
		r.Complete, r.StopReason = false, o.Class
	}
}

// committedAccount gives the report of an invocation that found its output
// committed (listing.ErrComplete) the dataset's status, the manifest's
// coverage and its dataset account (rows, part bytes, object bytes); this
// invocation's own counts stay 0.
func committedAccount(dir string, r *listing.Report) {
	r.Outcome.Status = listing.StatusCommitted
	if m, ok := committedManifest(dir); ok {
		r.Outcome.Complete, r.Outcome.StopReason, r.Outcome.Unreadable = m.Coverage.Complete, m.Coverage.StopReason, m.Coverage.Unreadable
		r.Output.Committed(m)
	}
}

// finalizedSample is err, a resume's listing.ErrComplete, as the user reads
// it for dir: a dataset committed with partial coverage (a --max-keys
// sample) is a finalized partial sample, which nothing continues, not a
// complete one; so is one that left out entries it could not read. It
// still is ErrComplete to classify: incomplete, exit 3.
func finalizedSample(dir string, err error) error {
	m, ok := committedManifest(dir)
	switch {
	case !ok:
	case !m.Coverage.Complete:
		return &restated{err, fmt.Sprintf("%s holds a finalized partial sample (coverage complete=false stop=%s): it cannot be resumed", dir, m.Coverage.StopReason)}
	case m.Coverage.Unreadable > 0:
		n := m.Coverage.Unreadable
		return &restated{err, fmt.Sprintf("%s left out %s it could not read (coverage unreadable=%d): it cannot be resumed; list the tree again once %s can be read",
			dir, plural(n, "an entry", count(n)+" entries"), n, plural(n, "it", "they"))}
	}
	return err
}

// restated is an error under another message.
type restated struct {
	error
	msg string
}

func (e *restated) Error() string { return e.msg }

func (e *restated) Unwrap() error { return e.error }

// alreadyComplete accounts for a resume that found its output committed
// and complete (exit 0): the report says so from the manifest's coverage,
// and the text report says it in one line.
func alreadyComplete(stdout io.Writer, report, dir string, r *listing.Report) {
	r.Outcome.Error, r.Outcome.Unrecorded = "", listing.UnrecordedComplete
	committedAccount(dir, r)
	if report != "-" { // else stdout carries the report
		fmt.Fprintf(stdout, "%s is already complete: nothing to resume\n", dir)
	}
}

// classified is an error with the provider's class of its cause.
type classified struct {
	error
	class string
}

func (c *classified) Class() string { return c.class }
func (c *classified) Unwrap() error { return c.error }

// exitError carries the exit code main uses, and the outcome it ends with
// (conclude).
type exitError struct {
	error
	code int
	out  outcome
}

func (e *exitError) Unwrap() error { return e.error }

func hasJournal(dir string) bool {
	_, err := journal.Read(dir)
	return err == nil
}

// errorName is the outcome file written beside the report when a run does
// not complete.
const errorName = dataset.ErrorName

// writeOutcome records a failed run's outcome (an incomplete committed
// dataset included) in DIR/error.json (durable replace), so a supervisor
// never parses stderr; a completed run removes a stale one. The caller
// owns the output (listing.Config.Settle, settleOutside).
func writeOutcome(dir string, o outcome) error {
	if dir == "" {
		return nil
	}
	path := filepath.Join(dir, errorName)
	if o.ExitCode == exitCompleted {
		err := os.Remove(path)
		switch {
		case os.IsNotExist(err):
			return nil
		case err != nil:
			return err
		}
		// A removal that a power loss undid would leave a stale failure
		// beside the committed dataset.
		return dataset.SyncDir(dir)
	}
	return dataset.ReplaceJSON(path, o, dataset.Durable)
}

// errOut is where the command reports an outcome its output does not hold.
var errOut io.Writer = os.Stderr

// settleOutside records o in dir's error.json for a command that stopped
// before any run owned the output (a resume refused on a damaged journal),
// over the unfinished run the command read (seen, its journal.Witness):
// listing.SettleUnfinished, whose unrecorded says why not, never empty.
func settleOutside(dir, seen string, o outcome) (recorded bool, unrecorded string) {
	why := listing.SettleUnfinished(dir, seen, func() error {
		if err := writeOutcome(dir, o); err != nil {
			return fmt.Errorf("write %s: %w", errorName, err)
		}
		return nil
	})
	return why == "", why
}

// reportUnrecorded warns that the output's error.json does not hold the
// outcome (another owner's outcome stands there, or the write failed),
// with the reason; the report holds it, and -v or --log-format json print
// it (conclude).
func reportUnrecorded(dir, why string) {
	if why != "" {
		elog().warn(fmt.Sprintf("the outcome is not recorded in %s: %s", filepath.Join(dir, errorName), why))
	}
}

// diagWait bounds one diagnostic write to stderr: a pipe nobody drains
// must not hold a stopping run, the watchdog or the forced exit.
const diagWait = 250 * time.Millisecond

// forcedAllowance bounds the forced exit after shutdownGrace: the dump
// (stacksWait), the outcome (stacksWait) and the diagnostic lines
// (diagWait each) fit in it; os.Exit follows at the latest then.
const forcedAllowance = 5 * time.Second

// diagQueue bounds the lines waiting for one writer (stderr) that does not
// drain: past it a line is dropped, and counted (dropped).
const diagQueue = 256

// dropped counts the lines note dropped (the report's
// requests.log_dropped).
var dropped atomic.Int64

// lineClass is what a stderr line may become when stderr does not drain.
type lineClass uint8

const (
	// disposable: progress records and the request log (-vv, -vvv),
	// dropped first.
	disposable lineClass = iota
	// kept: warnings, errors and the statistics, which a reader needs:
	// bounded apart (diagQueue of them, the oldest dropped past it), and
	// written before the outcome.
	kept
	// final: the command's last lines (its outcome).
	final
)

// diagLine is a line waiting for its writer; done closes once it is
// written.
type diagLine struct {
	s     string
	class lineClass
	done  chan struct{}
}

// diagSink is one writer's queue, drained in order by one goroutine while
// it holds lines. stuck: a line waited diagWait unwritten, and no write
// has completed since.
type diagSink struct {
	queue []diagLine
	stuck bool
}

var (
	diagMu    sync.Mutex
	diagSinks = map[io.Writer]*diagSink{}
)

// note writes s, a disposable line, to w through w's one writer
// goroutine, waiting at most diagWait. While a write to w is stuck, lines
// queue without waiting; past diagQueue of them a disposable one is
// dropped. A queued line is lost at exit.
func note(w io.Writer, s string) { send(w, s, disposable) }

// noteKept is note for a warning, an error or the statistics: queued past
// diagQueue disposable lines, up to diagQueue kept ones.
func noteKept(w io.Writer, s string) { send(w, s, kept) }

// noteFinal is note for a command's last lines (its outcome). The queued
// disposable lines are dropped (counted) so that a slow reader's backlog
// does not hold the outcome past its wait (the process exits after it);
// the kept ones are written before it, within its wait, and dropped
// (counted) if that wait ends first, when the outcome gets one more.
func noteFinal(w io.Writer, s string) { send(w, s, final) }

// shed drops k's queued lines below class keep (diagMu held), counted.
func (k *diagSink) shed(keep lineClass) {
	q := k.queue[:0]
	for _, l := range k.queue {
		if l.class >= keep {
			q = append(q, l)
		} else {
			dropped.Add(1)
			close(l.done)
		}
	}
	clear(k.queue[len(q):])
	k.queue = q
}

// bound makes room for one more kept line (diagMu held): past diagQueue
// of them the oldest is dropped, counted.
func (k *diagSink) bound() {
	n := 0
	for _, l := range k.queue {
		if l.class == kept {
			n++
		}
	}
	if n < diagQueue {
		return
	}
	i := slices.IndexFunc(k.queue, func(l diagLine) bool { return l.class == kept })
	dropped.Add(1)
	close(k.queue[i].done)
	k.queue = slices.Delete(k.queue, i, i+1)
}

func send(w io.Writer, s string, class lineClass) {
	if c, ok := w.(*stderrCoord); ok {
		c.put(s, class) // the line keeps its class through the progress line
		return
	}
	l := diagLine{s, class, make(chan struct{})}
	diagMu.Lock()
	k := diagSinks[w]
	if k == nil {
		k = &diagSink{}
		diagSinks[w] = k
		go drain(w, k)
	}
	full, stuck := len(k.queue) >= diagQueue, k.stuck
	switch class {
	case final:
		k.shed(kept)
	case kept:
		k.bound()
	}
	if !full || class != disposable {
		k.queue = append(k.queue, l)
	}
	diagMu.Unlock()
	switch {
	case full && class == disposable:
		dropped.Add(1)
		return
	case stuck && class != final:
		return
	}
	t := time.NewTimer(diagWait)
	defer t.Stop()
	select {
	case <-l.done:
		return
	case <-t.C:
	}
	diagMu.Lock()
	k.stuck = true
	if class == final {
		k.shed(final)
	}
	diagMu.Unlock()
	if class == final {
		t.Reset(diagWait)
		select {
		case <-l.done:
		case <-t.C:
		}
	}
}

// drain writes k's lines to w in order, and ends once none waits.
func drain(w io.Writer, k *diagSink) {
	for {
		diagMu.Lock()
		if len(k.queue) == 0 {
			delete(diagSinks, w)
			diagMu.Unlock()
			return
		}
		l := k.queue[0]
		k.queue[0] = diagLine{}
		k.queue = k.queue[1:]
		diagMu.Unlock()
		_, _ = io.WriteString(w, l.s)
		diagMu.Lock()
		k.stuck = false
		diagMu.Unlock()
		close(l.done)
	}
}

// stacksMax bounds a goroutine dump on stderr (the file holds it whole).
const stacksMax = 1 << 20

// stacksWait bounds the dump's write: a disk that blocks the run may block
// it too, and a forced exit must not wait for it.
const stacksWait = 2 * time.Second

// dumpStacks writes every goroutine's stack with the progress counts, for
// a run stopped as stuck or forced to exit. They are forensics, not the
// run's output: whole to DIR/.swath/stacks-WHY.txt while the run owns the
// output, else to a new file in the temporary directory, and stderr gets
// one line naming the file. The stacks go to stderr as well only with -v,
// or instead when no file could be written within stacksWait, cut at
// stacksMax bytes. Under --log-format json a run without an output
// directory writes no file: its warning event carries them (stacks), for
// the supervisor that collects the log. The stacks are taken on entry, as
// the goroutines are then. It returns where the dump went: the file, or
// "stderr".
func dumpStacks(dir string, pr *listing.Progress, why, detail string, s listing.ProgressSnapshot) string {
	var stacks bytes.Buffer
	_ = pprof.Lookup("goroutine").WriteTo(&stacks, 2) // runtime.Stack's format, never truncated
	buf := stacks.Bytes()
	sj, _ := json.Marshal(s)
	head := fmt.Sprintf("swath goroutine dump: %s (%s) at %s\nprogress: %s\n\n", why, detail, time.Now().UTC().Format(time.RFC3339Nano), sj)
	l := elog()
	done := make(chan string, 1)
	go func() {
		path := ""
		if dir != "" {
			p, ok := filepath.Join(dir, dataset.Workspace, "stacks-"+why+".txt"), false
			pr.Owned(func() {
				ok = os.MkdirAll(filepath.Dir(p), 0o755) == nil &&
					os.WriteFile(p, append([]byte(head), buf...), 0o644) == nil
			})
			if ok {
				path = p
			}
		}
		if path == "" && !l.json {
			path = tempStacks(why, head, buf)
		}
		done <- path
	}()
	path := ""
	select {
	case path = <-done:
	case <-time.After(stacksWait):
	}
	if path != "" {
		l.warn("goroutine dump written to " + path)
		if !l.shows(lvInfo) {
			return path
		}
	}
	if len(buf) > stacksMax {
		buf = append(buf[:stacksMax:stacksMax], "\n... (cut at 1 MiB)"...)
	}
	if l.json {
		l.print(lvWarn, evWarning, "goroutine dump: "+why+" ("+detail+")", stacksFields{string(buf)})
	} else {
		note(errOut, head+string(buf)+"\n")
	}
	if path != "" {
		return path
	}
	return "stderr"
}

// tempStacks writes a goroutine dump to a new file in the temporary
// directory and returns its path ("" when it could not).
func tempStacks(why, head string, buf []byte) string {
	f, err := os.CreateTemp("", "swath-stacks-"+why+"-*.txt")
	if err != nil {
		return ""
	}
	_, err = f.WriteString(head)
	if err == nil {
		_, err = f.Write(buf)
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(f.Name())
		return ""
	}
	return f.Name()
}

// stacksFields are a goroutine dump's event under --log-format json.
type stacksFields struct {
	Stacks string `json:"stacks"`
}

// forcedErr is the error of a run forced to exit (it did not stop within
// the shutdown grace).
func forcedErr(why string) error {
	return fmt.Errorf("forced exit: still stopping (%s) after %s", why, shutdownGrace)
}

// forcedSnapshot takes the forced exit's one snapshot of the terminal
// result (terminal.claim): the run's classification once made (a committed
// run stays committed and complete), else the forced one, classified as
// the graceful path classifies the same stop, with exit code code. ok
// false: none within stacksWait (a commit or a classification holds the
// result, perhaps on a disk that does not return); late gets it if it
// comes after all.
func forcedSnapshot(f *forensicTarget, code int, why string, late func(snapshot)) (snapshot, bool) {
	if f.ctx == nil {
		return snapshot{}, false
	}
	return f.t.claim(func() outcome {
		o := classify(forcedErr(why), context.Cause(f.ctx), f.dir)
		o.ExitCode = code
		return o
	}, stacksWait, late)
}

// forcedAccount writes a forced exit's outcome, from its snapshot s, where
// the graceful path would have: DIR/error.json while the run owns the
// output, and the report (--report): the run's last accounted report once
// it was classified, else a minimal one without the run's statistics. The
// exit code is the snapshot's. Bounded by stacksWait: a disk that blocks
// the run may block these writes too.
func forcedAccount(f *forensicTarget, s snapshot, why string) {
	if f.ctx == nil {
		return
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		err := forcedErr(why)
		o := s.out
		if s.run && o.ExitCode != exitCompleted {
			o.Message = err.Error() + ": " + o.Message
		}
		r, ok := s.report()
		if !ok {
			r = listing.Unstarted(f.argv, f.dir, f.att, err)
		}
		// The escalation is part of the provenance: the stop that began
		// it, and the forced exit that ended it.
		if o.StopSource != "" {
			o.StopSource += "+forced_exit"
		} else {
			o.StopSource = "forced_exit"
		}
		o.Attempt = f.att
		account(&r.Outcome, o)
		if f.dir != "" {
			f.pr.Owned(func() { _ = writeOutcome(f.dir, outcomeOf(r)) })
		}
		_ = writeReport(f.report, r, nil)
	}()
	select {
	case <-done:
	case <-time.After(stacksWait):
		elog().warn("the forced exit's outcome could not be written in time")
	}
}

// progressName is the live progress file, inside the private workspace:
// rewritten (atomic replace) every progressEvery while a run goes.
var progressName = filepath.Join(dataset.Workspace, "progress.json")

const progressEvery = 2 * time.Second

// supervise runs list with the run's liveness owner: it rewrites the
// progress file, and cancels the run as stuck when its liveness
// (ProgressSnapshot.Liveness) has not risen for stall (half of it earns a
// warning first). Liveness is progress in key space: rows in any phase,
// parts, and the source's pages (Frontier: a page that held an entry or
// ended a chain), so an answered page keeps the clock alive although no
// row moved yet (queued work at a low limit). An empty continuing page
// counts only within the first keyrange.EmptyChainLive of its run: a
// service that answers forever with empty pages and fresh tokens stops as
// stuck (cause stuck_empty_chain) within that many pages plus stall,
// whatever --max-duration is. A failed or throttled request, or one that
// never returns, does not count, so a total outage or a hung source still
// stops as stuck. A source without Frontier is live by its answered
// requests (Answered). It is the only place that decides a run cannot go
// on; nothing in the pipeline gives up on a slow source.
func supervise(ctx context.Context, cancel context.CancelCauseFunc, pr *listing.Progress, dir string, stall time.Duration, warn func(string), d *display) (stop func()) {
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		// The file's cadence; the terminal display (Java's 1 s redraw) may
		// sample more often, from the same snapshot.
		every := min(progressEvery, max(stall/4, 10*time.Millisecond))
		tick := every
		if d != nil {
			tick = min(tick, d.interval)
		}
		t := time.NewTicker(tick)
		defer t.Stop()
		var written time.Time
		last, since, warned := int64(-1), time.Now(), false
		var at listing.ProgressSnapshot // the counts at the last progress
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case now := <-t.C:
				s := pr.Snapshot()
				d.offer(s)
				if dir != "" && now.Sub(written) >= every-tick/2 {
					// Only while the run owns the output: a refused run
					// never overwrites its owner's progress.
					written = now
					pr.Owned(func() { _ = dataset.ReplaceJSON(filepath.Join(dir, progressName), s, dataset.Atomic) })
				}
				if live := s.Liveness(); live != last {
					last, since, warned, at = live, now, false, s
					continue
				}
				if stall <= 0 {
					continue
				}
				idle := now.Sub(since)
				switch {
				case idle >= stall:
					cause := stuckCause(at, s)
					warn(fmt.Sprintf("no progress for %s (%s): stopping the run as stuck", idle.Round(time.Second), cause))
					// The goroutines as they hung, before the cancel moves them.
					dumpStacks(dir, pr, "stuck", cause, s)
					cancel(&stuckError{cause})
					return
				case idle >= stall/2 && !warned:
					warned = true
					warn(fmt.Sprintf("no progress for %s (the run is stopped as stuck at %s)", idle.Round(time.Second), stall))
				}
			}
		}
	}()
	return func() { close(done); <-finished }
}

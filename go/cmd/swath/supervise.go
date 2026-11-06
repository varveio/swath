// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/journal"
	"github.com/varveio/swath/go/internal/listing"
)

// Exit codes: the outcome a supervising process acts on without reading
// stderr (error.json carries the same, with the cause).
const (
	exitCompleted   = 0   // the dataset is committed (or a dry run finished)
	exitFatal       = 1   // a cause that a resume will not fix
	exitUsage       = 2   // a malformed command line
	exitStuck       = 75  // no progress for the stall timeout; resumable when the journal is intact
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

// signalError is a run stopped by a signal.
type signalError struct{ sig os.Signal }

func (e *signalError) Error() string { return "stopped by " + e.sig.String() }

// outcome is how a command ended: its class and exit code.
type outcome struct {
	Class     string `json:"class"` // completed, fatal, usage, stuck, deadline, interrupted, terminated
	ExitCode  int    `json:"exit_code"`
	Message   string `json:"message,omitempty"`
	Resumable bool   `json:"resumable"` // swath resume DIR can continue it
	// Cause is the provider's error class when one is known (S3: auth,
	// access_denied, throttle, ...).
	Cause string `json:"cause,omitempty"`
	// Attempt is the invocation this outcome belongs to; the report
	// (--report) carries the same one.
	Attempt *listing.Attempt `json:"attempt,omitempty"`
}

// classify maps a command's error (and the cause its context was
// cancelled with) to an outcome. dir is the output directory, if any.
func classify(err, cause error, dir string) outcome {
	if err == nil {
		return outcome{Class: "completed", ExitCode: exitCompleted}
	}
	resumable := dir != "" && !dataset.Committed(dir) && hasJournal(dir)
	o := outcome{Message: err.Error(), Resumable: resumable}
	var u usageError
	var se *signalError
	switch {
	case errors.As(err, &u), errors.Is(err, flag.ErrHelp):
		o.Class, o.ExitCode, o.Resumable = "usage", exitUsage, false
	case errors.Is(err, errIncomplete):
		o.Class, o.ExitCode, o.Resumable = "incomplete", exitFatal, false
	case errors.Is(cause, errStuck):
		o.Class, o.ExitCode = "stuck", exitStuck
	case errors.Is(cause, errDeadline):
		o.Class, o.ExitCode = "deadline", exitDeadline
	case errors.As(cause, &se) && se.sig == syscall.SIGTERM:
		o.Class, o.ExitCode = "terminated", exitTerminated
	case errors.As(cause, &se):
		o.Class, o.ExitCode = "interrupted", exitInterrupted
	case errors.Is(err, syscall.ENOSPC):
		// A full disk stops the run (a journal append or a part cannot be
		// written) but loses nothing durable: resumable once there is
		// space, when the journal is intact.
		o.Class, o.ExitCode, o.Cause = "fatal", exitFatal, "no_space"
	default:
		o.Class, o.ExitCode, o.Resumable = "fatal", exitFatal, false
		var ce interface{ Class() string }
		if errors.As(err, &ce) {
			o.Cause = ce.Class()
		}
	}
	return o
}

// classified is an error with the provider's class of its cause.
type classified struct {
	error
	class string
}

func (c *classified) Class() string { return c.class }
func (c *classified) Unwrap() error { return c.error }

// exitError carries the exit code main uses.
type exitError struct {
	error
	code int
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
// dataset included) in DIR/error.json (atomic replace), so a supervisor
// never parses stderr; a completed run removes a stale one. The caller
// owns the output (listing.Config.Settle, settleOutside).
func writeOutcome(dir string, o outcome) error {
	if dir == "" {
		return nil
	}
	path := filepath.Join(dir, errorName)
	if o.ExitCode == exitCompleted {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	return writeFileAtomic(path, o)
}

func writeFileAtomic(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o644); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// errOut is where the command reports an outcome its output does not hold.
var errOut io.Writer = os.Stderr

// settleOutside records o in dir's error.json for a command that stopped
// before any run owned the output (a resume refused on a damaged journal),
// under the output lock, when dir holds the unfinished run the command
// read: seen is its journal.Witness then, and must still match under the
// lock. A committed dataset, a directory without a journal (nothing is
// created there, not even the lock), an output another process owns and a
// run another process completed, replaced or continued since are left as
// they are: unrecorded says why, empty when there was nothing to replace.
func settleOutside(dir, seen string, o outcome) (recorded bool, unrecorded string) {
	if dir == "" || seen == "" || dataset.Committed(dir) {
		return false, ""
	}
	if err := faults.At("settle-lock"); err != nil {
		return false, err.Error()
	}
	lock, err := journal.TakeLock(dir)
	if err != nil {
		return false, err.Error()
	}
	defer lock.Release()
	if dataset.Committed(dir) || journal.Witness(dir) != seen {
		return false, "another process completed or replaced the run before this one held the lock"
	}
	if err := writeOutcome(dir, o); err != nil {
		return false, "write " + errorName + ": " + err.Error()
	}
	return true, ""
}

// reportUnrecorded puts on stderr, as one JSON line, an outcome that the
// output's error.json does not hold (another owner's outcome stands there,
// or the write failed), with the reason.
func reportUnrecorded(dir string, o outcome, why string) {
	if why != "" {
		fmt.Fprintf(errOut, "swath: the outcome is not recorded in %s: %s\n", filepath.Join(dir, errorName), why)
	}
	b, _ := json.Marshal(o)
	fmt.Fprintf(errOut, "swath: outcome: %s\n", b)
}

// progressName is the live progress file, inside the private workspace:
// rewritten (atomic replace) every progressEvery while a run goes.
var progressName = filepath.Join(dataset.Workspace, "progress.json")

const progressEvery = 2 * time.Second

// supervise runs list with the run's liveness owner: it rewrites the
// progress file, and cancels the run as stuck when neither Progress.Advance
// (rows in any phase, parts) nor Progress.Answered (requests the service
// answered usably) has risen for stall (half of it earns a warning first).
// An answered request keeps the clock alive although no row moved (queued
// work at a low limit, empty continuing pages); a failed or throttled one,
// or one that never returns, does not, so a total outage or a hung source
// still stops as stuck. It is the only place that decides a run cannot go
// on; nothing in the pipeline gives up on a slow source.
func supervise(ctx context.Context, cancel context.CancelCauseFunc, pr *listing.Progress, dir string, stall time.Duration, warn func(string)) (stop func()) {
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		t := time.NewTicker(min(progressEvery, max(stall/4, 10*time.Millisecond)))
		defer t.Stop()
		last, since, warned := int64(-1), time.Now(), false
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case now := <-t.C:
				s := pr.Snapshot()
				if dir != "" {
					// Only while the run owns the output: a refused run
					// never overwrites its owner's progress.
					pr.Owned(func() { _ = writeFileAtomic(filepath.Join(dir, progressName), s) })
				}
				if live := s.Advance + s.Answered; live != last {
					last, since, warned = live, now, false
					continue
				}
				if stall <= 0 {
					continue
				}
				idle := now.Sub(since)
				switch {
				case idle >= stall:
					warn(fmt.Sprintf("no progress for %s: stopping the run as stuck", idle.Round(time.Second)))
					cancel(errStuck)
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
